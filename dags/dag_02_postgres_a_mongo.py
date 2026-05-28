"""
dag_02_postgres_a_mongo.py

DAG 2: Lee las transacciones APROBADAS de PostgreSQL,
       las agrupa por cliente, y las guarda en MongoDB
       como un documento por cliente con transacciones embebidas.

Regla de negocio:
  - Solo transacciones con estado = 'aprobada'
  - Cada documento en Mongo representa un cliente con:
      * Sus datos personales
      * Array de transacciones
      * Total gastado
      * Total de operaciones
      * Ultima transaccion

Frecuencia: manual (para la demo)
"""

import logging
from datetime import datetime
from collections import defaultdict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook

log = logging.getLogger(__name__)

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────

DAG_ID       = "dag_02_postgres_a_mongo"
PG_CONN_ID   = "postgres_transacciones"
MONGO_CONN_ID = "mongo_transacciones"
MONGO_DB     = "transacciones_db"
MONGO_COLL   = "clientes"

# ─── TAREAS ───────────────────────────────────────────────────────────────────

def leer_transacciones_aprobadas(**context):
    """Lee solo las transacciones aprobadas de PostgreSQL."""
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT transaccion_id, rut, nombre, tipo, monto,
               moneda, comercio, region, estado, fecha
        FROM transacciones
        WHERE estado = 'aprobada'
        ORDER BY rut, fecha
    """)

    columnas = [desc[0] for desc in cursor.description]
    filas    = cursor.fetchall()

    cursor.close()
    conn.close()

    # Convertir a lista de dicts serializables
    registros = []
    for fila in filas:
        d = dict(zip(columnas, fila))
        d["monto"] = float(d["monto"])
        d["fecha"] = d["fecha"].isoformat()
        registros.append(d)

    log.info("Transacciones aprobadas leidas: %d", len(registros))
    context["ti"].xcom_push(key="transacciones", value=registros)


def agrupar_por_cliente(**context):
    """Agrupa las transacciones por RUT y arma un documento por cliente."""
    transacciones = context["ti"].xcom_pull(
        key="transacciones",
        task_ids="leer_transacciones_aprobadas"
    )

    grupos = defaultdict(list)
    for txn in transacciones:
        grupos[txn["rut"]].append(txn)

    documentos = []
    for rut, txns in grupos.items():
        txns_ordenadas = sorted(txns, key=lambda x: x["fecha"])

        total_monto = sum(t["monto"] for t in txns_ordenadas)
        ultima_txn  = txns_ordenadas[-1]

        # Resumen por tipo
        resumen_tipos = defaultdict(lambda: {"cantidad": 0, "monto_total": 0.0})
        for t in txns_ordenadas:
            resumen_tipos[t["tipo"]]["cantidad"] += 1
            resumen_tipos[t["tipo"]]["monto_total"] += t["monto"]

        doc = {
            "rut":               rut,
            "nombre":            txns_ordenadas[0]["nombre"],
            "region":            txns_ordenadas[0]["region"],
            "total_transacciones": len(txns_ordenadas),
            "monto_total_clp":   round(total_monto, 2),
            "ultima_transaccion": {
                "transaccion_id": ultima_txn["transaccion_id"],
                "tipo":           ultima_txn["tipo"],
                "monto":          ultima_txn["monto"],
                "fecha":          ultima_txn["fecha"],
            },
            "resumen_por_tipo":  dict(resumen_tipos),
            "transacciones":     txns_ordenadas,
            "actualizado_en":    datetime.utcnow().isoformat(),
        }
        documentos.append(doc)

    log.info("Documentos por cliente preparados: %d", len(documentos))
    context["ti"].xcom_push(key="documentos", value=documentos)


def cargar_en_mongo(**context):
    """Upsert de cada documento de cliente en MongoDB."""
    documentos = context["ti"].xcom_pull(
        key="documentos",
        task_ids="agrupar_por_cliente"
    )

    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db     = client[MONGO_DB]
    coll   = db[MONGO_COLL]

    insertados   = 0
    actualizados = 0

    for doc in documentos:
        resultado = coll.replace_one(
            {"rut": doc["rut"]},   # filtro
            doc,                    # documento completo
            upsert=True             # insertar si no existe
        )
        if resultado.upserted_id:
            insertados += 1
        else:
            actualizados += 1

    client.close()
    log.info("MongoDB -- Insertados: %d | Actualizados: %d", insertados, actualizados)


def verificar_mongo(**context):
    """Verifica y loguea los documentos cargados en MongoDB."""
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db   = client[MONGO_DB]
    coll = db[MONGO_COLL]

    total = coll.count_documents({})
    log.info("Total documentos en MongoDB coleccion '%s': %d", MONGO_COLL, total)

    for doc in coll.find({}, {"rut": 1, "nombre": 1, "total_transacciones": 1, "monto_total_clp": 1}):
        log.info(
            "  Cliente: %s (%s) | %d txns | $%s CLP",
            doc["nombre"],
            doc["rut"],
            doc["total_transacciones"],
            f"{doc['monto_total_clp']:,.0f}",
        )

    client.close()


# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id=DAG_ID,
    description="Agrupa transacciones aprobadas por cliente y carga en MongoDB",
    start_date=datetime(2024, 11, 1),
    schedule=None,
    catchup=False,
    tags=["nect", "etl", "postgres", "mongo"],
) as dag:

    t1 = PythonOperator(
        task_id="leer_transacciones_aprobadas",
        python_callable=leer_transacciones_aprobadas,
    )

    t2 = PythonOperator(
        task_id="agrupar_por_cliente",
        python_callable=agrupar_por_cliente,
    )

    t3 = PythonOperator(
        task_id="cargar_en_mongo",
        python_callable=cargar_en_mongo,
    )

    t4 = PythonOperator(
        task_id="verificar_mongo",
        python_callable=verificar_mongo,
    )

    t1 >> t2 >> t3 >> t4
