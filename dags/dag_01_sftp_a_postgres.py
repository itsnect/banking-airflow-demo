"""
dag_01_sftp_a_postgres.py

DAG 1: Lee el CSV de transacciones desde el SFTP
       y lo carga en PostgreSQL mapeando cada campo.

Frecuencia: manual (para la demo)
"""

from datetime import datetime, date
from decimal import Decimal

import csv
import io
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────

DAG_ID          = "dag_01_sftp_a_postgres"
SFTP_CONN_ID    = "sftp_default"
PG_CONN_ID      = "postgres_transacciones"
SFTP_PATH       = "/upload/transacciones.csv"

# ─── TAREAS ───────────────────────────────────────────────────────────────────

def extraer_csv_de_sftp(**context):
    """Lee el CSV desde el SFTP y lo guarda en XCom."""
    log.info("Conectando al SFTP...")
    hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)

    with hook.get_conn() as sftp:
        buf = io.BytesIO()
        sftp.getfo(SFTP_PATH, buf)
        buf.seek(0)
        contenido = buf.read().decode("utf-8")

    log.info("CSV leido correctamente desde %s", SFTP_PATH)
    context["ti"].xcom_push(key="csv_contenido", value=contenido)


def transformar_y_cargar_postgres(**context):
    """Parsea el CSV, mapea los campos y los inserta en PostgreSQL."""
    contenido = context["ti"].xcom_pull(key="csv_contenido", task_ids="extraer_csv_de_sftp")

    reader = csv.DictReader(io.StringIO(contenido))
    filas = list(reader)
    log.info("Registros encontrados en CSV: %d", len(filas))

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    insertados = 0
    omitidos   = 0

    for fila in filas:
        try:
            cursor.execute("""
                INSERT INTO transacciones
                    (transaccion_id, rut, nombre, tipo, monto, moneda,
                     comercio, region, estado, fecha)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaccion_id) DO NOTHING
            """, (
                fila["transaccion_id"].strip(),
                fila["rut"].strip(),
                fila["nombre"].strip(),
                fila["tipo"].strip(),
                Decimal(fila["monto"].strip()),
                fila["moneda"].strip(),
                fila["comercio"].strip() or None,
                fila["region"].strip(),
                fila["estado"].strip(),
                datetime.strptime(fila["fecha"].strip(), "%Y-%m-%d").date(),
            ))
            insertados += 1
        except Exception as e:
            log.warning("Error al insertar %s: %s", fila.get("transaccion_id"), e)
            omitidos += 1

    conn.commit()
    cursor.close()
    conn.close()

    log.info("Insertados: %d | Omitidos: %d", insertados, omitidos)
    context["ti"].xcom_push(key="insertados", value=insertados)


def verificar_carga(**context):
    """Verifica que los registros quedaron en PostgreSQL."""
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM transacciones")
    total = cursor.fetchone()[0]

    cursor.execute("""
        SELECT tipo, COUNT(*), SUM(monto)
        FROM transacciones
        GROUP BY tipo
        ORDER BY tipo
    """)
    resumen = cursor.fetchall()

    cursor.close()
    conn.close()

    log.info("Total registros en PostgreSQL: %d", total)
    for tipo, count, total_monto in resumen:
        log.info("  %s: %d transacciones | $%s CLP", tipo, count, f"{total_monto:,.0f}")


# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id=DAG_ID,
    description="Lee CSV desde SFTP y carga en PostgreSQL",
    start_date=datetime(2024, 11, 1),
    schedule=None,          # Manual para la demo
    catchup=False,
    tags=["nect", "etl", "sftp", "postgres"],
) as dag:

    t1 = PythonOperator(
        task_id="extraer_csv_de_sftp",
        python_callable=extraer_csv_de_sftp,
    )

    t2 = PythonOperator(
        task_id="transformar_y_cargar_postgres",
        python_callable=transformar_y_cargar_postgres,
    )

    t3 = PythonOperator(
        task_id="verificar_carga",
        python_callable=verificar_carga,
    )

    t1 >> t2 >> t3
