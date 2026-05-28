# NECT — Airflow Demo: ETL Transacciones Bancarias

Demo para el video de YouTube **"Airflow desde cero"**.

## Stack

| Servicio | Puerto | Descripción |
|---|---|---|
| Airflow Webserver | 8080 | UI de Airflow (admin / nect123) |
| PostgreSQL (datos) | 5432 | Tabla de transacciones procesadas |
| MongoDB | 27017 | Colección de clientes agrupados |
| SFTP | 2222 | Archivo CSV fuente |
| Grafana | 3000 | Dashboard de transacciones |

## Pipeline ETL

```
SFTP (transacciones.csv)
        ↓ DAG 1
PostgreSQL — tabla transacciones (15 filas, una por transacción)
        ↓ DAG 2 (solo aprobadas, agrupadas por cliente)
MongoDB — colección clientes (5 documentos, uno por cliente)
        ↓
Grafana — dashboard en tiempo real sobre PostgreSQL
```

---

## Setup

### 1. Variables de entorno necesarias

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### 2. Instalar providers de Airflow

```bash
docker compose build
```

> O agregar al Dockerfile si usas imagen custom.

### 3. Levantar el stack

```bash
docker compose up -d
```

Esperar ~60 segundos. Verificar con:

```bash
docker compose ps
```

Deben aparecer 7 servicios en `Up`.

### 4. Acceder a Airflow

- URL: http://localhost:8080
- Usuario: `admin`
- Contraseña: `nect123`

### 5. Configurar conexiones en Airflow

En la UI: Admin → Connections → agregar:

**SFTP:**
- Conn ID: `sftp_default`
- Conn Type: SFTP
- Host: `sftp`
- Port: `22`
- Login: `nect`
- Password: `nect123`

**PostgreSQL:**
- Conn ID: `postgres_transacciones`
- Conn Type: Postgres
- Host: `postgres-data`
- Port: `5432`
- Schema: `transacciones_db`
- Login: `nect`
- Password: `nect123`

**MongoDB:**
- Conn ID: `mongo_transacciones`
- Conn Type: MongoDB
- Host: `mongodb`
- Port: `27017`
- Login: `nect`
- Password: `nect123`

### 6. Ejecutar los DAGs

En la UI de Airflow:
1. Activar `dag_01_sftp_a_postgres` → trigger manual → esperar que termine
2. Activar `dag_02_postgres_a_mongo` → trigger manual → esperar que termine

---

## Consultar los datos

### SFTP — ver el CSV fuente

```bash
sftp -P 2222 nect@localhost
# password: nect123
ls upload/
get upload/transacciones.csv /tmp/
exit
```

O directamente:

```bash
docker exec -it sftp ls /home/nect/upload/
```

### PostgreSQL — ver las transacciones

```bash
docker exec -it postgres-data psql -U nect -d transacciones_db
```

Queries útiles:

```sql
-- Todas las transacciones
SELECT * FROM transacciones ORDER BY fecha;

-- Resumen por tipo
SELECT tipo, COUNT(*), SUM(monto) FROM transacciones GROUP BY tipo;

-- Solo aprobadas
SELECT * FROM transacciones WHERE estado = 'aprobada';

-- Por cliente
SELECT rut, nombre, COUNT(*), SUM(monto)
FROM transacciones
GROUP BY rut, nombre
ORDER BY SUM(monto) DESC;
```

### MongoDB — ver los documentos de clientes

```bash
docker exec -it mongodb mongosh \
  --username nect \
  --password nect123 \
  --authenticationDatabase admin \
  transacciones_db
```

Queries útiles:

```javascript
// Ver todos los clientes
db.clientes.find({}, { nombre: 1, rut: 1, total_transacciones: 1, monto_total_clp: 1 })

// Ver un cliente completo
db.clientes.findOne({ rut: "12.345.678-9" })

// Clientes ordenados por monto total
db.clientes.find().sort({ monto_total_clp: -1 })

// Cuántos documentos hay
db.clientes.countDocuments()
```

### Grafana — dashboard

- URL: http://localhost:3000
- Usuario: `admin`
- Contraseña: `nect123`
- Dashboard: **NECT Airflow Demo → Transacciones ETL**

Ve a Connections → Data sources en Grafana
Haz clic en el datasource PostgreSQL
Completa estos campos:

Host: postgres-data:5432
Database: transacciones_db
User: nect
Password: nect123
TLS/SSL Mode: disable


Clic en Save & test


---

## Estructura del proyecto

```
airflow-demo/
├── docker-compose.yml
├── requirements.txt
├── .env                          # AIRFLOW_UID (crear con el comando de setup)
├── init-scripts/
│   └── init.sql                  # Schema PostgreSQL
├── sample-data/
│   └── transacciones.csv         # 15 transacciones bancarias chilenas
├── dags/
│   ├── dag_01_sftp_a_postgres.py # SFTP → PostgreSQL
│   └── dag_02_postgres_a_mongo.py # PostgreSQL → MongoDB (agrupado por cliente)
├── logs/                         # Logs de Airflow (autogenerado)
├── plugins/                      # Plugins custom (vacío)
├── config/                       # Config extra (vacío)
└── grafana/
    ├── datasources/postgres.yml
    └── dashboards/
        ├── dashboard.yml
        └── nect-airflow.json
```
