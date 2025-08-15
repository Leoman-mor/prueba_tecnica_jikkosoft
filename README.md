# Prueba Técnica Jikkosoft – Data Engineer

Implementación de la prueba técnica usando **Python**, **FastAPI** y **PostgreSQL** (Neon en la nube).

## Estructura del proyecto

prueba_tecnica_jikkosoft/
├─ data/ # CSV originales
├─ src/ # Código fuente
├─ logs/ # Registro de filas rechazadas
├─ backups/ # Archivos Parquet/Avro de respaldo
├─ .env # Variables de entorno (no se suben a Git)
├─ requirements.txt # Dependencias
├─ schema.sql # Definición de tablas
└─ README.md

## Requisitos previos

- Python 3.10+  
- Cuenta gratuita en [Neon.tech](https://neon.tech) con base PostgreSQL creada  
- `psql` instalado para aplicar schema
- Git

## Instalación y configuración

1. Clonar repositorio:
    ```bash
    git clone <url-del-repo>
    cd prueba_tecnica_jikkosoft

2. Crear y activar entorno virtual:
    python3 -m venv .venv
    source .venv/bin/activate

3. Instalar dependencias:
    pip install -r requirements.txt

4. Configurar .env con la cadena de conexión de Neon:
    DATABASE_URL=postgresql://<user>:<pass>@<host>/<db>?sslmode=require&channel_binding=require

5. Crear tablas:
    python src/apply_schema.py


## Carga histórica de datos
Inserta datos en lotes de máximo 1000 filas (requisito de la prueba).
Valida tipos y campos obligatorios según el diccionario de datos:
    departments.csv y jobs.csv → sin encabezado (campos: id, name)
    hired_employees.csv → con encabezado (campos: id, name, datetime, department_id, job_id)
Normaliza fechas en formato ISO con microsegundos → YYYY-MM-DD HH:MM:SS.
Registra filas inválidas en logs/rejected.log con el motivo del rechazo.
No se insertan registros inválidos.

Ejecución:
    python src/load_historico.py

### Nota sobre la tasa de rechazo en hired_employees
Durante la carga histórica, se presenta una tasa de rechazo considerable en hired_employees.
Esto se debe al resultado de las validaciones estrictas aplicadas:
    * Formato de fecha/hora inválido o no conforme al patrón YYYY-MM-DD HH:MM:SS.
    * Campos numéricos (id, department_id, job_id) con valores no enteros o nulos.
    * Campo name vacío o con solo espacios.
Todas las filas rechazadas se registran en logs/rejected.log junto con el motivo específico, permitiendo su revisión y corrección antes de un reintento de carga.


## Uso de la API
* La API está construida con FastAPI y permite:
* Ingesta de datos en lotes de hasta 1000 registros.
* Respaldo (backup) y restauración de tablas en formato Parquet.
* Consultas analíticas de contrataciones.

1. Arrancar el servidor
    uvicorn src.main:app --reload
    Por defecto, está en http://127.0.0.1:8000.

    Documentación para interactuar:
        Swagger UI: http://127.0.0.1:8000/docs
        Redoc: http://127.0.0.1:8000/redoc

2. Endpoints principales
    POST /ingest
    
    Parámetros JSON:
        table: "departments" | "jobs" | "hired_employees"
        rows: Lista de registros (máximo 1000 por llamada)

### Backup
POST /backup/{table}
* Guarda la tabla como .parquet en la carpeta backups/.
    curl -X POST http://127.0.0.1:8000/backup/hired_employees
* Restore
    POST /restore/{table}?path=backups/archivo.parquet
* Restaura una tabla desde un archivo .parquet.
    curl -X POST "http://127.0.0.1:8000/restore/hired_employees?path=backups/hired_employees_20250101T120000Z.parquet"

### Métricas
* Contrataciones por trimestre
    GET /metrics/hired_by_quarter?year=2021
* Departamentos con contrataciones sobre el promedio
GET /metrics/top_departments?year=2021


## Backups AVRO: 
POST /backup_avro/{table} → genera backups/<table>_YYYYMMDDTHHMMSSZ.avro

## Restore AVRO: 
POST /restore_avro/{table}?path=backups/archivo.avro