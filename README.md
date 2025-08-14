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




