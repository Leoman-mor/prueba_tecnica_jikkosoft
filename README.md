# Prueba Técnica – Jikkosoft (Data Engineer)
# Fase 1 - Migración masiva

Implementación de la prueba técnica usando **Python**, **FastAPI** y **PostgreSQL** (Neon en la nube).

---

## Estructura del Proyecto

```bash
prueba_tecnica_jikkosoft/
├─ data/            # CSV originales
├─ src/             # Código fuente
├─ logs/            # Registro de filas rechazadas
├─ backups/         # Archivos Parquet/Avro de respaldo
├─ .env             # Variables de entorno (no se suben a Git)
├─ requirements.txt # Dependencias
├─ schema.sql       # Definición de tablas
└─ README.md
```

---

## Requisitos Previos

- Python 3.10+
- Cuenta gratuita en [Neon.tech](https://neon.tech) con base PostgreSQL creada
- `psql` instalado para aplicar el esquema
- Git
- Docker (opcional, para despliegue con contenedores)

---

## Instalación y Configuración

1. Clonar repositorio
   ```bash
   git clone <url-del-repo>
   cd prueba_tecnica_jikkosoft
   ```

2. Crear y activar entorno virtual
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Instalar dependencias
   ```bash
   pip install -r requirements.txt
   ```

4. Configurar `.env`
   ```env
   DATABASE_URL=postgresql://<user>:<pass>@<host>/<db>?sslmode=require&channel_binding=require
   API_KEY="Jikkosoft_#2025*"
   ```

5. Crear tablas
   ```bash
   python src/apply_schema.py
   ```

---

## Carga Histórica de Datos

- Límite: 1000 filas por lote.
- Validaciones:
  - `departments.csv` y `jobs.csv`: sin encabezado (`id`, `name`)
  - `hired_employees.csv`: con encabezado (`id`, `name`, `datetime`, `department_id`, `job_id`)
  - Fechas en formato ISO `YYYY-MM-DD HH:MM:SS`
  - Campos obligatorios completos y tipos correctos
- Rechazo de filas inválidas: se registran en `logs/rejected.log`

Ejecución:
```bash
python src/load_historico.py
```

Notas sobre rechazos:
- Formato de fecha inválido
- Campos numéricos con valores no enteros o nulos
- Campo `name` vacío

---

## Uso de la API

La API está construida con FastAPI y permite:

- Ingesta en lotes (máx. 1000 registros)
- Backup y restauración en formato Parquet o Avro
- Consultas analíticas

### 1. Arrancar servidor
```bash
uvicorn src.main:app --reload
```
Por defecto: http://127.0.0.1:8000

Documentación:
- Swagger UI: http://127.0.0.1:8000/docs
- Redoc: http://127.0.0.1:8000/redoc

---

### 2. Endpoints Principales

#### Ingesta de datos
```bash
POST /ingest
```
Body JSON:
```json
{
  "table": "hired_employees",
  "rows": [
    {"id": 999999, "name": "Alice", "datetime": "2021-04-12T03:44:47.673375", "department_id": 1, "job_id": 1}
  ]
}
```

#### Backups
Parquet:
```bash
POST /backup/{table}
```
Avro:
```bash
POST /backup_avro/{table}
```

#### Restauración
Parquet:
```bash
POST /restore/{table}?path=backups/archivo.parquet
```
Avro:
```bash
POST /restore_avro/{table}?path=backups/archivo.avro
```

#### Métricas
- Contrataciones por trimestre:
  ```
  GET /metrics/hired_by_quarter?year=2021
  ```
- Departamentos sobre promedio:
  ```
  GET /metrics/top_departments?year=2021
  ```

---

## Ejecución con Docker

1. Requisitos
   - Docker Desktop en ejecución
   - Archivo `.env` configurado

2. Construir y levantar contenedor
   ```bash
   docker compose build
   docker compose up -d
   ```

3. Ver logs / estado
   ```bash
   docker compose logs -f api
   docker ps
   ```

4. Detener y limpiar
   ```bash
   docker compose down
   docker compose down -v  # Elimina volúmenes
   ```

---

## Seguridad

Todos los endpoints están protegidos con API Key vía header:
```bash
x-api-key: Jikkosoft_#2025*
```
En Swagger UI, usar el botón Authorize para agregar la API Key.

---

## Resumen de Comandos Clave

| Acción                | Comando |
|-----------------------|---------|
| Crear tablas          | `python src/apply_schema.py` |
| Cargar histórico      | `python src/load_historico.py` |
| Arrancar API          | `uvicorn src.main:app --reload` |
| Backup Parquet        | `POST /backup/{table}` |
| Backup Avro           | `POST /backup_avro/{table}` |
| Restore Parquet       | `POST /restore/{table}?path=...` |
| Restore Avro          | `POST /restore_avro/{table}?path=...` |


# Fase 2 – Visualización en Looker Studio

En esta fase se conecta la base de datos de PostgreSQL (Neon) a Looker Studio para crear un dashboard interactivo con los datos cargados.

## Conexión a PostgreSQL desde Looker Studio

En Looker Studio, con las credenciales de acceso a la base de datos, se llamaron los datos desde un conector PostgreSQL. 
Se usó una la certificación del servidor 'isrgrootx1.pem' como SSL 
Se usó la visual que está en script_view_looker.txt

## Construcción del dashboard

Se usaron las indicaciones que los stakeholders requieren para construir el tablero. 