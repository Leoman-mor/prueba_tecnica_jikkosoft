import os
from typing import List, Dict
from datetime import datetime
from fastavro import writer, reader, parse_schema
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy import text as sqltext

# Carga de variables de entorno
load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

# Diccionario de datos
SCHEMAS = {
    "departments": ["id", "name"],
    "jobs": ["id", "name"],
    "hired_employees": ["id", "name", "datetime", "department_id", "job_id"],
}

# Función para obtener el schema AVRO
def _avro_schema_for(table: str) -> Dict:
    if table == "departments":
        fields = [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
        ]
    elif table == "jobs":
        fields = [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
        ]
    elif table == "hired_employees":
        # datetime como string normalizado (YYYY-MM-DD HH:MM:SS) para simplicidad
        fields = [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "datetime", "type": "string"},
            {"name": "department_id", "type": "int"},
            {"name": "job_id", "type": "int"},
        ]
    else:
        raise ValueError("Tabla no soportada")

    return {
        "name": f"{table}",
        "type": "record",
        "fields": fields
    }

# Función para exportar a AVRO
def backup_avro(table: str, out_dir="backups") -> str:
    cols = SCHEMAS[table]
    os.makedirs(out_dir, exist_ok=True)

    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT {', '.join(cols)} FROM {table}"))
        rows = [dict(r._mapping) for r in res]

    # Normalizar datetime de hired_employees a string YYYY-MM-DD HH:MM:SS
    if table == "hired_employees":
        for r in rows:
            v = r.get("datetime")
            if v is None:
                r["datetime"] = ""
            elif isinstance(v, str):
                pass
            else:
                r["datetime"] = v.strftime("%Y-%m-%d %H:%M:%S")

    schema = parse_schema(_avro_schema_for(table))
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(out_dir, f"{table}_{ts}.avro")

    with open(path, "wb") as out:
        writer(out, schema, rows)

    return path

# Función para restaurar desde un AVRO
def restore_avro(table: str, path: str, batch_size: int = 1000) -> int:
    if not os.path.exists(path):
        raise FileNotFoundError(f"No existe: {path}")

    cols = SCHEMAS[table]
    inserted = 0

    placeholders = ", ".join([f":{c}" for c in cols])
    collist = ", ".join(cols)
    insert_sql = sqltext(f"INSERT INTO {table} ({collist}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING")

    buffer: List[Dict] = []
    with open(path, "rb") as f, engine.begin() as conn:
        for rec in reader(f):
            if table in ("departments", "jobs"):
                rec["id"] = int(rec["id"])
                rec["name"] = str(rec["name"])
            else:
                rec["id"] = int(rec["id"])
                rec["name"] = str(rec["name"])
                rec["datetime"] = str(rec["datetime"])
                rec["department_id"] = int(rec["department_id"])
                rec["job_id"] = int(rec["job_id"])

            buffer.append(rec)
            if len(buffer) >= batch_size:
                conn.execute(insert_sql, buffer)
                inserted += len(buffer)
                buffer.clear()

        if buffer:
            conn.execute(insert_sql, buffer)
            inserted += len(buffer)

    return inserted
