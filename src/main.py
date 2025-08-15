from typing import List, Literal, Dict, Any
import os, json, datetime as dt
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, validator
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
from src.avro_utils import backup_avro, restore_avro

# Carga de variables de entorno
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Diccionario de datos
SCHEMAS = {
    "departments": ["id", "name"],
    "jobs": ["id", "name"],
    "hired_employees": ["id", "name", "datetime", "department_id", "job_id"],
}

app = FastAPI(title="Jikkosoft Reto Técnico, Data API")

# Solicitud de ingestión
# Ingesta de datos en la base de datos
class IngestRequest(BaseModel):
    table: Literal["departments", "jobs", "hired_employees"]
    rows: List[Dict[str, Any]] = Field(..., min_items=1, max_items=1000)

    @validator("rows")
    def validate_rows(cls, v, values):
        table = values.get("table")
        required = SCHEMAS[table]
        for i, r in enumerate(v, start=1):
            miss = [c for c in required if c not in r or r[c] in (None, "", "NULL")]
            if miss:
                raise ValueError(f"Fila {i}: faltan campos {miss}")
        return v

# Normalización de fechas
def normalize_datetime(s: str) -> str:
    # acepta ISO con T/Z/microsegundos; devuelve 'YYYY-MM-DD HH:MM:SS'
    if not isinstance(s, str) or not s.strip():
        raise ValueError("datetime vacío")
    s = s.replace("T", " ").replace("Z", "").split(".")[0].strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return dt.datetime.strptime(s, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    # Intento final con pandas
    # Pandas es más flexible con formatos
    ts = pd.to_datetime(s, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"datetime inválido: {s}")
    return ts.strftime("%Y-%m-%d %H:%M:%S")

# Solicitud de ingestión
@app.post("/ingest")
def ingest(payload: IngestRequest):
    table = payload.table
    cols = SCHEMAS[table]

    # Validaciones de tipos y normalización de datos
    valid_rows, rejected = [], []
    for idx, row in enumerate(payload.rows, start=1):
        try:
            data = {}
            for c in cols:
                data[c] = row[c]
            if table in ("departments", "jobs"):
                data["id"] = int(data["id"])
                if not str(data["name"]).strip():
                    raise ValueError("name vacío")
            else:
                data["id"] = int(data["id"])
                data["department_id"] = int(data["department_id"])
                data["job_id"] = int(data["job_id"])
                data["name"] = str(data["name"]).strip()
                if not data["name"]:
                    raise ValueError("name vacío")
                data["datetime"] = normalize_datetime(data["datetime"])
            valid_rows.append(data)
        except Exception as e:
            rejected.append({"row": row, "error": str(e)})

    # Chequeo básico de FKs para hired_employees
    if table == "hired_employees" and valid_rows:
        dept_ids = {r["department_id"] for r in valid_rows}
        job_ids  = {r["job_id"] for r in valid_rows}
        with engine.begin() as conn:
            existing_depts = {r[0] for r in conn.execute(text("SELECT id FROM departments WHERE id = ANY(:ids)"), {"ids": list(dept_ids)})}
            existing_jobs  = {r[0] for r in conn.execute(text("SELECT id FROM jobs WHERE id = ANY(:ids)"), {"ids": list(job_ids)})}
        still_valid = []
        for r in valid_rows:
            if (r["department_id"] in existing_depts) and (r["job_id"] in existing_jobs):
                still_valid.append(r)
            else:
                rejected.append({"row": r, "error": "ID inexistente (department_id o job_id)"})
        valid_rows = still_valid

    # Insertar válidos (≤1000)
    if valid_rows:
        placeholders = ", ".join([f":{c}" for c in cols])
        collist = ", ".join(cols)
        sql = text(f"INSERT INTO {table} ({collist}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING")
        with engine.begin() as conn:
            conn.execute(sql, valid_rows)

    # Log de rechazados
    if rejected:
        os.makedirs("logs", exist_ok=True)
        with open("logs/rejected.log", "a", encoding="utf-8") as f:
            for r in rejected:
                f.write(f"{table} | {json.dumps(r, ensure_ascii=False)}\n")

    return {"Insertados": len(valid_rows), "Rechazados": len(rejected)}

# Backup y restauración
def df_from_table(table: str) -> pd.DataFrame:
    return pd.read_sql_query(f"SELECT * FROM {table}", engine)

@app.post("/backup/{table}")
def backup_table(table: Literal["departments","jobs","hired_employees"]):
    os.makedirs("backups", exist_ok=True)
    df = df_from_table(table)
    if df.empty:
        return {"ok": True, "path": None, "note": "tabla vacía"}
    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = f"backups/{table}_{ts}.parquet"
    df.to_parquet(path, index=False)
    return {"ok": True, "path": path}

# Restauración de backups
@app.post("/restore/{table}")
def restore_table(table: Literal["departments","jobs","hired_employees"], path: str):
    if not os.path.exists(path):
        raise HTTPException(status_code=400, detail="archivo no existe")
    df = pd.read_parquet(path)
    # Inserción simple con ON CONFLICT DO NOTHING
    cols = SCHEMAS[table]
    placeholders = ", ".join([f":{c}" for c in cols])
    collist = ", ".join(cols)
    sql = text(f"INSERT INTO {table} ({collist}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING")
    rows = df[cols].to_dict(orient="records")
    with engine.begin() as conn:
        # sublotes de 1000
        for i in range(0, len(rows), 1000):
            conn.execute(sql, rows[i:i+1000])
    return {"correcto": True, "restaurados": len(rows)}

# Endpoint

@app.post("/backup_avro/{table}")
def backup_table_avro(table: Literal["departments","jobs","hired_employees"]):
    path = backup_avro(table)
    return {"correcto": True, "path": path}

@app.post("/restore_avro/{table}")
def restore_table_avro(table: Literal["departments","jobs","hired_employees"], path: str):
    try:
        restored = restore_avro(table, path)
        return {"correcto": True, "restaurados": restored}
    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/metrics/hired_by_quarter")
def hired_by_quarter(year: int = Query(..., ge=1900, le=2100)):
    sql = """
    SELECT d.name AS department,
           j.name AS job,
           SUM(CASE WHEN EXTRACT(QUARTER FROM he.datetime)=1 THEN 1 ELSE 0 END) AS q1,
           SUM(CASE WHEN EXTRACT(QUARTER FROM he.datetime)=2 THEN 1 ELSE 0 END) AS q2,
           SUM(CASE WHEN EXTRACT(QUARTER FROM he.datetime)=3 THEN 1 ELSE 0 END) AS q3,
           SUM(CASE WHEN EXTRACT(QUARTER FROM he.datetime)=4 THEN 1 ELSE 0 END) AS q4
    FROM hired_employees he
    JOIN departments d ON d.id = he.department_id
    JOIN jobs j ON j.id = he.job_id
    WHERE EXTRACT(YEAR FROM he.datetime) = :year
    GROUP BY d.name, j.name
    ORDER BY d.name, j.name;
    """
    with engine.connect() as conn:
        res = conn.execute(text(sql), {"year": year})
        return [dict(r._mapping) for r in res]

@app.get("/metrics/top_departments")
def top_departments(year: int = Query(..., ge=1900, le=2100)):
    sql = """
    WITH counts AS (
      SELECT department_id, COUNT(*) AS hired
      FROM hired_employees
      WHERE EXTRACT(YEAR FROM datetime) = :year
      GROUP BY department_id
    ),
    avg_all AS (
      SELECT AVG(hired)::numeric AS avg_hired FROM counts
    )
    SELECT d.id, d.name AS department, c.hired
    FROM counts c
    JOIN departments d ON d.id = c.department_id, avg_all a
    WHERE c.hired > a.avg_hired
    ORDER BY c.hired DESC;
    """
    with engine.connect() as conn:
        res = conn.execute(text(sql), {"year": year})
        return [dict(r._mapping) for r in res]