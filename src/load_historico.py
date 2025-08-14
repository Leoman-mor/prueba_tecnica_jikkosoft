import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2.extras as _extras

# Carga de variables de entorno
load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

# Configuración de logs
os.makedirs("logs", exist_ok=True)
LOG_PATH = "logs/rejected.log"

SCHEMAS = {
    "departments": ["id", "name"],  # Documento sin encabezado
    "jobs": ["id", "name"],         # Documento sin encabezado
    "hired_employees": ["id", "name", "datetime", "department_id", "job_id"],  # Documento con encabezado
}

BATCH_SIZE = 1000 # Restricción de tamaño de lote según reto
READ_CHUNK = 100_000 # Filas a verificar por chunk de pandas, no restrictivo en el reto

# Inserción por lotes
def insert_batch(table: str, rows: list):
    if not rows:
        return
    cols = SCHEMAS[table]
    collist = ", ".join(cols)
    tpl = "(" + ",".join(["%s"] * len(cols)) + ")"
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        _extras.execute_values(
            cur,
            f"INSERT INTO {table} ({collist}) VALUES %s ON CONFLICT (id) DO NOTHING",
            [tuple(r[c] for c in cols) for r in rows],
            template=tpl,
            page_size=len(rows)
        )
        raw.commit()
    finally:
        raw.close()

# Inserción por lotes de 1000 filas
def insert_in_sublots(table: str, df: pd.DataFrame):
    if df.empty:
        return
    cols = SCHEMAS[table]
    n = len(df)
    for start in range(0, n, BATCH_SIZE):
        sub_df = df.iloc[start:start + BATCH_SIZE]
        rows = sub_df.to_dict(orient="records")
        insert_batch(table, rows)

# Registro de filas rechazadas
def log_rejected(table: str, rej_df: pd.DataFrame, reason: str):
    if rej_df.empty:
        return
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        for _, r in rej_df.iterrows():
            f.write(f"{table} | {reason} | {r.to_dict()}\n")

# Normalización de fechas
def normalize_datetime_series(series: pd.Series) -> pd.Series:
    """
    Normaliza cualquier datetime ISO:
    - acepta 'T', 'Z', micro/milisegundos
        pandas detecta T/Z/microsegundos; errors='coerce' -> NaT para inválidos
        utc=False para evitar tz-aware; si viene con tz, lo quita con tz_localize(None)
    - retorna 'YYYY-MM-DD HH:MM:SS'
    - valores inválidos -> NaT -> se loggean como rechazados
    """
    parsed = pd.to_datetime(series, errors="coerce", utc=False)
    # Si alguna quedó tz-aware (por 'Z' con heurística), remueve tz
    try:
        parsed = parsed.dt.tz_localize(None)
    except AttributeError:
        pass
    # Formato a guardar
    return parsed.dt.strftime("%Y-%m-%d %H:%M:%S")

# Carga CSV sin encabezado
def load_csv_no_header(table: str, path: str):
    total_correct = total_bad = total_seen = 0
    cols = SCHEMAS[table]

    for chunk in pd.read_csv(path, header=None, names=cols, dtype=str, chunksize=READ_CHUNK):
        total_seen += len(chunk)

        v_id = chunk["id"].str.match(r"^\d+$", na=False)
        v_name = chunk["name"].notna() & (chunk["name"].str.strip() != "")
        valid_mask = v_id & v_name

        rej = chunk.loc[~valid_mask].copy()
        log_rejected(table, rej, "id entero y name no vacío")
        total_bad += len(rej)

        val = chunk.loc[valid_mask].copy()
        if not val.empty:
            val["id"] = val["id"].astype(int)
            insert_in_sublots(table, val)
            total_correct += len(val)

        print(f"{table} | correcto={total_correct} | rechazados={total_bad} | procesadas={total_seen}")

    print(f"RESUMEN {table} | correcto={total_correct} | rechazados={total_bad} | leídas={total_seen}")

# Carga CSV con encabezado
def load_csv_with_header(table: str, path: str):
    total_correct = total_bad = total_seen = 0
    cols = SCHEMAS[table]

    for chunk in pd.read_csv(path, header=0, names=cols, dtype=str, chunksize=READ_CHUNK):
        # Si el header está como primera fila, eliminarlo
        if len(chunk) and chunk.iloc[0].equals(pd.Series(cols, index=cols)):
            chunk = chunk.iloc[1:].copy()

        total_seen += len(chunk)

        # Normalizar datetime de forma flexible (T/Z/microsegundos)
        chunk["datetime"] = normalize_datetime_series(chunk["datetime"])

        # Validaciones vectorizadas (tipos y vacíos mínimos)
        v_id   = chunk["id"].str.match(r"^\d+$", na=False)
        v_dep  = chunk["department_id"].str.match(r"^\d+$", na=False)
        v_job  = chunk["job_id"].str.match(r"^\d+$", na=False)
        v_name = chunk["name"].notna() & (chunk["name"].str.strip() != "")
        v_dt   = chunk["datetime"].notna() & (chunk["datetime"].str.len() > 0)

        type_mask = v_id & v_dep & v_job & v_name & v_dt

        rej_type = chunk.loc[~type_mask].copy()
        log_rejected(table, rej_type, "Campos vacíos o tipo inválido")
        total_bad += len(rej_type)

        val = chunk.loc[type_mask].copy()
        if not val.empty:
            # Conversión de tipos finales para insertar
            val["id"] = val["id"].astype(int)
            val["department_id"] = val["department_id"].astype(int)
            val["job_id"] = val["job_id"].astype(int)

            insert_in_sublots(table, val)
            total_correct += len(val)

        print(f"{table} | correcto={total_correct} | rechazados={total_bad} | procesadas={total_seen}")

    print(f"RESUMEN {table} | correcto={total_correct} | rechazados={total_bad} | leídas={total_seen}")

# Ejecución principal
if __name__ == "__main__":
    load_csv_no_header("departments", "data/departments.csv")
    load_csv_no_header("jobs", "data/jobs.csv")
    load_csv_with_header("hired_employees", "data/hired_employees.csv")
