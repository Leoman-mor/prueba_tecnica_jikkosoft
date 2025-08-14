import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

tables = ["hired_employees", "departments", "jobs"]

with engine.begin() as conn:
    for t in tables:
        conn.execute(text(f"TRUNCATE TABLE {t} RESTART IDENTITY CASCADE"))
        print(f"Tabla {t} vaciada.")
