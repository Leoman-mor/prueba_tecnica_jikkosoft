import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

# Probar creación de dimensiones y hecho
with engine.connect() as conn:
    res = conn.execute(text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        ORDER BY table_name;
    """))
    print("Tablas en public:")
    for r in res:
        print("-", r[0])