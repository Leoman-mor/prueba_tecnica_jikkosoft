import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv() # Cargar variables .env
DATABASE_URL = os.getenv("DATABASE_URL") # Database
engine = create_engine(DATABASE_URL, pool_pre_ping=True) # Motor de conexión

# Función para probar la conexión
def test_connection():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("Conexión exitosa:", list(result))
    except Exception as e:
        print("Error al conectar:", e)

if __name__ == "__main__":
    test_connection()
