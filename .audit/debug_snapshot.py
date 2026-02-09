import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import traceback

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

print("--- DEBUGGING SNAPSHOT PROC ---")
try:
    with engine.begin() as conn:
        conn.execute(text("CALL reporting.actualizar_current_values_v4();"))
    print("SUCCESS!")
except Exception as e:
    print(f"FAILED: {e}")
    traceback.print_exc()
