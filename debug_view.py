
import os
import sys
import sqlalchemy
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load params
load_dotenv()
DB_USER = "audit"
DB_PASS = "audit" 
DB_HOST = "localhost"
DB_NAME = "etl_data"
DB_PORT = "5433"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def check_referencial():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            print(">>> Checking Schema 'referencial'...")
            
            # Check Tables
            result_tables = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'referencial'")).fetchall()
            print("Tables in referencial:")
            for r in result_tables:
                print(f" - {r[0]}")
                
            # Check Views
            result_views = conn.execute(text("SELECT table_name FROM information_schema.views WHERE table_schema = 'referencial'")).fetchall()
            print("\nViews in referencial:")
            for r in result_views:
                print(f" - {r[0]}")

            # Check if our view exists
            view_name = 'vw_limites_pozo_pivot_v4'
            exists = any(r[0] == view_name for r in result_views)
            if exists:
                print(f"\n[OK] View {view_name} EXISTS.")
                # Try simple select
                try:
                    conn.execute(text(f"SELECT * FROM referencial.{view_name} LIMIT 1"))
                    print(f"[OK] Select from {view_name} successful.")
                except Exception as e:
                    print(f"[ERROR] Select from {view_name} failed: {e}")
            else:
                print(f"\n[ERROR] View {view_name} DOES NOT EXIST.")

    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    check_referencial()
