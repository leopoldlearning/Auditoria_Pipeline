
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER', 'audit')
DB_PASS = os.getenv('DEV_DB_PASSWORD', 'audit')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5433')
DB_NAME = os.getenv('DB_NAME', 'etl_data')
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def verify_counts():
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        dq_count = conn.execute(text("SELECT COUNT(*) FROM referencial.tbl_dq_rules")).scalar()
        rc_count = conn.execute(text("SELECT COUNT(*) FROM referencial.tbl_reglas_consistencia")).scalar()
        vars_count = conn.execute(text("SELECT COUNT(*) FROM referencial.tbl_maestra_variables")).scalar()
        
        print("VERIFICACIÓN DE REGLAS REFERENCIAL")
        print("==================================")
        print(f"Total Variables en Maestra: {vars_count}")
        print(f"Total Reglas DQ: {dq_count} (Esperado: ~35)")
        print(f"Total Reglas Consistencia: {rc_count} (Esperado: 6)")
        
        if dq_count >= 30: # 35 specific rules + critical ones
            print("✅ DQ Rules count looks correct.")
        else:
            print("❌ DQ Rules count is lower than expected.")
            
        if rc_count == 6:
            print("✅ Consistency Rules count is correct.")
        else:
            print(f"❌ Consistency Rules count is {rc_count} (Expected 6).")

if __name__ == "__main__":
    verify_counts()
