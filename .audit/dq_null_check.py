import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

def audit_dq_nulls():
    print("--- AUDITORÍA ESPECÍFICA: stage.tbl_pozo_scada_dq ---")
    
    with engine.connect() as conn:
        try:
            df = pd.read_sql("SELECT * FROM stage.tbl_pozo_scada_dq", conn)
            
            if df.empty:
                print("   [!] Tabla sin datos.")
                return
            
            total_rows = len(df)
            null_report = []
            
            for col in df.columns:
                null_count = df[col].isnull().sum()
                pct = (null_count / total_rows) * 100
                null_report.append({
                    'Columna': col,
                    'Nulos': null_count,
                    'Porcentaje': f"{pct:.2f}%"
                })
            
            print(pd.DataFrame(null_report).to_string(index=False))
            
            print("\n--- MUESTRA DE DATOS (Primeros 3 registros) ---")
            print(df.head(3).to_string(index=False))
                    
        except Exception as e:
            print(f"   [ERROR] No se pudo leer la tabla: {e}")

if __name__ == "__main__":
    audit_dq_nulls()
