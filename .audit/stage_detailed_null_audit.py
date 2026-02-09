import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

def audit_stage_nulls():
    print("--- AUDITORÍA DETALLADA DE NULOS: ESQUEMA STAGE ---")
    tables = ['tbl_pozo_maestra', 'tbl_pozo_produccion', 'tbl_pozo_scada_dq']
    
    with engine.connect() as conn:
        for table in tables:
            print(f"\n>> ANALIZANDO: stage.{table}")
            try:
                df = pd.read_sql(f"SELECT * FROM stage.{table}", conn)
                
                if df.empty:
                    print("   [!] Tabla sin datos.")
                    continue
                
                total_rows = len(df)
                null_report = []
                
                for col in df.columns:
                    null_count = df[col].isnull().sum()
                    if null_count > 0:
                        pct = (null_count / total_rows) * 100
                        null_report.append({
                            'Columna': col,
                            'Nulos': null_count,
                            'Porcentaje': f"{pct:.2f}%"
                        })
                
                if not null_report:
                    print(f"   [OK] 0 nulos detectados en {total_rows} registros.")
                else:
                    print(pd.DataFrame(null_report).to_string(index=False))
                    
            except Exception as e:
                print(f"   [ERROR] No se pudo leer la tabla: {e}")

if __name__ == "__main__":
    audit_stage_nulls()
