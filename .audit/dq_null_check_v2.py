import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

def audit_dq_null_detailed():
    print("--- AUDITORÍA DETALLADA: stage.tbl_pozo_scada_dq ---")
    
    with engine.connect() as conn:
        df = pd.read_sql("SELECT dq_id, nombre_columna, valor_leido, valor_esperado_minimo, valor_esperado_maximo, resultado_dq FROM stage.tbl_pozo_scada_dq", conn)
        
        print("\n[Audit Result]")
        print(df.to_string(index=False))
        
        total = len(df)
        print(f"\nTotal registros: {total}")
        print(f"Nulos en valor_esperado_minimo: {df['valor_esperado_minimo'].isnull().sum()}")
        print(f"Nulos en valor_esperado_maximo: {df['valor_esperado_maximo'].isnull().sum()}")

if __name__ == "__main__":
    audit_dq_null_detailed()
