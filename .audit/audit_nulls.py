import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

def audit_schema_nulls(schema_name):
    print(f"\n--- AUDITANDO ESQUEMA: {schema_name} ---")
    query_tables = text(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_type = 'BASE TABLE'")
    
    with engine.connect() as conn:
        tables = conn.execute(query_tables).fetchall()
        
        for (table_name,) in tables:
            print(f"\n> Analizando tabla: {schema_name}.{table_name}")
            df = pd.read_sql(f"SELECT * FROM {schema_name}.{table_name}", conn)
            
            if df.empty:
                print("  [!] Tabla vacía.")
                continue
                
            total_rows = len(df)
            null_counts = df.isnull().sum()
            null_pct = (null_counts / total_rows) * 100
            
            report = pd.DataFrame({
                'Columna': null_counts.index,
                'Nulos': null_counts.values,
                'Porcentaje': null_pct.values
            })
            
            # Filtrar solo las que tienen algún nulo para ser concisos
            report = report[report['Nulos'] > 0].sort_values(by='Porcentaje', ascending=False)
            
            if report.empty:
                print("  [OK] No se detectaron nulos.")
            else:
                print(report.to_string(index=False))

if __name__ == "__main__":
    audit_schema_nulls('reporting')
