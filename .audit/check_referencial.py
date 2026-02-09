
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

tables = [
    'tbl_dq_rules', 
    'tbl_limites_pozo', 
    'tbl_maestra_variables', 
    'tbl_ref_estados_operativos', 
    'tbl_ref_paneles_bi', 
    'tbl_ref_unidades', 
    'tbl_reglas_consistencia', 
    'tbl_var_scada_map'
]

print("--- REFERENTIAL SCHEMA AUDIT ---")
with engine.connect() as conn:
    for table in tables:
        try:
            count = conn.execute(text(f"SELECT count(*) FROM referencial.{table}")).scalar()
            print(f"Table referencial.{table}: {count} rows")
            if count > 0:
                # Sample
                sample = conn.execute(text(f"SELECT * FROM referencial.{table} LIMIT 2")).fetchall()
                print(f"  Sample: {sample}")
        except Exception as e:
            print(f"Table referencial.{table}: ERROR -> {str(e)}")

print("\n--- VIEW CHECK ---")
views = ['vw_limites_pozo_pivot_v4', 'vw_variables_scada_stage']
for view in views:
    try:
        count = conn.execute(text(f"SELECT count(*) FROM referencial.{view}")).scalar()
        print(f"View referencial.{view}: {count} rows")
    except Exception as e:
        print(f"View referencial.{view}: ERROR -> {str(e)}")
