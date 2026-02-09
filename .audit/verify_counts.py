
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))
tables = [
    'tbl_dq_rules', 
    'tbl_maestra_variables', 
    'tbl_ref_estados_operativos', 
    'tbl_ref_paneles_bi', 
    'tbl_ref_unidades', 
    'tbl_reglas_consistencia',
    'tbl_limites_pozo',
    'tbl_var_scada_map'
]

print("--- VERIFICACIÓN FINAL DE CONTEOS ---")
with engine.connect() as conn:
    for t in tables:
        count = conn.execute(text(f"SELECT count(*) FROM referencial.{t}")).scalar()
        print(f"{t}: {count} filas")
        if t == 'tbl_dq_rules' and count != 35:
            print(f"  [WARN] Fallo en DQ rules. Se esperaban 35.")
        if t == 'tbl_reglas_consistencia' and count != 6:
            print(f"  [WARN] Fallo en RC rules. Se esperaban 6.")
