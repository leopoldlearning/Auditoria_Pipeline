import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

tables = [
    'stage.tbl_pozo_produccion', 
    'stage.tbl_pozo_scada_dq', 
    'reporting.dataset_current_values', 
    'reporting.fact_operaciones_horarias', 
    'reporting.fact_operaciones_diarias'
]

print("--- FINAL DATA VERIFICATION ---")
with engine.connect() as conn:
    for t in tables:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
        print(f"{t}: {count} rows")
