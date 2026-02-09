import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv('DATABASE_URL')
engine = create_engine(DB_URL)

CSV_UNIDADES = "inputs_referencial/05_unidades.csv"
CSV_ID_TRUTH = "data/Variables_ID_stage.csv"

print("--- DEBUGGING NumericValueOutOfRange ---")

try:
    df_units = pd.read_csv(CSV_UNIDADES, sep=';', encoding='latin-1')
    id_col = next((c for c in df_units.columns if 'ID_formato1vfinal' in c), None)
    if id_col:
        print(f"Max ID in Units CSV: {df_units[id_col].dropna().astype(str).str.split('.').str[0].str.extract('(\d+)')[0].astype(float).max()}")
except Exception as e:
    print(f"Unit Check Error: {e}")

try:
    df_truth = pd.read_csv(CSV_ID_TRUTH, sep=';', encoding='utf-8', on_bad_lines='skip')
    df_truth['ID_raw'] = df_truth['ID'].astype(str).str.strip()
    df_numeric = df_truth[df_truth['ID_raw'].str.match(r'^\d+$')].copy()
    print(f"Max ID in Truth CSV: {df_numeric['ID_raw'].astype(int).max()}")
except Exception as e:
    print(f"Truth Check Error: {e}")

print("\nAttempting Maestra load with try/except per row...")
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE referencial.tbl_maestra_variables RESTART IDENTITY CASCADE"))
    for _, row in df_numeric.iterrows():
        try:
            id_val = int(row['ID_raw'])
            tech_name = str(row['Nombre_Variable']).strip()
            conn.execute(text("INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico) VALUES (:id, :name)"), {"id": id_val, "name": tech_name})
        except Exception as e:
            print(f"FAILED ROW ID {row['ID_raw']}: {e}")
            break
