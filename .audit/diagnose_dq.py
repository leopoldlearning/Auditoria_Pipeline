
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

CSV_REGLAS = "inputs_referencial/02_reglas_calidad.csv"
df_reglas = pd.read_csv(CSV_REGLAS, sep=';', encoding='utf-8').iloc[:35]

with engine.connect() as conn:
    print("--- DIAGNÓSTICO VARIABLES DQ FALTANTES ---")
    for _, row in df_reglas.iterrows():
        id_f1 = str(row['ID_FORMATO_1']).strip().replace('*', '')
        original_name = str(row['Nombre columna  de variable original']).strip()
        
        v_id = None
        if id_f1 == 'S/I':
            v_id = conn.execute(text("SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico ILIKE :n"), {"n": f"%{original_name}%"}).scalar()
        elif id_f1 != 'nan' and id_f1.isdigit():
            v_id = conn.execute(text("SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = :id"), {"id": int(id_f1)}).scalar()
        
        if not v_id:
            print(f"MISSING: ID={id_f1} | Name='{original_name}'")
        else:
            # Check if rule exists
            rule_exists = conn.execute(text("SELECT count(*) FROM referencial.tbl_dq_rules WHERE variable_id = :v"), {"v": v_id}).scalar()
            if not rule_exists:
                print(f"RULE NOT LOADED: ID={id_f1} | Name='{original_name}' (Variable found but no rule)")
