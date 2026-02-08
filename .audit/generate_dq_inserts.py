
import pandas as pd
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

csv_path = r"D:\ITMeet\Operaciones\BP010-data-pipelines-auditoria\inputs_referencial\02_reglas_calidad.csv"

def generate_inserts():
    try:
        # 1. Read CSV
        # Separator seems to be semicolon based on view_file output
        df = pd.read_csv(csv_path, sep=';')
        
        # 2. Get Variables from DB
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            vars_df = pd.read_sql("SELECT variable_id, nombre_tecnico, id_formato1 FROM referencial.tbl_maestra_variables", conn)
        
        print("-- GENERATED DQ RULES FROM CSV")
        print("TRUNCATE TABLE referencial.tbl_dq_rules RESTART IDENTITY CASCADE;")
        
        matched_count = 0
        missing_vars = []

        for index, row in df.iterrows():
            # Parse ID (id_formato1)
            raw_id = str(row.iloc[0]).strip() # First column is ID (Format 1)
            
            # Handle multiple IDs like "24, 41" or empty
            ids = []
            if raw_id and raw_id.lower() != 'nan':
                parts = raw_id.split(',')
                for p in parts:
                    try:
                        ids.append(int(float(p.strip())))
                    except:
                        pass
            
            # Get Rule details
            name = row['Nombre columna']
            rule_rep = str(row['Reglas de calidad-Representatividad']).strip()
            rule_lat = str(row['Reglas de calidad-Latencia']).strip()
            
            # 1. LATENCY (Default 2s if specified, else keep default)
            lat_seconds = 'NULL'
            if '< 2 s' in rule_lat:
                lat_seconds = 2
            
            # 2. RANGES (Representativity)
            min_val = 'NULL'
            max_val = 'NULL'
            
            if '>0' in rule_rep:
                min_val = 0.0001
            elif '0-100%' in rule_rep:
                min_val = 0
                max_val = 100
                
            severity = 'WARNING' # Default per user request/standard context? User said >0 represents basic validity.
            
            # Manual Mapping for known discrepancies
            manual_map = {
                '7': [108], # Production BOPD -> prod_petroleo_diaria_bpd
                'nan': []
            }
            
            if raw_id in manual_map:
                ids = manual_map[raw_id]
            
            # Name based mapping for missing IDs
            name_map = {
                'Flowing Bottom Hole Pressure (FBHP)': 'pwf_psi_act',
                'Surface RodPosition': 'current_stroke_length_act_in',
                'Damage Factor': 'damage_factor' # Placeholder if we add it
            }
            
            target_tech_name = name_map.get(name)

            # Find Variable ID
            # Try by IDs first
            found_vars = pd.DataFrame()
            if ids:
                 # Ensure ids are numeric
                 clean_ids = []
                 for i in ids:
                     try:
                         clean_ids.append(int(i))
                     except:
                         pass
                 if clean_ids:
                    found_vars = vars_df[vars_df['id_formato1'].isin(clean_ids)]
            
            # Try by Technical Name if ID failed
            if found_vars.empty and target_tech_name:
                 found_vars = vars_df[vars_df['nombre_tecnico'] == target_tech_name]

            
            # If no ID matched or no ID provided, try fuzzy name match or just skip (user said "rules 35", CSV has 37 rows)
            # Some rows might not be in our scope yet (Design parameters vs Operational)
            
            if not found_vars.empty:
                for _, v_row in found_vars.iterrows():
                    var_id = v_row['variable_id']
                    
                    print(f"INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES")
                    print(f"({var_id}, {min_val}, {max_val}, {lat_seconds}, '{severity}', 'CSV Reglas Calidad'); -- {name} ({v_row['nombre_tecnico']})")
                    matched_count += 1
            else:
                missing_vars.append(f"{name} (ID: {raw_id})")

        print(f"\n-- Matched Rules: {matched_count}")
        print(f"-- Missing Matches: {len(missing_vars)}")
        for m in missing_vars:
            print(f"-- MISSING: {m}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    generate_inserts()
