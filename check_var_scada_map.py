#!/usr/bin/env python3
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = f"postgresql://{os.getenv('DB_USER', 'audit')}:{os.getenv('DEV_DB_PASSWORD', 'audit')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5433')}/{os.getenv('DB_NAME', 'etl_data')}"
engine = create_engine(DB_URL)

with engine.begin() as conn:
    result = conn.execute(text('SELECT COUNT(*) as total, COUNT(DISTINCT var_id_scada) as vars_unicas FROM referencial.var_scada_map'))
    for row in result:
        print(f'Total registros: {row[0]}, Variables únicas: {row[1]}')
        
    print('\nPrimeros 10 registros:')
    result = conn.execute(text('SELECT var_id_scada, id_formato1, columna_stage FROM referencial.var_scada_map LIMIT 10'))
    for row in result:
        print(f'  var_id_scada={row[0]}, id_formato1={row[1]}, columna_stage={row[2]}')
