#!/usr/bin/env python3
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = f"postgresql://{os.getenv('DB_USER', 'audit')}:{os.getenv('DEV_DB_PASSWORD', 'audit')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5433')}/{os.getenv('DB_NAME', 'etl_data')}"
engine = create_engine(DB_URL)

with engine.begin() as conn:
    result = conn.execute(text('''
        SELECT COUNT(*) as total, 
               COUNT(DISTINCT variable_id) as vars_unicas,
               MIN(min_warning) as min_global,
               MAX(max_warning) as max_global
        FROM referencial.tbl_limites_pozo 
        WHERE pozo_id IS NULL
    '''))
    for row in result:
        print(f'Total: {row[0]}, Variables únicas: {row[1]}, Min global: {row[2]}, Max global: {row[3]}')
        
    print('\nPrimeros 10 registros:')
    result = conn.execute(text('''
        SELECT v.nombre_tecnico, l.min_warning, l.max_warning 
        FROM referencial.tbl_limites_pozo l
        JOIN referencial.tbl_maestra_variables v ON l.variable_id = v.variable_id
        WHERE l.pozo_id IS NULL
        LIMIT 10
    '''))
    for row in result:
        print(f'  {row[0]}: min={row[1]}, max={row[2]}')
