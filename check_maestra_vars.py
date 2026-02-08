#!/usr/bin/env python3
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = f"postgresql://{os.getenv('DB_USER', 'audit')}:{os.getenv('DEV_DB_PASSWORD', 'audit')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5433')}/{os.getenv('DB_NAME', 'etl_data')}"
engine = create_engine(DB_URL)

with engine.begin() as conn:
    print("Total variables en tbl_maestra_variables:")
    result = conn.execute(text('SELECT COUNT(*) FROM referencial.tbl_maestra_variables'))
    print(f"  {result.scalar()}")
    
    print("\nPrimeros 10 nombres_tecnico:")
    result = conn.execute(text('SELECT nombre_tecnico FROM referencial.tbl_maestra_variables LIMIT 10'))
    for row in result:
        print(f"  {row[0]}")
