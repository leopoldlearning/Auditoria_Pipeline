#!/usr/bin/env python3
from sqlalchemy import create_engine, text, inspect
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = f"postgresql://{os.getenv('DB_USER', 'audit')}:{os.getenv('DEV_DB_PASSWORD', 'audit')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5433')}/{os.getenv('DB_NAME', 'etl_data')}"
engine = create_engine(DB_URL)

# Inspeccionar tabla
inspector = inspect(engine)
columns = inspector.get_columns('tbl_limites_pozo', schema='referencial')
pk = inspector.get_pk_constraint('tbl_limites_pozo', schema='referencial')
unique_constraints = inspector.get_unique_constraints('tbl_limites_pozo', schema='referencial')

print("Columnas:")
for col in columns:
    print(f"  {col['name']}: {col['type']}")

print(f"\nPrimary Key: {pk}")
print(f"Unique Constraints: {unique_constraints}")
