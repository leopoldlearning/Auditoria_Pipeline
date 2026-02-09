
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

# Verify the 35 specific variables mapping
query = text("""
    SELECT 
        id_formato1,
        nombre_tecnico,
        clasificacion_logica
    FROM referencial.tbl_maestra_variables
    ORDER BY id_formato1 NULLS LAST, nombre_tecnico
""")

with engine.connect() as conn:
    result = conn.execute(query)
    print("VERIFICACIÓN DE MAESTRA V4 (ALINEACIÓN 35 REGLAS):")
    print("-" * 60)
    print(f"{'ID_F1':<10} | {'Nombre Técnico':<35} | Clasificación")
    print("-" * 60)
    for row in result:
        id_val = str(row[0]) if row[0] is not None else "S/I"
        print(f"{id_val:<10} | {row[1]:<35} | {row[2]}")
    print("-" * 60)

dq_count = text("SELECT count(*) FROM referencial.tbl_dq_rules")
rc_count = text("SELECT count(*) FROM referencial.tbl_reglas_consistencia")

with engine.connect() as conn:
    print(f"Total DQ Rules active: {conn.execute(dq_count).scalar()}")
    print(f"Total RC Rules active: {conn.execute(rc_count).scalar()}")
