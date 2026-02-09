import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

def check_rc_table():
    print("--- REVISANDO referencial.tbl_reglas_consistencia ---")
    with engine.connect() as conn:
        query = text("""
            SELECT 
                r.codigo_rc, 
                r.operador, 
                v1.nombre_tecnico as var_a,
                v2.nombre_tecnico as var_b,
                r.descripcion
            FROM referencial.tbl_reglas_consistencia r
            LEFT JOIN referencial.tbl_maestra_variables v1 ON r.variable_a_id = v1.variable_id
            LEFT JOIN referencial.tbl_maestra_variables v2 ON r.variable_b_id = v2.variable_id
        """)
        df = pd.read_sql(query, conn)
        print(df.to_string(index=False))

    print("\n--- REVISANDO NOMBRES TÉCNICOS EN MAESTRA (Muestra) ---")
    with engine.connect() as conn:
        q_maestra = text("SELECT variable_id, id_formato1, nombre_tecnico FROM referencial.tbl_maestra_variables WHERE nombre_tecnico ~* 'rod_load|presion|profundidad|diametro' LIMIT 20")
        df_m = pd.read_sql(q_maestra, conn)
        print(df_m.to_string(index=False))

if __name__ == "__main__":
    check_rc_table()
