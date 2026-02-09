import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

def run_concise_audit():
    print("--- INICIANDO AUDITORÍA INFORMATIVA (SIN MODIFICACIONES) ---")
    
    with engine.connect() as conn:
        # 1. Verificar Maestra de Variables (id_formato1)
        print("\n[1] Verificando referencial.tbl_maestra_variables (id_formato1):")
        total_vars = conn.execute(text("SELECT COUNT(*) FROM referencial.tbl_maestra_variables")).scalar()
        null_ids = conn.execute(text("SELECT COUNT(*) FROM referencial.tbl_maestra_variables WHERE id_formato1 IS NULL")).scalar()
        print(f"    Total variables: {total_vars}")
        print(f"    Variables con id_formato1 NULO: {null_ids}")
        
        # 2. Verificar Maestra de Pozos (API Number y Coordenadas)
        print("\n[2] Verificando stage.tbl_pozo_maestra (Datos de pozo):")
        total_wells = conn.execute(text("SELECT COUNT(*) FROM stage.tbl_pozo_maestra")).scalar()
        null_api = conn.execute(text("SELECT COUNT(*) FROM stage.tbl_pozo_maestra WHERE api_number IS NULL")).scalar()
        null_coords = conn.execute(text("SELECT COUNT(*) FROM stage.tbl_pozo_maestra WHERE coordenadas_pozo IS NULL")).scalar()
        print(f"    Total pozos: {total_wells}")
        print(f"    API Numbers NULOS: {null_api}")
        print(f"    Coordenadas NULAS: {null_coords}")
        
        # 3. Muestra de los primeros 3 registros de variables para validar contenido
        print("\n[3] Muestra de variables (Primeras 3):")
        vars_sample = conn.execute(text("SELECT variable_id, id_formato1, nombre_tecnico FROM referencial.tbl_maestra_variables LIMIT 3")).fetchall()
        for r in vars_sample:
            print(f"    ID: {r[0]} | F1: {r[1]} | Nombre: {r[2]}")

        # 4. Muestra de los primeros registros de pozos
        print("\n[4] Muestra de pozos:")
        wells_sample = conn.execute(text("SELECT well_id, nombre_pozo, api_number, coordenadas_pozo FROM stage.tbl_pozo_maestra LIMIT 2")).fetchall()
        for r in wells_sample:
            print(f"    WellID: {r[0]} | Pozo: {r[1]} | API: {r[2]} | Coords: {r[3]}")

if __name__ == "__main__":
    run_concise_audit()
