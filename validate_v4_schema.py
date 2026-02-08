import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    DB_URL = f"postgresql://{os.getenv('DB_USER', 'audit')}:{os.getenv('DEV_DB_PASSWORD', 'audit')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5433')}/{os.getenv('DB_NAME', 'etl_data')}"

engine = create_engine(DB_URL)

def run_check(name, sql, params=None):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), params or {}).scalar()
            print(f"[PASS] {name}: {result}")
            return True
    except Exception as e:
        print(f"[FAIL] {name}: {e}")
        return False

print("=== VALIDACIÓN ESQUEMA V4 ===")

# 1. Verificar Tablas Referencial
run_check("Referencial Variables Count", "SELECT COUNT(*) FROM referencial.tbl_maestra_variables")
run_check("Referencial Unidades Count", "SELECT COUNT(*) FROM referencial.tbl_ref_unidades")
run_check("Referencial Límites Count", "SELECT COUNT(*) FROM referencial.tbl_limites_pozo")

# 2. Verificar Tablas Reporting V4
run_check("Reporting Current Values Exists", "SELECT to_regclass('reporting.dataset_current_values') IS NOT NULL")
# Verificar columna renombrada
run_check("Column well_head_pressure_psi_act exists", 
          "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='reporting' AND table_name='dataset_current_values' AND column_name='well_head_pressure_psi_act'")

# 3. Verificar Función
run_check("Función fnc_evaluar_universal", "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = 'fnc_evaluar_universal')")

# 4. Verificar Procedimientos SP
run_check("SP actualizar_current_values_v4", "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = 'actualizar_current_values_v4')")

# 5. Verificar Datos Semilla Específicos
run_check("Variable 'well_head_pressure_psi_act' en Maestra", 
          "SELECT COUNT(*) FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'well_head_pressure_psi_act'")

print("=== FIN VALIDACIÓN ===")
