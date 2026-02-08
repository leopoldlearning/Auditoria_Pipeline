#!/usr/bin/env python3

#INGESTA REAL
#    ↓
#V5 UNIVERSAL ENGINE
#    - Funciones universales
#    - Vista pivoteada
#    - Motor DQ
#    - Motor Color Logic
#    ↓
#V2 REPORTING ENGINE
#    - Horario
#    - Diario
#    - Mensual
#    ↓
#KPIs DE NEGOCIO
#    ↓
#V3 SNAPSHOT ENGINE
#    ↓
#COLOR LOGIC (SP)
#    ↓
#DATASETS FINALES

"""
MASTER PIPELINE RUNNER - BP010 Data Pipelines
=============================================
Orquestador Maestro: DDL -> Ingesta Real -> Lógica V5 -> DQ -> Reporting -> Snapshot -> Semáforos.
"""

import os
import subprocess
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Cargar variables de entorno (.env)
load_dotenv()

# -----------------------------------------------------------------------------
# CONFIGURACIÓN
# -----------------------------------------------------------------------------

# Detectar Python del entorno virtual (auditor) o fallback al sistema
POSSIBLE_PATHS = [
    os.path.join("auditor", "Scripts", "python.exe"),  # Windows venv
    os.path.join("auditor", "bin", "python"),          # Linux/Mac venv
    sys.executable                                     # System python
]

PYTHON_EXE = sys.executable
for path in POSSIBLE_PATHS:
    if os.path.exists(path):
        PYTHON_EXE = path
        break

# Configuración de Base de Datos
DB_USER = os.getenv('DB_USER', 'audit')
DB_PASS = os.getenv('DEV_DB_PASSWORD', 'audit')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5433')
DB_NAME = os.getenv('DB_NAME', 'etl_data')

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# -----------------------------------------------------------------------------
# FUNCIONES AUXILIARES
# -----------------------------------------------------------------------------

def execute_sql_file(filename, description):
    """Lee y ejecuta un archivo SQL completo."""
    print(f"\n>>> Executing SQL File: {filename} ({description})...")
    engine = create_engine(DB_URL)

    # Buscar archivo en src/sql/schema o fallback a raíz
    base_path = os.path.join("src", "sql", "schema")
    file_path = os.path.join(base_path, filename)

    if not os.path.exists(file_path):
        file_path = filename  # fallback

    if not os.path.exists(file_path):
        print(f"[ERROR] Archivo no encontrado: {file_path}")
        sys.exit(1)

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            sql_content = f.read()

        with engine.begin() as conn:
            conn.execute(text(sql_content))

        print(f"[OK] {filename} applied successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to execute {filename}: {e}")
        sys.exit(1)


def execute_sql_query(query):
    """Ejecuta una consulta SQL puntual (CALL procedure)."""
    engine = create_engine(DB_URL)
    try:
        with engine.begin() as conn:
            conn.execute(text(query))
    except Exception as e:
        print(f"[ERROR] Query Failed: {query}")
        print(f"Details: {e}")
        sys.exit(1)

# -----------------------------------------------------------------------------
# EJECUCIÓN PRINCIPAL
# -----------------------------------------------------------------------------

def run_pipeline():
    print("="*60)
    print("STARTING MASTER PIPELINE ORCHESTRATION")
    print(f"Python Executable: {PYTHON_EXE}")
    print("="*60)

    # PASO 1: REINICIO DE ESQUEMAS
    print("\n>>> 1. Re-initializing Schemas (Full Reset)...")
    subprocess.run([PYTHON_EXE, "init_schemas.py"], check=True)
    print("[OK] Init Schemas completed.")

    # PASO 1.5: REFERENCIAL MASTER 
    print("\n>>> 1.5 Loading Referencial Master (Variables, Rangos, Mapas SCADA)...")
    subprocess.run([PYTHON_EXE, "load_referencial.py"], check=True) 
    print("[OK] Referencial Master Loaded.")
    
    # PASO 2: INGESTA REAL
    print("\n>>> 2. Ingesting Real Telemetry (SQL Dumps)...")
    subprocess.run([PYTHON_EXE, "ingest_real_telemetry.py"], check=True)
    print("[OK] Ingestion completed.")

    # PASO 3: LÓGICA UNIVERSAL (V5)
    print("\n>>> 3. Loading Universal Logic Engine (V5)...")
    # execute_sql_file("V5__stored_procedures.sql", "Updating Logic V5") -- OBSOLETO, reemplazado por V6 en init_schemas
    print("[SKIP] V5 Logic skipped (Covered by V6 in init_schemas).")

    # PASO 3.1: MOTOR DQ
    print("\n>>> 3.1 Running Data Quality Engine (V5 DQ)...")
    execute_sql_query("""
        CALL stage.sp_execute_dq_validation(
            '2020-01-01'::DATE,
            '2030-12-31'::DATE,
            NULL::INT
        );
    """)
    print("[OK] Data Quality Validation completed.")

    # PASO 3.5: REPORTING ENGINE V2
    print("\n>>> 3.5 Loading Reporting Engine V2...")
    # execute_sql_file("V2_reporting_engine.sql", "Loading Reporting Engine V2") -- OBSOLETO, reemplazado por V6.1 en init_schemas
    print("[SKIP] Reporting Engine V2 skipped (Covered by V6.1 in init_schemas).")

    # PASO 5: CARGA HISTÓRICA (FACTS)
    print("\n>>> 5. Running Reporting Load (Daily, Hourly, Monthly)...")
    execute_sql_query("""
        CALL reporting.sp_load_to_reporting(
            '2020-01-01'::DATE,
            '2030-12-31'::DATE,
            TRUE, TRUE, TRUE
        );
    """)
    print("[OK] History loaded.")

    # PASO 5.5: KPIs DE NEGOCIO
    print("\n>>> 5.5 Loading Business KPIs...")
    execute_sql_query("""
        CALL reporting.sp_load_kpi_business(
            '2020-01-01'::DATE,
            '2030-12-31'::DATE
        );
    """)
    print("[OK] Business KPIs loaded.")

    # PASO 6: SNAPSHOT ENGINE V3
    print("\n>>> 6. Updating Snapshot (dataset_current_values)...")
    execute_sql_query("CALL reporting.actualizar_current_values_v4();") # Updated to V4
    print("[OK] Snapshot updated (V4).")

    # PASO 6.5: LÓGICA DE COLORES Y TARGETS
    print("\n>>> 6.5 Applying Color & Target Logic...")
    # execute_sql_query("CALL reporting.sp_apply_color_logic();") -- OBSOLETO, integrado en actualizar_current_values_v4
    print("[SKIP] Color Logic (Integrated in V4).")
    
    # 6.6 Sincronización Dim Pozo (Recuperado de V5)
    print("\n>>> 6.6 Syncing Dim Pozo Targets...")
    execute_sql_query("CALL reporting.sp_sync_dim_pozo_targets();")
    print("[OK] Dim Pozo Targets Synced.")

    print("\n>>> PIPELINE COMPLETED SUCCESSFULLY <<<")


if __name__ == "__main__":
    run_pipeline()
