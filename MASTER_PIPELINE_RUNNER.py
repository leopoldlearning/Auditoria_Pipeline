#!/usr/bin/env python3
"""
MASTER PIPELINE RUNNER - BP010 Data Pipelines
=============================================
Orquestador Maestro (v2 - Optimizado):
  1. INIT      → DDL + SPs (full reset)
  2. LOAD      → Referencial + Ingesta + Seeds
  3. DQ        → Validación calidad de datos
  4. TRANSFORM → Facts (hora/día/mes) + Snapshot
  5. ENRICH    → Targets + Evaluación + Derivados + KPIs
  6. DEFAULTS  → Baselines desde tbl_config_kpi

Flujo: init_schemas → load_referencial → ingest → seeds → DQ
       → sp_load_to_reporting → current_values → sync_targets
       → evaluación_universal → derivados_completos
       → poblar_kpi_business → sp_populate_defaults
"""

import os
import subprocess
import sys
from datetime import date, timedelta
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

# Ventana de fechas parametrizable (env: LOOKBACK_DAYS, FECHA_INICIO, FECHA_FIN)
LOOKBACK_DAYS = int(os.getenv('LOOKBACK_DAYS', '3650'))  # Default: ~10 años
FECHA_FIN = os.getenv('FECHA_FIN', str(date.today() + timedelta(days=365*5)))
FECHA_INICIO = os.getenv('FECHA_INICIO', str(date.today() - timedelta(days=LOOKBACK_DAYS)))

# -----------------------------------------------------------------------------
# FUNCIONES AUXILIARES
# -----------------------------------------------------------------------------

def execute_sql_file(filename, description):
    """Lee y ejecuta un archivo SQL completo."""
    print(f"\n>>> Executing SQL File: {filename} ({description})...")
    engine = create_engine(DB_URL)

    # Buscar archivo en src/sql/schema, src/sql/process, o raíz
    search_paths = [
        os.path.join("src", "sql", "schema"),
        os.path.join("src", "sql", "process"),
    ]
    
    file_path = None
    for base_path in search_paths:
        candidate = os.path.join(base_path, filename)
        if os.path.exists(candidate):
            file_path = candidate
            break
    
    if file_path is None:
        # Fallback: buscar en raíz
        if os.path.exists(filename):
            file_path = filename
        else:
            print(f"[ERROR] Archivo no encontrado: {filename}")
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
    print("MASTER PIPELINE ORCHESTRATION (v2)")
    print(f"Python: {PYTHON_EXE}")
    print(f"Fechas: {FECHA_INICIO} → {FECHA_FIN}")
    print(f"DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("="*60)

    # =========================================================================
    # FASE 1: INIT (DDL + SPs) — Full Reset
    # =========================================================================
    # Carga 12 SQL files: V4 schemas, V1 universal, V6.* SPs, V7 KPI+clasificación,
    # V8 evaluación, V9 funciones. Todo con CREATE OR REPLACE / DROP+CREATE.
    print("\n>>> 1. Initializing Schemas (Full Reset)...")
    subprocess.run([PYTHON_EXE, "init_schemas.py"], check=True)
    print("[OK] Init Schemas completed.")

    # =========================================================================
    # FASE 2: LOAD (Referencial + Ingesta + Seeds)
    # =========================================================================
    print("\n>>> 2.1 Loading Referencial Master...")
    subprocess.run([PYTHON_EXE, "load_referencial.py"], check=True)
    print("[OK] Referencial loaded.")

    print("\n>>> 2.2 Ingesting Telemetry (SQL Dumps + Excel)...")
    subprocess.run([PYTHON_EXE, "ingest_real_telemetry.py"], check=True)
    print("[OK] Ingestion completed.")

    print("\n>>> 2.3 Seeding Missing Referencial Data...")
    execute_sql_query("CALL referencial.sp_seed_defaults();")
    print("[OK] Seeds populated.")

    # =========================================================================
    # FASE 3: DATA QUALITY
    # =========================================================================
    print("\n>>> 3. Running Data Quality Validation...")
    execute_sql_query(f"""
        CALL stage.sp_execute_dq_validation(
            '{FECHA_INICIO}'::DATE,
            '{FECHA_FIN}'::DATE,
            NULL::INT
        );
    """)
    print("[OK] DQ Validation completed.")

    # =========================================================================
    # FASE 4: TRANSFORM (Stage → Reporting Facts + Snapshot)
    # =========================================================================
    print("\n>>> 4.1 Loading Reporting Facts (Hourly, Daily, Monthly)...")
    execute_sql_query(f"""
        CALL reporting.sp_load_to_reporting(
            '{FECHA_INICIO}'::DATE,
            '{FECHA_FIN}'::DATE,
            TRUE, TRUE, TRUE
        );
    """)
    print("[OK] Facts loaded.")

    print("\n>>> 4.2 Updating Snapshot (dataset_current_values)...")
    execute_sql_query("CALL reporting.actualizar_current_values_v4();")
    print("[OK] Snapshot updated.")

    # =========================================================================
    # FASE 5: ENRICH (Targets + Evaluación + Derivados + KPIs)
    # =========================================================================
    print("\n>>> 5.1 Syncing Dim Pozo Targets...")
    execute_sql_query("CALL reporting.sp_sync_dim_pozo_targets();")
    print("[OK] Targets synced.")

    print("\n>>> 5.2 Applying Universal Evaluation (Semáforos V8)...")
    execute_sql_query("CALL reporting.aplicar_evaluacion_universal();")
    print("[OK] Evaluation applied.")

    # sp_calcular_derivados_completos ejecuta: derivados_current_values,
    # derivados_horarios, kpis_horarios, promedios_diarios,
    # completar_fact_diarias, reagregar_mensuales, kpis_business
    print("\n>>> 5.3 Running Derived Calculations (V9)...")
    execute_sql_query(f"""
        CALL reporting.sp_calcular_derivados_completos(
            '{FECHA_INICIO}'::DATE,
            '{FECHA_FIN}'::DATE
        );
    """)
    print("[OK] Derived calculations completed.")

    print("\n>>> 5.4 Populating KPI Business (V7 WIDE)...")
    execute_sql_query(f"""
        CALL reporting.poblar_kpi_business(
            '{FECHA_INICIO}'::DATE,
            '{FECHA_FIN}'::DATE
        );
    """)
    print("[OK] KPI Business populated.")

    # =========================================================================
    # FASE 6: DEFAULTS (Baselines desde tbl_config_kpi)
    # =========================================================================
    print("\n>>> 6. Populating Baselines & Defaults...")
    execute_sql_query("CALL reporting.sp_populate_defaults();")
    print("[OK] Defaults populated.")

    # =========================================================================
    # FASE 7: CONSISTENCY VALIDATION (RC-001..RC-006)
    # =========================================================================
    print("\n>>> 7. Running Consistency Rules Validation...")
    execute_sql_query("CALL stage.sp_execute_consistency_validation();")
    print("[OK] Consistency validation completed.")

    print("\n" + "="*60)
    print(">>> PIPELINE COMPLETED SUCCESSFULLY <<<")
    print("="*60)


if __name__ == "__main__":
    run_pipeline()
