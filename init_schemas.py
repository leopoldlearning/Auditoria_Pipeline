#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    DB_URL = f"postgresql://{os.getenv('DB_USER', 'audit')}:{os.getenv('DEV_DB_PASSWORD', 'audit')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5433')}/{os.getenv('DB_NAME', 'etl_data')}"

# =============================================================================
# ESQUEMA SQL CONSOLIDADO EN 5 FAMILIAS (Decisión #3 Plan Consolidación)
# =============================================================================
# Familia 1: DDL Schemas (CREATE SCHEMA + CREATE TABLE)
# Familia 2: Funciones & Clasificación (tipos, funciones puras, config evaluación)
# Familia 3: Stored Procedures ETL (motores de reporting, DQ, snapshot)
# Familia 4: Business KPI (config KPI + dataset_kpi_business + semáforos)
# Familia 5: Evaluación & Cálculos Derivados (evaluación universal + derivados)
# =============================================================================
SCHEMA_FILES = [
    # ─────────────────────────────────────────────────────────────
    # FAMILIA 1: DDL SCHEMAS — Estructura de tablas y esquemas
    # ─────────────────────────────────────────────────────────────
    "V4__stage_schema_redesign.sql",              # stage.* (landing, maestra, produccion, reservas, dq)
    "V4__referencial_schema_redesign.sql",         # referencial.* (unidades, estados, variables, limites, dq_rules, funciones eval)
    "V2__universal_schema.sql",                    # universal.* (patron, stroke, diagnostico, validacion, ipr, arps, bombeo)
    "V4__reporting_schema_redesign.sql",            # reporting.* (dims, facts PARTICIONADA, datasets) ← PARTICIONAMIENTO APLICADO

    # ─────────────────────────────────────────────────────────────
    # FAMILIA 2: FUNCIONES & SISTEMA DE CLASIFICACIÓN
    # ─────────────────────────────────────────────────────────────
    "V9__calculos_derivados_funciones.sql",         # stage.fnc_calc_* (fluid_level, pwf, hydralift, road_load, variance)
    "V7__sistema_clasificacion_universal.sql",      # referencial.fnc_evaluar_variable() + config_evaluacion + catalogo_status

    # ─────────────────────────────────────────────────────────────
    # FAMILIA 3: STORED PROCEDURES ETL — Motores de procesamiento
    # ─────────────────────────────────────────────────────────────
    "V6.1__historical_reporting_engine_v4.sql",     # reporting.sp_load_to_reporting() (horario/diario/mensual)
    "V6.2__dq_engine_v4.sql",                       # stage.sp_execute_dq_validation()
    "V6.3__sync_dim_pozo_targets_v4.sql",           # reporting.sp_sync_dim_pozo_targets()
    "V6__stored_procedures_v4_compatible.sql",       # reporting.actualizar_current_values_v4() (snapshot zero-calc)

    # ─────────────────────────────────────────────────────────────
    # FAMILIA 4: BUSINESS KPI — Configuración y dataset de negocio
    # ─────────────────────────────────────────────────────────────
    "V7__kpi_business_redesign.sql",                # dataset_kpi_business + poblar_kpi_business() ← HORARIO ELIMINADO

    # ─────────────────────────────────────────────────────────────
    # FAMILIA 5: EVALUACIÓN UNIVERSAL & CÁLCULOS DERIVADOS
    # ─────────────────────────────────────────────────────────────
    "V8__evaluacion_semaforos_reporting.sql",       # aplicar_evaluacion_universal() ← REESCRITO SET-BASED (sin FOR LOOP)
    "V9__calculos_derivados_process.sql",           # sp_calcular_derivados_completos() + KPIs horarios (src/sql/process/)

    # ─────────────────────────────────────────────────────────────
    # FAMILIA 6: PUENTE UNIVERSAL → REPORTING (SPs listos, se invocan cuando ML escriba datos)
    # ─────────────────────────────────────────────────────────────
    "V10__universal_to_reporting_bridge.sql",        # sp_sync_cdi/ipr/arps_to_reporting() ← CDI+IPR+ARPS → reporting

    # ─────────────────────────────────────────────────────────────
    # FAMILIA 7: VISTAS HELPER FRONTEND
    # ─────────────────────────────────────────────────────────────
    "V12__vistas_helper_frontend.sql",              # vw_dashboard_main, vw_kpi_daily/monthly, vw_well_selector/alerts
]

def init_db():
    engine = create_engine(DB_URL)
    # Ajusta esta ruta si tus SQLs no están en src/sql/schema
    base_path = Path(__file__).parent / "src" / "sql" / "schema"
    
    logger.info(">>> INICIANDO CREACIÓN DE ESQUEMAS <<<")
    
    # FORZAR RESET LIMPIO (Atomic)
    with engine.connect() as conn:
        logger.info("Forzando eliminación de esquemas antiguos...")
        conn.execute(text("DROP SCHEMA IF EXISTS referencial CASCADE;"))
        conn.execute(text("DROP SCHEMA IF EXISTS stage CASCADE;"))
        conn.execute(text("DROP SCHEMA IF EXISTS reporting CASCADE;"))
        conn.execute(text("DROP SCHEMA IF EXISTS universal CASCADE;"))
        conn.commit()
    
    with engine.begin() as conn:
        for filename in SCHEMA_FILES:
            file_path = base_path / filename
            
            if not file_path.exists():
                # Fallback 1: buscar en src/sql/process/
                file_path = Path(__file__).parent / "src" / "sql" / "process" / filename
            
            if not file_path.exists():
                # Fallback 2: buscar en la raíz
                file_path = Path(__file__).parent / filename
            
            if not file_path.exists():
                logger.error(f"❌ ARCHIVO NO ENCONTRADO: {filename}")
                sys.exit(1)
                
            logger.info(f"Exec: {filename}...")
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    sql = f.read()
                    if sql.strip():
                        conn.execute(text(sql))
                        logger.info(f"✅ {filename} completado.")
            except Exception as e:
                logger.error(f"❌ Error en {filename}: {e}")
                sys.exit(1)

    logger.info(">>> BD INICIALIZADA CORRECTAMENTE <<<")

if __name__ == "__main__":
    init_db()