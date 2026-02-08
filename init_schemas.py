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

# LISTA CORREGIDA CON TUS NOMBRES DE ARCHIVO EXACTOS
SCHEMA_FILES = [
    # 1. Estructura (DDL)
    "V4__referencial_schema_redesign.sql",
    "V4__stage_schema_redesign.sql",
      
    "V1__universal_schema.sql",
    "V4__reporting_schema_redesign.sql",
    "V6.1__historical_reporting_engine_v4.sql",
    "V6.2__dq_engine_v4.sql",
    "V6.3__sync_dim_pozo_targets_v4.sql",
    
    # 2. MOTOR DE REPORTING (PROCEDIMIENTOS)
    #"V2_reporting_engine.sql",
    
    "V6__stored_procedures_v4_compatible.sql",

    # 3. Datos Semilla (Referencial)
    "V4__referencial_seed_data.sql",
 
]

def init_db():
    engine = create_engine(DB_URL)
    # Ajusta esta ruta si tus SQLs no están en src/sql/schema
    base_path = Path(__file__).parent / "src" / "sql" / "schema"
    
    logger.info(">>> INICIANDO CREACIÓN DE ESQUEMAS <<<")
    
    with engine.begin() as conn:
        for filename in SCHEMA_FILES:
            file_path = base_path / filename
            
            if not file_path.exists():
                # Fallback: buscar en la raíz si no está en src/sql/schema
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