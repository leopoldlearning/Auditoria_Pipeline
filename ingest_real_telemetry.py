#!/usr/bin/env python3
"""
Ingesta híbrida:
- Maestra desde SQL
- Producción desde SQL
- Reservas desde Excel
- Landing SCADA desde SQL
"""

import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

# Logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

# === CONFIGURACIÓN ===
DATA_DIR = r"D:\ITMeet\Operaciones\API Hydrog manual"
EXCEL_PATH = r"data/udf/Formato1_Excel_Reservas.xlsx"

DB_URL = (
    f"postgresql://{os.getenv('DB_USER', 'audit')}:"
    f"{os.getenv('DEV_DB_PASSWORD', 'audit')}@"
    f"{os.getenv('DB_HOST', 'localhost')}:"
    f"{os.getenv('DB_PORT', '5433')}/"
    f"{os.getenv('DB_NAME', 'etl_data')}"
)

engine = create_engine(DB_URL)


# ------------------------------------------------------------
# Ejecutar archivo SQL (maestra, producción, landing)
# ------------------------------------------------------------
def execute_sql_file(file_path: str) -> None:
    logger.info(f"📂 Ejecutando SQL: {os.path.basename(file_path)}")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            sql = f.read()
        with engine.begin() as conn:
            conn.execute(text(sql))
        logger.info(f"✅ OK: {os.path.basename(file_path)}")
    except Exception as e:
        logger.error(f"❌ Error ejecutando {file_path}: {e}")


# ------------------------------------------------------------
# Transformación interna de reservas
# ------------------------------------------------------------
def transformar_reservas(df_raw: pd.DataFrame) -> pd.DataFrame:
    mapeo_reservas = {
        1: 'well_id',
        10: 'gravedad_api',
        18: 'viscosidad_crudo',
        24: 'presion_burbujeo',
        25: 'presion_estatica_yacimiento',
        27: 'presion_fondo_fluyente_critico',
        29: 'viscosidad_superficie',
        30: 'factor_volumetrico',
        31: 'otros_pvt',
        32: 'wc_critico',
        48: 'llenado_bomba_minimo',
        58: 'contenido_finos',
        63: 'gravedad_especifica_agua',
        128: 'reserva_inicial_teorica',
        152: 'q_esperado',
        159: 'radio_equivalente',
        160: 'longitud_horizontal',
        161: 'factor_dano',
        162: 'permeabilidad_vertical',
    }

    df_filtrado = df_raw[df_raw["ID"].isin(mapeo_reservas.keys())]

    datos = {}
    for _, row in df_filtrado.iterrows():
        col = mapeo_reservas[int(row["ID"])]
        datos[col] = [row["Valor"]]

    df_final = pd.DataFrame(datos)
    df_final["well_id"] = int(df_final["well_id"].iloc[0])
    df_final["fecha_registro"] = datetime.now().date()

    return df_final


# ------------------------------------------------------------
# Insertar reservas desde Excel
# ------------------------------------------------------------
def insertar_reservas_desde_excel() -> None:
    logger.info("📘 Ingestando RESERVAS desde Excel...")
    df_raw = pd.read_excel(EXCEL_PATH, sheet_name="Datos Reserva")
    df_reservas = transformar_reservas(df_raw)

    df_reservas.to_sql(
        name="tbl_pozo_reservas",
        schema="stage",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    logger.info("✅ Reservas insertadas correctamente desde Excel.")


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def main() -> None:
    logger.info("====================================================")
    logger.info(">>> INGESTA HÍBRIDA + LANDING SCADA (SQL)")
    logger.info("====================================================")

    files = os.listdir(DATA_DIR)

    # 1. MAESTRA
    for f in files:
        if "maestra" in f.lower() and f.endswith(".sql"):
            execute_sql_file(os.path.join(DATA_DIR, f))

    # 2. PRODUCCIÓN
    for f in files:
        if "produccion" in f.lower() and f.endswith(".sql"):
            execute_sql_file(os.path.join(DATA_DIR, f))

    # 3. RESERVAS (Excel)
    insertar_reservas_desde_excel()

    # 4. LANDING SCADA
    for f in files:
        if "landing_scada_data" in f.lower() and f.endswith(".sql"):
            execute_sql_file(os.path.join(DATA_DIR, f))

    logger.info(">>> INGESTA COMPLETA <<<")


if __name__ == "__main__":
    main()
