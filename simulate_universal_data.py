import os
import random
import json
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# Database Connection
DB_USER = os.getenv('DB_USER', 'audit')
DB_PASSWORD = os.getenv('DEV_DB_PASSWORD', 'audit')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5433')
DB_NAME = os.getenv('DB_NAME', 'etl_data')
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

def generate_ipr_curve(qmax, pres_res):
    # Vogel's IPR Equation: Qo / Qmax = 1 - 0.2*(Pwf/Pr) - 0.8*(Pwf/Pr)^2
    points = 10
    q_vals = []
    pwf_vals = []
    
    for i in range(points + 1):
        pwf = (pres_res / points) * i
        ratio = pwf / pres_res
        q = qmax * (1 - 0.2 * ratio - 0.8 * (ratio ** 2))
        q_vals.append(round(max(0, q), 2))
        pwf_vals.append(round(pwf, 2))
        
    return {"q": q_vals, "pwf": pwf_vals}

def populate_universal():
    logger.info("Starting Universal Data Simulation...")
    
    with engine.begin() as conn:
        # Get active wells from Master
        wells = conn.execute(text("SELECT well_id, nombre_pozo FROM stage.tbl_pozo_maestra")).fetchall()
        
        if not wells:
            logger.warning("No wells found in stage.tbl_pozo_maestra. Skipping generation.")
            return

        logger.info(f"Found {len(wells)} wells. Generating IPR and ARPS data...")

        for well in wells:
            well_id = well[0]
            well_name = well[1]
            str_well_id = str(well_id) # Universal uses VARCHAR for IDs sometimes

            # 1. IPR Results
            # Randomized Physics parameters
            pres_res = random.uniform(2000, 4500)
            qmax = random.uniform(500, 3000)
            ip = qmax / pres_res # Simplified PI
            
            curva = generate_ipr_curve(qmax, pres_res)
            
            # Insert IPR
            conn.execute(text("""
                INSERT INTO universal.ipr_resultados 
                (id_pozo, fecha_calculo, metodo, qmax, ip, curva_yacimiento, alertas)
                VALUES (:wid, :fecha, 'Vogel', :qmax, :ip, :curva, :alertas)
            """), {
                "wid": str_well_id,
                "fecha": datetime.now(),
                "qmax": round(qmax, 2),
                "ip": round(ip, 4),
                "curva": json.dumps(curva),
                "alertas": json.dumps(["Low Efficiency"] if random.random() > 0.8 else [])
            })

            # 2. ARPS Results (Decline Curve)
            # Hyperbolic decline parameters
            qi = random.uniform(100, 800)
            di = random.uniform(0.1, 0.3) # 10-30% annual decline
            b = random.uniform(0.1, 0.9)
            
            conn.execute(text("""
                INSERT INTO universal.arps_resultados_declinacion
                (id_pozo, fecha_analisis, tipo_curva, qi, di, b, r_squared, eur_total)
                VALUES (:wid, :fecha, 'Hiperbolica', :qi, :di, :b, 0.98, :eur)
                ON CONFLICT (id_pozo, fecha_analisis, tipo_curva) DO NOTHING
            """), {
                "wid": str_well_id,
                "fecha": datetime.now(),
                "qi": round(qi, 2),
                "di": round(di, 4),
                "b": round(b, 2),
                "eur": round(qi * 365 * 10, 2) # Rough estimate
            })
            
    logger.info("Universal Data Population Complete.")

if __name__ == "__main__":
    populate_universal()