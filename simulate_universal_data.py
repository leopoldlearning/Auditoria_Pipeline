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
    """Genera curva IPR teórica usando ecuación de Vogel."""
    curve = {"q": [], "pwf": []}
    steps = 20
    for i in range(steps + 1):
        pwf = (i / steps) * pres_res
        # Vogel: q/qmax = 1 - 0.2(pwf/pr) - 0.8(pwf/pr)^2
        ratio_p = pwf / pres_res
        q = qmax * (1 - 0.2 * ratio_p - 0.8 * (ratio_p ** 2))
        curve["pwf"].append(round(pwf, 2))
        curve["q"].append(round(q, 2))
    return curve

def seed_patrones_cdi(conn):
    """Inserta catálogo de patrones CDI estándar de la industria."""
    patrones = [
        ("Operación Normal",        "BAJA",    "Carta sin anomalías, forma rectangular típica."),
        ("Golpe de Fluido",         "ALTA",    "Fluid Pound — llenado incompleto, impacto en fondo de carrera."),
        ("Gas Lock",                "CRITICA", "Interferencia de gas libre, compresión sin desplazamiento de líquido."),
        ("Fuga Válvula Viajera",    "ALTA",    "Travelling valve leak — pérdida de carga en carrera ascendente."),
        ("Fuga Válvula Fija",       "ALTA",    "Standing valve leak — pérdida de carga en carrera descendente."),
        ("Anclaje Deficiente",      "MEDIA",   "Tubing movement — elongación excesiva por falta de ancla."),
        ("Varilla Partida",         "CRITICA", "Rod parting — pérdida súbita de carga en la carta."),
        ("Fricción Excesiva",       "MEDIA",   "Alta fricción en la sarta de varillas o camisa de bomba."),
    ]

    for nombre, criticidad, descripcion in patrones:
        conn.execute(text("""
            INSERT INTO universal.patron (nombre, criticidad, descripcion)
            VALUES (:nombre, :crit, :desc)
            ON CONFLICT DO NOTHING
        """), {"nombre": nombre, "crit": criticidad, "desc": descripcion})
    
    logger.info(f"  [CDI] {len(patrones)} patrones CDI insertados.")
    return len(patrones)

def populate_universal():
    """Genera datos simulados y los inserta en el esquema Universal."""
    logger.info("Starting Universal Data Simulation...")
    
    with engine.begin() as conn:
        # Get active wells from Master
        wells = conn.execute(text("SELECT well_id, nombre_pozo FROM stage.tbl_pozo_maestra")).fetchall()
        
        if not wells:
            logger.warning("No wells found in stage.tbl_pozo_maestra. Skipping generation.")
            return

        logger.info(f"Found {len(wells)} wells. Generating IPR, ARPS & CDI data...")

        # ─────────────────────────────────────────
        # 0. Seed CDI Patrones (catálogo)
        # ─────────────────────────────────────────
        num_patrones = seed_patrones_cdi(conn)

        for well in wells:
            well_id = well[0]
            well_name = well[1]
            str_well_id = str(well_id)

            # ─────────────────────────────────────
            # 1. IPR Results
            # ─────────────────────────────────────
            pres_res = random.uniform(2000, 4500)
            qmax = random.uniform(500, 3000)
            ip = qmax / pres_res
            
            curva = generate_ipr_curve(qmax, pres_res)

            conn.execute(text("""
                INSERT INTO universal.ipr_resultados 
                (well_id, fecha_calculo, metodo, qmax_bpd, ip_factor, curva_yacimiento, alertas)
                VALUES (:wid, :fecha, 'Vogel', :qmax, :ip, :curva, :alertas)
                ON CONFLICT (well_id, fecha_calculo) DO NOTHING
            """), {
                "wid": well_id,
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