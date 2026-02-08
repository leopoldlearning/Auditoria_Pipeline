import os
import random
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()

# Configuration
DB_USER = os.getenv("DB_USER", "audit")
DB_PASSWORD = os.getenv("DEV_DB_PASSWORD", "audit")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433") 
DB_NAME = os.getenv("DB_NAME", "etl_data")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

print(f"Connecting to: {DATABASE_URL}")
engine = create_engine(DATABASE_URL)

# Simulation Data for Well ID 5
WELL_ID = 5
LOCATION_ID = 1
MODDATE = datetime.now().strftime("%Y-%m-%d")
# MODTIME handled by datetime.now()

# Full mapping from V1__stage_to_stage.sql
SIMULATED_DATA = {
    # Operación y Monitoreo
    727: (10.5, "SPM Average"),
    12058: (10.0, "Request SPM Up"),
    12059: (10.0, "Request SPM Down"),
    11: (3000.0, "FLOP (Fluid Level)"),
    692: (85.5, "Pump Fill Monitor"),
    717: (50.5, "Current HP Motor"),
    12295: (25.0, "Current AMP Motor"),
    12277: (950.0, "Motor RPM"),
    321: (120.0, "Drive Current Time"),
    3: (1, "Motor ON Status"),
    728: (99.0, "S Rod Stroke"),
    714: (100.0, "Unit RTD Stroke"),
    
    # Presiones y Temperaturas
    137: (250.0, "WHP"),
    131: (150.0, "CHP"),
    140: (120.5, "THP"),
    740: (900.0, "PIP"),
    741: (950.0, "Pump Discharge Pressure"),
    12282: (1500.0, "Hyd Cyl Prs"),
    12285: (140.0, "Oil Tank Temp"),
    
    # Producción (Diarios)
    772: (80.5, "Water Cut %"),
    12184: (90.0, "Gas Fill Monitor"),
    284: (225.7, "Daily Fluid Production"),
    1216: (45.2, "Oil Production Daily"),
    1217: (180.5, "Water Production Daily"),
    286: (10.0, "Gas Production Daily"),
    883: (0.0, "Daily Leakage"),
    65: (200.0, "Fluid Flow Monitor BPD"),
    866: (95.0, "Liquid Fill Monitor"),
    
    # Producción (Acumulados)
    13: (50000.0, "Fluid Production Meter"),
    1218: (10000.0, "Accum Oil Production"),
    1219: (40000.0, "Water Production Meter"),
    298: (5000.0, "Gas Production Meter"),
    
    # Cargas y Carreras
    776: (18000.0, "Rod Weight In Air (Inactive)"),
    733: (12000.0, "Rod Weight Buoyant"),
    793: (15000.0, "Pump Load Monitor"),
    917: (22000.0, "API Max Fluid Load (Inactive)"),
    715: (20000.0, "Max Rod Load"),
    716: (5000.0, "Min Rod Load"),
    1135: (65.0, "Gearbox Load %"),
    12296: (100.0, "API Pump Stroke"),
    
    # Eficiencia y POC
    282: (2.5, "Kwh/Bbl"),
    293: (98.5, "Daily Run Percent"),
    292: (0.5, "Daily POC Downtime"),
    291: (2, "Daily POC Count"),
    8: (0.85, "Lift Efficiency"),
    
    # Sensores y Otros
    766: (5000.0, "Anchor Vertical Depth"),
    12283: (0.0, "Stem Tilt"),
    12279: (0.1, "Cylinder Tilt X"),
    12280: (0.1, "Cylinder Tilt Y"),
    12268: (5.0, "Cyl Tilt Warn Deg"),
    12269: (10.0, "Cyl Tilt Fault Deg"),
    12281: (50.0, "Linear Pos"),
    
    # Acumuladores
    694: (500, "Pump Stroke Counter"),
    1206: (100.5, "Cumulative Run Hours"),
    294: (2400.0, "Gauge Run Time Accum"),
    934: (50000.0, "Gauge Power Meter Accum"),
    299: (10000, "Gauge Strokes Accum"),
    289: (1000, "Daily Strokes"),
    290: (90.0, "Daily Avg Fill"),
    283: (150.0, "Gauge Power Meter Daily"),
    898: (50, "POC Powerup Strokes"),
    899: (10, "POC Standby Strokes"),
    896: (500, "Gauge POC Count Accum"),
    1188: (10.0, "Gauge POC Downtime Accum"),
    288: (10.0, "Daily Gauge Avg SPM"),
    
    # Dinamómetro (Textos)
    10000: ("ArrayData...", "Surface Rod Position"),
    10001: ("ArrayData...", "Surface Rod Load"),
    10002: ("ArrayData...", "Downhole Pump Position"),
    10003: ("ArrayData...", "Downhole Pump Load"),
    
    # Miscelaneos
    10: (3000.0, "Fluid Level TVD")
}

def simulate_ingestion():
    try:
        with engine.connect() as connection:
            print("Cleaning existing stage data for this well...")
            connection.execute(text("DELETE FROM stage.landing_scada_data WHERE unit_id = :uid"), {"uid": WELL_ID})
            
            print(f"Inserting {len(SIMULATED_DATA)} simulated records for Well {WELL_ID}...")
            
            insert_query = text("""
                INSERT INTO stage.landing_scada_data (
                    unit_id, location_id, var_id, measure, moddate, modtime
                ) VALUES (
                    :unit_id, :location_id, :var_id, :measure, :moddate, :modtime
                )
            """)
            
            for var_id, (value, desc) in SIMULATED_DATA.items():
                connection.execute(insert_query, {
                    "unit_id": WELL_ID,
                    "location_id": LOCATION_ID,
                    "var_id": var_id,
                    "measure": str(value),
                    "moddate": datetime.now(),
                    "modtime": datetime.now()
                })
            
            connection.commit()
            print("✅ Simulation completed successfully.")
            
    except Exception as e:
        print(f"❌ Error during simulation: {e}")

if __name__ == "__main__":
    simulate_ingestion()
