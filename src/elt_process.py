import os
import sqlalchemy
from sqlalchemy import create_engine, text, insert
from sqlalchemy.engine import URL

# --- 1. Configuración de las Conexiones ---
# Lee las URLs de las variables de entorno
# Ej: "postgresql://user:pass@host-raw:5432/db"
RAW_DB_URL = os.environ.get("RAW_DB_URL") 
# Ej: "postgresql://testuser:testpassword@localhost:5432/elt_test_db"
STAGE_DB_URL = os.environ.get("DATABASE_URL")

# Crea ambos motores
engine_raw = create_engine(RAW_DB_URL)
engine_stage = create_engine(STAGE_DB_URL)

# Define el tamaño del lote. 1000 es un buen punto de partida.
CHUNK_SIZE = 1000

def run_extraction_load():
    """
    Extrae datos de Raw y los carga en la tabla landing de Stage 
    usando streaming por bloques.
    """
    
    # Idealmente, obtén este valor de una tabla de log/watermark
    last_watermark = "2023-11-29 00:00:00" 
    
    query_raw = text(
        """
        SELECT idn, unit_id, location_id, var_id, measure, 
               datatime, createuser, craetedate, moduser, moddate
        FROM scada_raw_table 
        WHERE datatime > :last_watermark
        ORDER BY datatime ASC
        """
    ).bindparams(last_watermark=last_watermark)
    
    # Obtenemos la definición de la tabla de destino
    metadata_stage = sqlalchemy.MetaData()
    landing_table = sqlalchemy.Table(
        "landing_scada_data", 
        metadata_stage, 
        autoload_with=engine_stage
    )

    try:
        # --- 2. Abre ambas conexiones ---
        with engine_raw.connect() as conn_raw, engine_stage.connect() as conn_stage:
            
            # Inicia una transacción en el destino (Stage)
            trans_stage = conn_stage.begin() 
            
            print("Iniciando extracción por streaming desde RAW...")
            
            # --- 3. Streaming de Lectura (Extracción) ---
            # stream_results=True usa un cursor de servidor.
            # ¡Esto es súper eficiente en memoria!
            result_stream = conn_raw.execute(query_raw, execution_options={"stream_results": True})
            
            total_rows = 0
            
            while True:
                # --- 4. Carga en Bloques (Carga) ---
                
                # Obtiene un bloque de N filas del cursor
                chunk = result_stream.fetchmany(CHUNK_SIZE)
                
                if not chunk:
                    # No hay más filas, hemos terminado.
                    print("Extracción finalizada.")
                    break
                
                # Mapea las filas a diccionarios para la inserción
                # (SQLAlchemy 2.0+ con RowMapping)
                data_to_insert = [row._asdict() for row in chunk]
                
                # Ejecuta el INSERT del bloque
                conn_stage.execute(
                    insert(landing_table),
                    data_to_insert
                )
                
                total_rows += len(data_to_insert)
                print(f"Cargadas {total_rows} filas en landing_scada_data...")

            # Si todo salió bien, confirma la transacción en Stage
            trans_stage.commit()
            print(f"¡Éxito! Total de {total_rows} filas transferidas a Stage.")
            
            # Aquí deberías actualizar tu watermark
            
    except Exception as e:
        print(f"Error durante el proceso EL: {e}")
        # Si algo falla, revierte la transacción de Stage
        if 'trans_stage' in locals() and trans_stage:
            trans_stage.rollback()
        raise

# --- Fin del script ---