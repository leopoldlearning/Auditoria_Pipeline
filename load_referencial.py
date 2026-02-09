#!/usr/bin/env python3
import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import importlib.util

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
DB_URL = os.getenv('DATABASE_URL')
if not DB_URL:
    DB_URL = "postgresql://audit:audit@localhost:5433/etl_data"

engine = create_engine(DB_URL)

# PATHS
CSV_ID_TRUTH = "data/Variables_ID_stage.csv"
CSV_REGLAS = "inputs_referencial/02_reglas_calidad.csv"
CSV_VALIDACION = "data/hoja_validacion.csv"
CSV_UNIDADES = "inputs_referencial/05_unidades.csv"
PATH_V1_STAGE = "src/sql/process/V1__stage_to_stage.sql"
PATH_RANGOS = "inputs_referencial/Rangos_validacion_variables_petroleras_limpio.py"

def load_maestra_and_metadata():
    print(">>> Carga Maestra de Variables (STRICT IDs) y Metadatos...")
    
    df_truth = pd.read_csv(CSV_ID_TRUTH, sep=';', encoding='utf-8', on_bad_lines='skip')
    df_truth['ID_raw'] = df_truth['ID'].astype(str).str.strip()
    df_strict = df_truth[df_truth['ID_raw'].str.match(r'^\d+$')].copy()
    df_strict['ID_clean'] = df_strict['ID_raw'].astype(int)
    df_strict = df_strict.drop_duplicates(subset=['ID_clean'], keep='first')
    
    try:
        df_units_cat = pd.read_csv(CSV_UNIDADES, sep=';', encoding='latin-1')
    except:
        df_units_cat = pd.read_csv(CSV_UNIDADES, sep=';', encoding='utf-8', errors='replace')
        
    units_map = {}
    id_col = next((c for c in df_units_cat.columns if 'ID_formato1vfinal' in c), None)
    unit_col = next((c for c in df_units_cat.columns if 'Stage: Unidad' in c), None)
    
    if id_col and unit_col:
        for _, row in df_units_cat.iterrows():
            id_val = str(row[id_col]).strip().split('.')[0]
            unit = str(row[unit_col]).strip()
            if id_val.isdigit() and unit not in ['nan', '', 'N/A', 'lista']:
                units_map[int(id_val)] = unit

    df_val = pd.read_csv(CSV_VALIDACION, sep=';', encoding='utf-8')
    col_panel = next(c for c in df_val.columns if 'Panel(es) de Uso' in c)
    col_ident = next(c for c in df_val.columns if 'Ident Dashboard Element' in c)
    col_id = next(c for c in df_val.columns if 'ID asociado' in c or 'ID_formato1' in c or 'ID' in c)
    
    panel_map = {}
    for _, row in df_val.iterrows():
        id_ref_str = str(row[col_id]).strip().replace('.0', '').replace('*', '')
        if id_ref_str.isdigit():
            id_ref = int(id_ref_str)
            panels = str(row[col_panel])
            ident = str(row[col_ident]).strip().strip('"')
            chosen_panel = "Transversal" if 'transversal' in panels.lower() else panels.split('/')[-1].strip()
            if id_ref not in panel_map or chosen_panel == "Transversal":
                panel_map[id_ref] = {'panel': chosen_panel, 'ident': ident if ident != 'nan' else None}

    cat_map = {
        'Identificación y Ubicación': 'DISEÑO', 'Completación y Geología': 'YACIMIENTO',
        'Presiones y Fluidos': 'YACIMIENTO', 'Equipos (Nominales)': 'DISEÑO',
        'Operación y Monitoreo del Equipo': 'SENSOR', 'Sensores y Otros': 'SENSOR',
        'Cargas y Carreras': 'SENSOR', 'Tarjetas de Dinamómetro': 'SENSOR',
        'Producción y Fluidos (Diarios)': 'KPI', 'Indicadores de Eficiencia y POC': 'KPI',
        'Acumuladores y Contadores': 'KPI', 'Propiedades Dinámicas/Calculadas': 'YACIMIENTO'
    }

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE referencial.tbl_ref_estados_operativos RESTART IDENTITY CASCADE;"))
        states = [
            ('NORMAL', '#00C851', 'Operación Estable', 5, 0, 'check_circle'),
            ('WARNING', '#FFBB33', 'Fuera de rango leve', 3, 1, 'warning'),
            ('CRITICAL', '#FF4444', 'Falla Crítica / Paro', 1, 3, 'error'),
            ('OFFLINE', '#616161', 'Sin Comunicación', 4, -1, 'wifi_off'),
            ('UNKNOWN', '#33B5E5', 'Mantenimiento / Sin Dato', 2, 0, 'help_outline')
        ]
        for s in states:
            try:
                conn.execute(text("INSERT INTO referencial.tbl_ref_estados_operativos (codigo_estado, color_hex, descripcion, prioridad_visual, nivel_severidad, icono_web) VALUES (:c, :col, :d, :p, :s, :i)"), {"c":s[0], "col":s[1], "d":s[2], "p":s[3], "s":s[4], "i":s[5]})
            except Exception as e:
                logger.error(f"Error cargando estado {s[0]}: {e}")
                raise

        conn.execute(text("TRUNCATE TABLE referencial.tbl_ref_paneles_bi RESTART IDENTITY CASCADE;"))
        for p in sorted(set(p['panel'] for p in panel_map.values() if p['panel']) | {"Transversal", "Production", "Hydralift T4 Surface Operations", "Business KPIs"}):
            conn.execute(text("INSERT INTO referencial.tbl_ref_paneles_bi (nombre_panel) VALUES (:p)"), {"p": p})

        conn.execute(text("TRUNCATE TABLE referencial.tbl_ref_unidades RESTART IDENTITY CASCADE;"))
        for u in sorted(set(units_map.values())): 
            conn.execute(text("INSERT INTO referencial.tbl_ref_unidades (simbolo, descripcion) VALUES (:u, :u) ON CONFLICT DO NOTHING"), {"u": u})

        conn.execute(text("TRUNCATE TABLE referencial.tbl_maestra_variables RESTART IDENTITY CASCADE;"))
        for _, row in df_strict.iterrows():
            try:
                id_val = int(row['ID_clean'])
                tech_name = str(row['Nombre_Variable']).strip()
                cat = cat_map.get(row['Categoria_Clasificacion'], 'SENSOR')
                p_info = panel_map.get(id_val, {'panel': None, 'ident': None})
                p_id = conn.execute(text("SELECT panel_id FROM referencial.tbl_ref_paneles_bi WHERE nombre_panel = :p"), {"p": p_info['panel']}).scalar() if p_info['panel'] else None
                u_id = conn.execute(text("SELECT unidad_id FROM referencial.tbl_ref_unidades WHERE simbolo = :u"), {"u": units_map.get(id_val)}).scalar() if id_val in units_map else None
                conn.execute(text("INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, clasificacion_logica, panel_id, ident_dashboard_element, unidad_id) VALUES (:id, :name, :cat, :pid, :ident, :uid)"), {"id": id_val, "name": tech_name, "cat": cat, "pid": p_id, "ident": p_info['ident'], "uid": u_id})
            except Exception as e:
                logger.error(f"Error cargando variable {row.get('Nombre_Variable', id_val)}: {e}")
                raise

def load_rules():
    print(">>> Carga de Reglas DQ y RC...")
    SI_MAP = {'Damage Factor': 161, 'Equivalent Radius': 159}
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE referencial.tbl_dq_rules RESTART IDENTITY CASCADE;"))
        df_reglas = pd.read_csv(CSV_REGLAS, sep=';', encoding='utf-8').iloc[:35]
        for _, row in df_reglas.iterrows():
            id_f1_raw = str(row['ID_FORMATO_1']).strip().replace('*', '')
            original_name = str(row['Nombre columna  de variable original']).strip()
            lookup_id = id_f1_raw if id_f1_raw.isdigit() else SI_MAP.get(original_name)
            if lookup_id:
                v_id = conn.execute(text("SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = :id"), {"id": int(lookup_id)}).scalar()
                if v_id: conn.execute(text("INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, severidad, origen_regla) VALUES (:v, 0.0001, 'WARNING', '02_reglas_calidad.csv')"), {"v": v_id})

        conn.execute(text("TRUNCATE TABLE referencial.tbl_reglas_consistencia RESTART IDENTITY CASCADE;"))
        rcs = [
            ('RC-001', 'Carga Max > Min', 'max_rod_load_lb_act', '>', 'min_rod_load_lb_act'), 
            ('RC-002', 'Carga Max > Peso Sarta', 'max_rod_load_lb_act', '>', 'rod_weight_buoyant_lb_act'), 
            ('RC-003', 'Presión Cabezal < Fondo', 'well_head_pressure_psi_act', '<', 'flowing_bottom_hole_pressure_psi'), 
            ('RC-004', 'Presión Fondo < Estática', 'flowing_bottom_hole_pressure_psi', '<', 'presion_estatica_yacimiento'), 
            ('RC-005', 'Prof. Bomba < Prof. Vertical', 'profundidad_vertical_bomba', '<', 'profundidad_vertical_yacimiento'), 
            ('RC-006', 'Díametro Émbolo < Radio Pozo', 'diametro_embolo_bomba', '<', 'radio_pozo')
        ]
        for code, desc, v1, op, v2 in rcs:
            conn.execute(text("""
                INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, variable_a_id, operador, variable_b_id) 
                SELECT 
                    :c, 
                    :d,
                    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = :v1 LIMIT 1), 
                    :op, 
                    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = :v2 LIMIT 1)
            """), {"c": code, "d": desc, "v1": v1, "op": op, "v2": v2})

def load_limites_pozo():
    print(">>> Carga de Límites Operativos (Strict matching with Rangos)...")
    if not os.path.exists(PATH_RANGOS): return
    with open(PATH_RANGOS, "r", encoding="utf-8") as f: content = f.read()

    # Extract ID and following technical name block from the Rangos file
    entries = re.finditer(r"#\s*ID:\s*(\d+).*?'([a-z0-9_]+)':\s*\{", content, re.DOTALL | re.I)
    
    spec = importlib.util.spec_from_file_location("variables_petroleras", PATH_RANGOS)
    vp_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(vp_module)
    variables_petroleras = getattr(vp_module, 'VARIABLES_PETROLERAS', {})

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE referencial.tbl_limites_pozo RESTART IDENTITY CASCADE;"))
        # pozo_id = 1 will represent the default well in V4 local audit
        pozo_id = 1 
        loaded_count = 0
        
        for m in entries:
            id_val = int(m.group(1))
            key_name = m.group(2)
            if key_name in variables_petroleras:
                data = variables_petroleras[key_name]
                # Match by ID to the Maestra
                v_id = conn.execute(text("SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = :id"), {"id": id_val}).scalar()
                if v_id:
                    conn.execute(text("""
                        INSERT INTO referencial.tbl_limites_pozo (pozo_id, variable_id, min_warning, max_warning, target_value)
                        VALUES (:pid, :vid, :mi, :ma, :t)
                        ON CONFLICT (pozo_id, variable_id) DO NOTHING
                    """), {"pid": pozo_id, "vid": v_id, "mi": data.get('Rango_Min'), "ma": data.get('Rango_Max'), "t": data.get('ejemplo')})
                    loaded_count += 1
        print(f"    Límites cargados: {loaded_count}")

def build_scada_map():
    print(">>> Sincronización de Mapa SCADA...")
    if not os.path.exists(PATH_V1_STAGE): return
    with open(PATH_V1_STAGE, "r", encoding="utf-8") as f: content = f.read()
    
    # Robust regex for ID/IDN mapping in V1 script, specifically targeting the alias after END)
    matches = re.finditer(r"--\s*ID:\s*(\d+)\s*\|\s*IDN:\s*(NULL|\w+)\s*\|\s*(.*?)\n\s*(?:MAX|BOOL_OR)\(CASE\s*WHEN\s*l\.var_id\s*=\s*(\d+)\s*THEN.*?END\)\s*AS\s*(\w+)", content, re.I | re.S)
    
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE referencial.tbl_var_scada_map RESTART IDENTITY CASCADE;"))
        for m in matches:
            id_fmt, idn_str, desc, var_id, col = m.groups()
            if idn_str.upper() != 'NULL':
                if len(col) < 2:
                    logger.warning(f"⚠️ Posible truncación detectada: ID {id_fmt} -> '{col}'")
                conn.execute(text("INSERT INTO referencial.tbl_var_scada_map (var_id_scada, id_formato1, columna_stage, comentario) VALUES (:vid, :id_f, :col, :desc) ON CONFLICT (var_id_scada) DO NOTHING"), {"vid": int(var_id), "id_f": int(id_fmt), "col": col, "desc": desc.strip()})

if __name__ == "__main__":
    load_maestra_and_metadata()
    load_rules()
    load_limites_pozo()
    build_scada_map()
    print("=== REFERENCIAL V4 (STRICT & INTEGRATED) CARGADO CORRECTAMENTE ===")
