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
CSV_UNIDADES_STD = "inputs_referencial/06_unidades_standar.csv"
PATH_V1_STAGE = "src/sql/process/_archive/V1__stage_to_stage.sql"
PATH_RANGOS = "inputs_referencial/Rangos_validacion_variables_petroleras_limpio.py"

def load_maestra_and_metadata():
    print(">>> Carga Maestra de Variables (STRICT IDs) y Metadatos...")
    
    df_truth = pd.read_csv(CSV_ID_TRUTH, sep=';', encoding='utf-8', on_bad_lines='skip')
    df_truth['ID_raw'] = df_truth['ID'].astype(str).str.strip()
    df_strict = df_truth[df_truth['ID_raw'].str.match(r'^\d+$')].copy()
    df_strict['ID_clean'] = df_strict['ID_raw'].astype(int)
    df_strict = df_strict.drop_duplicates(subset=['ID_clean'], keep='first')
    
    # ── Mapa de normalización: unidad cruda del CSV-05 → abreviatura estándar del CSV-06 ──
    # Construido manualmente cruzando las 44 variantes del 05 con las 39 del 06
    UNIT_NORMALIZE = {
        # Presiones
        'PSI': 'psi', 'Psi': 'psi', 'psi': 'psi',
        # Longitudes
        'Pies': 'ft', 'Pie': 'ft', 'ft': 'ft', 'In': 'in', 'in': 'in', 'inches': 'in',
        # Temperaturas
        'F': '°F',
        # Producción volumétrica
        'bpd': 'bpd', 'Barriles/día o BPD': 'bpd', 'Barriles/dÃ\xada o BPD': 'bpd', 'bbl': 'bbl', 'bl': 'bbl', 'BLPD': 'BLPD',
        'scf/D': 'scf/d', 'scf': 'scf', 'mcf': 'mcf',
        # Velocidad/frecuencia rotacional
        'SPM': 'spm', 'RPM': 'rpm', 'Hz': 'Hz',
        # Fuerzas y cargas
        'Lb': 'lbf',
        # Potencia y energía
        'HP': 'hp', 'Hp': 'hp',
        'kwh': 'kWh', 'kwh/bl': 'kWh/bbl', 'kwh/bbl': 'kWh/bbl',
        # Viscosidad y permeabilidad
        'Centipoises': 'cP', 'cP': 'cP',
        'mili Darcy': 'mD',
        # Eléctricos
        'A': 'A', 'AMP': 'A', 'VAC': 'V',
        # Proporcionales
        '%': '%', 'Fracion (0-1)': '1',
        'Adimensional': '-', '°API o Adimensional': '°API',
        'Â°API o Adimensional': '°API',  # encoding latin-1 variant
        # Concentración
        'mg/L': 'mg/L',
        # Contadores
        'num': 'num', 'strokes': 'num', 'Strokes/D': 'strokes/d',
        # Volumen unitario
        'By/BN': '1',  # Factor volumétrico es adimensional
        # Tiempo
        'hh.mm': 'h',
        # Coordenadas
        'Grados, min, seg': '°', 'Grados?': '°',
    }

    # ── Cargar catálogo estándar de unidades desde 06_unidades_standar.csv ──
    df_std_units = pd.read_csv(CSV_UNIDADES_STD, sep=';', encoding='utf-8')
    std_units = {}  # abreviatura → nombre
    for _, row in df_std_units.iterrows():
        nombre = str(row.iloc[0]).strip()
        abrev = str(row.iloc[1]).strip()
        if nombre and abrev and nombre != 'nan':
            std_units[abrev] = nombre

    try:
        df_units_cat = pd.read_csv(CSV_UNIDADES, sep=';', encoding='latin-1')
    except:
        df_units_cat = pd.read_csv(CSV_UNIDADES, sep=';', encoding='utf-8', errors='replace')
        
    units_map = {}  # id_formato1 → abreviatura estándar
    id_col = next((c for c in df_units_cat.columns if 'ID_formato1vfinal' in c), None)
    unit_col = next((c for c in df_units_cat.columns if 'Stage: Unidad' in c), None)
    
    if id_col and unit_col:
        for _, row in df_units_cat.iterrows():
            id_val = str(row[id_col]).strip().split('.')[0]
            unit_raw = str(row[unit_col]).strip()
            if id_val.isdigit() and unit_raw not in ['nan', '', 'N/A', 'lista', 'Segun formato VSD', 'Formato Drive']:
                std_symbol = UNIT_NORMALIZE.get(unit_raw)
                if std_symbol:
                    units_map[int(id_val)] = std_symbol
                else:
                    logger.warning(f"⚠️ Unidad no normalizada: '{unit_raw}' (id_formato1={id_val})")
                    units_map[int(id_val)] = unit_raw  # fallback

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
        # Insertar unidades estándar desde 06_unidades_standar.csv
        for abrev, nombre in sorted(std_units.items()):
            conn.execute(text(
                "INSERT INTO referencial.tbl_ref_unidades (simbolo, nombre, descripcion) "
                "VALUES (:s, :n, :n) ON CONFLICT DO NOTHING"
            ), {"s": abrev, "n": nombre})
        # Agregar unidades usadas en units_map que no están en el estándar (fallback)
        for std_sym in sorted(set(units_map.values())):
            if std_sym not in std_units:
                conn.execute(text(
                    "INSERT INTO referencial.tbl_ref_unidades (simbolo, nombre, descripcion) "
                    "VALUES (:s, :s, 'No estandarizada') ON CONFLICT DO NOTHING"
                ), {"s": std_sym})

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
    # id_formato1 excluidos del mapeo de consistencia (sin RC válido en CSV)
    EXCLUDE_CONSISTENCIA = {155}
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE referencial.tbl_dq_rules RESTART IDENTITY CASCADE;"))
        df_reglas = pd.read_csv(CSV_REGLAS, sep=';', encoding='utf-8').iloc[:35]

        # ── Paso 1: Insertar DQ rules con parsing correcto de Representatividad y Latencia ──
        for _, row in df_reglas.iterrows():
            id_f1_raw = str(row['ID_FORMATO_1']).strip().replace('*', '')
            original_name = str(row['Nombre columna  de variable original']).strip()
            lookup_id = id_f1_raw if id_f1_raw.isdigit() else SI_MAP.get(original_name)
            if not lookup_id:
                continue
            v_id = conn.execute(text("SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = :id"), {"id": int(lookup_id)}).scalar()
            if not v_id:
                continue

            # ── Parsear Representatividad: ">0" → min=0.0001 | "0-100%" → min=0, max=100 ──
            repres = str(row.get('Reglas de Calidad: Representatividad', '')).strip()
            if repres == '0-100%':
                v_min, v_max = 0.0, 100.0
            elif repres.startswith('>'):
                v_min, v_max = 0.0001, None
            else:
                v_min, v_max = 0.0001, None  # fallback

            # ── Parsear Latencia: "< 2 s" → 2 segundos ──
            latencia_raw = str(row.get('Reglas de Calidad: Latencia', '')).strip()
            latencia_seg = 300  # default
            lat_match = re.search(r'<\s*(\d+)', latencia_raw)
            if lat_match:
                latencia_seg = int(lat_match.group(1))

            conn.execute(text("""
                INSERT INTO referencial.tbl_dq_rules 
                    (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) 
                VALUES (:v, :vmin, :vmax, :lat, 'WARNING', '02_reglas_calidad.csv')
            """), {"v": v_id, "vmin": v_min, "vmax": v_max, "lat": latencia_seg})

        # ── Paso 2: Enriquecer valor_max desde tbl_limites_pozo (solo donde no hay max del CSV) ──
        updated = conn.execute(text("""
            UPDATE referencial.tbl_dq_rules r
            SET valor_max = lp.max_warning
            FROM referencial.tbl_limites_pozo lp
            WHERE r.variable_id = lp.variable_id
              AND lp.max_warning IS NOT NULL
              AND lp.max_warning > 0
              AND r.valor_max IS NULL
        """))
        print(f"    DQ reglas: valor_max actualizado desde tbl_limites_pozo: {updated.rowcount} filas")

        # ── Paso 3: Cargar Reglas de Consistencia (RC-001..RC-006) ──
        conn.execute(text("TRUNCATE TABLE referencial.tbl_reglas_consistencia RESTART IDENTITY CASCADE;"))
        rcs = [
            ('RC-001', 'Validación Cargas de Varilla', 'CARGAS', 'max_rod_load_lb_act', '>', 'min_rod_load_lb_act', 'CRITICAL', 'La carga máxima debe ser mayor a la mínima durante el ciclo'),
            ('RC-002', 'Carga Máxima vs Peso Sarta', 'CARGAS', 'max_rod_load_lb_act', '>', 'rod_weight_buoyant_lb_act', 'HIGH', 'La carga máxima debe superar el peso flotante de la sarta'),
            ('RC-003', 'Gradiente Presión Vertical', 'PRESIONES', 'well_head_pressure_psi_act', '<', 'presion_fondo_fluyente_critico', 'CRITICAL', 'Presión cabezal menor a presión de fondo'),
            ('RC-004', 'Validación Inflow Performance', 'PRESIONES', 'presion_fondo_fluyente_critico', '<', 'presion_estatica_yacimiento', 'HIGH', 'Presión fondo debe ser menor a presión estática'),
            ('RC-005', 'Profundidad Bomba vs Yacimiento', 'GEOMETRIA', 'profundidad_vertical_bomba', '<', 'profundidad_vertical_yacimiento', 'MEDIUM', 'Bomba no puede estar más profunda que yacimiento'),
            ('RC-006', 'Validación Geometría Radial', 'GEOMETRIA', 'radio_pozo', '<', 'radio_drenaje', 'MEDIUM', 'Radio del pozo debe ser menor al radio de drenaje')
        ]
        for code, nombre, cat, v_med, op, v_ref, sev, desc in rcs:
            conn.execute(text("""
                INSERT INTO referencial.tbl_reglas_consistencia (
                    codigo_regla, nombre_regla, categoria,
                    variable_medida_id, operador_comparacion, variable_referencia_id,
                    severidad, descripcion
                ) 
                SELECT 
                    :c, :n, :cat,
                    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = :v_med LIMIT 1), 
                    :op, 
                    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = :v_ref LIMIT 1),
                    :sev, :desc
            """), {"c": code, "n": nombre, "cat": cat, "v_med": v_med, "op": op, "v_ref": v_ref, "sev": sev, "desc": desc})

        # ── Paso 4: Vincular Variables ↔ Reglas Consistencia desde columna CSV ──
        conn.execute(text("TRUNCATE TABLE referencial.tbl_dq_consistencia_map;"))
        for _, row in df_reglas.iterrows():
            id_f1_raw = str(row['ID_FORMATO_1']).strip().replace('*', '')
            original_name = str(row['Nombre columna  de variable original']).strip()
            lookup_id = id_f1_raw if id_f1_raw.isdigit() else SI_MAP.get(original_name)
            if not lookup_id:
                continue
            lookup_id_int = int(lookup_id)
            if lookup_id_int in EXCLUDE_CONSISTENCIA:
                continue

            consistencia_raw = str(row.get('Reglas de Calidad: Consistencia', '')).strip()
            if not consistencia_raw or consistencia_raw == 'nan':
                continue

            v_id = conn.execute(text("SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = :id"), {"id": lookup_id_int}).scalar()
            if not v_id:
                continue

            # Parsear "RC-003, RC-004" → ['RC-003', 'RC-004']
            rc_codes = [c.strip() for c in consistencia_raw.split(',') if c.strip().startswith('RC-')]
            for rc_code in rc_codes:
                rc_id = conn.execute(text("SELECT regla_id FROM referencial.tbl_reglas_consistencia WHERE codigo_regla = :c"), {"c": rc_code}).scalar()
                if rc_id:
                    conn.execute(text("""
                        INSERT INTO referencial.tbl_dq_consistencia_map (variable_id, regla_consistencia_id) 
                        VALUES (:v, :r)
                        ON CONFLICT DO NOTHING
                    """), {"v": v_id, "r": rc_id})

        # Contar vínculos creados
        map_count = conn.execute(text("SELECT count(*) FROM referencial.tbl_dq_consistencia_map")).scalar()
        print(f"    DQ consistencia map: {map_count} vínculos variable↔RC creados")

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
    load_limites_pozo()       # Antes de load_rules (provee max_warning para valor_max)
    load_rules()
    build_scada_map()
    print("=== REFERENCIAL V4 (STRICT & INTEGRATED) CARGADO CORRECTAMENTE ===")
