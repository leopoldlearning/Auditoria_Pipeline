
import pandas as pd
import numpy as np

# FILES
CSV_ID_TRUTH = "data/Variables_ID_stage.csv"
CSV_REGLAS = "inputs_referencial/02_reglas_calidad.csv"
CSV_VALIDACION = "data/hoja_validacion.csv"

# 1. LOAD TRUTH MAPPING (ID -> Tech_Name)
df_truth = pd.read_csv(CSV_ID_TRUTH, sep=';', encoding='utf-8', on_bad_lines='skip')
# Clean ID column (strip spaces, remove *)
df_truth['ID_clean'] = df_truth['ID'].astype(str).str.replace('*', '').str.strip()

# Build Mapping (ID -> {name, category})
truth_map = {}
for _, row in df_truth.iterrows():
    id_val = str(row['ID_clean'])
    name = str(row['Nombre_Variable']).strip()
    cat = str(row['Categoria_Clasificacion']).strip()
    if id_val != 'nan' and id_val != '':
        if id_val not in truth_map:
            truth_map[id_val] = {'name': name, 'cat': cat}

def slugify(s):
    import re
    s = s.lower().strip()
    s = re.sub(r'[^\w\s]', '', s) # Remove parens, etc.
    s = re.sub(r'\s+', '_', s)    # Spaces to _
    return s

# 2. LOAD REGLAS
df_reglas = pd.read_csv(CSV_REGLAS, sep=';', encoding='utf-8')
df_reglas = df_reglas.iloc[:35]

# 3. LOAD VALIDACION (For Panels)
df_val = pd.read_csv(CSV_VALIDACION, sep=';', encoding='utf-8')
COL_VAL_PANEL = next(c for c in df_val.columns if c.strip().startswith('Panel(es) de Uso'))
COL_VAL_IDENT = next(c for c in df_val.columns if c.strip().startswith('Ident Dashboard Element'))
COL_VAL_ID = next(c for c in df_val.columns if c.strip().startswith('ID asociado'))

panel_map = {}
for _, row in df_val.iterrows():
    id_ref = str(row[COL_VAL_ID]).strip().replace('.0', '').replace('*', '')
    if id_ref and id_ref != 'nan' and id_ref != '-':
        panels = str(row[COL_VAL_PANEL])
        ident = str(row[COL_VAL_IDENT]).strip().strip('"')
        
        chosen_panel = None
        if 'transversal' in panels.lower():
            chosen_panel = "Transversal"
        elif panels != 'nan':
            chosen_panel = panels.replace(' / ', '/').split('/')[-1].strip()
        
        if id_ref not in panel_map or chosen_panel == "Transversal":
            panel_map[id_ref] = {'panel': chosen_panel, 'ident': ident if ident != 'nan' else None}

# 4. GENERATE SQL
sql = []
sql.append("/* CARGA SEMILLA V4 (ITERACIÓN 11) - SINCRONIZADA CON FUENTE DE VERDAD */")
sql.append("TRUNCATE TABLE referencial.tbl_ref_estados_operativos RESTART IDENTITY CASCADE;")
sql.append("INSERT INTO referencial.tbl_ref_estados_operativos (codigo_estado, color_hex, descripcion, nivel_severidad, prioridad_visual) VALUES")
sql.append("('NORMAL', '#00C851', 'Estable', 0, 5), ('WARNING', '#FFBB33', 'Advertencia', 1, 3), ('CRITICAL', '#FF4444', 'Crítico', 3, 1);")

panels_set = {"Transversal", "Production", "Hydralift T4 Surface Operations", "Business KPIs"}
for entry in panel_map.values():
    if entry['panel']: panels_set.add(entry['panel'])

sql.append("\nTRUNCATE TABLE referencial.tbl_ref_paneles_bi RESTART IDENTITY CASCADE;")
for p in sorted(list(panels_set)):
    sql.append(f"INSERT INTO referencial.tbl_ref_paneles_bi (nombre_panel) VALUES ('{p}');")

sql.append("\nTRUNCATE TABLE referencial.tbl_maestra_variables RESTART IDENTITY CASCADE;")

CATEGORY_MAP = {
    'Identificación y Ubicación': 'DISEÑO',
    'Completación y Geología': 'YACIMIENTO',
    'Presiones y Fluidos': 'YACIMIENTO',
    'Equipos (Nominales)': 'DISEÑO',
    'Operación y Monitoreo del Equipo': 'SENSOR',
    'Sensores y Otros': 'SENSOR',
    'Cargas y Carreras': 'SENSOR',
    'Tarjetas de Dinamómetro': 'SENSOR',
    'Producción y Fluidos (Diarios)': 'KPI',
    'Indicadores de Eficiencia y POC': 'KPI',
    'Acumuladores y Contadores': 'KPI',
    'Propiedades Dinámicas/Calculadas': 'YACIMIENTO'
}

SI_MAP = {
    'Damage Factor': '161',
    'Equivalent Radius': '159'
}

for _, row in df_reglas.iterrows():
    id_f1 = str(row['ID_FORMATO_1']).strip().replace('*', '')
    original_name = str(row['Nombre columna  de variable original']).strip()
    
    lookup_id = id_f1
    if lookup_id == 'S/I' and original_name in SI_MAP:
        lookup_id = SI_MAP[original_name]
    
    info = truth_map.get(lookup_id, {'name': original_name, 'cat': 'General'})
    tech_name = slugify(info['name'])
    
    # Priority for Transversal in Panel
    p_info = panel_map.get(lookup_id, {'panel': None, 'ident': None})
    panel_name = p_info['panel']
    ident_element = p_info['ident']
    
    panel_sql = "NULL"
    if panel_name:
        panel_sql = f"(SELECT panel_id FROM referencial.tbl_ref_paneles_bi WHERE nombre_panel = '{panel_name}' LIMIT 1)"
    
    ident_sql = f"'{ident_element}'" if ident_element else "NULL"
    id_f1_sql = id_f1 if id_f1 != 'S/I' else "NULL"
    
    cat_final = CATEGORY_MAP.get(info['cat'], 'SENSOR')

    sql.append(f"INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1, panel_id, ident_dashboard_element)")
    sql.append(f"VALUES ('{tech_name}', '{cat_final}', {id_f1_sql}, {panel_sql}, {ident_sql});")

# DQ & RC rules logic remains similar as it is dynamic based on nombre_tecnico
sql.append("\nTRUNCATE TABLE referencial.tbl_dq_rules RESTART IDENTITY CASCADE;")
sql.append("INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)")
sql.append("SELECT variable_id, 0.0001, NULL, 5, 'WARNING', 'Alineado a 02_reglas_calidad.csv' FROM referencial.tbl_maestra_variables WHERE nombre_tecnico NOT IN ('porcentaje_agua', 'pump_fill_monitor_pct');")
sql.append("INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)")
sql.append("SELECT variable_id, 0.0, 100.0, 5, 'WARNING', 'Alineado a 02_reglas_calidad.csv' FROM referencial.tbl_maestra_variables WHERE nombre_tecnico IN ('porcentaje_agua', 'pump_fill_monitor_pct');")

sql.append("\nTRUNCATE TABLE referencial.tbl_reglas_consistencia RESTART IDENTITY CASCADE;")
# Define RC from 02_reglas_calidad mappings
rc_logic = [
    ('RC-001', 'Carga Max > Min', 'max_rod_load_lb_act', '>', 'min_rod_load_lb_act'),
    ('RC-002', 'Carga Max > Peso Flotante', 'max_rod_load_lb_act', '>', 'rod_weight_buoyant_lb_act'),
    ('RC-003', 'Gradiente Presión', 'well_head_pressure_psi_act', '<', 'flowing_bottom_hole_pressure_psi'), # Fixed direction
    ('RC-004', 'Inflow', 'flowing_bottom_hole_pressure_psi', '<', 'presion_estatica_yacimiento'),
    ('RC-005', 'Verticality', 'profundidad_vertical_bomba', '<', 'profundidad_vertical_yacimiento'),
    ('RC-006', 'Radiality', 'wellbore_radius_ft', '<', 'drainage_radius_ft')
]
for code, desc, v_a, op, v_b in rc_logic:
    sql.append(f"INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, variable_a_id, operador, variable_b_id)")
    sql.append(f"SELECT '{code}', '{desc}', (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = '{v_a}'), '{op}', (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = '{v_b}');")

# SAVE
with open("src/sql/schema/V4__referencial_seed_data.sql", "w", encoding='utf-8') as f:
    f.write("\n".join(sql))

print("DONE: Generated V4__referencial_seed_data.sql based on STRICT ID mapping.")
