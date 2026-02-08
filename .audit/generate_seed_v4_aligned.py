
import pandas as pd
import io

csv_content = """ID_FORMATO_1;ID_INCREMENTAL;Nombre columna  de variable original;Origen;Observación;Unidad;Tipo dato;Reglas de Calidad: Representatividad;Reglas de Calidad: Latencia;Reglas de Calidad: Tolerancia;Reglas de Calidad: Consistencia
23;1;Formation Thickness;Cliente;v2;ft;Fijo;>0;< 2 s;;
S/I;2;Damage Factor ;Cliente;v2;Dimensionless;Semi-Fijo;>0;< 2 s;;
30;3;Formation Volume Factor;Cliente;v2;By/BN;Semi-Fijo;>0;< 2 s;;
75;4;API Maximum Fluid Load;Cliente, SCADA;;Lb;Semi-Fijo;>0;< 2 s;;
63;5;Water specific gravity;Cliente;;Dimensionless;Fijo;>0;< 2 s;;
28;6;Absolute Permeability;Cliente;v2;mD (milidarcys);Semi-Fijo;>0;< 2 s;;
162;7;Vertical Permeability;Cliente;v2;mD (milidarcys);Semi-Fijo;>0;< 2 s;;
28;8;Horizontal Permeability;Cliente;v2;mD (milidarcys);Semi-Fijo;>0;< 2 s;;
57;9;Water cut;Cliente, SCADA;;%;Variable;0-100%;< 2 s;;
24;10;Bubble Point Pressure;Cliente;;psi;Fijo;>0;< 2 s;;
54;11;Well head pressure (WHP);Cliente, SCADA;;psi;Variable;>0;< 2 s;;RC-003
151;12;Flowing Bottom Hole Pressure (FBHP);Cliente, SCADA;;psi;Variable;>0;< 2 s;;RC-003, RC-004
27;13;Critical Flowing Bottom Hole Pressure;Cliente;;psi;Semi-Fijo;>0;< 2 s;;
55;14;Casing head pressure (CHP);Cliente, SCADA;;psi;Semi-Fijo;>0;< 2 s;;
25;15;Reservoir Static Pressure;Cliente;;psi;Semi-Fijo;>0;< 2 s;; RC-004
108;16;Production (BOPD);Cliente, SCADA;;barrels/day;Variable;>0;< 2 s;;
39;17;Pump True Vertical Depth;Cliente;;ft;Fijo;>0;< 2 s;;RC-005
38;18;Vertical Depth of Reservoir;Cliente;;ft;Fijo;>0;< 2 s;;RC-005
S/I;19;Equivalent Radius;Cliente;v2;ft;Fijo;>0;< 2 s;;RC-006
20;20;Drainage Radius;Cliente;v2;ft;Fijo;>0;< 2 s;;RC-006
19;21;Wellbore Radius;Cliente;v2;ft;Fijo;>0;< 2 s;;RC-006
29;22;Crude Oil Viscosity;Cliente;v2;cP (centipoise);Semi-Fijo;>0;< 2 s;;
3;23;Well type;Cliente;;N/A;Fijo;>0;< 2 s;;
160;24;Horizontal Length;Cliente;v2;ft;Fijo;>0;< 2 s;;
59;25;Corrected Dynamic Fluid Level;SCADA;;ft;Semi-Fijo;>0;< 2 s;;
77;26;minimum rod load;SCADA;;lb;Variable;>0;< 2 s;;RC-001
155;27;Surface RodPosition;SCADA;;in;Variable;>0;< 2 s;;
156;28;Surface Rod Load;SCADA;;lb;Variable;>0;< 2 s;;
157;29;Downhole Pump Position;SCADA;;in;Variable;>0;< 2 s;;
158;30;Downhole Pump Load;SCADA;;lb;Variable;>0;< 2 s;;
78;31;tubing anchor depth;SCADA;;ft;Variable;>0;< 2 s;;
64;32;pump fill monitor;SCADA;;%;Variable;0-100%;< 2 s;;
73;33;rod weight buoyant;SCADA;;lb;Variable;>0;< 2 s;; RC-002
76;34;maximum rod load;SCADA;;lb;Variable;>0;< 2 s;;RC-001, RC-002
44;35;motor current;SCADA;;A (ampere);Variable;>0;< 2 s;;"""

df = pd.read_csv(io.StringIO(csv_content), sep=';')

# Mapping English names to our V4 technical names
tech_map = {
    "Formation Thickness": "formation_thickness_ft",
    "Damage Factor ": "damage_factor",
    "Formation Volume Factor": "formation_volume_factor",
    "API Maximum Fluid Load": "max_fluid_load_lb",
    "Water specific gravity": "water_specific_gravity",
    "Absolute Permeability": "absolute_permeability_md",
    "Vertical Permeability": "vertical_permeability_md",
    "Horizontal Permeability": "horizontal_permeability_md",
    "Water cut": "porcentaje_agua",
    "Bubble Point Pressure": "bubble_point_pressure_psi",
    "Well head pressure (WHP)": "well_head_pressure_psi_act",
    "Flowing Bottom Hole Pressure (FBHP)": "flowing_bottom_hole_pressure_psi",
    "Critical Flowing Bottom Hole Pressure": "critical_fbhp_psi",
    "Casing head pressure (CHP)": "casing_head_pressure_psi_act",
    "Reservoir Static Pressure": "presion_estatica_yacimiento",
    "Production (BOPD)": "prod_petroleo_diaria_bpd",
    "Pump True Vertical Depth": "profundidad_vertical_bomba",
    "Vertical Depth of Reservoir": "profundidad_vertical_yacimiento",
    "Equivalent Radius": "equivalent_radius_ft",
    "Drainage Radius": "drainage_radius_ft",
    "Wellbore Radius": "wellbore_radius_ft",
    "Crude Oil Viscosity": "crude_oil_viscosity_cp",
    "Well type": "well_type",
    "Horizontal Length": "horizontal_length_ft",
    "Corrected Dynamic Fluid Level": "corrected_dynamic_fluid_level_ft",
    "minimum rod load": "min_rod_load_lb_act",
    "Surface RodPosition": "well_stroke_position_in",
    "Surface Rod Load": "surface_rod_load_lb",
    "Downhole Pump Position": "downhole_pump_position_in",
    "Downhole Pump Load": "downhole_pump_load_lb",
    "tubing anchor depth": "tubing_anchor_depth_ft",
    "pump fill monitor": "pump_fill_monitor_pct",
    "rod weight buoyant": "rod_weight_buoyant_lb_act",
    "maximum rod load": "max_rod_load_lb_act",
    "motor current": "motor_current_a_act"
}

print("-- SECCIÓN 2: MAESTRA DE VARIABLES (ALINEADA A CSV DQ)")
print("TRUNCATE TABLE referencial.tbl_maestra_variables RESTART IDENTITY CASCADE;")

# Group into blocks for readability
blocks = {
    "BLOQUE A: Sensores & Operación": ["well_head_pressure_psi_act", "casing_head_pressure_psi_act", "flowing_bottom_hole_pressure_psi", "min_rod_load_lb_act", "max_rod_load_lb_act", "motor_current_a_act", "pump_fill_monitor_pct", "well_stroke_position_in", "surface_rod_load_lb", "downhole_pump_position_in", "downhole_pump_load_lb"],
    "BLOQUE B: Yacimiento": ["formation_thickness_ft", "damage_factor", "formation_volume_factor", "water_specific_gravity", "absolute_permeability_md", "vertical_permeability_md", "horizontal_permeability_md", "bubble_point_pressure_psi", "critical_fbhp_psi", "presion_estatica_yacimiento", "crude_oil_viscosity_cp"],
    "BLOQUE C: Diseño & Pozos": ["profundidad_vertical_bomba", "profundidad_vertical_yacimiento", "equivalent_radius_ft", "drainage_radius_ft", "wellbore_radius_ft", "well_type", "horizontal_length_ft", "tubing_anchor_depth_ft", "max_fluid_load_lb"],
    "BLOQUE D: KPIs & Calculados": ["prod_petroleo_diaria_bpd", "porcentaje_agua", "corrected_dynamic_fluid_level_ft"]
}

added_tech = set()

for block_name, tech_list in blocks.items():
    print(f"\n-- {block_name}")
    print("INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES")
    
    values_raw = []
    for tech_name in tech_list:
        # Find ID in CSV
        csv_row = df[df['Nombre columna  de variable original'].apply(lambda x: tech_name in [tech_map.get(x), x])]
        id_val = "NULL"
        if not csv_row.empty:
            rid = csv_row.iloc[0]['ID_FORMATO_1']
            if str(rid).lower() not in ['s/i', 'nan']:
                id_val = rid
        
        # Determine classification
        class_log = "SENSOR"
        if "depth" in tech_name or "length" in tech_name or "radius" in tech_name or "radius" in tech_name or "type" in tech_name or "load" in tech_name and "max" in tech_name:
            class_log = "DISEÑO"
        if "thickness" in tech_name or "perm" in tech_name or "gravity" in tech_name or "visco" in tech_name or "factor" in tech_name or "static" in tech_name:
            class_log = "YACIMIENTO"
        if "prod_" in tech_name or "pct" in tech_name or "agua" in tech_name:
            class_log = "KPI"
        if "act" in tech_name:
            class_log = "SENSOR"
        
        values_raw.append(f"('{tech_name}', '{class_log}', {id_val})")
        added_tech.add(tech_name)
    
    # Check if we missed any from CSV
    print(",\n".join(values_raw) + ";")

print("\n-- REGLAS DE CALIDAD (DQ) - CARGA FILA POR FILA DESDE CSV")
print("TRUNCATE TABLE referencial.tbl_dq_rules RESTART IDENTITY CASCADE;")

for index, row in df.iterrows():
    name = row['Nombre columna  de variable original']
    tech_name = tech_map.get(name)
    if not tech_name: continue
    
    rule_rep = str(row['Reglas de Calidad: Representatividad']).strip()
    min_v = "NULL"
    max_v = "NULL"
    if ">0" in rule_rep:
        min_v = "0.0001"
    elif "0-100%" in rule_rep:
        min_v = "0.0"
        max_v = "100.0"
    
    print(f"INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)")
    print(f"SELECT variable_id, {min_v}, {max_v}, 2, 'WARNING', 'CSV DQ Rule {index+1}'")
    print(f"FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = '{tech_name}';")
