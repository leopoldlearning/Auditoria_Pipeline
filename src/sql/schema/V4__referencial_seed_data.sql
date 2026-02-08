/*
--------------------------------------------------------------------------------
-- SEED DATA V4 (PROD READY): Carga Masiva de Referencial ALINEADO A NOMENCLATURA V4 Y CSV DQ
-- REQUISITOS: 35 Reglas de Calidad (DQ) y 6 Reglas de Consistencia (RC)
-- TRAZABILIDAD: IDs de Formato 1 cruzados con el CSV 02_reglas_calidad.csv
--------------------------------------------------------------------------------
*/

-- =============================================================================
-- 1. ESTADOS OPERATIVOS Y UNIDADES
-- =============================================================================
TRUNCATE TABLE referencial.tbl_ref_estados_operativos RESTART IDENTITY CASCADE;
INSERT INTO referencial.tbl_ref_estados_operativos 
(codigo_estado, color_hex, descripcion, prioridad_visual, nivel_severidad, icono_web) VALUES
('NORMAL',   '#00C851', 'Operación Estable',       5, 0, 'check_circle'),
('WARNING',  '#FFBB33', 'Fuera de rango leve',     3, 1, 'warning'),
('CRITICAL', '#FF4444', 'Falla Crítica / Paro',    1, 3, 'error'),
('OFFLINE',  '#B0B0B0', 'Sin Comunicación',        4, -1,'wifi_off'),
('UNKNOWN',  '#33B5E5', 'Mantenimiento / Sin Dato',2, 0, 'help_outline');

TRUNCATE TABLE referencial.tbl_ref_unidades RESTART IDENTITY CASCADE;
INSERT INTO referencial.tbl_ref_unidades (simbolo, descripcion) VALUES 
('psi', 'Libras por pulgada cuadrada'), ('bbl/d', 'Barriles por día'),
('mcf/d', 'Miles de pies cúbicos día'), ('spm', 'Golpes por minuto'),
('Hz', 'Hertz'), ('F', 'Grados Fahrenheit'), ('A', 'Amperios'),
('hp', 'Caballos de Fuerza'), ('%', 'Porcentaje'), ('lb', 'Libras'),
('in', 'Pulgadas'), ('ft', 'Pies'), ('cP', 'Centipoise'), ('mD', 'Milidarcys')
ON CONFLICT (simbolo) DO NOTHING;

-- =============================================================================
-- 2. MAESTRA DE VARIABLES (Alineación Estricta ID Formato 1)
-- =============================================================================
TRUNCATE TABLE referencial.tbl_maestra_variables RESTART IDENTITY CASCADE;

-- BLOQUE A: Sensores & Operación (Alineados a CSV)
INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES
('well_head_pressure_psi_act', 'SENSOR', 54),
('flowing_bottom_hole_pressure_psi', 'SENSOR', 151),
('casing_head_pressure_psi_act', 'SENSOR', 55),
('min_rod_load_lb_act', 'SENSOR', 77),
('max_rod_load_lb_act', 'SENSOR', 76),
('motor_current_a_act', 'SENSOR', 44),           -- ID 44 según CSV DQ (Alineado)
('pump_fill_monitor_pct', 'SENSOR', 64),
('well_stroke_position_in', 'SENSOR', 155),
('surface_rod_load_lb', 'SENSOR', 156),
('downhole_pump_position_in', 'SENSOR', 157),
('downhole_pump_load_lb', 'SENSOR', 158);

-- BLOQUE B: Yacimiento & Parámetros Estáticos (Alineados a CSV)
INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES
('formation_thickness_ft', 'YACIMIENTO', 23),
('damage_factor', 'YACIMIENTO', NULL),           -- S/I
('formation_volume_factor', 'YACIMIENTO', 30),
('water_specific_gravity', 'YACIMIENTO', 63),
('absolute_permeability_md', 'YACIMIENTO', 28),
('vertical_permeability_md', 'YACIMIENTO', 162),
('horizontal_permeability_md', 'YACIMIENTO', NULL), -- Conflicto ID 28 con Absolute
('bubble_point_pressure_psi', 'YACIMIENTO', 24),
('critical_fbhp_psi', 'YACIMIENTO', 27),
('presion_estatica_yacimiento', 'YACIMIENTO', 25),
('crude_oil_viscosity_cp', 'YACIMIENTO', 29);

-- BLOQUE C: Diseño & Pozos (Alineados a CSV)
INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES
('profundidad_vertical_bomba', 'DISEÑO', 39),
('profundidad_vertical_yacimiento', 'DISEÑO', 38),
('equivalent_radius_ft', 'DISEÑO', NULL),        -- S/I
('drainage_radius_ft', 'DISEÑO', 20),
('wellbore_radius_ft', 'DISEÑO', 19),
('well_type', 'PARAMETRO', 3),
('horizontal_length_ft', 'DISEÑO', 160),
('tubing_anchor_depth_ft', 'DISEÑO', 78),
('max_fluid_load_lb', 'DISEÑO', 75);             -- ID 75 según CSV (Alineado)

-- BLOQUE D: KPIs & Calculados (Alineados a CSV)
INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES
('prod_petroleo_diaria_bpd', 'KPI', 108),
('porcentaje_agua', 'KPI', 57),
('corrected_dynamic_fluid_level_ft', 'CALCULADO', 59);

-- BLOQUE E: Extras para Dashboard (Sin romper pipeline)
INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES
('rod_weight_buoyant_lb_act', 'PARAMETRO', 73),
('pump_avg_spm_act', 'SENSOR', 51),
('motor_power_hp_act', 'SENSOR', 66),
('motor_running_flag', 'SENSOR', 120),
('temperatura_cabezal', 'SENSOR', 56),
('pump_discharge_pressure_psi_act', 'SENSOR', 62),
('pump_intake_pressure_psi_act', 'SENSOR', 61);

-- =============================================================================
-- 3. REGLAS DE CALIDAD (DQ) - 35 REGLAS SEGÚN CSV
-- =============================================================================
TRUNCATE TABLE referencial.tbl_dq_rules RESTART IDENTITY CASCADE;

-- Reglas Genéricas para variables de representatividad > 0 (vínculo por tech name del CSV)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule: ' || nombre_tecnico
FROM referencial.tbl_maestra_variables 
WHERE nombre_tecnico IN (
    'formation_thickness_ft', 'damage_factor', 'formation_volume_factor', 'max_fluid_load_lb',
    'water_specific_gravity', 'absolute_permeability_md', 'vertical_permeability_md', 'horizontal_permeability_md',
    'bubble_point_pressure_psi', 'well_head_pressure_psi_act', 'flowing_bottom_hole_pressure_psi',
    'critical_fbhp_psi', 'casing_head_pressure_psi_act', 'presion_estatica_yacimiento',
    'prod_petroleo_diaria_bpd', 'profundidad_vertical_bomba', 'profundidad_vertical_yacimiento',
    'equivalent_radius_ft', 'drainage_radius_ft', 'wellbore_radius_ft', 'crude_oil_viscosity_cp',
    'well_type', 'horizontal_length_ft', 'corrected_dynamic_fluid_level_ft', 'min_rod_load_lb_act',
    'well_stroke_position_in', 'surface_rod_load_lb', 'downhole_pump_position_in', 'downhole_pump_load_lb',
    'tubing_anchor_depth_ft', 'rod_weight_buoyant_lb_act', 'max_rod_load_lb_act', 'motor_current_a_act'
);

-- Reglas con rango 0-100% (Water Cut y Pump Fill)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0, 100.0, 2, 'WARNING', 'CSV DQ Rule: ' || nombre_tecnico
FROM referencial.tbl_maestra_variables
WHERE nombre_tecnico IN ('porcentaje_agua', 'pump_fill_monitor_pct');

-- =============================================================================
-- 4. REGLAS DE CONSISTENCIA (RC 1-6) - SEGÚN TRAZABILIDAD CSV
-- =============================================================================
TRUNCATE TABLE referencial.tbl_reglas_consistencia RESTART IDENTITY CASCADE;

-- RC-001: Carga Max > Min
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto, variable_a_id, operador, variable_b_id)
SELECT 'RC-001', 'Carga Max > Min', 'MaxRodLoad > MinRodLoad',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'max_rod_load_lb_act'), '>',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'min_rod_load_lb_act')
WHERE EXISTS (SELECT 1 FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'max_rod_load_lb_act');

-- RC-002: Carga Max > Peso Flotante
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto, variable_a_id, operador, variable_b_id)
SELECT 'RC-002', 'Carga Max > Peso Flotante', 'MaxRodLoad > Buoyant',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'max_rod_load_lb_act'), '>',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'rod_weight_buoyant_lb_act')
WHERE EXISTS (SELECT 1 FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'rod_weight_buoyant_lb_act');

-- RC-003: Gradiente de Presión (FBHP > WHP)
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto, variable_a_id, operador, variable_b_id)
SELECT 'RC-003', 'Gradiente de Presión', 'FBHP > WHP',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'flowing_bottom_hole_pressure_psi'), '>',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'well_head_pressure_psi_act')
WHERE EXISTS (SELECT 1 FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'flowing_bottom_hole_pressure_psi');

-- RC-004: Inflow (Productividad: FBHP < P_Estatica)
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto, variable_a_id, operador, variable_b_id)
SELECT 'RC-004', 'Inflow', 'FBHP < P_Estatico',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'flowing_bottom_hole_pressure_psi'), '<',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'presion_estatica_yacimiento')
WHERE EXISTS (SELECT 1 FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'presion_estatica_yacimiento');

-- RC-005: Geometría Vertical (Prof Bomba < Prof Yacimiento)
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto, variable_a_id, operador, variable_b_id)
SELECT 'RC-005', 'Geometría Vertical', 'PumpDepth < ReservoirDepth',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'profundidad_vertical_bomba'), '<',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'profundidad_vertical_yacimiento')
WHERE EXISTS (SELECT 1 FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'profundidad_vertical_bomba');

-- RC-006: Geometría Radial (WellRadius < DrainageRadius)
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto, variable_a_id, operador, variable_b_id)
SELECT 'RC-006', 'Geometría Radial', 'WellRadius < DrainageRadius',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'wellbore_radius_ft'), '<',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'drainage_radius_ft')
WHERE EXISTS (SELECT 1 FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'wellbore_radius_ft');
