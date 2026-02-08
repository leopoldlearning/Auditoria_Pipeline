-- SECCIÓN 2: MAESTRA DE VARIABLES (ALINEADA A CSV DQ)
TRUNCATE TABLE referencial.tbl_maestra_variables RESTART IDENTITY CASCADE;

-- BLOQUE A: Sensores & Operación
INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES
('well_head_pressure_psi_act', 'SENSOR', 54),
('casing_head_pressure_psi_act', 'SENSOR', 55),
('flowing_bottom_hole_pressure_psi', 'SENSOR', 151),
('min_rod_load_lb_act', 'SENSOR', 77),
('max_rod_load_lb_act', 'SENSOR', 76),
('motor_current_a_act', 'SENSOR', 44),
('pump_fill_monitor_pct', 'KPI', 64),
('well_stroke_position_in', 'SENSOR', 155),
('surface_rod_load_lb', 'SENSOR', 156),
('downhole_pump_position_in', 'SENSOR', 157),
('downhole_pump_load_lb', 'SENSOR', 158);

-- BLOQUE B: Yacimiento
INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES
('formation_thickness_ft', 'YACIMIENTO', 23),
('damage_factor', 'SENSOR', NULL),
('formation_volume_factor', 'SENSOR', 30),
('water_specific_gravity', 'YACIMIENTO', 63),
('absolute_permeability_md', 'YACIMIENTO', 28),
('vertical_permeability_md', 'YACIMIENTO', 162),
('horizontal_permeability_md', 'YACIMIENTO', 28),
('bubble_point_pressure_psi', 'SENSOR', 24),
('critical_fbhp_psi', 'SENSOR', 27),
('presion_estatica_yacimiento', 'YACIMIENTO', 25),
('crude_oil_viscosity_cp', 'YACIMIENTO', 29);

-- BLOQUE C: Diseño & Pozos
INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES
('profundidad_vertical_bomba', 'SENSOR', 39),
('profundidad_vertical_yacimiento', 'SENSOR', 38),
('equivalent_radius_ft', 'DISEÑO', NULL),
('drainage_radius_ft', 'DISEÑO', 20),
('wellbore_radius_ft', 'DISEÑO', 19),
('well_type', 'DISEÑO', 3),
('horizontal_length_ft', 'DISEÑO', 160),
('tubing_anchor_depth_ft', 'DISEÑO', 78),
('max_fluid_load_lb', 'DISEÑO', 75);

-- BLOQUE D: KPIs & Calculados
INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES
('prod_petroleo_diaria_bpd', 'KPI', 108),
('porcentaje_agua', 'KPI', 57),
('corrected_dynamic_fluid_level_ft', 'SENSOR', 59);

-- REGLAS DE CALIDAD (DQ) - CARGA FILA POR FILA DESDE CSV
TRUNCATE TABLE referencial.tbl_dq_rules RESTART IDENTITY CASCADE;
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 1'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'formation_thickness_ft';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 2'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'damage_factor';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 3'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'formation_volume_factor';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 4'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'max_fluid_load_lb';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 5'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'water_specific_gravity';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 6'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'absolute_permeability_md';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 7'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'vertical_permeability_md';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 8'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'horizontal_permeability_md';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0, 100.0, 2, 'WARNING', 'CSV DQ Rule 9'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'porcentaje_agua';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 10'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'bubble_point_pressure_psi';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 11'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'well_head_pressure_psi_act';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 12'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'flowing_bottom_hole_pressure_psi';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 13'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'critical_fbhp_psi';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 14'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'casing_head_pressure_psi_act';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 15'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'presion_estatica_yacimiento';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 16'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'prod_petroleo_diaria_bpd';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 17'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'profundidad_vertical_bomba';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 18'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'profundidad_vertical_yacimiento';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 19'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'equivalent_radius_ft';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 20'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'drainage_radius_ft';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 21'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'wellbore_radius_ft';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 22'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'crude_oil_viscosity_cp';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 23'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'well_type';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 24'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'horizontal_length_ft';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 25'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'corrected_dynamic_fluid_level_ft';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 26'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'min_rod_load_lb_act';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 27'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'well_stroke_position_in';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 28'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'surface_rod_load_lb';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 29'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'downhole_pump_position_in';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 30'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'downhole_pump_load_lb';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 31'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'tubing_anchor_depth_ft';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0, 100.0, 2, 'WARNING', 'CSV DQ Rule 32'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'pump_fill_monitor_pct';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 33'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'rod_weight_buoyant_lb_act';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 34'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'max_rod_load_lb_act';
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla)
SELECT variable_id, 0.0001, NULL, 2, 'WARNING', 'CSV DQ Rule 35'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'motor_current_a_act';
