-- GENERATED DQ RULES FROM CSV
TRUNCATE TABLE referencial.tbl_dq_rules RESTART IDENTITY CASCADE;
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(27, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Formation Thickness (espesor_formacion)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(30, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Formation Volume Factor (factor_volumetrico)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(36, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- API Maximum Fluid Load (gravedad_api)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(37, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Water specific gravity (gravedad_especifica_agua)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(65, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Absolute Permeability (permeabilidad_horizontal)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(69, 0, 100, 2, 'WARNING', 'CSV Reglas Calidad'); -- Water cut (porcentaje_agua)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(75, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Bubble Point Pressure (presion_burbujeo)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(76, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Well head pressure (WHP) (well_head_pressure_psi_act)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(81, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Critical Flowing Bottom Hole Pressure (presion_fondo_fluyente_critico)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(77, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Casing head pressure (CHP) (casing_head_pressure_psi_act)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(80, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Reservoir Static Pressure (presion_estatica_yacimiento)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(82, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Reservoir Static Pressure (presion_inicial_yacimiento)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(87, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Production (BOPD) (prod_petroleo_diaria_bpd)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(89, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Pump True Vertical Depth (profundidad_vertical_bomba)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(90, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Vertical Depth of Reservoir (profundidad_vertical_yacimiento)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(93, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Drainage Radius (radio_drenaje)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(95, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Wellbore Radius (radio_pozo)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(117, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Crude Oil Viscosity (viscosidad_superficie)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(115, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- Well type (tipo_pozo)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(53, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- minimum rod load (min_rod_load_lb_act)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(2, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- tubing anchor depth (anchor_vertical_depth)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(91, 0, 100, 2, 'WARNING', 'CSV Reglas Calidad'); -- pump fill monitor (pump_fill_monitor_pct)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(98, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- rod weight buoyant (rod_weight_buoyant_lb_act)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(49, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- maximum rod load (max_rod_load_lb_act)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos, severidad, origen_regla) VALUES
(16, 0.0001, NULL, 2, 'WARNING', 'CSV Reglas Calidad'); -- motor current (corriente_nominal_motor)

-- Matched Rules: 25
-- Missing Matches: 11
-- MISSING: Damage Factor  (ID: nan)
-- MISSING: Vertical Permeability (ID: nan)
-- MISSING: Horizontal Permeability (ID: nan)
-- MISSING: Flowing Bottom Hole Pressure (FBHP) (ID: nan)
-- MISSING: Equivalent Radius (ID: nan)
-- MISSING: Horizontal Length (ID: nan)
-- MISSING: Corrected Dynamic Fluid Level (ID: nan)
-- MISSING: Surface RodPosition (ID: 68)
-- MISSING: Surface Rod Load (ID: nan)
-- MISSING: Downhole Pump Position (ID: nan)
-- MISSING: Downhole Pump Load (ID: nan)
