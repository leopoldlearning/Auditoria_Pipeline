/*
--------------------------------------------------------------------------------
-- SEED DATA V3.1 (FULL PRODUCTION): Carga Masiva de Referencial
-- INCLUYE: 
-- 1. Estados Operativos (Semáforos)
-- 2. Unidades y Variables (Maestra Completa)
-- 3. Reglas de Calidad (DQ)
-- 4. Reglas de Consistencia (RC-001 a RC-006)
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
('in', 'Pulgadas'), ('ft', 'Pies')
ON CONFLICT (simbolo) DO NOTHING;

-- =============================================================================
-- 2. MAESTRA DE VARIABLES (Catálogo Completo para Stage V4)
-- =============================================================================
TRUNCATE TABLE referencial.tbl_maestra_variables RESTART IDENTITY CASCADE;

-- Bloque A: Sensores Presión/Temp
INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES 
('presion_cabezal', 'SENSOR', 54), ('presion_casing', 'SENSOR', 55),
('temperatura_cabezal', 'SENSOR', 56), ('presion_descarga_bomba', 'SENSOR', 62),
('pip', 'SENSOR', 61), ('presion_cilindro_hidraulico', 'SENSOR', 93),
('temperatura_tanque_aceite', 'SENSOR', 94);

-- Bloque B: Dinámica y Bombeo
INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES 
('spm_promedio', 'SENSOR', 51), ('spm_solicitado_arriba', 'SETPOINT', 52),
('pump_fill_monitor', 'SENSOR', 64), ('nivel_fluido_dinamico', 'CALCULADO', 59),
('maximum_rod_load', 'SENSOR', 76), ('minimum_rod_load', 'SENSOR', 77),
('rod_weight_buoyant', 'PARAMETRO', 73), ('monitor_carga_bomba', 'SENSOR', 74);

-- Bloque C: Producción
INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES 
('produccion_fluido_diaria', 'KPI', 107), ('produccion_petroleo_diaria', 'KPI', 108),
('produccion_agua_diaria', 'KPI', 109), ('produccion_gas_diaria', 'KPI', 110),
('porcentaje_agua', 'CALCULADO', 57), ('monitor_llenado_gas', 'SENSOR', 96);

-- Bloque D: Eléctrico y Motor
INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES 
('potencia_actual_motor', 'SENSOR', 66), ('current_amperage', 'SENSOR', 67),
('rpm_motor', 'SENSOR', 86), ('frecuencia_nominal_motor', 'PARAMETRO', 130),
('kwh_por_barril', 'KPI', 71), ('tiempo_actual_drive', 'SENSOR', 95);

-- Bloque E: Diseño (Para Reglas Consistencia)
INSERT INTO referencial.tbl_maestra_variables (nombre_tecnico, clasificacion_logica, id_formato1) VALUES 
('presion_estatica_yacimiento', 'YACIMIENTO', 25), ('profundidad_vertical_bomba', 'DISEÑO', 39),
('profundidad_completacion', 'DISEÑO', 2), ('diametro_embolo_bomba', 'DISEÑO', 33),
('radio_pozo', 'DISEÑO', 19);

-- =============================================================================
-- 3. REGLAS DE CALIDAD (DQ) - CARGA MASIVA
-- =============================================================================
TRUNCATE TABLE referencial.tbl_dq_rules RESTART IDENTITY CASCADE;

-- 3.1 Reglas Críticas
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, severidad, origen_regla)
SELECT variable_id, 0.0, 20.0, 'CRITICAL', 'Física de Bombeo'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'spm_promedio';

INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, severidad, origen_regla)
SELECT variable_id, 0.0, 100.0, 'CRITICAL', 'Porcentaje Físico'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'porcentaje_agua';

INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, severidad, origen_regla)
SELECT variable_id, 0.0, 100.0, 'CRITICAL', 'Eficiencia Física'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'pump_fill_monitor';

-- 3.2 Reglas de Advertencia
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, severidad, origen_regla)
SELECT variable_id, 0.0, 5000.0, 'WARNING', 'Seguridad Cabezal'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'presion_cabezal';

INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, severidad, origen_regla)
SELECT variable_id, 0.0, 1500.0, 'WARNING', 'Seguridad Casing'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'presion_casing';

INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, severidad, origen_regla)
SELECT variable_id, 0.0, 200.0, 'WARNING', 'Temp Motor'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'temperatura_tanque_aceite';

INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, severidad, origen_regla)
SELECT variable_id, 0.0, 40000.0, 'WARNING', 'Carga Varillas'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'maximum_rod_load';

INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, severidad, origen_regla)
SELECT variable_id, 0.0, 150.0, 'WARNING', 'Potencia Motor'
FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'potencia_actual_motor';

-- =============================================================================
-- 4. REGLAS DE CONSISTENCIA (RC) - LÓGICA CRUZADA
-- =============================================================================
TRUNCATE TABLE referencial.tbl_reglas_consistencia RESTART IDENTITY CASCADE;

-- RC-001: Carga Max > Min
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto, variable_a_id, operador, variable_b_id)
SELECT 'RC-001', 'Carga Max > Min', 'MaxRodLoad > MinRodLoad',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'maximum_rod_load'), '>',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'minimum_rod_load')
WHERE EXISTS (SELECT 1 FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'maximum_rod_load');

-- RC-002: Carga Max > Peso Flotante
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto, variable_a_id, operador, variable_b_id)
SELECT 'RC-002', 'Carga Max > Peso Flotante', 'MaxRodLoad > Buoyant',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'maximum_rod_load'), '>',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'rod_weight_buoyant')
WHERE EXISTS (SELECT 1 FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'rod_weight_buoyant');

-- RC-003: Gradiente Bomba
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto, variable_a_id, operador, variable_b_id)
SELECT 'RC-003', 'Gradiente Bomba', 'P_Descarga > PIP',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'presion_descarga_bomba'), '>',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'pip')
WHERE EXISTS (SELECT 1 FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'presion_descarga_bomba');

-- RC-004: Inflow (PIP < P_Estatica)
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto, variable_a_id, operador, variable_b_id)
SELECT 'RC-004', 'Inflow', 'PIP < P_Estatica',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'pip'), '<',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'presion_estatica_yacimiento')
WHERE EXISTS (SELECT 1 FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'presion_estatica_yacimiento');

-- RC-005: Geometría Vertical
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto, variable_a_id, operador, variable_b_id)
SELECT 'RC-005', 'Geometría Vertical', 'Depth_Pump < Depth_Total',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'profundidad_vertical_bomba'), '<',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'profundidad_completacion')
WHERE EXISTS (SELECT 1 FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'profundidad_vertical_bomba');

-- RC-006: Geometría Radial
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto, variable_a_id, operador, variable_b_id)
SELECT 'RC-006', 'Geometría Radial', 'Diam_Pump < Radio_Well',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'diametro_embolo_bomba'), '<',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'radio_pozo')
WHERE EXISTS (SELECT 1 FROM referencial.tbl_maestra_variables WHERE nombre_tecnico = 'radio_pozo');