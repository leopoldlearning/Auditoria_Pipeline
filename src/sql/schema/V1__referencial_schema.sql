/*
------------------------------------------------------------------------------------------------------------------------
-- SCRIPT INFORMATION BLOCK
------------------------------------------------------------------------------------------------------------------------
-- FILE NAME:         V1__referencial_schema.sql
-- DESCRIPTION:       Creación de Esquema Referencial, Tablas Maestras y Carga de Reglas (AUT-76)
-- AUTHOR:            ITMEET / Data Architecture
-- DATE:              2025-12-12
-- DEPENDENCIES:      Ninguna (Es el esquema base nivel 0)
------------------------------------------------------------------------------------------------------------------------
*/

-- 1. GESTIÓN DE ESQUEMA
-- Se elimina en cascada para garantizar una recreación limpia si se ejecuta multiples veces en dev.
DROP SCHEMA IF EXISTS referencial CASCADE;
CREATE SCHEMA referencial;

-- =============================================================================
-- 2. TABLAS DIMENSIONALES (CATÁLOGOS)
-- =============================================================================

-- 2.1 Unidades de Medida
-- Estandariza las magnitudes para evitar ambigüedades en cálculos.
CREATE TABLE referencial.tbl_ref_unidades (
    unidad_id SERIAL PRIMARY KEY,
    simbolo VARCHAR(20) NOT NULL UNIQUE, -- ej. 'psi', 'bbl/d'
    descripcion VARCHAR(100)
);

-- 2.2 Maestra de Variables (Cerebro del sistema)
-- Vincula el ID del Excel (Formato 1) con el nombre técnico en PostgreSQL.
CREATE TABLE referencial.tbl_maestra_variables (
    variable_id SERIAL PRIMARY KEY,
    id_formato1 INTEGER UNIQUE,          -- ID Oficial del Excel (ej. 54)
    nombre_tecnico VARCHAR(100) NOT NULL UNIQUE, -- Nombre Técnico Único en Stage
    tabla_origen VARCHAR(50),            -- 'tbl_pozo_produccion', 'tbl_pozo_maestra'
    clasificacion_logica VARCHAR(50),    -- 'SENSOR', 'CALCULADO', 'INPUT_MANUAL'
    volatilidad VARCHAR(20),             -- 'VARIABLE', 'FIJO', 'SEMI-FIJO'
    unidad_id INTEGER REFERENCES referencial.tbl_ref_unidades(unidad_id)
);

-- =============================================================================
-- 3. TABLAS DE REGLAS (MOTOR DE CALIDAD)
-- =============================================================================

-- 3.1 Reglas de Calidad (Evolución de tbl_dq_rules)
-- Define los límites físicos duros y la latencia permitida.
CREATE TABLE referencial.tbl_dq_rules (
    regla_id SERIAL PRIMARY KEY,
    variable_id INTEGER REFERENCES referencial.tbl_maestra_variables(variable_id),
    valor_min DECIMAL(12, 4),
    valor_max DECIMAL(12, 4),
    latencia_max_segundos INTEGER DEFAULT 2, -- Nuevo Pilar: Latencia
    severidad VARCHAR(20) DEFAULT 'WARNING', -- Nuevo Pilar: Tolerancia ('ERROR', 'WARNING')
    origen_regla VARCHAR(100) DEFAULT 'Excel Matriz Calidad'
);

-- 3.2 Consistencia Lógica (RC)
-- Define validaciones cruzadas entre dos variables (A > B).
CREATE TABLE referencial.tbl_reglas_consistencia (
    codigo_rc VARCHAR(20) PRIMARY KEY,   -- ej. 'RC-001'
    descripcion TEXT,
    criterio_texto TEXT,                 -- Texto original para auditoría
    variable_a_id INTEGER REFERENCES referencial.tbl_maestra_variables(variable_id), -- Sujeto
    operador VARCHAR(5),                 -- '>', '<', '=', '!='
    variable_b_id INTEGER REFERENCES referencial.tbl_maestra_variables(variable_id)  -- Objeto
);

-- =============================================================================
-- 4. SEED DATA (CARGA INICIAL AUTOMÁTICA)
-- =============================================================================

-- 4.1 Carga de Unidades Básicas
INSERT INTO referencial.tbl_ref_unidades (simbolo, descripcion) VALUES
('psi', 'Libras por pulgada cuadrada'),
('bbl/d', 'Barriles por día'),
('in', 'Pulgadas'),
('ft', 'Pies'),
('spm', 'Strokes per minute'),
('%', 'Porcentaje'),
('hp', 'Horsepower'),
('kWh/bbl', 'Kilowatt-hora por barril'),
('mD', 'Milidarcys'),
('cP', 'Centipoise'),
('A', 'Amperios'),
('V', 'Voltios'),
('dias', 'Días'),
('horas', 'Horas'),
('Dimensionless', 'Adimensional'),
('By/BN', 'Barriles yacimiento por barril normal');

-- 4.2 Carga de Variables (Mapeo: ID Excel -> Nombre Técnico Stage)
-- Solo se incluyen variables con ID Oficial presente en la Hoja de Calidad.
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica, volatilidad) VALUES
-- GRUPO 1: Sensores (Alta frecuencia - Variables)
(65, 'fluid_flow_monitor_bpd', 'tbl_pozo_produccion', 'SENSOR', 'VARIABLE'),
(51, 'spm_promedio', 'tbl_pozo_produccion', 'SENSOR', 'VARIABLE'),
(64, 'pump_fill_monitor', 'tbl_pozo_produccion', 'SENSOR', 'VARIABLE'),
(54, 'presion_cabezal', 'tbl_pozo_produccion', 'SENSOR', 'VARIABLE'), -- WHP
(55, 'presion_casing', 'tbl_pozo_produccion', 'SENSOR', 'SEMI-FIJO'), -- CHP
(61, 'pip', 'tbl_pozo_produccion', 'SENSOR', 'VARIABLE'),
(66, 'potencia_actual_motor', 'tbl_pozo_produccion', 'SENSOR', 'VARIABLE'),
(57, 'porcentaje_agua', 'tbl_pozo_produccion', 'SENSOR', 'VARIABLE'), -- Water Cut
(7,  'tasa_produccion_bopd', 'tbl_pozo_produccion', 'SENSOR', 'VARIABLE'),
(77, 'minimum_rod_load', 'tbl_pozo_produccion', 'SENSOR', 'VARIABLE'),
(68, 'surface_rod_position', 'tbl_pozo_produccion', 'SENSOR', 'VARIABLE'), -- Corregido Snake Case
(78, 'profundidad_ancla_tuberia', 'tbl_pozo_maestra', 'SENSOR', 'VARIABLE'),
(73, 'rod_weight_buoyant', 'tbl_pozo_produccion', 'SENSOR', 'VARIABLE'),
(76, 'maximum_rod_load', 'tbl_pozo_produccion', 'SENSOR', 'VARIABLE'),
(44, 'corriente_motor', 'tbl_pozo_produccion', 'SENSOR', 'VARIABLE'),

-- GRUPO 2: Diseño / Manuales (Fijos o Semi-Fijos)
(23, 'espesor_formacion', 'tbl_pozo_maestra', 'INPUT_MANUAL', 'FIJO'),
(30, 'factor_volumetrico', 'tbl_pozo_reservas', 'INPUT_MANUAL', 'SEMI-FIJO'),
(10, 'api_max_fluid_load', 'tbl_pozo_maestra', 'INPUT_MANUAL', 'SEMI-FIJO'),
(63, 'gravedad_especifica_agua', 'tbl_pozo_reservas', 'INPUT_MANUAL', 'FIJO'),
(28, 'permeabilidad_absoluta', 'tbl_pozo_maestra', 'INPUT_MANUAL', 'SEMI-FIJO'),
(24, 'presion_burbujeo', 'tbl_pozo_reservas', 'INPUT_MANUAL', 'FIJO'),
(27, 'presion_fondo_critica', 'tbl_pozo_reservas', 'INPUT_MANUAL', 'SEMI-FIJO'),
(25, 'presion_estatica_yacimiento', 'tbl_pozo_reservas', 'INPUT_MANUAL', 'SEMI-FIJO'),
(39, 'profundidad_vertical_bomba', 'tbl_pozo_maestra', 'INPUT_MANUAL', 'FIJO'),
(38, 'profundidad_vertical_yacimiento', 'tbl_pozo_maestra', 'INPUT_MANUAL', 'FIJO'),
(20, 'radio_drenaje', 'tbl_pozo_maestra', 'INPUT_MANUAL', 'FIJO'),
(19, 'radio_pozo', 'tbl_pozo_maestra', 'INPUT_MANUAL', 'FIJO'),
(29, 'viscosidad_crudo', 'tbl_pozo_reservas', 'INPUT_MANUAL', 'SEMI-FIJO'),
(3,  'tipo_pozo', 'tbl_pozo_maestra', 'INPUT_MANUAL', 'FIJO'),
(33, 'diametro_embolo_bomba', 'tbl_pozo_maestra', 'INPUT_MANUAL', 'FIJO'),
(42, 'longitud_carrera_nominal', 'tbl_pozo_maestra', 'INPUT_MANUAL', 'FIJO'),

-- GRUPO 3: Variables Calculadas (Necesarias para RC)
(150,'qmax_bpd', 'tbl_pozo_reservas', 'CALCULADO', 'SEMI-FIJO'),
(151,'pwf_actual_psi', 'tbl_pozo_produccion', 'CALCULADO', 'VARIABLE'), -- FBHP
(159,'radio_equivalente', 'tbl_pozo_reservas', 'CALCULADO', 'FIJO');


-- 4.3 Carga de Reglas de Calidad (tbl_dq_rules)
-- Se aplican reglas generales y especificas basadas en la Matriz Excel.

-- REGLA GENERAL 1: Valores positivos y Latencia < 2s
-- Aplica a la mayoría de variables físicas (Presiones, Profundidades, Cargas).
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos)
SELECT variable_id, 0.0001, NULL, 2 
FROM referencial.tbl_maestra_variables 
WHERE nombre_tecnico NOT IN ('porcentaje_agua', 'pump_fill_monitor', 'tipo_pozo');

-- REGLA GENERAL 2: Porcentajes (0 - 100%)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos)
SELECT variable_id, 0, 100, 2 
FROM referencial.tbl_maestra_variables 
WHERE nombre_tecnico IN ('porcentaje_agua', 'pump_fill_monitor');

-- REGLAS ESPECÍFICAS (Sobreescritura o Adición si fuera necesario)
-- Ejemplo: Fluid Flow Monitor tiene un tope conocido de 5000 bpd
UPDATE referencial.tbl_dq_rules 
SET valor_max = 5000 
WHERE variable_id = (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 65);

-- Ejemplo: SPM Promedio tope 10
UPDATE referencial.tbl_dq_rules 
SET valor_max = 10 
WHERE variable_id = (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 51);


-- 4.4 Carga de Reglas de Consistencia (RC-001 a RC-006)
-- Implementación de la hoja "RC - Reglas de consistencia".

INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto, variable_a_id, operador, variable_b_id)
VALUES 
-- RC-001: Max Rod Load (76) > Min Rod Load (77)
('RC-001', 'Cargas de la Barra 1', 'Maximum rod load > Minimum rod load',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 76), '>', 
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 77)),

-- RC-002: Max Rod Load (76) > Rod Weight Buoyant (73)
('RC-002', 'Cargas de la Barra 2', 'Maximum rod load > Rod weight buoyant',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 76), '>', 
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 73)),

-- RC-003: FBHP (151) > WHP (54)
('RC-003', 'Gradiente de Presión', 'Presión de fondo fluyente (FBHP) > Well head pressure (WHP)',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 151), '>', 
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 54)),

-- RC-004: FBHP (151) < Ps (25)
('RC-004', 'Relaciones de Presión', 'Presión de fondo fluyente (FBHP) < Presión estática del yacimiento (Ps)',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 151), '<', 
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 25)),

-- RC-005: Pump Depth (39) < Reservoir Depth (38)
('RC-005', 'Relaciones de Profundidad', 'Profundidad vertical de la bomba < Profundidad vertical del yacimiento',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 39), '<', 
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 38)),

-- RC-006: Well Radius (19) < Equivalent Radius (159)
('RC-006', 'Relaciones de Geometría', 'Radio del pozo < Radio equivalente',
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 19), '<', 
    (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 159));

COMMIT;