/*
------------------------------------------------------------------------------------------------------------------------
-- SCRIPT INFORMATION BLOCK
------------------------------------------------------------------------------------------------------------------------
-- FILE NAME:         V2__referencial_schema_complete.sql
-- DESCRIPTION:       Esquema Referencial TOTAL. Fusiona la carga original (Legacy) con los nuevos requerimientos visuales (Zero-Calc).
-- DATABASE PLATFORM: PostgreSQL 17
-- AUTHOR:            ITMEET / Data Architecture
------------------------------------------------------------------------------------------------------------------------
*/

-- 1. GESTIÓN DE ESQUEMA
DROP SCHEMA IF EXISTS referencial CASCADE;
CREATE SCHEMA referencial;

-- =============================================================================
-- 2. TABLAS DIMENSIONALES (CATÁLOGOS)
-- =============================================================================

-- 2.1 Unidades de Medida
CREATE TABLE referencial.tbl_ref_unidades (
    unidad_id SERIAL PRIMARY KEY,
    simbolo VARCHAR(20) NOT NULL UNIQUE, -- ej. 'psi', 'bbl/d'
    descripcion VARCHAR(100)
);

-- 2.2 Maestra de Variables (Cerebro del sistema)
CREATE TABLE referencial.tbl_maestra_variables (
    variable_id SERIAL PRIMARY KEY,
    id_formato1 INTEGER UNIQUE,          -- ID Oficial del Excel/CSV (ej. 54)
    nombre_tecnico VARCHAR(100) NOT NULL UNIQUE, -- Nombre Técnico en Stage
    tabla_origen VARCHAR(50),            -- 'tbl_pozo_produccion', 'tbl_pozo_maestra'
    clasificacion_logica VARCHAR(50),    -- 'SENSOR', 'CALCULADO', 'INPUT_MANUAL'
    volatilidad VARCHAR(20),             -- 'VARIABLE', 'FIJO', 'SEMI-FIJO'
    unidad_id INTEGER REFERENCES referencial.tbl_ref_unidades(unidad_id)
);

-- [NUEVO] 2.3 Catálogo de Estados Operativos (Semáforos para Dashboard)
-- Requerimiento: Fila 112 del CSV. Reporting lee 'color_hex' directamente.
CREATE TABLE referencial.tbl_ref_estados_operativos (
    estado_id SERIAL PRIMARY KEY,
    codigo_estado VARCHAR(20) UNIQUE, -- 'NORMAL', 'WARNING', 'CRITICAL'
    color_hex VARCHAR(7) NOT NULL,    -- '#00FF00', '#FF0000'
    descripcion VARCHAR(100),
    prioridad_visual INT              -- 1 = Crítico, 5 = Normal
);

-- [NUEVO] 2.4 Catálogo de Paneles BI (Gobernanza)
CREATE TABLE referencial.tbl_ref_paneles_bi (
    panel_id SERIAL PRIMARY KEY,
    nombre_panel VARCHAR(100) UNIQUE, 
    descripcion TEXT
);

-- =============================================================================
-- 3. TABLAS DE REGLAS (MOTOR DE CALIDAD Y CONFIGURACIÓN)
-- =============================================================================

-- 3.1 Reglas de Calidad (Límites Físicos Duros)
CREATE TABLE referencial.tbl_dq_rules (
    regla_id SERIAL PRIMARY KEY,
    variable_id INTEGER REFERENCES referencial.tbl_maestra_variables(variable_id),
    valor_min DECIMAL(12, 4),
    valor_max DECIMAL(12, 4),
    latencia_max_segundos INTEGER DEFAULT 2,
    severidad VARCHAR(20) DEFAULT 'WARNING',
    origen_regla VARCHAR(100) DEFAULT 'Excel Matriz Calidad'
);

-- 3.2 Consistencia Lógica (Reglas Cruzadas)
CREATE TABLE referencial.tbl_reglas_consistencia (
    codigo_rc VARCHAR(20) PRIMARY KEY,   -- ej. 'RC-001'
    descripcion TEXT,
    criterio_texto TEXT,                 -- Texto original para auditoría
    variable_a_id INTEGER REFERENCES referencial.tbl_maestra_variables(variable_id),
    operador VARCHAR(5),                 -- '>', '<', '=', '!='
    variable_b_id INTEGER REFERENCES referencial.tbl_maestra_variables(variable_id)
);

-- [NUEVO] 3.3 Límites y Targets por Pozo (Configuración Dinámica)
-- Aquí vive el "Target" (Meta) y los umbrales para colorear el Dashboard.
CREATE TABLE referencial.tbl_limites_pozo (
    limite_id SERIAL PRIMARY KEY,
    pozo_id INT NOT NULL, -- Referencia lógica a DIM_POZO
    variable_id INT NOT NULL REFERENCES referencial.tbl_maestra_variables(variable_id),
    
    target_value DECIMAL(12,4), -- Meta ideal
    
    -- Umbrales Semáforo
    min_critical DECIMAL(12,4),
    min_warning DECIMAL(12,4),
    max_warning DECIMAL(12,4),
    max_critical DECIMAL(12,4),
    
    -- Configuración IA
    ai_confidence_threshold DECIMAL(5,2) DEFAULT 95.0,
    
    fecha_vigencia_inicio DATE DEFAULT CURRENT_DATE,
    activo BOOLEAN DEFAULT TRUE,
    
    UNIQUE(pozo_id, variable_id)
);

-- =============================================================================
-- 4. SEED DATA (CARGA INICIAL - RECUPERADA DEL ORIGINAL + NUEVOS)
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
('Hz', 'Hertz'),
('dias', 'Días'),
('horas', 'Horas'),
('Dimensionless', 'Adimensional'),
('By/BN', 'Barriles yacimiento por barril normal');

-- 4.2 Carga de Variables (RECUPERADO DEL ORIGINAL)
-- Se incluyen todas las variables del script original. 
-- NOTA: Se deben agregar aquí el resto de las 114 variables del CSV si faltan en este bloque.
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
(68, 'surface_rod_position', 'tbl_pozo_produccion', 'SENSOR', 'VARIABLE'),
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


-- 4.3 Carga de Reglas de Calidad (RECUPERADO DEL ORIGINAL)
-- REGLA GENERAL 1: Valores positivos y Latencia < 2s
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos)
SELECT variable_id, 0.0001, NULL, 2 
FROM referencial.tbl_maestra_variables 
WHERE nombre_tecnico NOT IN ('porcentaje_agua', 'pump_fill_monitor', 'tipo_pozo');

-- REGLA GENERAL 2: Porcentajes (0 - 100%)
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, valor_max, latencia_max_segundos)
SELECT variable_id, 0, 100, 2 
FROM referencial.tbl_maestra_variables 
WHERE nombre_tecnico IN ('porcentaje_agua', 'pump_fill_monitor');

-- REGLAS ESPECÍFICAS
UPDATE referencial.tbl_dq_rules 
SET valor_max = 5000 
WHERE variable_id = (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 65);

UPDATE referencial.tbl_dq_rules 
SET valor_max = 10 
WHERE variable_id = (SELECT variable_id FROM referencial.tbl_maestra_variables WHERE id_formato1 = 51);


-- 4.4 Carga de Reglas de Consistencia (RECUPERADO DEL ORIGINAL: RC-001 a RC-006)
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

-- 4.5 [NUEVO] Carga de Estados y Paneles (Seed Data para Dashboard)
INSERT INTO referencial.tbl_ref_estados_operativos (codigo_estado, color_hex, descripcion, prioridad_visual) VALUES
('NORMAL',   '#00FF00', 'Operación Normal dentro de rangos', 5),
('WARNING',  '#FFFF00', 'Alerta temprana, desviación leve', 3),
('ALERT',    '#FFA500', 'Alerta media', 2),
('CRITICAL', '#FF0000', 'Parada o peligro inminente', 1),
('OFFLINE',  '#808080', 'Sin Comunicación', 4);

INSERT INTO referencial.tbl_ref_paneles_bi (nombre_panel, descripcion) VALUES
('Surface Operations', 'Panel 1: Operaciones de Superficie y Dinamometría'),
('Production', 'Panel 2: Producción y Yacimiento (IPR)'),
('Business KPIs', 'Panel 3: Indicadores Financieros y de Negocio');

COMMIT;