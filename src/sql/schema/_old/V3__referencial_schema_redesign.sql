/*
--------------------------------------------------------------------------------
-- ESQUEMA REFERENCIAL V3.5 (MASTER STANDARD)
-- DESCRIPCIÓN: Configuración centralizada de Reglas, Límites y Estándares Visuales.
-- INCLUYE: Motor de funciones para cálculo automático de semáforos (Zero-Calc).
--------------------------------------------------------------------------------
*/

-- 1. GESTIÓN DE ESQUEMA (Reset Limpio)
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

-- 2.2 Estados Operativos (EL CORAZÓN DEL SEMÁFORO)
-- Define los colores y severidades para toda la empresa.
CREATE TABLE referencial.tbl_ref_estados_operativos (
    estado_id SERIAL PRIMARY KEY,
    codigo_estado VARCHAR(20) UNIQUE NOT NULL, -- 'NORMAL', 'CRITICAL', 'WARNING'
    color_hex VARCHAR(7) NOT NULL,             -- '#FF0000', '#00FF00'
    descripcion VARCHAR(100),
    nivel_severidad INT DEFAULT 0,             -- 0=Ok, 1=Warn, 3=Critico (Para KPIs de fallo)
    icono_web VARCHAR(50),                     -- 'fa-check', 'fa-exclamation' (Para Frontend)
    prioridad_visual INT                       -- Para ordenar en leyendas (5=Normal, 1=Critico)
);

-- 2.3 Paneles BI
CREATE TABLE referencial.tbl_ref_paneles_bi (
    panel_id SERIAL PRIMARY KEY,
    nombre_panel VARCHAR(100) UNIQUE, 
    descripcion TEXT
);

-- 2.4 Maestra de Variables
CREATE TABLE referencial.tbl_maestra_variables (
    variable_id SERIAL PRIMARY KEY,
    id_formato1 INTEGER UNIQUE,          -- ID legado del Excel/CSV
    nombre_tecnico VARCHAR(100) NOT NULL UNIQUE, -- Clave para búsquedas (ej. 'whp_psi')
    tabla_origen VARCHAR(50),            -- 'stage.tbl_pozo_produccion'
    clasificacion_logica VARCHAR(50),    -- 'INPUT_SENSOR', 'CALCULADO', 'KPI'
    volatilidad VARCHAR(20),             -- 'ALTA', 'BAJA'
    unidad_id INTEGER REFERENCES referencial.tbl_ref_unidades(unidad_id)
);

-- =============================================================================
-- 3. REGLAS DE NEGOCIO Y CONFIGURACIÓN DINÁMICA
-- =============================================================================

-- 3.1 Límites y Targets por Pozo (Configuración de Alarmas)
CREATE TABLE referencial.tbl_limites_pozo (
    limite_id SERIAL PRIMARY KEY,
    pozo_id INT NOT NULL, -- Referencia lógica a reporting.dim_pozo
    variable_id INT NOT NULL REFERENCES referencial.tbl_maestra_variables(variable_id),
    
    -- Metas y Referencias
    target_value DECIMAL(12,4),   -- Valor ideal (Target)
    baseline_value DECIMAL(12,4), -- Valor base histórico
    tolerancia_variacion_pct DECIMAL(5,2) DEFAULT 10.0, -- [NUEVO] % permitido antes de alertar
    
    -- Umbrales para Semáforo (Rojo/Amarillo/Verde)
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

-- 3.2 Reglas de Calidad de Datos (DQ)
CREATE TABLE referencial.tbl_dq_rules (
    regla_id SERIAL PRIMARY KEY,
    variable_id INTEGER REFERENCES referencial.tbl_maestra_variables(variable_id),
    valor_min DECIMAL(12, 4),
    valor_max DECIMAL(12, 4),
    latencia_max_segundos INTEGER DEFAULT 300,
    severidad VARCHAR(20) DEFAULT 'WARNING',
    origen_regla VARCHAR(100) DEFAULT 'Excel Matriz Calidad' -- [Solicitado]
);

-- 3.3 Reglas de Consistencia (Física)
CREATE TABLE referencial.tbl_reglas_consistencia (
    codigo_rc VARCHAR(20) PRIMARY KEY,
    descripcion TEXT,
    criterio_texto TEXT,
    variable_a_id INTEGER REFERENCES referencial.tbl_maestra_variables(variable_id),
    operador VARCHAR(5),
    variable_b_id INTEGER REFERENCES referencial.tbl_maestra_variables(variable_id)
);

-- =============================================================================
-- 4. DATOS SEMILLA (CONFIGURACIÓN VISUAL ESTÁNDAR)
-- =============================================================================
INSERT INTO referencial.tbl_ref_estados_operativos 
(codigo_estado, color_hex, descripcion, prioridad_visual, nivel_severidad, icono_web) VALUES
('NORMAL',   '#00C851', 'Operación Óptima', 5, 0, 'check_circle'),
('WARNING',  '#FFBB33', 'Atención Requerida', 3, 1, 'warning'),
('CRITICAL', '#FF4444', 'Fuera de Rango Crítico', 1, 3, 'error'),
('OFFLINE',  '#B0B0B0', 'Sin Comunicación', 4, -1, 'wifi_off'),
('UNKNOWN',  '#33B5E5', 'Sin Datos / Mantenimiento', 2, 0, 'help');

-- =============================================================================
-- 5. MOTOR LÓGICO (FUNCIONES DE ESTANDARIZACIÓN)
-- =============================================================================

/*
--------------------------------------------------------------------------------
-- FUNCIÓN: fnc_evaluar_kpi
-- DESCRIPCIÓN: Recibe un valor y sus límites, devuelve el objeto de estado completo.
--------------------------------------------------------------------------------
*/
CREATE OR REPLACE FUNCTION referencial.fnc_evaluar_kpi(
    p_valor DECIMAL,
    p_min_crit DECIMAL,
    p_min_warn DECIMAL,
    p_max_warn DECIMAL,
    p_max_crit DECIMAL
)
RETURNS TABLE (
    codigo_estado VARCHAR,
    color_hex VARCHAR,
    nivel_severidad INT
) 
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_estado_key VARCHAR;
BEGIN
    -- 1. Manejo de Nulos
    IF p_valor IS NULL THEN
        v_estado_key := 'UNKNOWN';
    
    -- 2. Evaluación de Rangos (Lógica de Negocio Centralizada)
    ELSIF (p_max_crit IS NOT NULL AND p_valor >= p_max_crit) OR 
          (p_min_crit IS NOT NULL AND p_valor <= p_min_crit) THEN
        v_estado_key := 'CRITICAL';
        
    ELSIF (p_max_warn IS NOT NULL AND p_valor >= p_max_warn) OR 
          (p_min_warn IS NOT NULL AND p_valor <= p_min_warn) THEN
        v_estado_key := 'WARNING';
        
    ELSE
        v_estado_key := 'NORMAL';
    END IF;

    -- 3. Retornar propiedades
    RETURN QUERY
    SELECT eo.codigo_estado, eo.color_hex, eo.nivel_severidad
    FROM referencial.tbl_ref_estados_operativos eo
    WHERE eo.codigo_estado = v_estado_key;
END;
$$;

/*
--------------------------------------------------------------------------------
-- FUNCIÓN: fnc_evaluar_variacion
-- DESCRIPCIÓN: Calcula % de variación y determina si es crítico según tolerancia.
--------------------------------------------------------------------------------
*/
CREATE OR REPLACE FUNCTION referencial.fnc_evaluar_variacion(
    p_valor_actual DECIMAL,
    p_valor_target DECIMAL,
    p_tolerancia_pct DECIMAL
)
RETURNS TABLE (
    variacion_pct DECIMAL,
    color_variacion VARCHAR,
    es_desviacion_critica BOOLEAN
)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_var_pct DECIMAL;
    v_estado_key VARCHAR;
BEGIN
    -- Evitar división por cero
    IF p_valor_target IS NULL OR p_valor_target = 0 THEN
        RETURN QUERY SELECT 0.0::DECIMAL, '#B0B0B0'::VARCHAR, FALSE;
        RETURN;
    END IF;

    -- Calcular variación
    v_var_pct := ((p_valor_actual - p_valor_target) / p_valor_target) * 100.0;

    -- Evaluar contra tolerancia
    IF ABS(v_var_pct) > COALESCE(p_tolerancia_pct, 10.0) THEN
        v_estado_key := 'CRITICAL';
    ELSE
        v_estado_key := 'NORMAL';
    END IF;

    RETURN QUERY
    SELECT 
        ROUND(v_var_pct, 2),
        (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = v_estado_key),
        (v_estado_key = 'CRITICAL');
END;
$$;