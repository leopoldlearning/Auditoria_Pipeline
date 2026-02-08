/*
--------------------------------------------------------------------------------
-- ESQUEMA REFERENCIAL V4 (MASTER STANDARD & INTEGRADO)
-- DESCRIPCIÓN: Configuración centralizada de Reglas, Límites y Estándares Visuales.
-- INCLUYE: 
--   1. Tablas Dimensionales (Unidades, Estados, Paneles)
--   2. Maestra de Variables (Alineada a Reporting V4)
--   3. Mapa SCADA (Integración Stage - Anterior referencial_master.sql)
--   4. Reglas de Negocio (Límites Pozo, DQ, Consistencia)
--   5. Funciones Utilitarias (Motores de Cálculo Semáforo)
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
    nombre_tecnico VARCHAR(100) NOT NULL UNIQUE, -- Clave para búsquedas (ej. 'well_head_pressure_psi_act')
    tabla_origen VARCHAR(50),            -- 'stage.tbl_pozo_produccion'
    clasificacion_logica VARCHAR(50),    -- 'SENSOR', 'CALCULADO', 'KPI'
    volatilidad VARCHAR(20),             -- 'ALTA', 'BAJA'
    unidad_id INTEGER REFERENCES referencial.tbl_ref_unidades(unidad_id)
);

-- 2.5 Mapa SCADA → Formato1 → Stage (Integrado desde referencial_master.sql)
CREATE TABLE referencial.tbl_var_scada_map (
    var_id_scada INT PRIMARY KEY,
    id_formato1 INT NOT NULL,
    columna_stage VARCHAR(100) NOT NULL, -- Alineado con nombre_tecnico idealmente
    comentario TEXT
);

-- =============================================================================
-- 3. REGLAS DE NEGOCIO Y CONFIGURACIÓN DINÁMICA
-- =============================================================================

-- 3.1 Límites y Targets por Pozo (Configuración de Alarmas)
CREATE TABLE referencial.tbl_limites_pozo (
    limite_id SERIAL PRIMARY KEY,
    pozo_id INT NOT NULL, -- Referencia lógica a reporting.dim_pozo (INT, no FK dura para evitar ciclos)
    variable_id INT NOT NULL REFERENCES referencial.tbl_maestra_variables(variable_id),
    
    -- Metas y Referencias
    target_value DECIMAL(12,4),   -- Valor ideal (Target)
    baseline_value DECIMAL(12,4), -- Valor base histórico
    tolerancia_variacion_pct DECIMAL(5,2) DEFAULT 10.0, -- % permitido antes de alertar
    
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
    origen_regla VARCHAR(100) DEFAULT 'Excel Matriz Calidad'
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
-- 4. VISTAS Y UTILIDADES INTEGRADOS
-- =============================================================================

-- 4.1 Vista Unificada de Variables (Integrada desde referencial_master.sql)
CREATE OR REPLACE VIEW referencial.vw_variables_scada_stage AS
SELECT
    mv.variable_id,
    mv.id_formato1,
    mv.nombre_tecnico,
    mv.tabla_origen,
    mv.clasificacion_logica,
    mv.volatilidad,
    u.simbolo AS unidad,
    vsm.var_id_scada,
    vsm.columna_stage,
    vsm.comentario
FROM referencial.tbl_maestra_variables mv
LEFT JOIN referencial.tbl_var_scada_map vsm
       ON vsm.id_formato1 = mv.id_formato1
LEFT JOIN referencial.tbl_ref_unidades u
       ON u.unidad_id = mv.unidad_id;

-- 4.2 VISTA PIVOTEADA DE LÍMITES (V4) - [MOVIDA DESDE V6 PARA DISPONIBILIDAD INMEDIATA]
CREATE OR REPLACE VIEW referencial.vw_limites_pozo_pivot_v4 AS
SELECT 
    pozo_id,
    -- WHP (well_head_pressure_psi_act)
    MAX(CASE WHEN v.nombre_tecnico = 'well_head_pressure_psi_act' THEN l.min_critical END) as whp_min_crit,
    MAX(CASE WHEN v.nombre_tecnico = 'well_head_pressure_psi_act' THEN l.min_warning END) as whp_min_warn,
    MAX(CASE WHEN v.nombre_tecnico = 'well_head_pressure_psi_act' THEN l.max_warning END) as whp_max_warn,
    MAX(CASE WHEN v.nombre_tecnico = 'well_head_pressure_psi_act' THEN l.max_critical END) as whp_max_crit,
    -- SPM (pump_avg_spm_act)
    MAX(CASE WHEN v.nombre_tecnico = 'pump_avg_spm_act' THEN l.target_value END) as spm_target,
    MAX(CASE WHEN v.nombre_tecnico = 'pump_avg_spm_act' THEN l.tolerancia_variacion_pct END) as spm_tol,
    -- FILL (pump_fill_monitor_pct)
    MAX(CASE WHEN v.nombre_tecnico = 'pump_fill_monitor_pct' THEN l.min_critical END) as fill_min_crit,
    MAX(CASE WHEN v.nombre_tecnico = 'pump_fill_monitor_pct' THEN l.min_warning END) as fill_min_warn,
    MAX(CASE WHEN v.nombre_tecnico = 'pump_fill_monitor_pct' THEN l.max_warning END) as fill_max_warn,
    MAX(CASE WHEN v.nombre_tecnico = 'pump_fill_monitor_pct' THEN l.max_critical END) as fill_max_crit,
    MAX(CASE WHEN v.nombre_tecnico = 'pump_fill_monitor_pct' THEN l.target_value END) as fill_target_val,
    -- ROAD LOAD (rod_weight_buoyant_lb_act / carga_varilla_pct)
    MAX(CASE WHEN v.nombre_tecnico = 'carga_varilla_pct' THEN l.min_warning END) as rl_min_warn,
    MAX(CASE WHEN v.nombre_tecnico = 'carga_varilla_pct' THEN l.max_warning END) as rl_max_warn
FROM referencial.tbl_limites_pozo l
JOIN referencial.tbl_maestra_variables v ON l.variable_id = v.variable_id
GROUP BY pozo_id;

-- =============================================================================
-- 5. MOTOR LÓGICO BASE (FUNCIONES DE ESTANDARIZACIÓN)
-- Nota: Funciones complejas como fnc_evaluar_universal pueden residir aquí o en SPs
-- Se mantienen las funciones base de V3 por compatibilidad si se requieren
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

-- =============================================================================
-- 6. LÓGICA UNIVERSAL ZERO-CALC (Migrado de V6)
-- =============================================================================

-- 6.1 Tipo de Respuesta Estandarizada
DO $$ BEGIN
    CREATE TYPE referencial.ty_evaluacion_result AS (
        codigo_estado VARCHAR(20),      -- 'NORMAL', 'WARNING', 'CRITICAL'
        color_hex VARCHAR(7),           -- '#00C851', '#FFBB33', '#FF4444'
        nivel_severidad INT,            -- 0, 1, 3
        variacion_pct DECIMAL(10,2),    -- +15.5%
        es_parametro_critico BOOLEAN,   -- TRUE si falló un límite CRITICAL
        mensaje_diagnostico TEXT,       -- 'Alta Presión > 500'
        target_value DECIMAL(12,4)      -- El valor contra el que se comparó
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- 6.2 Función de Evaluación Universal (Zero-Calc Core)
CREATE OR REPLACE FUNCTION referencial.fnc_evaluar_universal(
    p_valor_actual DECIMAL,
    -- Rango Absoluto (KPI)
    p_min_crit DECIMAL DEFAULT NULL,
    p_min_warn DECIMAL DEFAULT NULL,
    p_max_warn DECIMAL DEFAULT NULL,
    p_max_crit DECIMAL DEFAULT NULL,
    -- Variación Relativa (Target)
    p_target DECIMAL DEFAULT NULL,
    p_tolerancia_pct DECIMAL DEFAULT NULL
)
RETURNS referencial.ty_evaluacion_result
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    v_res referencial.ty_evaluacion_result;
    v_estado_key VARCHAR := 'NORMAL';
BEGIN
    -- Inicializar valores
    v_res.codigo_estado := 'NORMAL';
    v_res.color_hex := '#00C851'; -- Green default
    v_res.nivel_severidad := 0;
    v_res.variacion_pct := 0;
    v_res.es_parametro_critico := FALSE;
    v_res.target_value := p_target;

    -- Si valor es NULL, retornar UNKNOWN
    IF p_valor_actual IS NULL THEN
        v_res.codigo_estado := 'UNKNOWN';
        v_res.color_hex := '#B0B0B0'; -- Grey
        RETURN v_res;
    END IF;

    -- A. EVALUACIÓN POR RANGOS ABSOLUTOS
    IF p_max_crit IS NOT NULL AND p_valor_actual >= p_max_crit THEN
        v_estado_key := 'CRITICAL';
        v_res.mensaje_diagnostico := 'Sobrepasa Máximo Crítico';
    ELSIF p_min_crit IS NOT NULL AND p_valor_actual <= p_min_crit THEN
        v_estado_key := 'CRITICAL';
        v_res.mensaje_diagnostico := 'Debajo Mínimo Crítico';
    ELSIF p_max_warn IS NOT NULL AND p_valor_actual >= p_max_warn THEN
        v_estado_key := 'WARNING';
        v_res.mensaje_diagnostico := 'Sobrepasa Máximo Advertencia';
    ELSIF p_min_warn IS NOT NULL AND p_valor_actual <= p_min_warn THEN
        v_estado_key := 'WARNING';
         v_res.mensaje_diagnostico := 'Debajo Mínimo Advertencia';
    END IF;

    -- B. EVALUACIÓN POR VARIACIÓN (Si no falló por rangos críticos)
    IF v_estado_key != 'CRITICAL' AND p_target IS NOT NULL AND p_target <> 0 THEN
        v_res.variacion_pct := ((p_valor_actual - p_target) / p_target) * 100.0;
        
        IF ABS(v_res.variacion_pct) > COALESCE(p_tolerancia_pct, 10.0) THEN
            -- Solo marcar como warning si es variación (discutible, puede ser crítico)
            -- Asumimos WARNING para variaciones salvo config contraria
            IF v_estado_key = 'NORMAL' THEN 
                v_estado_key := 'WARNING';
                v_res.mensaje_diagnostico := 'Desviación de Target > ' || p_tolerancia_pct || '%';
            END IF;
        END IF;
    END IF;

    -- C. ASIGNAR RESULTADOS FINALES
    v_res.codigo_estado := v_estado_key;
    IF v_estado_key = 'CRITICAL' THEN
        v_res.color_hex := '#FF4444';
        v_res.nivel_severidad := 3;
        v_res.es_parametro_critico := TRUE;
    ELSIF v_estado_key = 'WARNING' THEN
        v_res.color_hex := '#FFBB33';
        v_res.nivel_severidad := 1;
    ELSE
        v_res.color_hex := '#00C851';
        v_res.nivel_severidad := 0;
    END IF;

    RETURN v_res;
END;
$$;
