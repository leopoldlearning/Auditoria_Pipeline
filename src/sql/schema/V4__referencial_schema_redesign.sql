/*
--------------------------------------------------------------------------------
-- ESQUEMA REFERENCIAL V4 (MASTER STANDARD & INTEGRADO)
-- DESCRIPCIÓN: Configuración centralizada de Reglas, Límites y Estándares Visuales.
-- INCLUYE: 
--   1. Tablas Dimensionales (Unidades, Estados, Paneles)
--   2. Maestra de Variables (Alineada a Reporting V4)
--   3. Mapa SCADA (Integración Stage - Anterior referencial_master.sql)
--   4. Reglas de Negocio (Límites Pozo, DQ, Consistencia, Mapa DQ↔RC)
--   5. Funciones Utilitarias (Motores de Cálculo Semáforo)
--------------------------------------------------------------------------------
*/

-- 1. GESTIÓN DE ESQUEMA (Reset Limpio)
DROP SCHEMA IF EXISTS referencial CASCADE;
CREATE SCHEMA referencial;

-- =============================================================================
-- 2. TABLAS DIMENSIONALES (CATÁLOGOS)
-- =============================================================================

-- 2.1 Unidades de Medida (Estandarizadas desde 06_unidades_standar.csv)
CREATE TABLE referencial.tbl_ref_unidades (
    unidad_id SERIAL PRIMARY KEY,
    simbolo VARCHAR(20) NOT NULL UNIQUE, -- Abreviatura canónica: 'psi', 'bbl', 'ft', 'mD'
    nombre VARCHAR(100) NOT NULL,       -- Nombre completo: 'libra por pulgada cuadrada'
    descripcion VARCHAR(200)             -- Descripción adicional (opcional)
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
    id_formato1 INTEGER,          -- ID legado del Excel/CSV (Permite duplicados según lista usuario)
    nombre_tecnico VARCHAR(100) NOT NULL UNIQUE, -- Clave para búsquedas (ej. 'well_head_pressure_psi_act')
    tabla_origen VARCHAR(50),            -- 'stage.tbl_pozo_produccion'
    clasificacion_logica VARCHAR(50),    -- 'SENSOR', 'CALCULADO', 'KPI'
    volatilidad VARCHAR(20),             -- 'ALTA', 'BAJA'
    unidad_id INTEGER REFERENCES referencial.tbl_ref_unidades(unidad_id),
    
    -- Metadatos para Dashboard (Alineado a hoja_validacion.csv)
    panel_id INTEGER REFERENCES referencial.tbl_ref_paneles_bi(panel_id),
    ident_dashboard_element VARCHAR(50) -- Número o identificador del elemento en el panel
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
    regla_id SERIAL PRIMARY KEY,
    codigo_regla VARCHAR(20) NOT NULL UNIQUE,
    nombre_regla VARCHAR(100) NOT NULL,
    categoria VARCHAR(50),
    
    -- Variable que se mide/evalúa vs Variable de referencia/límite
    variable_medida_id INTEGER NOT NULL 
        REFERENCES referencial.tbl_maestra_variables(variable_id),
    operador_comparacion VARCHAR(10) NOT NULL 
        CHECK (operador_comparacion IN ('>', '<', '>=', '<=', '=', '!=')),
    variable_referencia_id INTEGER NOT NULL 
        REFERENCES referencial.tbl_maestra_variables(variable_id),
    
    -- Metadatos
    severidad VARCHAR(20) DEFAULT 'MEDIUM' NOT NULL
        CHECK (severidad IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    activo BOOLEAN DEFAULT TRUE NOT NULL,
    descripcion TEXT,
    
    -- Auditoría
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3.4 Mapa Variable ↔ Regla de Consistencia (Junction Table)
-- Vincula cada variable al conjunto de reglas de consistencia física
-- que debe cumplir, según la columna "Reglas de Calidad: Consistencia"
-- del archivo 02_reglas_calidad.csv.
-- Ejemplo: id_formato1=151 (FBHP) → debe cumplir RC-003 y RC-004.
CREATE TABLE referencial.tbl_dq_consistencia_map (
    variable_id INTEGER NOT NULL
        REFERENCES referencial.tbl_maestra_variables(variable_id),
    regla_consistencia_id INTEGER NOT NULL
        REFERENCES referencial.tbl_reglas_consistencia(regla_id),
    PRIMARY KEY (variable_id, regla_consistencia_id)
);

COMMENT ON TABLE referencial.tbl_dq_consistencia_map IS
'Tabla de cruce: vincula variables con las reglas de consistencia física (RC-001..RC-006) que les aplican, según 02_reglas_calidad.csv.';

-- Vista legible para consultas
CREATE OR REPLACE VIEW referencial.vw_reglas_consistencia_legible AS
SELECT 
    rc.regla_id,
    rc.codigo_regla,
    rc.nombre_regla,
    rc.categoria,
    mv_med.nombre_tecnico AS variable_medida,
    COALESCE(u_med.simbolo, '') AS unidad_medida,
    rc.operador_comparacion,
    mv_ref.nombre_tecnico AS variable_referencia,
    COALESCE(u_ref.simbolo, '') AS unidad_referencia,
    CONCAT(
        mv_med.nombre_tecnico, 
        ' ', rc.operador_comparacion, ' ', 
        mv_ref.nombre_tecnico
    ) AS expresion_formula,
    CONCAT(
        mv_med.nombre_tecnico, 
        CASE WHEN u_med.simbolo IS NOT NULL THEN ' (' || u_med.simbolo || ')' ELSE '' END,
        ' ', rc.operador_comparacion, ' ',
        mv_ref.nombre_tecnico,
        CASE WHEN u_ref.simbolo IS NOT NULL THEN ' (' || u_ref.simbolo || ')' ELSE '' END
    ) AS expresion_completa,
    rc.severidad,
    rc.activo,
    rc.descripcion,
    rc.fecha_creacion,
    rc.fecha_actualizacion
FROM referencial.tbl_reglas_consistencia rc
INNER JOIN referencial.tbl_maestra_variables mv_med 
    ON rc.variable_medida_id = mv_med.variable_id
INNER JOIN referencial.tbl_maestra_variables mv_ref 
    ON rc.variable_referencia_id = mv_ref.variable_id
LEFT JOIN referencial.tbl_ref_unidades u_med 
    ON mv_med.unidad_id = u_med.unidad_id
LEFT JOIN referencial.tbl_ref_unidades u_ref 
    ON mv_ref.unidad_id = u_ref.unidad_id
WHERE rc.activo = TRUE;

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
WITH DistinctWells AS (
    SELECT well_id as pozo_id FROM stage.tbl_pozo_maestra
    UNION
    SELECT DISTINCT pozo_id FROM referencial.tbl_limites_pozo
),
VariablesToPivot AS (
    -- ID Formato 1: 54 (WHP), 55 (CASING), 127 (SPM), 48 (FILL), 121 (ROAD LOAD)
    SELECT variable_id, id_formato1 FROM referencial.tbl_maestra_variables 
    WHERE id_formato1 IN (54, 55, 127, 48, 121)
)
SELECT 
    dw.pozo_id,
    -- WHP (ID: 54)
    MAX(CASE WHEN v.id_formato1 = 54 THEN COALESCE(l_spec.min_critical, l_def.min_critical) END) as whp_min_crit,
    MAX(CASE WHEN v.id_formato1 = 54 THEN COALESCE(l_spec.min_warning, l_def.min_warning) END) as whp_min_warn,
    MAX(CASE WHEN v.id_formato1 = 54 THEN COALESCE(l_spec.max_warning, l_def.max_warning) END) as whp_max_warn,
    MAX(CASE WHEN v.id_formato1 = 54 THEN COALESCE(l_spec.max_critical, l_def.max_critical) END) as whp_max_crit,
    -- CASING (ID: 55)
    MAX(CASE WHEN v.id_formato1 = 55 THEN COALESCE(l_spec.max_warning, l_def.max_warning) END) as casing_max_warn,
    -- SPM (ID: 127) - Target and Tolerance
    MAX(CASE WHEN v.id_formato1 = 127 THEN COALESCE(l_spec.target_value, l_def.target_value) END) as spm_target,
    MAX(CASE WHEN v.id_formato1 = 127 THEN COALESCE(l_spec.tolerancia_variacion_pct, l_def.tolerancia_variacion_pct) END) as spm_tol,
    -- FILL (ID: 48) - Limits and Target
    MAX(CASE WHEN v.id_formato1 = 48 THEN COALESCE(l_spec.min_critical, l_def.min_critical) END) as fill_min_crit,
    MAX(CASE WHEN v.id_formato1 = 48 THEN COALESCE(l_spec.min_warning, l_def.min_warning) END) as fill_min_warn,
    MAX(CASE WHEN v.id_formato1 = 48 THEN COALESCE(l_spec.max_warning, l_def.max_warning) END) as fill_max_warn,
    MAX(CASE WHEN v.id_formato1 = 48 THEN COALESCE(l_spec.max_critical, l_def.max_critical) END) as fill_max_crit,
    MAX(CASE WHEN v.id_formato1 = 48 THEN COALESCE(l_spec.target_value, l_def.target_value) END) as fill_target_val,
    -- ROAD LOAD (ID: 121) - [NEW]
    MAX(CASE WHEN v.id_formato1 = 121 THEN COALESCE(l_spec.min_warning, l_def.min_warning) END) as rl_min_warn,
    MAX(CASE WHEN v.id_formato1 = 121 THEN COALESCE(l_spec.max_warning, l_def.max_warning) END) as rl_max_warn
FROM DistinctWells dw
CROSS JOIN VariablesToPivot v
LEFT JOIN referencial.tbl_limites_pozo l_spec 
    ON l_spec.variable_id = v.variable_id AND l_spec.pozo_id = dw.pozo_id
LEFT JOIN referencial.tbl_limites_pozo l_def 
    ON l_def.variable_id = v.variable_id AND l_def.pozo_id = 1 -- Fallback to Template
GROUP BY dw.pozo_id;

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


-- =============================================================================
-- SP: SEED DEFAULTS — Completa datos faltantes en tablas referenciales
-- Idempotente: solo actualiza NULL → valor derivado. Seguro para re-run.
-- =============================================================================
CREATE OR REPLACE PROCEDURE referencial.sp_seed_defaults()
LANGUAGE plpgsql AS $$
BEGIN
    -- 1. tbl_limites_pozo: baseline = target, critical = warning * factor
    UPDATE referencial.tbl_limites_pozo
    SET 
        baseline_value = COALESCE(baseline_value, target_value),
        min_critical   = COALESCE(min_critical, 
            CASE WHEN min_warning IS NOT NULL AND min_warning > 0 THEN min_warning * 0.5
                 WHEN min_warning = 0 THEN 0 ELSE NULL END),
        max_critical   = COALESCE(max_critical, 
            CASE WHEN max_warning IS NOT NULL THEN max_warning * 1.5 ELSE NULL END)
    WHERE baseline_value IS NULL OR min_critical IS NULL OR max_critical IS NULL;

    -- 2. tbl_maestra_variables: tabla_origen y volatilidad por clasificacion
    UPDATE referencial.tbl_maestra_variables
    SET 
        tabla_origen = COALESCE(tabla_origen,
            CASE clasificacion_logica
                WHEN 'SENSOR' THEN 'landing_scada_data'
                WHEN 'DISEÑO' THEN 'tbl_pozo_maestra'
                WHEN 'KPI'    THEN 'dataset_current_values'
                ELSE 'otros' END),
        volatilidad = COALESCE(volatilidad,
            CASE clasificacion_logica
                WHEN 'SENSOR' THEN 'ALTA'
                WHEN 'DISEÑO' THEN 'BAJA'
                WHEN 'KPI'    THEN 'MEDIA'
                ELSE 'MEDIA' END)
    WHERE tabla_origen IS NULL OR volatilidad IS NULL;

    -- 3. tbl_limites_pozo: corregir target kWh/bbl si viene de ejemplo del Rangos file
    --    El campo 'ejemplo' del archivo Rangos suele tener ~2.0 (valor de muestra),
    --    pero el target operativo real de industria es ~10.0 kWh/bbl.
    UPDATE referencial.tbl_limites_pozo lp
    SET target_value = 10.0
    WHERE lp.variable_id = (
        SELECT v.variable_id FROM referencial.tbl_maestra_variables v 
        WHERE v.nombre_tecnico = 'kpi_kwh_bbl' OR v.id_formato1 = 49 
        LIMIT 1
    )
    AND lp.target_value IS NOT NULL
    AND lp.target_value < 5.0;  -- solo corregir si parece un valor de ejemplo

    -- 4. tbl_ref_paneles_bi: descripcion
    UPDATE referencial.tbl_ref_paneles_bi
    SET descripcion = COALESCE(descripcion,
        CASE panel_id
            WHEN 1 THEN 'Panel de KPIs principales del pozo'
            WHEN 2 THEN 'Panel de monitoreo de equipos y sensores'
            WHEN 3 THEN 'Panel de producción y presiones'
            WHEN 4 THEN 'Panel de información general del pozo'
            ELSE 'Panel de operaciones #' || panel_id::TEXT END)
    WHERE descripcion IS NULL;

    RAISE NOTICE '[SEED] Defaults referenciales completados';
END;
$$;

COMMENT ON PROCEDURE referencial.sp_seed_defaults IS 
'Completa datos faltantes en tbl_limites_pozo, tbl_maestra_variables, tbl_ref_paneles_bi.
Idempotente: solo actualiza columnas NULL. Seguro para re-run.';