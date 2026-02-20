/*
================================================================================
V7 - SISTEMA UNIVERSAL DE CLASIFICACIÓN Y SEMÁFOROS
================================================================================
Fecha: 2026-02-09
Propósito: Sistema configurable para evaluar cualquier variable/KPI contra
           targets o baselines, asignando automáticamente status, severity y color.
================================================================================

FILOSOFÍA:
─────────────────────────────────────────────────────────────────────────────
1. CADA VARIABLE define su modo de comparación (target/baseline)
2. CADA VARIABLE define su dirección (mayor_mejor / menor_mejor)
3. LOS UMBRALES son configurables por variable
4. LA FUNCIÓN de evaluación es ÚNICA y UNIVERSAL
5. EL RESULTADO incluye: status_level, status_label, severity_label, color

================================================================================
*/

-- =============================================================================
-- 1. TABLA DE CONFIGURACIÓN DE EVALUACIÓN POR VARIABLE
-- =============================================================================
DROP TABLE IF EXISTS referencial.tbl_config_evaluacion CASCADE;

CREATE TABLE referencial.tbl_config_evaluacion (
    config_id SERIAL PRIMARY KEY,
    
    -- Identificación de la variable
    variable_nombre VARCHAR(100) NOT NULL UNIQUE,  -- Ej: 'pump_fill_monitor', 'kpi_mtbf', 'road_load'
    variable_descripcion TEXT,
    categoria VARCHAR(50),                          -- 'OPERATIVO', 'KPI', 'EFICIENCIA', 'CARGA', 'PRESION'
    
    -- Modo de comparación
    comparar_contra VARCHAR(10) NOT NULL DEFAULT 'TARGET',  -- 'TARGET' o 'BASELINE'
    direccion VARCHAR(15) NOT NULL DEFAULT 'MAYOR_MEJOR',   -- 'MAYOR_MEJOR' o 'MENOR_MEJOR'
    
    -- Umbrales de variación (% respecto al target/baseline)
    -- Para MAYOR_MEJOR: variance >= optimal es óptimo, variance < critical es crítico
    -- Para MENOR_MEJOR: variance <= optimal es óptimo, variance > critical es crítico
    
    umbral_optimo_pct DECIMAL(5,2) DEFAULT 10.00,      -- Variance >= +10% del target = ÓPTIMO
    umbral_normal_pct DECIMAL(5,2) DEFAULT 0.00,       -- Variance >= 0% = NORMAL (cumple)
    umbral_warning_pct DECIMAL(5,2) DEFAULT -10.00,    -- Variance >= -10% = WARNING
    umbral_critical_pct DECIMAL(5,2) DEFAULT -20.00,   -- Variance >= -20% = CRITICAL
    -- Por debajo de critical = FAILURE
    
    -- Colores asignados a cada nivel
    color_optimo VARCHAR(7) DEFAULT '#00CC66',      -- Verde brillante
    color_normal VARCHAR(7) DEFAULT '#99CC00',      -- Verde lima
    color_warning VARCHAR(7) DEFAULT '#FFBB33',     -- Naranja
    color_critical VARCHAR(7) DEFAULT '#FF4444',    -- Rojo
    color_failure VARCHAR(7) DEFAULT '#CC0000',     -- Rojo oscuro
    color_no_data VARCHAR(7) DEFAULT '#B0B0B0',     -- Gris
    color_maintenance VARCHAR(7) DEFAULT '#0088FF', -- Azul
    
    -- Metadatos
    unidad VARCHAR(20),
    activo BOOLEAN DEFAULT TRUE,
    fecha_creacion TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT chk_comparar CHECK (comparar_contra IN ('TARGET', 'BASELINE')),
    CONSTRAINT chk_direccion CHECK (direccion IN ('MAYOR_MEJOR', 'MENOR_MEJOR'))
);

-- Comentario
COMMENT ON TABLE referencial.tbl_config_evaluacion IS 
'Configuración de umbrales y colores para evaluación de variables/KPIs. 
Define cómo clasificar cada variable según su variación respecto a target o baseline.';

-- =============================================================================
-- 2. TABLA DE STATUS_LEVEL Y LABELS (Catálogo)
-- =============================================================================
DROP TABLE IF EXISTS referencial.tbl_catalogo_status CASCADE;

CREATE TABLE referencial.tbl_catalogo_status (
    status_level INTEGER PRIMARY KEY,
    status_label VARCHAR(20) NOT NULL,
    severity_label VARCHAR(20) NOT NULL,
    descripcion TEXT,
    prioridad_atencion INTEGER,  -- 1=máxima prioridad
    requiere_accion_inmediata BOOLEAN DEFAULT FALSE
);

INSERT INTO referencial.tbl_catalogo_status VALUES
    (0, 'Óptimo',       'Excelente',   'Rendimiento excepcional, supera expectativas', 6, FALSE),
    (1, 'Normal',       'Normal',      'Operando dentro de parámetros aceptables',     5, FALSE),
    (2, 'Degradado',    'Bajo',        'Operando pero con pérdida de eficiencia',      4, FALSE),
    (3, 'Alerta',       'Medio',       'Cerca del límite, requiere atención pronto',   3, FALSE),
    (4, 'Crítico',      'Alto',        'Fuera de límites, acción prioritaria hoy',     2, TRUE),
    (5, 'En Falla',     'Crítico',     'Falla activa detectada',                       1, TRUE),
    (6, 'Emergencia',   'Emergencia',  'Escalamiento a supervisión inmediato',         0, TRUE),
    (7, 'Sin Datos',    'Sin Datos',   'No hay telemetría disponible',                 4, FALSE),
    (8, 'Mantenimiento','Info',        'Parada programada',                            7, FALSE),
    (9, 'Arrancando',   'Info',        'En proceso de arranque',                       5, FALSE);

-- =============================================================================
-- 3. POBLAR CONFIGURACIÓN INICIAL POR VARIABLE
-- =============================================================================
INSERT INTO referencial.tbl_config_evaluacion (
    variable_nombre, variable_descripcion, categoria, 
    comparar_contra, direccion,
    umbral_optimo_pct, umbral_normal_pct, umbral_warning_pct, umbral_critical_pct,
    unidad
) VALUES
    -- === KPIs ===
    ('kpi_mtbf', 'Mean Time Between Failures', 'KPI', 
     'TARGET', 'MAYOR_MEJOR', 
     20.00, 0.00, -20.00, -50.00, 'horas'),
    
    ('kpi_uptime', 'Disponibilidad/Uptime', 'KPI', 
     'TARGET', 'MAYOR_MEJOR', 
     5.00, 0.00, -5.00, -10.00, '%'),
    
    ('kpi_kwh_bbl', 'Eficiencia Energética', 'KPI', 
     'TARGET', 'MENOR_MEJOR',  -- Menor consumo = mejor
     -10.00, 0.00, 20.00, 50.00, 'kWh/bbl'),  -- Invertido para MENOR_MEJOR
    
    ('kpi_vol_eff', 'Eficiencia Volumétrica', 'KPI', 
     'TARGET', 'MAYOR_MEJOR', 
     10.00, 0.00, -15.00, -30.00, '%'),
    
    -- === OPERATIVOS - Presiones ===
    ('well_head_pressure', 'Presión de Cabezal (WHP)', 'PRESION', 
     'BASELINE', 'MAYOR_MEJOR', 
     10.00, -5.00, -15.00, -30.00, 'psi'),
    
    ('pump_intake_pressure', 'Presión de Intake (PIP)', 'PRESION', 
     'BASELINE', 'MAYOR_MEJOR', 
     10.00, -5.00, -15.00, -30.00, 'psi'),
    
    ('casing_head_pressure', 'Presión de Casing (CHP)', 'PRESION', 
     'BASELINE', 'MAYOR_MEJOR', 
     10.00, -10.00, -20.00, -40.00, 'psi'),
    
    -- === OPERATIVOS - Llenado/Eficiencia ===
    ('pump_fill_monitor', 'Llenado de Bomba', 'EFICIENCIA', 
     'TARGET', 'MAYOR_MEJOR', 
     10.00, 0.00, -10.00, -30.00, '%'),
    
    ('gas_fill_monitor', 'Llenado de Gas', 'EFICIENCIA', 
     'TARGET', 'MENOR_MEJOR',  -- Menos gas = mejor
     -10.00, 0.00, 15.00, 30.00, '%'),
    
    ('lift_efficiency', 'Eficiencia de Levantamiento', 'EFICIENCIA', 
     'TARGET', 'MAYOR_MEJOR', 
     10.00, 0.00, -15.00, -30.00, '%'),
    
    -- === OPERATIVOS - Cargas ===
    ('road_load', 'Carga Road Load', 'CARGA', 
     'TARGET', 'MAYOR_MEJOR',  -- Dentro del rango
     5.00, -5.00, -15.00, -25.00, '%'),
    
    ('hydraulic_load', 'Carga Hidráulica', 'CARGA', 
     'TARGET', 'MAYOR_MEJOR', 
     5.00, -5.00, -15.00, -25.00, '%'),
    
    ('carga_unidad', 'Carga de Unidad', 'CARGA', 
     'TARGET', 'MENOR_MEJOR',  -- Menos carga = mejor (no sobrecargado)
     -10.00, 0.00, 10.00, 25.00, '%'),
    
    -- === OPERATIVOS - SPM ===
    ('pump_spm', 'Strokes Por Minuto', 'OPERATIVO', 
     'TARGET', 'MAYOR_MEJOR', 
     5.00, -5.00, -15.00, -30.00, 'spm'),
    
    -- === OPERATIVOS - Temperatura ===
    ('tank_fluid_temperature', 'Temperatura de Tanque', 'OPERATIVO', 
     'TARGET', 'MENOR_MEJOR',  -- Menor temp = mejor (no sobrecalentamiento)
     -10.00, 0.00, 15.00, 30.00, 'F'),
    
    -- === OPERATIVOS - Downtime ===
    ('daily_downtime', 'Tiempo de Paro Diario', 'OPERATIVO', 
     'TARGET', 'MENOR_MEJOR',  -- Menos paro = mejor
     -20.00, 0.00, 30.00, 60.00, 'min'),
    
    -- === AI ===
    ('ai_accuracy', 'Precisión del Modelo IA', 'KPI', 
     'TARGET', 'MAYOR_MEJOR', 
     5.00, 0.00, -10.00, -25.00, '%')
    
ON CONFLICT (variable_nombre) DO UPDATE SET
    variable_descripcion = EXCLUDED.variable_descripcion,
    categoria = EXCLUDED.categoria,
    comparar_contra = EXCLUDED.comparar_contra,
    direccion = EXCLUDED.direccion,
    umbral_optimo_pct = EXCLUDED.umbral_optimo_pct,
    umbral_normal_pct = EXCLUDED.umbral_normal_pct,
    umbral_warning_pct = EXCLUDED.umbral_warning_pct,
    umbral_critical_pct = EXCLUDED.umbral_critical_pct,
    unidad = EXCLUDED.unidad;

-- =============================================================================
-- 4. FUNCIÓN UNIVERSAL DE EVALUACIÓN
-- =============================================================================
DROP FUNCTION IF EXISTS referencial.fnc_evaluar_variable CASCADE;

CREATE OR REPLACE FUNCTION referencial.fnc_evaluar_variable(
    p_variable_nombre VARCHAR(100),
    p_valor_actual DECIMAL,
    p_valor_target DECIMAL DEFAULT NULL,
    p_valor_baseline DECIMAL DEFAULT NULL,
    p_override_direccion VARCHAR(15) DEFAULT NULL  -- Permite override manual
)
RETURNS TABLE (
    variance_pct DECIMAL(10,2),
    status_level INTEGER,
    status_label VARCHAR(20),
    severity_label VARCHAR(20),
    status_color VARCHAR(7),
    comparado_contra VARCHAR(10),
    valor_referencia DECIMAL
) AS $$
DECLARE
    v_config RECORD;
    v_referencia DECIMAL;
    v_variance DECIMAL;
    v_level INTEGER;
    v_direccion VARCHAR(15);
BEGIN
    -- Obtener configuración de la variable
    SELECT * INTO v_config 
    FROM referencial.tbl_config_evaluacion 
    WHERE variable_nombre = p_variable_nombre AND activo = TRUE;
    
    -- Si no existe configuración, usar defaults
    IF NOT FOUND THEN
        v_config.comparar_contra := 'TARGET';
        v_config.direccion := 'MAYOR_MEJOR';
        v_config.umbral_optimo_pct := 10;
        v_config.umbral_normal_pct := 0;
        v_config.umbral_warning_pct := -10;
        v_config.umbral_critical_pct := -20;
        v_config.color_optimo := '#00CC66';
        v_config.color_normal := '#99CC00';
        v_config.color_warning := '#FFBB33';
        v_config.color_critical := '#FF4444';
        v_config.color_failure := '#CC0000';
        v_config.color_no_data := '#B0B0B0';
    END IF;
    
    -- Permitir override de dirección
    v_direccion := COALESCE(p_override_direccion, v_config.direccion);
    
    -- Si no hay valor actual, retornar NO_DATA
    IF p_valor_actual IS NULL THEN
        RETURN QUERY SELECT 
            NULL::DECIMAL, 7, 'Sin Datos'::VARCHAR(20), 'Sin Datos'::VARCHAR(20), 
            v_config.color_no_data, 'NONE'::VARCHAR(10), NULL::DECIMAL;
        RETURN;
    END IF;
    
    -- Determinar valor de referencia (target o baseline)
    IF v_config.comparar_contra = 'TARGET' THEN
        v_referencia := p_valor_target;
    ELSE
        v_referencia := p_valor_baseline;
    END IF;
    
    -- Si no hay referencia, no se puede evaluar
    IF v_referencia IS NULL OR v_referencia = 0 THEN
        RETURN QUERY SELECT 
            NULL::DECIMAL, 1, 'Normal'::VARCHAR(20), 'Normal'::VARCHAR(20), 
            v_config.color_normal, v_config.comparar_contra, NULL::DECIMAL;
        RETURN;
    END IF;
    
    -- Calcular variación porcentual
    v_variance := ROUND(((p_valor_actual - v_referencia) / v_referencia) * 100, 2);
    
    -- Determinar status_level según dirección
    IF v_direccion = 'MAYOR_MEJOR' THEN
        -- Mayor valor = mejor (MTBF, Uptime, Eficiencias)
        IF v_variance >= v_config.umbral_optimo_pct THEN
            v_level := 0; -- ÓPTIMO
        ELSIF v_variance >= v_config.umbral_normal_pct THEN
            v_level := 1; -- NORMAL
        ELSIF v_variance >= v_config.umbral_warning_pct THEN
            v_level := 3; -- ALERTA
        ELSIF v_variance >= v_config.umbral_critical_pct THEN
            v_level := 4; -- CRÍTICO
        ELSE
            v_level := 5; -- EN FALLA
        END IF;
    ELSE
        -- Menor valor = mejor (kWh/bbl, Temperatura, Gas Fill)
        IF v_variance <= v_config.umbral_optimo_pct THEN
            v_level := 0; -- ÓPTIMO (está por debajo del target = eficiente)
        ELSIF v_variance <= v_config.umbral_normal_pct THEN
            v_level := 1; -- NORMAL
        ELSIF v_variance <= v_config.umbral_warning_pct THEN
            v_level := 3; -- ALERTA (excediendo)
        ELSIF v_variance <= v_config.umbral_critical_pct THEN
            v_level := 4; -- CRÍTICO
        ELSE
            v_level := 5; -- EN FALLA
        END IF;
    END IF;
    
    -- Retornar resultado
    RETURN QUERY 
    SELECT 
        v_variance,
        v_level,
        cs.status_label,
        cs.severity_label,
        CASE v_level
            WHEN 0 THEN v_config.color_optimo
            WHEN 1 THEN v_config.color_normal
            WHEN 3 THEN v_config.color_warning
            WHEN 4 THEN v_config.color_critical
            WHEN 5 THEN v_config.color_failure
            ELSE v_config.color_no_data
        END,
        v_config.comparar_contra,
        v_referencia
    FROM referencial.tbl_catalogo_status cs
    WHERE cs.status_level = v_level;
    
END;
$$ LANGUAGE plpgsql STABLE;

-- Comentario
COMMENT ON FUNCTION referencial.fnc_evaluar_variable IS 
'Función universal para evaluar cualquier variable/KPI.
Calcula variance_pct y asigna status_level, labels y color según configuración.
Uso: SELECT * FROM referencial.fnc_evaluar_variable(''kpi_mtbf'', 2500, 2000, NULL)';

-- =============================================================================
-- 5. VISTA DE CONFIGURACIÓN PARA CONSULTA RÁPIDA
-- =============================================================================
CREATE OR REPLACE VIEW referencial.vw_config_evaluacion_resumen AS
SELECT 
    ce.variable_nombre,
    ce.categoria,
    ce.comparar_contra,
    ce.direccion,
    CONCAT(
        CASE ce.direccion 
            WHEN 'MAYOR_MEJOR' THEN '↑ '
            ELSE '↓ '
        END,
        'vs ', ce.comparar_contra
    ) AS modo_evaluacion,
    CONCAT(
        'Óptimo: ', ce.umbral_optimo_pct, '% | ',
        'Normal: ', ce.umbral_normal_pct, '% | ',
        'Warning: ', ce.umbral_warning_pct, '% | ',
        'Critical: ', ce.umbral_critical_pct, '%'
    ) AS umbrales,
    ce.unidad,
    ce.activo
FROM referencial.tbl_config_evaluacion ce
ORDER BY ce.categoria, ce.variable_nombre;

-- =============================================================================
-- 6. EJEMPLO DE USO EN STORED PROCEDURE
-- =============================================================================

/*
-- Ejemplo de cómo usar la función en el SP de actualización:

UPDATE reporting.dataset_current_values dcv
SET 
    pump_fill_monitor_status_level = eval.status_level,
    pump_fill_monitor_status_label = eval.status_label,
    pump_fill_monitor_severity_label = eval.severity_label,
    pump_fill_monitor_status_color = eval.status_color,
    pump_fill_monitor_variance_pct = eval.variance_pct
FROM (
    SELECT 
        dcv2.well_id,
        (referencial.fnc_evaluar_variable(
            'pump_fill_monitor',
            dcv2.pump_fill_monitor_pct,
            lim.fill_target,  -- target desde límites
            NULL              -- baseline
        )).*
    FROM reporting.dataset_current_values dcv2
    LEFT JOIN referencial.vw_limites_pozo_pivot_v4 lim ON dcv2.well_id = lim.pozo_id
) eval
WHERE dcv.well_id = eval.well_id;

*/

-- =============================================================================
-- 7. TEST DE LA FUNCIÓN
-- =============================================================================
DO $$
DECLARE
    r RECORD;
BEGIN
    RAISE NOTICE '=== TEST FUNCIÓN EVALUACIÓN ===';
    
    -- Test MTBF (MAYOR_MEJOR)
    RAISE NOTICE 'MTBF 2500 vs Target 2000 (mayor es mejor):';
    FOR r IN SELECT * FROM referencial.fnc_evaluar_variable('kpi_mtbf', 2500, 2000, NULL) LOOP
        RAISE NOTICE '  Variance: %, Level: %, Label: %, Severity: %, Color: %', 
            r.variance_pct, r.status_level, r.status_label, r.severity_label, r.status_color;
    END LOOP;
    
    -- Test kWh/bbl (MENOR_MEJOR)
    RAISE NOTICE 'kWh/bbl 8 vs Target 10 (menor es mejor):';
    FOR r IN SELECT * FROM referencial.fnc_evaluar_variable('kpi_kwh_bbl', 8, 10, NULL) LOOP
        RAISE NOTICE '  Variance: %, Level: %, Label: %, Severity: %, Color: %', 
            r.variance_pct, r.status_level, r.status_label, r.severity_label, r.status_color;
    END LOOP;
    
    -- Test kWh/bbl alto (malo)
    RAISE NOTICE 'kWh/bbl 15 vs Target 10 (consumiendo más = malo):';
    FOR r IN SELECT * FROM referencial.fnc_evaluar_variable('kpi_kwh_bbl', 15, 10, NULL) LOOP
        RAISE NOTICE '  Variance: %, Level: %, Label: %, Severity: %, Color: %', 
            r.variance_pct, r.status_level, r.status_label, r.severity_label, r.status_color;
    END LOOP;
    
    -- Test sin datos
    RAISE NOTICE 'Pump Fill NULL:';
    FOR r IN SELECT * FROM referencial.fnc_evaluar_variable('pump_fill_monitor', NULL, 70, NULL) LOOP
        RAISE NOTICE '  Variance: %, Level: %, Label: %, Severity: %, Color: %', 
            r.variance_pct, r.status_level, r.status_label, r.severity_label, r.status_color;
    END LOOP;
END $$;

-- Fin del script
SELECT 'V7 - Sistema Universal de Clasificación cargado correctamente' AS resultado;
