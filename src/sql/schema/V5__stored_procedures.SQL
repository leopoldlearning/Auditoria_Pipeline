/*
--------------------------------------------------------------------------------
-- MOTOR LÓGICO V5 (PRODUCTION READY - CLEAN VERSION)
-- Contiene:
--   - Tipo y función universal de evaluación
--   - Vista pivoteada de límites por pozo
--   - Motor de Calidad de Datos (DQ)
--   - Snapshot Zero-Calc (actualizar_current_values_v3)
-- NO CONTIENE:
--   - Ningún sp_load_to_reporting
--   - Ningún cálculo horario/diario/mensual
--------------------------------------------------------------------------------
*/

-- =============================================================================
-- 0. PRE-REQUISITOS (Tipos y Funciones)
-- =============================================================================

-- A. Tipo de Retorno
DROP TYPE IF EXISTS referencial.ty_evaluacion_result CASCADE;
CREATE TYPE referencial.ty_evaluacion_result AS (
    color_hex VARCHAR(7),
    nivel_severidad INT,
    variacion_pct DECIMAL(5,2),
    target_value DECIMAL(10,2)
);

-- B. Función Evaluadora Universal
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
    v_pct_diff DECIMAL;
BEGIN
    -- Inicializar valores por defecto (Gris/Unknown)
    v_res.color_hex := '#B0B0B0'; 
    v_res.nivel_severidad := -1;
    v_res.variacion_pct := 0;
    v_res.target_value := COALESCE(p_target, 0);

    -- CASO 1: Evaluación por Target (Variación %)
    IF p_target IS NOT NULL AND p_tolerancia_pct IS NOT NULL THEN
        IF p_target = 0 THEN
             v_res.color_hex := '#33B5E5'; 
        ELSE
             v_pct_diff := ((p_valor_actual - p_target) / p_target) * 100.0;
             v_res.variacion_pct := v_pct_diff;
             
             IF ABS(v_pct_diff) <= p_tolerancia_pct THEN
                 v_res.color_hex := '#00C851'; -- Verde
                 v_res.nivel_severidad := 0;
             ELSE
                 v_res.color_hex := '#FFBB33'; -- Amarillo
                 v_res.nivel_severidad := 2;
             END IF;
        END IF;
        RETURN v_res;
    END IF;

    -- CASO 2: Evaluación por Rangos Absolutos
    IF p_valor_actual IS NULL THEN
        RETURN v_res;
    END IF;

    IF (p_min_crit IS NOT NULL AND p_valor_actual <= p_min_crit) OR 
       (p_max_crit IS NOT NULL AND p_valor_actual >= p_max_crit) THEN
        v_res.color_hex := '#FF4444'; -- Rojo
        v_res.nivel_severidad := 3;
    ELSIF (p_min_warn IS NOT NULL AND p_valor_actual <= p_min_warn) OR 
          (p_max_warn IS NOT NULL AND p_valor_actual >= p_max_warn) THEN
        v_res.color_hex := '#FFBB33'; -- Amarillo
        v_res.nivel_severidad := 1;
    ELSE
        v_res.color_hex := '#00C851'; -- Verde
        v_res.nivel_severidad := 0;
    END IF;

    RETURN v_res;
END;
$$;

-- C. Vista Pivoteada de Límites por Pozo
CREATE OR REPLACE VIEW referencial.vw_limites_pozo_pivot AS
SELECT 
    pozo_id,
    -- WHP
    MAX(CASE WHEN v.nombre_tecnico = 'presion_cabezal' THEN l.min_critical END) as whp_min_crit,
    MAX(CASE WHEN v.nombre_tecnico = 'presion_cabezal' THEN l.min_warning END) as whp_min_warn,
    MAX(CASE WHEN v.nombre_tecnico = 'presion_cabezal' THEN l.max_warning END) as whp_max_warn,
    MAX(CASE WHEN v.nombre_tecnico = 'presion_cabezal' THEN l.max_critical END) as whp_max_crit,
    -- SPM
    MAX(CASE WHEN v.nombre_tecnico = 'spm_promedio' THEN l.target_value END) as spm_target,
    MAX(CASE WHEN v.nombre_tecnico = 'spm_promedio' THEN l.tolerancia_variacion_pct END) as spm_tol,
    -- FILL
    MAX(CASE WHEN v.nombre_tecnico = 'pump_fill_monitor' THEN l.min_critical END) as fill_min_crit,
    MAX(CASE WHEN v.nombre_tecnico = 'pump_fill_monitor' THEN l.min_warning END) as fill_min_warn,
    MAX(CASE WHEN v.nombre_tecnico = 'pump_fill_monitor' THEN l.max_warning END) as fill_max_warn,
    MAX(CASE WHEN v.nombre_tecnico = 'pump_fill_monitor' THEN l.max_critical END) as fill_max_crit,
    MAX(CASE WHEN v.nombre_tecnico = 'pump_fill_monitor' THEN l.target_value END) as fill_target_val
FROM referencial.tbl_limites_pozo l
JOIN referencial.tbl_maestra_variables v ON l.variable_id = v.variable_id
GROUP BY pozo_id;

-- =============================================================================
-- 1. MOTOR DE CALIDAD DE DATOS (DQ ENGINE)
-- =============================================================================
DROP PROCEDURE IF EXISTS stage.sp_execute_dq_validation(DATE, DATE);
DROP PROCEDURE IF EXISTS stage.sp_execute_dq_validation(DATE, DATE, INT);

CREATE OR REPLACE PROCEDURE stage.sp_execute_dq_validation(
    p_fecha_inicio DATE,
    p_fecha_fin DATE,
    p_well_id INT DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rule RECORD;
    v_sql_check TEXT;
    v_column_exists BOOLEAN;
BEGIN
    RAISE NOTICE 'Iniciando Motor de Calidad de Datos (DQ)...';

    FOR v_rule IN 
        SELECT r.regla_id, r.variable_id, r.valor_min, r.valor_max, v.nombre_tecnico
        FROM referencial.tbl_dq_rules r
        JOIN referencial.tbl_maestra_variables v ON r.variable_id = v.variable_id
    LOOP
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_schema = 'stage' 
              AND table_name = 'tbl_pozo_produccion' 
              AND column_name = v_rule.nombre_tecnico
        ) INTO v_column_exists;

        IF NOT v_column_exists THEN
            CONTINUE;
        END IF;

        v_sql_check := format(
            'INSERT INTO stage.tbl_pozo_scada_dq (
                produccion_id, timestamp_lectura, variable_id, regla_id,
                valor_observado, valor_esperado_min, valor_esperado_max, resultado_dq
            )
             SELECT 
                produccion_id, 
                timestamp_lectura, 
                %L::INT, 
                %L::INT, 
                %I::DECIMAL, 
                %L::DECIMAL, 
                %L::DECIMAL, 
                CASE 
                    WHEN %I < %L::DECIMAL OR %I > %L::DECIMAL THEN %L 
                    ELSE %L 
                END
             FROM stage.tbl_pozo_produccion
             WHERE timestamp_lectura::DATE BETWEEN %L::DATE AND %L::DATE
               AND (%I IS NOT NULL)',
             v_rule.variable_id, v_rule.regla_id, v_rule.nombre_tecnico,
             v_rule.valor_min, v_rule.valor_max,
             v_rule.nombre_tecnico, v_rule.valor_min,
             v_rule.nombre_tecnico, v_rule.valor_max,
             'FAIL', 'PASS',
             p_fecha_inicio::TEXT, p_fecha_fin::TEXT,
             v_rule.nombre_tecnico
        );

        IF p_well_id IS NOT NULL THEN
            v_sql_check := v_sql_check || format(' AND well_id = %L', p_well_id);
        END IF;

        v_sql_check := v_sql_check || ' ON CONFLICT DO NOTHING';

        EXECUTE v_sql_check;
    END LOOP;

    RAISE NOTICE 'Validación DQ completada.';
END;
$$;


-- =============================================================================
-- 2. SNAPSHOT ZERO-CALC (CORREGIDO - CROSS JOIN LATERAL)
-- =============================================================================
DROP PROCEDURE IF EXISTS reporting.actualizar_current_values_v3;

CREATE OR REPLACE PROCEDURE reporting.actualizar_current_values_v3()
LANGUAGE plpgsql AS $$
BEGIN
    -- A. UPSERT DE DATOS CRUDOS (última lectura por pozo)
    INSERT INTO reporting.dataset_current_values AS target (
        well_id,
        nombre_pozo,
        cliente,
        region,
        campo,
        ultima_actualizacion,
        whp_psi,
        spm_actual,
        pump_fill_monitor_pct,
        updated_at
    )
    SELECT DISTINCT ON (m.well_id)
        m.well_id,
        m.nombre_pozo,
        m.cliente,
        m.region,
        m.campo,
        p.timestamp_lectura,
        p.presion_cabezal,
        p.spm_promedio,
        p.pump_fill_monitor,
        NOW()
    FROM stage.tbl_pozo_maestra m
    LEFT JOIN stage.tbl_pozo_produccion p
        ON m.well_id = p.well_id
    ORDER BY m.well_id, p.timestamp_lectura DESC
    ON CONFLICT (well_id) DO UPDATE SET 
        nombre_pozo            = EXCLUDED.nombre_pozo,
        cliente                = EXCLUDED.cliente,
        region                 = EXCLUDED.region,
        campo                  = EXCLUDED.campo,
        whp_psi                = EXCLUDED.whp_psi, 
        spm_actual             = EXCLUDED.spm_actual, 
        pump_fill_monitor_pct  = EXCLUDED.pump_fill_monitor_pct,
        ultima_actualizacion   = EXCLUDED.ultima_actualizacion,
        updated_at             = NOW();

    -- B. CÁLCULO MASIVO EN MEMORIA (Evaluación Universal)
    WITH calc AS (
        SELECT 
            curr.well_id,
            
            -- WHP
            res_whp.color_hex       AS whp_color,
            res_whp.nivel_severidad AS whp_sev,
            
            -- SPM
            res_spm.color_hex       AS spm_color,
            res_spm.variacion_pct   AS spm_var,
            res_spm.target_value    AS spm_target_calc,
            
            -- FILL
            res_fill.color_hex      AS fill_color,
            res_fill.target_value   AS fill_target_calc,
            
            -- Targets estáticos de la vista
            lim.spm_target          AS spm_target_lim,
            lim.fill_target_val     AS fill_target_lim

        FROM reporting.dataset_current_values curr
        
        LEFT JOIN referencial.vw_limites_pozo_pivot lim
            ON curr.well_id = lim.pozo_id
        
        -- WHP
        CROSS JOIN LATERAL referencial.fnc_evaluar_universal(
            curr.whp_psi, 
            lim.whp_min_crit,
            lim.whp_min_warn,
            lim.whp_max_warn,
            lim.whp_max_crit, 
            NULL,
            NULL
        ) AS res_whp
        
        -- SPM
        CROSS JOIN LATERAL referencial.fnc_evaluar_universal(
            curr.spm_actual, 
            NULL,
            NULL,
            NULL,
            NULL, 
            lim.spm_target,
            lim.spm_tol
        ) AS res_spm
        
        -- FILL
        CROSS JOIN LATERAL referencial.fnc_evaluar_universal(
            curr.pump_fill_monitor_pct, 
            lim.fill_min_crit,
            lim.fill_min_warn,
            lim.fill_max_warn,
            lim.fill_max_crit, 
            NULL,
            NULL
        ) AS res_fill
    )
    
    -- C. UPDATE ÚNICO SOBRE DATASET_CURRENT_VALUES
    UPDATE reporting.dataset_current_values tgt
    SET 
        -- WHP
        whp_status_color = c.whp_color,
        dq_status = CASE
            WHEN c.whp_sev >= 3 THEN 'FAIL'
            ELSE 'PASS'
        END,
        
        -- SPM
        spm_status_color = c.spm_color,
        pump_spm_var_pct = c.spm_var,
        spm_target       = c.spm_target_calc,
        
        -- FILL
        pump_fill_monitor_status_color = c.fill_color,
        pump_fill_monitor_target       = c.fill_target_lim,
        
        -- Estado de comunicación
        color_estado_comunicacion = CASE 
            WHEN EXTRACT(EPOCH FROM (NOW() - tgt.ultima_actualizacion))/60 > 60
                THEN '#B0B0B0' -- OFFLINE
            ELSE '#00C851'    -- NORMAL
        END
    FROM calc c
    WHERE tgt.well_id = c.well_id;

    RAISE NOTICE 'Snapshot actualizado (Zero-Calc + Evaluación Universal).';
END;
$$;


-- =============================================================================
-- 3. LOGIC COLOR CALCULATION- toma números y los convierte en semáforos y textos legibles
-- =============================================================================

CREATE OR REPLACE PROCEDURE reporting.sp_apply_color_logic()
LANGUAGE plpgsql
AS $$
BEGIN
    --------------------------------------------------------------------
    -- 1) Sincronizar límites y targets desde Referencial → dim_pozo
    --------------------------------------------------------------------
    UPDATE reporting.dim_pozo dp
    SET 
        mtbf_target = (
            SELECT target_value
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'kpi_mtbf'
        ),
        mtbf_baseline = (
            SELECT baseline_value
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'kpi_mtbf'
        ),
        pump_spm_target = (
            SELECT target_value
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'spm_promedio'
        ),
        pump_fill_monitor_target = (
            SELECT target_value
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'llenado_bomba_pct'
        ),
        road_load_status_eff_low = (
            SELECT min_warning
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'carga_varilla_pct'
        ),
        road_load_status_eff_high = (
            SELECT max_warning
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'carga_varilla_pct'
        ),
        hydraulic_load_status_eff_low = (
            SELECT min_warning
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'carga_unidad_pct'
        ),
        hydraulic_load_status_eff_high = (
            SELECT max_warning
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'carga_unidad_pct'
        );

    --------------------------------------------------------------------
    -- 2) Semáforos numéricos (WHP, SPM) sobre dataset_current_values
    --------------------------------------------------------------------
    UPDATE reporting.dataset_current_values curr
    SET 
        whp_status_color = CASE 
            WHEN curr.whp_psi >= lim.max_critical THEN (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'CRITICAL')
            WHEN curr.whp_psi >= lim.max_warning  THEN (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'WARNING')
            ELSE (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'NORMAL')
        END,
        spm_status_color = CASE
            WHEN (curr.spm_actual > lim.max_critical OR curr.spm_actual < lim.min_critical) THEN 
                (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'CRITICAL')
            WHEN (curr.spm_actual > lim.max_warning OR curr.spm_actual < lim.min_warning) THEN 
                (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'WARNING')
            ELSE 
                (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'NORMAL')
        END,
        spm_target = lim.target_value
    FROM referencial.tbl_limites_pozo lim
    JOIN referencial.tbl_maestra_variables var ON var.variable_id = lim.variable_id
    WHERE curr.well_id = lim.pozo_id
      AND var.nombre_tecnico IN ('presion_cabezal', 'spm_promedio');

    --------------------------------------------------------------------
    -- 3) Flags Zero-Calc y estados (cargas, comunicación)
    --------------------------------------------------------------------
    UPDATE reporting.dataset_current_values curr
    SET 
        color_estado_comunicacion = CASE 
            WHEN estado_comunicacion = 'OFFLINE' THEN (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'CRITICAL')
            WHEN estado_comunicacion = 'DELAYED' THEN (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'WARNING')
            ELSE (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'NORMAL')
        END,
        road_load_status_level = CASE
            WHEN (road_load_pct_act < dp.road_load_status_eff_low OR road_load_pct_act > dp.road_load_status_eff_high) THEN 3
            WHEN (road_load_pct_act >= (dp.road_load_status_eff_high - 5) AND road_load_pct_act <= dp.road_load_status_eff_high) THEN 2
            ELSE 1
        END,
        road_load_status_color = CASE
            WHEN (road_load_pct_act < dp.road_load_status_eff_low OR road_load_pct_act > dp.road_load_status_eff_high) THEN (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'CRITICAL')
            WHEN (road_load_pct_act >= (dp.road_load_status_eff_high - 5) AND road_load_pct_act <= dp.road_load_status_eff_high) THEN (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'WARNING')
            ELSE (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'NORMAL')
        END,
        road_load_status_legend_text = CONCAT(dp.road_load_status_eff_low, '% <= Target <= ', dp.road_load_status_eff_high, '%'),
        road_load_status_threshold_red = dp.road_load_status_eff_high
    FROM reporting.dim_pozo dp
    WHERE curr.well_id = dp.pozo_id;

    --------------------------------------------------------------------
    -- 4) Variaciones porcentuales vs target
    --------------------------------------------------------------------
    UPDATE reporting.dataset_current_values curr
    SET 
        pump_spm_var_pct = CASE 
            WHEN dp.pump_spm_target > 0 THEN ((curr.spm_actual / dp.pump_spm_target) - 1) * 100 
            ELSE 0 
        END,
        pump_fill_monitor_var = CASE
            WHEN dp.pump_fill_monitor_target > 0 THEN ((curr.pump_fill_monitor_pct / dp.pump_fill_monitor_target) - 1) * 100
            ELSE 0
        END,
        kpi_mtbf_variance_pct = CASE
            WHEN dp.mtbf_target > 0 THEN ((curr.kpi_mtbf_hrs_act / dp.mtbf_target) - 1) * 100
            ELSE 0
        END
    FROM reporting.dim_pozo dp
    WHERE curr.well_id = dp.pozo_id;

END;
$$;
