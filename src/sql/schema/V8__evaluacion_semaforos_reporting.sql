/*
================================================================================
V8 - INTEGRACIÓN FUNCIÓN EVALUACIÓN UNIVERSAL EN SP SNAPSHOT
================================================================================
Fecha: 2026-02-09
Propósito: Reemplazar evaluaciones CASE hardcodeadas por llamadas a 
           fnc_evaluar_variable() para consistencia y mantenibilidad.
================================================================================
*/

-- =============================================================================
-- 1. AGREGAR COLUMNAS FALTANTES A dataset_current_values
-- =============================================================================

-- MTBF: agregar status_level, status_label, severity_label
ALTER TABLE reporting.dataset_current_values 
    ADD COLUMN IF NOT EXISTS mtbf_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS mtbf_status_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS mtbf_severity_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS mtbf_target DECIMAL(10,2),
    ADD COLUMN IF NOT EXISTS mtbf_baseline DECIMAL(10,2);

-- WHP: agregar level y labels
ALTER TABLE reporting.dataset_current_values 
    ADD COLUMN IF NOT EXISTS whp_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS whp_status_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS whp_severity_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS whp_variance_pct DECIMAL(8,2),
    ADD COLUMN IF NOT EXISTS whp_target DECIMAL(10,2);

-- SPM: agregar level y labels
ALTER TABLE reporting.dataset_current_values 
    ADD COLUMN IF NOT EXISTS pump_spm_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS pump_spm_status_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS pump_spm_severity_label VARCHAR(20);

-- FILL: agregar level y labels
ALTER TABLE reporting.dataset_current_values 
    ADD COLUMN IF NOT EXISTS pump_fill_monitor_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS pump_fill_monitor_status_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS pump_fill_monitor_severity_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS pump_fill_monitor_variance_pct DECIMAL(8,2);

-- GAS FILL: agregar level y label
ALTER TABLE reporting.dataset_current_values 
    ADD COLUMN IF NOT EXISTS gas_fill_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS gas_fill_status_label VARCHAR(20);

-- VOL EFF / LIFT: agregar level
ALTER TABLE reporting.dataset_current_values 
    ADD COLUMN IF NOT EXISTS vol_eff_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS vol_eff_status_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS vol_eff_variance_pct DECIMAL(8,2),
    ADD COLUMN IF NOT EXISTS vol_eff_target DECIMAL(5,2);

-- UPTIME: agregar level y label
ALTER TABLE reporting.dataset_current_values 
    ADD COLUMN IF NOT EXISTS kpi_uptime_pct_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS kpi_uptime_pct_status_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS kpi_uptime_pct_variance_pct DECIMAL(8,2),
    ADD COLUMN IF NOT EXISTS kpi_uptime_pct_target DECIMAL(5,2);

-- KWH/BBL: agregar level y label
ALTER TABLE reporting.dataset_current_values 
    ADD COLUMN IF NOT EXISTS kpi_kwh_bbl_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS kpi_kwh_bbl_status_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS kpi_kwh_bbl_variance_pct DECIMAL(8,2),
    ADD COLUMN IF NOT EXISTS kpi_kwh_bbl_target DECIMAL(10,4);

-- TANK TEMP: agregar level y label
ALTER TABLE reporting.dataset_current_values 
    ADD COLUMN IF NOT EXISTS tank_fluid_temp_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS tank_fluid_temp_status_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS tank_fluid_temp_variance_pct DECIMAL(8,2),
    ADD COLUMN IF NOT EXISTS tank_fluid_temp_target DECIMAL(5,2);

-- DAILY DOWNTIME: agregar level y label
ALTER TABLE reporting.dataset_current_values 
    ADD COLUMN IF NOT EXISTS daily_downtime_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS daily_downtime_status_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS daily_downtime_variance_pct DECIMAL(8,2),
    ADD COLUMN IF NOT EXISTS daily_downtime_target DECIMAL(5,2);

-- AI ACCURACY: completar campos
ALTER TABLE reporting.dataset_current_values 
    ADD COLUMN IF NOT EXISTS ai_accuracy_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS ai_accuracy_status_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS ai_accuracy_variance_pct DECIMAL(8,2),
    ADD COLUMN IF NOT EXISTS ai_accuracy_target DECIMAL(5,2);

-- ROAD LOAD: agregar severity_label
ALTER TABLE reporting.dataset_current_values 
    ADD COLUMN IF NOT EXISTS road_load_severity_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS road_load_variance_pct DECIMAL(8,2),
    ADD COLUMN IF NOT EXISTS road_load_target DECIMAL(5,2);

-- =============================================================================
-- 2. SP SET-BASED PARA EVALUACIÓN USANDO FUNCIÓN UNIVERSAL (V8.1 OPTIMIZADO)
-- =============================================================================
-- MEJORA: Reescritura set-based que reemplaza FOR LOOP + N×UPDATE por
--         1 CTE + 11 LATERAL joins + 1 UPDATE único.
--         Rendimiento: O(1) queries vs O(N×22) del loop original.
-- =============================================================================
CREATE OR REPLACE PROCEDURE reporting.aplicar_evaluacion_universal()
LANGUAGE plpgsql AS $$
DECLARE
    -- =========================================================================
    -- TARGETS DESDE tbl_config_kpi (Zero-Hardcode: todo viene de config)
    -- Los fallbacks numéricos solo se usan si la tabla config no existe/está vacía
    -- =========================================================================
    v_default_mtbf_target DECIMAL := 2000;
    v_default_uptime_target DECIMAL := 95;
    v_default_kwh_bbl_target DECIMAL := 10;
    v_default_vol_eff_target DECIMAL := 85;
    v_default_fill_target DECIMAL := 70;
    v_default_gas_fill_target DECIMAL := 30;
    v_default_road_load_target DECIMAL := 100;
    v_default_tank_temp_target DECIMAL := 120;
    v_default_downtime_target DECIMAL := 90;
    v_default_spm_target DECIMAL := 3;
    v_default_ai_target DECIMAL := 85;
    v_default_whp_baseline DECIMAL := 1200;
    v_dq_fail_level INT := 4;  -- nivel mínimo para DQ FAIL (V8 scale)
    v_rows INT;
    v_start_time TIMESTAMP := clock_timestamp();
BEGIN
    -- Cargar TODOS los targets desde configuración centralizada
    BEGIN
        SELECT COALESCE(valor, 2000) INTO v_default_mtbf_target 
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_MTBF' AND parametro='target_default_hrs';
        
        SELECT COALESCE(valor, 95) INTO v_default_uptime_target 
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_UPTIME' AND parametro='target_default_pct';
        
        SELECT COALESCE(valor, 10) INTO v_default_kwh_bbl_target 
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_KWH_BBL' AND parametro='target_default';
        
        SELECT COALESCE(valor, 85) INTO v_default_vol_eff_target 
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_VOL_EFF' AND parametro='target_default_pct';

        SELECT COALESCE(valor, 70) INTO v_default_fill_target 
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='PUMP_FILL_MONITOR' AND parametro='target_default_pct';

        SELECT COALESCE(valor, 30) INTO v_default_gas_fill_target 
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='GAS_FILL_MONITOR' AND parametro='target_default_pct';

        SELECT COALESCE(valor, 100) INTO v_default_road_load_target 
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='ROAD_LOAD' AND parametro='target_default_pct';

        SELECT COALESCE(valor, 120) INTO v_default_tank_temp_target 
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='TANK_FLUID_TEMPERATURE' AND parametro='target_default_f';

        SELECT COALESCE(valor, 90) INTO v_default_downtime_target 
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='DAILY_DOWNTIME' AND parametro='target_default_min';

        SELECT COALESCE(valor, 3) INTO v_default_spm_target 
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='PUMP_SPM' AND parametro='target_default';

        SELECT COALESCE(valor, 85) INTO v_default_ai_target 
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='AI_ACCURACY' AND parametro='target_default_pct';

        SELECT COALESCE(valor, 1200) INTO v_default_whp_baseline 
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='WELL_HEAD_PRESSURE' AND parametro='baseline_default_psi';

        SELECT COALESCE(valor, 4)::INT INTO v_dq_fail_level
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='DQ' AND parametro='fail_threshold_level';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '[V8-SET] Usando targets por defecto (config no disponible)';
    END;

    RAISE NOTICE '[V8-SET] Evaluación universal SET-BASED para todos los pozos...';

    -- =========================================================================
    -- SINGLE SET-BASED UPDATE: 1 CTE + 11 LATERAL + 1 UPDATE
    -- Reemplaza: FOR LOOP × 11 SELECT + 11 UPDATE por pozo = 22N queries
    -- Nuevo:     1 query total independiente del número de pozos
    -- =========================================================================
    WITH well_data AS (
        SELECT 
            dcv.well_id,
            dcv.kpi_mtbf_hrs_act,
            dcv.kpi_uptime_pct_act,
            dcv.kpi_kwh_bbl_act,
            dcv.lift_efficiency_pct_act,
            dcv.pump_fill_monitor_pct,
            dcv.gas_fill_monitor_pct_act,
            dcv.road_load_pct_act,
            dcv.tank_fluid_temperature_f,
            dcv.daily_downtime_act,
            dcv.pump_avg_spm_act,
            dcv.ai_accuracy_act,
            dcv.well_head_pressure_psi_act,
            -- Targets con cascada: pozo → config (Zero-Hardcode)
            COALESCE(dp.mtbf_target, v_default_mtbf_target) AS mtbf_t,
            COALESCE(dp.kpi_uptime_pct_target, v_default_uptime_target) AS uptime_t,
            COALESCE(dp.kpi_kwh_bbl_target, v_default_kwh_bbl_target) AS kwh_t,
            COALESCE(dp.vol_eff_target, v_default_vol_eff_target) AS vol_eff_t,
            COALESCE(dp.pump_fill_monitor_target, v_default_fill_target) AS fill_t,
            COALESCE(dp.gas_fill_monitor_target, v_default_gas_fill_target) AS gas_t,
            v_default_road_load_target AS road_t,
            COALESCE(dp.tank_fluid_temperature_f_target, v_default_tank_temp_target) AS tank_t,
            v_default_downtime_target AS downtime_t,
            COALESCE(dp.pump_spm_target, v_default_spm_target) AS spm_t,
            v_default_ai_target AS ai_t,
            v_default_whp_baseline AS whp_bl
        FROM reporting.dataset_current_values dcv
        LEFT JOIN reporting.dim_pozo dp ON dcv.well_id = dp.pozo_id
        LEFT JOIN referencial.vw_limites_pozo_pivot_v4 lim ON dcv.well_id = lim.pozo_id
    )
    UPDATE reporting.dataset_current_values dcv
    SET
        -- 1. MTBF
        mtbf_variance_pct   = e1.variance_pct,
        mtbf_status_color   = e1.status_color,
        mtbf_status_level   = e1.status_level,
        mtbf_status_label   = e1.status_label,
        mtbf_severity_label = e1.severity_label,
        mtbf_target         = wd.mtbf_t,
        -- 2. UPTIME
        kpi_uptime_pct_variance_pct   = e2.variance_pct,
        kpi_uptime_pct_status_color   = e2.status_color,
        kpi_uptime_pct_status_level   = e2.status_level,
        kpi_uptime_pct_status_label   = e2.status_label,
        kpi_uptime_pct_severity_label = e2.severity_label,
        kpi_uptime_pct_target         = wd.uptime_t,
        -- 3. KWH/BBL
        kpi_kwh_bbl_variance_pct   = e3.variance_pct,
        kpi_kwh_bbl_status_color   = e3.status_color,
        kpi_kwh_bbl_status_level   = e3.status_level,
        kpi_kwh_bbl_status_label   = e3.status_label,
        kpi_kwh_bbl_severity_label = e3.severity_label,
        kpi_kwh_bbl_target         = wd.kwh_t,
        -- 4. VOL EFF / LIFT EFFICIENCY
        vol_eff_variance_pct   = e4.variance_pct,
        vol_eff_status_color   = e4.status_color,
        vol_eff_status_level   = e4.status_level,
        vol_eff_status_label   = e4.status_label,
        vol_eff_severity_label = e4.severity_label,
        lift_efficiency_severity_label = e4.severity_label,
        vol_eff_target         = wd.vol_eff_t,
        -- 5. PUMP FILL
        pump_fill_monitor_variance_pct   = e5.variance_pct,
        pump_fill_monitor_status_color   = e5.status_color,
        pump_fill_monitor_status_level   = e5.status_level,
        pump_fill_monitor_status_label   = e5.status_label,
        pump_fill_monitor_severity_label = e5.severity_label,
        pump_fill_monitor_target         = wd.fill_t,
        -- 6. GAS FILL
        gas_fill_status_color   = e6.status_color,
        gas_fill_status_level   = e6.status_level,
        gas_fill_status_label   = e6.status_label,
        gas_fill_severity_label = e6.severity_label,
        -- 7. ROAD LOAD
        road_load_variance_pct   = e7.variance_pct,
        road_load_status_color   = e7.status_color,
        road_load_status_level   = e7.status_level,
        road_load_status_label   = e7.status_label,
        road_load_severity_label = e7.severity_label,
        road_load_status_legend_text = CONCAT('Target: ', wd.road_t, '% | Actual: ', COALESCE(wd.road_load_pct_act::TEXT, 'N/A'), '%'),
        -- 8. TANK TEMPERATURE
        tank_fluid_temp_variance_pct   = e8.variance_pct,
        tank_fluid_temp_status_color   = e8.status_color,
        tank_fluid_temp_status_level   = e8.status_level,
        tank_fluid_temp_status_label   = e8.status_label,
        tank_fluid_temperature_f_severity_label = e8.severity_label,
        tank_fluid_temp_target         = wd.tank_t,
        -- 9. DAILY DOWNTIME
        daily_downtime_variance_pct   = e9.variance_pct,
        daily_downtime_status_color   = e9.status_color,
        daily_downtime_status_level   = e9.status_level,
        daily_downtime_status_label   = e9.status_label,
        daily_downtime_severity_label = e9.severity_label,
        daily_downtime_target         = wd.downtime_t,
        -- 10. SPM
        pump_spm_var_pct      = e10.variance_pct,
        pump_spm_status_color = e10.status_color,
        pump_spm_status_level = e10.status_level,
        pump_spm_status_label = e10.status_label,
        pump_spm_severity_label = e10.severity_label,
        spm_target            = wd.spm_t,
        -- 11. AI ACCURACY (condicional: solo si hay dato)
        ai_accuracy_variance_pct   = CASE WHEN wd.ai_accuracy_act IS NOT NULL THEN e11.variance_pct   ELSE dcv.ai_accuracy_variance_pct END,
        ai_accuracy_status_color   = CASE WHEN wd.ai_accuracy_act IS NOT NULL THEN e11.status_color   ELSE dcv.ai_accuracy_status_color END,
        ai_accuracy_status_level   = CASE WHEN wd.ai_accuracy_act IS NOT NULL THEN e11.status_level   ELSE dcv.ai_accuracy_status_level END,
        ai_accuracy_status_label   = CASE WHEN wd.ai_accuracy_act IS NOT NULL THEN e11.status_label   ELSE dcv.ai_accuracy_status_label END,
        ai_accuracy_severity_label = CASE WHEN wd.ai_accuracy_act IS NOT NULL THEN e11.severity_label ELSE dcv.ai_accuracy_severity_label END,
        ai_accuracy_target         = CASE WHEN wd.ai_accuracy_act IS NOT NULL THEN wd.ai_t ELSE dcv.ai_accuracy_target END,
        -- 12. WHP (Well Head Pressure) — migrado desde V6
        whp_variance_pct   = e12.variance_pct,
        whp_status_color   = e12.status_color,
        whp_status_level   = e12.status_level,
        whp_status_label   = e12.status_label,
        whp_severity_label = e12.severity_label,
        whp_target         = wd.whp_bl,
        -- DQ Status (basado en validaciones DQ reales en stage.tbl_pozo_scada_dq)
        -- CORREGIDO: Ya no se basa en whp_status_level, sino en resultados reales de DQ
        dq_status = (
            SELECT CASE 
                WHEN COUNT(*) FILTER (WHERE resultado_dq = 'FAIL') > 0 THEN 'FAIL'
                WHEN COUNT(*) FILTER (WHERE resultado_dq = 'WARNING') > 0 THEN 'WARNING'
                WHEN COUNT(*) = 0 THEN 'NO_DATA'
                ELSE 'PASS'
            END
            FROM stage.tbl_pozo_scada_dq dq
            JOIN stage.tbl_pozo_produccion p ON dq.produccion_id = p.produccion_id
            WHERE p.well_id = dcv.well_id
            AND dq.timestamp_lectura >= (NOW() - INTERVAL '24 hours')
        ),
        -- Road Load threshold para frontend (migrado desde V6)
        road_load_status_threshold_red = wd.road_t
    FROM well_data wd
    -- 11 LATERAL joins: cada uno evalúa 1 variable para TODOS los pozos en paralelo
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('kpi_mtbf',              wd.kpi_mtbf_hrs_act,          wd.mtbf_t,    NULL) e1
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('kpi_uptime',            wd.kpi_uptime_pct_act,        wd.uptime_t,  NULL) e2
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('kpi_kwh_bbl',           wd.kpi_kwh_bbl_act,           wd.kwh_t,     NULL) e3
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('kpi_vol_eff',           wd.lift_efficiency_pct_act,   wd.vol_eff_t, NULL) e4
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('pump_fill_monitor',     wd.pump_fill_monitor_pct,     wd.fill_t,    NULL) e5
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('gas_fill_monitor',      wd.gas_fill_monitor_pct_act,  wd.gas_t,     NULL) e6
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('road_load',             wd.road_load_pct_act,         wd.road_t,    NULL) e7
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('tank_fluid_temperature',wd.tank_fluid_temperature_f,  wd.tank_t,    NULL) e8
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('daily_downtime',        wd.daily_downtime_act,        wd.downtime_t,NULL) e9
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('pump_spm',             wd.pump_avg_spm_act,          wd.spm_t,     NULL) e10
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('ai_accuracy',           COALESCE(wd.ai_accuracy_act, 0), wd.ai_t,   NULL) e11
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('well_head_pressure',   wd.well_head_pressure_psi_act,   NULL, wd.whp_bl) e12
    WHERE dcv.well_id = wd.well_id;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    RAISE NOTICE '[V8-SET] Evaluación universal completada: % pozos en % ms (SET-BASED)',
        v_rows, EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start_time);
END;
$$;

-- =============================================================================
-- 3. AGREGAR LLAMADA A SP EN EL FLUJO PRINCIPAL
-- =============================================================================
-- El SP reporting.actualizar_current_values_v4() ahora puede llamar a
-- reporting.aplicar_evaluacion_universal() al final para aplicar
-- todas las clasificaciones de forma consistente.

-- Wrapper que ejecuta ambos
CREATE OR REPLACE PROCEDURE reporting.actualizar_current_values_completo()
LANGUAGE plpgsql AS $$
BEGIN
    -- Paso 1: Mapeo de datos crudos
    CALL reporting.actualizar_current_values_v4();
    
    -- Paso 2: Aplicar evaluación universal (status, labels, colors)
    CALL reporting.aplicar_evaluacion_universal();
    
    RAISE NOTICE 'Pipeline Current Values COMPLETO ejecutado.';
END;
$$;

-- =============================================================================
-- 4. VISTA RESUMEN DE EVALUACIONES ACTUALES
-- =============================================================================
CREATE OR REPLACE VIEW reporting.vw_resumen_status_pozo AS
SELECT 
    well_id,
    nombre_pozo,
    
    -- MTBF
    kpi_mtbf_hrs_act,
    mtbf_target,
    mtbf_variance_pct,
    mtbf_status_level,
    mtbf_status_label,
    mtbf_severity_label,
    mtbf_status_color,
    
    -- UPTIME
    kpi_uptime_pct_act,
    kpi_uptime_pct_target,
    kpi_uptime_pct_variance_pct,
    kpi_uptime_pct_status_level,
    kpi_uptime_pct_status_label,
    kpi_uptime_pct_severity_label,
    kpi_uptime_pct_status_color,
    
    -- KWH/BBL
    kpi_kwh_bbl_act,
    kpi_kwh_bbl_target,
    kpi_kwh_bbl_variance_pct,
    kpi_kwh_bbl_status_level,
    kpi_kwh_bbl_status_label,
    kpi_kwh_bbl_severity_label,
    kpi_kwh_bbl_status_color,
    
    -- VOL EFF
    lift_efficiency_pct_act AS vol_eff_actual,
    vol_eff_target,
    vol_eff_variance_pct,
    vol_eff_status_level,
    vol_eff_status_label,
    vol_eff_severity_label,
    vol_eff_status_color,
    
    -- PUMP FILL
    pump_fill_monitor_pct,
    pump_fill_monitor_target,
    pump_fill_monitor_variance_pct,
    pump_fill_monitor_status_level,
    pump_fill_monitor_status_label,
    pump_fill_monitor_severity_label,
    pump_fill_monitor_status_color,
    
    -- ROAD LOAD
    road_load_pct_act,
    road_load_status_level,
    road_load_status_label,
    road_load_severity_label,
    road_load_status_color,
    
    -- Timestamp
    ultima_actualizacion
    
FROM reporting.dataset_current_values;

-- Comentarios
COMMENT ON PROCEDURE reporting.aplicar_evaluacion_universal IS 
'Aplica la función fnc_evaluar_variable a todas las variables/KPIs configurados.
Asigna status_level, status_label, severity_label y status_color de forma consistente.';

COMMENT ON PROCEDURE reporting.actualizar_current_values_completo IS 
'Wrapper que ejecuta el mapeo de datos crudos y luego aplica la evaluación universal.';

-- =============================================================================
-- 5. SP POPULATE DEFAULTS — Centralización Zero-Hardcode
-- =============================================================================
-- Reemplaza el SQL inline del MASTER paso 7.5. Todos los valores se leen de
-- referencial.tbl_config_kpi en vez de estar hardcodeados en Python.
-- Este SP se ejecuta DESPUÉS de V8 y V9 para rellenar campos sin fuente automática.
-- =============================================================================
CREATE OR REPLACE PROCEDURE reporting.sp_populate_defaults()
LANGUAGE plpgsql AS $$
DECLARE
    -- Baselines (leídos de config)
    v_mtbf_baseline DECIMAL := 1500;
    v_uptime_baseline DECIMAL := 90;
    v_kwh_bbl_baseline DECIMAL := 12;
    v_vol_eff_baseline DECIMAL := 80;
    v_ai_baseline DECIMAL := 85;
    -- Targets
    v_whp_target DECIMAL := 1200;
    v_road_load_target DECIMAL := 100;
    v_ai_target DECIMAL := 90;
    v_mtbf_target DECIMAL := 2000;
    v_uptime_target DECIMAL := 95;
    v_kwh_bbl_target DECIMAL := 10;
    v_vol_eff_target DECIMAL := 85;
    -- Costos y constantes
    v_lifting_cost DECIMAL := 2.50;
    v_freq_vsd DECIMAL := 60;
    v_tarifa_kwh DECIMAL := 0.12;
    v_default_region VARCHAR(100) := 'PECOS VALLEY';
BEGIN
    -- =========================================================================
    -- CARGAR TODOS LOS DEFAULTS DESDE tbl_config_kpi
    -- =========================================================================
    BEGIN
        SELECT COALESCE(valor, 1500) INTO v_mtbf_baseline FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_MTBF' AND parametro='baseline_default_hrs';
        SELECT COALESCE(valor, 90) INTO v_uptime_baseline FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_UPTIME' AND parametro='baseline_default_pct';
        SELECT COALESCE(valor, 12) INTO v_kwh_bbl_baseline FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_KWH_BBL' AND parametro='baseline_default';
        SELECT COALESCE(valor, 80) INTO v_vol_eff_baseline FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_VOL_EFF' AND parametro='baseline_default_pct';
        SELECT COALESCE(valor, 85) INTO v_ai_baseline FROM referencial.tbl_config_kpi WHERE kpi_nombre='AI_ACCURACY' AND parametro='baseline_default_pct';
        SELECT COALESCE(valor, 1200) INTO v_whp_target FROM referencial.tbl_config_kpi WHERE kpi_nombre='WELL_HEAD_PRESSURE' AND parametro='baseline_default_psi';
        SELECT COALESCE(valor, 100) INTO v_road_load_target FROM referencial.tbl_config_kpi WHERE kpi_nombre='ROAD_LOAD' AND parametro='target_default_pct';
        SELECT COALESCE(valor, 85) INTO v_ai_target FROM referencial.tbl_config_kpi WHERE kpi_nombre='AI_ACCURACY' AND parametro='target_default_pct';
        SELECT COALESCE(valor, 2000) INTO v_mtbf_target FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_MTBF' AND parametro='target_default_hrs';
        SELECT COALESCE(valor, 95) INTO v_uptime_target FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_UPTIME' AND parametro='target_default_pct';
        SELECT COALESCE(valor, 10) INTO v_kwh_bbl_target FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_KWH_BBL' AND parametro='target_default';
        SELECT COALESCE(valor, 85) INTO v_vol_eff_target FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_VOL_EFF' AND parametro='target_default_pct';
        SELECT COALESCE(valor, 2.50) INTO v_lifting_cost FROM referencial.tbl_config_kpi WHERE kpi_nombre='LIFTING_COST' AND parametro='default_usd_bbl';
        SELECT COALESCE(valor, 60) INTO v_freq_vsd FROM referencial.tbl_config_kpi WHERE kpi_nombre='FREQ_VSD' AND parametro='default_hz';
        SELECT COALESCE(valor, 0.12) INTO v_tarifa_kwh FROM referencial.tbl_config_kpi WHERE kpi_nombre='ENERGIA' AND parametro='tarifa_kwh_usd';
        SELECT COALESCE(valor_texto, 'PECOS VALLEY') INTO v_default_region FROM referencial.tbl_config_kpi WHERE kpi_nombre='DEFAULT' AND parametro='region';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '[DEFAULTS] Usando valores por defecto internos (config no disponible)';
    END;

    -- =========================================================================
    -- A. dataset_current_values: baselines y placeholders
    -- =========================================================================
    UPDATE reporting.dataset_current_values cv
    SET 
        -- Baselines (desde config)
        mtbf_baseline          = COALESCE(cv.mtbf_baseline, v_mtbf_baseline),
        kpi_uptime_pct_baseline = COALESCE(cv.kpi_uptime_pct_baseline, v_uptime_baseline),
        kpi_kwh_bbl_baseline   = COALESCE(cv.kpi_kwh_bbl_baseline, v_kwh_bbl_baseline),
        vol_eff_baseline       = COALESCE(cv.vol_eff_baseline, v_vol_eff_baseline),
        -- WHP target (desde config)
        whp_target             = COALESCE(cv.whp_target, v_whp_target),
        -- Road load target (desde config)
        road_load_target       = COALESCE(cv.road_load_target, v_road_load_target),
        -- AI Accuracy: placeholder (sin modelo ML por ahora)
        -- Nivel 7 = "Sin Datos" en tbl_catalogo_status (antes era 5 = En Falla)
        ai_accuracy_act            = COALESCE(cv.ai_accuracy_act, 0.00),
        ai_accuracy_target         = COALESCE(cv.ai_accuracy_target, v_ai_target),
        ai_accuracy_status_level   = COALESCE(cv.ai_accuracy_status_level, 7),
        ai_accuracy_status_label   = COALESCE(cv.ai_accuracy_status_label, 'Sin Modelo'),
        ai_accuracy_status_color   = COALESCE(cv.ai_accuracy_status_color, '#B0B0B0'),
        ai_accuracy_severity_label = COALESCE(cv.ai_accuracy_severity_label, 'Sin Datos'),
        ai_accuracy_variance_pct   = COALESCE(cv.ai_accuracy_variance_pct, -100.00),
        -- Sensores faltantes: proxies/defaults (desde config)
        freq_vsd_hz                = COALESCE(cv.freq_vsd_hz, v_freq_vsd),
        max_pump_load_lb_act       = COALESCE(cv.max_pump_load_lb_act, cv.max_rod_load_lb_act),
        min_pump_load_lb_act       = COALESCE(cv.min_pump_load_lb_act, cv.min_rod_load_lb_act),
        hydraulic_load_status_legend_text = COALESCE(cv.hydraulic_load_status_legend_text,
            CASE cv.hydraulic_load_status_level
                WHEN 0 THEN 'Carga dentro de rango operativo normal'
                WHEN 1 THEN 'Carga cercana al límite permitido'
                WHEN 2 THEN 'Sobrecarga - requiere atención inmediata'
                ELSE 'Sin datos de carga hidráulica'
            END),
        -- IPR: estimación teórica basada en productividad actual
        ipr_qmax_bpd               = COALESCE(cv.ipr_qmax_bpd,
            CASE WHEN cv.produccion_fluido_bpd_act > 0 AND cv.pump_intake_pressure_psi_act > 0
                 THEN ROUND((cv.produccion_fluido_bpd_act / (1 - POWER(cv.pump_intake_pressure_psi_act / 
                       NULLIF(cv.pump_intake_pressure_psi_act + 500, 0), 2)))::NUMERIC, 2)
                 ELSE 0.00 END),
        ipr_eficiencia_flujo_pct   = COALESCE(cv.ipr_eficiencia_flujo_pct,
            CASE WHEN cv.produccion_fluido_bpd_act > 0 
                 THEN ROUND((cv.produccion_fluido_bpd_act / NULLIF(cv.total_fluid_today_bbl * 1.0, 0) * 100)::NUMERIC, 2)
                 ELSE 0.00 END),
        -- Vibración/Inclinación: sin sensor, defaults industriales
        falla_vibracion_grados     = COALESCE(cv.falla_vibracion_grados, 0.00),
        inclinacion_severidad_flag = COALESCE(cv.inclinacion_severidad_flag, 'NORMAL'),
        inclinacion_cilindro_x_act = COALESCE(cv.inclinacion_cilindro_x_act, 0.00),
        inclinacion_cilindro_y_act = COALESCE(cv.inclinacion_cilindro_y_act, 0.00);

    -- =========================================================================
    -- B. dataset_kpi_business: region, targets, baselines (desde config)
    --    COLUMNAS V7 WIDE: nomenclatura estandarizada
    -- =========================================================================
    UPDATE reporting.dataset_kpi_business kb
    SET 
        region             = COALESCE(kb.region,
            (SELECT cv.region FROM reporting.dataset_current_values cv 
             WHERE cv.well_id = kb.well_id LIMIT 1), v_default_region),
        -- KPI targets & baselines (nomenclatura estandarizada)
        kpi_mtbf_hrs_target        = COALESCE(kb.kpi_mtbf_hrs_target, v_mtbf_target),
        kpi_mtbf_hrs_baseline      = COALESCE(kb.kpi_mtbf_hrs_baseline, v_mtbf_baseline),
        kpi_uptime_pct_target      = COALESCE(kb.kpi_uptime_pct_target, v_uptime_target),
        kpi_uptime_pct_baseline    = COALESCE(kb.kpi_uptime_pct_baseline, v_uptime_baseline),
        kpi_kwh_bbl_target         = COALESCE(kb.kpi_kwh_bbl_target, v_kwh_bbl_target),
        kpi_kwh_bbl_baseline       = COALESCE(kb.kpi_kwh_bbl_baseline, v_kwh_bbl_baseline),
        kpi_vol_eff_pct_target     = COALESCE(kb.kpi_vol_eff_pct_target, v_vol_eff_target),
        kpi_vol_eff_pct_baseline   = COALESCE(kb.kpi_vol_eff_pct_baseline, v_vol_eff_baseline),
        -- AI Accuracy (sin prefijo kpi_)
        ai_accuracy_current        = COALESCE(kb.ai_accuracy_current, 0.00),
        ai_accuracy_target         = COALESCE(kb.ai_accuracy_target, v_ai_target),
        ai_accuracy_baseline       = COALESCE(kb.ai_accuracy_baseline, v_ai_baseline),
        ai_accuracy_variance_pct   = COALESCE(kb.ai_accuracy_variance_pct, -100.00),
        ai_accuracy_status_color   = COALESCE(kb.ai_accuracy_status_color, '#B0B0B0'),
        ai_accuracy_status_level   = COALESCE(kb.ai_accuracy_status_level, 7),
        ai_accuracy_severity_label = COALESCE(kb.ai_accuracy_severity_label, 'Sin Datos'),
        -- Contexto
        calidad_datos_pct          = COALESCE(kb.calidad_datos_pct, 100.00);

    RAISE NOTICE '[DEFAULTS] Baselines, targets y placeholders poblados desde tbl_config_kpi.';
END;
$$;

COMMENT ON PROCEDURE reporting.sp_populate_defaults IS 
'Pobla baselines, targets, placeholders AI y proxies de sensores faltantes.
Todos los valores se leen de referencial.tbl_config_kpi (Zero-Hardcode).
Se ejecuta DESPUÉS de V8 (evaluación) y V9 (derivados) como paso final.';

-- Fin del script
SELECT 'V8 - Integración Evaluación Universal + sp_populate_defaults completada' AS resultado;
