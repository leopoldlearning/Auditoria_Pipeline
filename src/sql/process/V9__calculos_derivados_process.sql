/*
================================================================================
V9 - STORED PROCEDURES: CÁLCULOS DERIVADOS (PROCESS)
================================================================================
Fecha: 2026-02-09
Propósito: SPs que EJECUTAN los cálculos derivados usando funciones de stage.
Ubicación: src/sql/process/ (separado del DDL/funciones en schema/)

DEPENDENCIAS:
  - schema/V9__calculos_derivados_funciones.sql (stage.fnc_calc_*)
  - schema/V4__reporting_schema_redesign.sql (tablas reporting)
  - schema/V7__sistema_clasificacion_universal.sql (referencial.fnc_evaluar_variable)

FLUJO EN MASTER_PIPELINE_RUNNER:
  Paso 7: Cargar este archivo (crea los SPs)
  Paso 7.1: CALL reporting.sp_calcular_derivados_completos()
================================================================================
*/

-- =============================================================================
-- 1. SP: CALCULAR DERIVADOS EN DATASET_CURRENT_VALUES
-- =============================================================================
CREATE OR REPLACE PROCEDURE reporting.sp_calcular_derivados_current_values()
LANGUAGE plpgsql AS $$
DECLARE
    v_count INT := 0;
    v_start_time TIMESTAMP := clock_timestamp();
    v_stroke_var_warning DECIMAL(5,2) := 5.00;
    v_stroke_var_critical DECIMAL(5,2) := 15.00;
BEGIN
    RAISE NOTICE '[V9] Iniciando cálculos derivados para dataset_current_values...';
    
    -- Leer thresholds de stroke variance desde config (si existen)
    BEGIN
        SELECT COALESCE(valor, 5) INTO v_stroke_var_warning
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='PUMP_STROKE' AND parametro='variance_warning_pct';
        SELECT COALESCE(valor, 15) INTO v_stroke_var_critical
        FROM referencial.tbl_config_kpi WHERE kpi_nombre='PUMP_STROKE' AND parametro='variance_critical_pct';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '[V9] Usando defaults stroke variance (5/15)';
    END;
    
    -- =========================================================================
    -- PASO 1: Mapeos directos (copiar valores existentes)
    -- =========================================================================
    UPDATE reporting.dataset_current_values dcv SET
        qf_fluid_flow_monitor_bpd = COALESCE(dcv.qf_fluid_flow_monitor_bpd, dcv.produccion_fluido_bpd_act),
        pump_stroke_length_act = COALESCE(dcv.pump_stroke_length_act, dcv.current_stroke_length_act_in)
    WHERE dcv.qf_fluid_flow_monitor_bpd IS NULL 
       OR dcv.pump_stroke_length_act IS NULL;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] Mapeos directos aplicados: % filas', v_count;
    
    -- =========================================================================
    -- PASO 2: Cálculos físicos (stage.fnc_calc_* — Zero-Calc Architecture)
    -- =========================================================================
    UPDATE reporting.dataset_current_values dcv SET
        fluid_level_tvd_ft = COALESCE(
            dcv.fluid_level_tvd_ft,
            stage.fnc_calc_fluid_level_tvd(
                dcv.pump_intake_pressure_psi_act,
                res.gravedad_api
            )
        ),
        pwf_psi_act = COALESCE(
            dcv.pwf_psi_act,
            stage.fnc_calc_pwf(
                dcv.pump_intake_pressure_psi_act,
                stage.fnc_calc_fluid_level_tvd(dcv.pump_intake_pressure_psi_act, res.gravedad_api),
                res.gravedad_api,
                dp.pump_depth_ft
            )
        ),
        hydralift_unit_load_pct = COALESCE(
            dcv.hydralift_unit_load_pct,
            stage.fnc_calc_hydralift_load_pct(
                dcv.max_rod_load_lb_act,
                dp.hydraulic_load_rated_klb
            )
        ),
        road_load_pct_act = COALESCE(
            dcv.road_load_pct_act,
            stage.fnc_calc_road_load_pct(
                dcv.max_rod_load_lb_act,
                dp.api_max_fluid_load_lb
            )
        )
    FROM reporting.dim_pozo dp
    LEFT JOIN stage.tbl_pozo_reservas res ON dp.pozo_id = res.well_id
    WHERE dcv.well_id = dp.pozo_id
      AND (dcv.fluid_level_tvd_ft IS NULL 
           OR dcv.pwf_psi_act IS NULL
           OR dcv.hydralift_unit_load_pct IS NULL
           OR dcv.road_load_pct_act IS NULL);
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] Cálculos físicos aplicados: % filas', v_count;
    
    -- =========================================================================
    -- PASO 3: Varianzas porcentuales (stage.fnc_calc_variance_pct)
    -- =========================================================================
    UPDATE reporting.dataset_current_values dcv SET
        pump_spm_var_pct = COALESCE(
            dcv.pump_spm_var_pct,
            stage.fnc_calc_variance_pct(dcv.spm_actual, dcv.spm_target)
        ),
        pump_stroke_length_var_pct = COALESCE(
            dcv.pump_stroke_length_var_pct,
            stage.fnc_calc_variance_pct(dcv.pump_stroke_length_act, dp.pump_stroke_length_target)
        ),
        pump_fill_monitor_var = COALESCE(
            dcv.pump_fill_monitor_var,
            stage.fnc_calc_variance_pct(dcv.pump_fill_monitor_pct, dcv.pump_fill_monitor_target)
        )
    FROM reporting.dim_pozo dp
    WHERE dcv.well_id = dp.pozo_id
      AND (dcv.pump_spm_var_pct IS NULL 
           OR dcv.pump_stroke_length_var_pct IS NULL
           OR dcv.pump_fill_monitor_var IS NULL);
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] Varianzas calculadas: % filas', v_count;
    
    -- =========================================================================
    -- PASO 4: Evaluaciones de status (semáforos) — Escala V8 unificada (0-9)
    -- =========================================================================
    UPDATE reporting.dataset_current_values dcv SET
        hydraulic_load_status_level = CASE
            WHEN dcv.hydralift_unit_load_pct IS NULL THEN 7  -- Sin Datos
            WHEN dcv.hydralift_unit_load_pct <= COALESCE(dp.hydraulic_load_status_eff_low, 70) THEN 0  -- Óptimo
            WHEN dcv.hydralift_unit_load_pct <= COALESCE(dp.hydraulic_load_status_eff_high, 85) THEN 3 -- Alerta
            ELSE 4  -- Crítico
        END,
        hydraulic_load_status_color = CASE
            WHEN dcv.hydralift_unit_load_pct IS NULL THEN '#B0B0B0'
            WHEN dcv.hydralift_unit_load_pct <= COALESCE(dp.hydraulic_load_status_eff_low, 70) THEN '#00CC66'
            WHEN dcv.hydralift_unit_load_pct <= COALESCE(dp.hydraulic_load_status_eff_high, 85) THEN '#FFBB33'
            ELSE '#FF4444'
        END,
        hydraulic_load_status_label = CASE
            WHEN dcv.hydralift_unit_load_pct IS NULL THEN 'SIN DATOS'
            WHEN dcv.hydralift_unit_load_pct <= COALESCE(dp.hydraulic_load_status_eff_low, 70) THEN 'NORMAL'
            WHEN dcv.hydralift_unit_load_pct <= COALESCE(dp.hydraulic_load_status_eff_high, 85) THEN 'ALERTA'
            ELSE 'CRÍTICO'
        END,
        hydraulic_load_status_threshold_red = COALESCE(dp.hydraulic_load_status_eff_high, 85)
    FROM reporting.dim_pozo dp
    WHERE dcv.well_id = dp.pozo_id;
    
    UPDATE reporting.dataset_current_values dcv SET
        road_load_status_level = CASE
            WHEN dcv.road_load_pct_act IS NULL THEN 7  -- Sin Datos
            WHEN dcv.road_load_pct_act <= COALESCE(dp.road_load_status_eff_low, 70) THEN 0  -- Óptimo
            WHEN dcv.road_load_pct_act <= COALESCE(dp.road_load_status_eff_high, 85) THEN 3 -- Alerta
            ELSE 4  -- Crítico
        END,
        road_load_status_color = CASE
            WHEN dcv.road_load_pct_act IS NULL THEN '#B0B0B0'
            WHEN dcv.road_load_pct_act <= COALESCE(dp.road_load_status_eff_low, 70) THEN '#00CC66'
            WHEN dcv.road_load_pct_act <= COALESCE(dp.road_load_status_eff_high, 85) THEN '#FFBB33'
            ELSE '#FF4444'
        END,
        road_load_status_label = CASE
            WHEN dcv.road_load_pct_act IS NULL THEN 'SIN DATOS'
            WHEN dcv.road_load_pct_act <= COALESCE(dp.road_load_status_eff_low, 70) THEN 'NORMAL'
            WHEN dcv.road_load_pct_act <= COALESCE(dp.road_load_status_eff_high, 85) THEN 'ALERTA'
            ELSE 'CRÍTICO'
        END,
        road_load_status_threshold_red = COALESCE(dp.road_load_status_eff_high, 85)
    FROM reporting.dim_pozo dp
    WHERE dcv.well_id = dp.pozo_id;
    
    UPDATE reporting.dataset_current_values dcv SET
        pump_stroke_length_status_color = CASE
            WHEN dcv.pump_stroke_length_var_pct IS NULL THEN '#B0B0B0'
            WHEN ABS(dcv.pump_stroke_length_var_pct) <= v_stroke_var_warning THEN '#00CC66'
            WHEN ABS(dcv.pump_stroke_length_var_pct) <= v_stroke_var_critical THEN '#FFBB33'
            ELSE '#FF4444'
        END;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] Evaluaciones de status aplicadas: % filas', v_count;
    
    UPDATE reporting.dataset_current_values SET updated_at = NOW();
    
    RAISE NOTICE '[V9] dataset_current_values completado en % ms', 
        EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start_time);
END;
$$;

COMMENT ON PROCEDURE reporting.sp_calcular_derivados_current_values IS 
'Calcula valores derivados en dataset_current_values. Ejecutar DESPUÉS de V8.aplicar_evaluacion_universal';


-- =============================================================================
-- 1.5 SP: CALCULAR DERIVADOS EN FACT_OPERACIONES_HORARIAS
-- =============================================================================
CREATE OR REPLACE PROCEDURE reporting.sp_calcular_derivados_horarios(
    p_fecha_inicio DATE DEFAULT NULL,
    p_fecha_fin DATE DEFAULT NULL
)
LANGUAGE plpgsql AS $$
DECLARE
    v_fecha_inicio DATE := COALESCE(p_fecha_inicio, CURRENT_DATE - INTERVAL '30 days');
    v_fecha_fin DATE := COALESCE(p_fecha_fin, CURRENT_DATE);
    v_count INT := 0;
BEGIN
    RAISE NOTICE '[V9] Calculando derivados horarios para % a %...', v_fecha_inicio, v_fecha_fin;
    
    -- Calcular fluid_level_tvd_ft, tank_temp proxy, bouyant_rod desde dim_pozo + reservas
    UPDATE reporting.fact_operaciones_horarias fh SET
        fluid_level_tvd_ft = COALESCE(
            fh.fluid_level_tvd_ft,
            stage.fnc_calc_fluid_level_tvd(fh.pip_psi, res.gravedad_api)
        ),
        tank_fluid_temp_f = COALESCE(fh.tank_fluid_temp_f, fh.temperatura_motor_f),
        bouyant_rod_weight_lb = COALESCE(
            fh.bouyant_rod_weight_lb,
            dp.rod_weight_in_air_lb * (1 - COALESCE(res.gravedad_api, 30) / 141.5)
        )
    FROM reporting.dim_pozo dp
    LEFT JOIN stage.tbl_pozo_reservas res ON dp.pozo_id = res.well_id
    WHERE fh.pozo_id = dp.pozo_id
      AND EXISTS (SELECT 1 FROM reporting.dim_tiempo dt WHERE dt.fecha_id = fh.fecha_id AND dt.fecha BETWEEN v_fecha_inicio AND v_fecha_fin)
      AND (fh.fluid_level_tvd_ft IS NULL 
           OR fh.tank_fluid_temp_f IS NULL
           OR fh.bouyant_rod_weight_lb IS NULL);
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] Derivados horarios (física): % filas', v_count;
    
    -- lift_efficiency_pct desde fact_diarias (join separado para evitar referencia circular)
    UPDATE reporting.fact_operaciones_horarias fh SET
        lift_efficiency_pct = COALESCE(
            fh.lift_efficiency_pct,
            CASE 
                WHEN fd.volumen_teorico_bbl > 0 AND fd.produccion_fluido_bbl > 0 
                THEN ROUND((fd.produccion_fluido_bbl / fd.volumen_teorico_bbl * 100)::NUMERIC, 2)
                ELSE NULL
            END
        )
    FROM reporting.fact_operaciones_diarias fd
    WHERE fh.fecha_id = fd.fecha_id AND fh.pozo_id = fd.pozo_id
      AND EXISTS (SELECT 1 FROM reporting.dim_tiempo dt WHERE dt.fecha_id = fh.fecha_id AND dt.fecha BETWEEN v_fecha_inicio AND v_fecha_fin)
      AND fh.lift_efficiency_pct IS NULL;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] Derivados horarios calculados: % filas', v_count;
END;
$$;


-- =============================================================================
-- 2. SP: CALCULAR PROMEDIOS EN FACT_OPERACIONES_DIARIAS
-- =============================================================================
CREATE OR REPLACE PROCEDURE reporting.sp_calcular_promedios_diarios(
    p_fecha_inicio DATE DEFAULT NULL,
    p_fecha_fin DATE DEFAULT NULL
)
LANGUAGE plpgsql AS $$
DECLARE
    v_fecha_inicio DATE := COALESCE(p_fecha_inicio, CURRENT_DATE - INTERVAL '30 days');
    v_fecha_fin DATE := COALESCE(p_fecha_fin, CURRENT_DATE);
    v_count INT := 0;
    v_start_time TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '[V9] Calculando promedios diarios para % a %...', v_fecha_inicio, v_fecha_fin;
    
    WITH promedios_horarios AS (
        SELECT 
            fh.fecha_id, fh.pozo_id,
            AVG(fh.lift_efficiency_pct) AS avg_lift_efficiency,
            AVG(fh.bouyant_rod_weight_lb) AS avg_bouyant_rod,
            AVG(fh.fluid_level_tvd_ft) AS avg_fluid_level,
            AVG(fh.pdp_psi) AS avg_pdp,
            AVG(fh.tank_fluid_temp_f) AS avg_tank_temp,
            AVG(fh.motor_power_hp) AS avg_motor_power,
            AVG(fh.fluid_flow_monitor_bpd) AS avg_fluid_flow
        FROM reporting.fact_operaciones_horarias fh
        JOIN reporting.dim_tiempo dt ON fh.fecha_id = dt.fecha_id
        WHERE dt.fecha BETWEEN v_fecha_inicio AND v_fecha_fin
        GROUP BY fh.fecha_id, fh.pozo_id
    )
    UPDATE reporting.fact_operaciones_diarias fd SET
        promedio_lift_efficiency_pct = COALESCE(fd.promedio_lift_efficiency_pct, ph.avg_lift_efficiency),
        promedio_bouyant_rod_weight_lb = COALESCE(fd.promedio_bouyant_rod_weight_lb, ph.avg_bouyant_rod),
        promedio_fluid_level_tvd_ft = COALESCE(fd.promedio_fluid_level_tvd_ft, ph.avg_fluid_level),
        promedio_pdp_psi = COALESCE(fd.promedio_pdp_psi, ph.avg_pdp),
        promedio_tank_fluid_temp_f = COALESCE(fd.promedio_tank_fluid_temp_f, ph.avg_tank_temp),
        promedio_motor_power_hp = COALESCE(fd.promedio_motor_power_hp, ph.avg_motor_power),
        promedio_fluid_flow_monitor_bpd = COALESCE(fd.promedio_fluid_flow_monitor_bpd, ph.avg_fluid_flow)
    FROM promedios_horarios ph
    WHERE fd.fecha_id = ph.fecha_id AND fd.pozo_id = ph.pozo_id;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] Promedios actualizados desde horarios: % filas', v_count;
    
    -- Fallback: calcular desde stage si horarios no tiene datos
    WITH promedios_stage AS (
        SELECT 
            TO_CHAR(p.timestamp_lectura, 'YYYYMMDD')::INT AS fecha_id,
            p.well_id AS pozo_id,
            AVG(p.eficiencia_levantamiento) AS avg_lift_efficiency,
            AVG(p.rod_weight_buoyant) AS avg_bouyant_rod,
            AVG(p.nivel_fluido_dinamico) AS avg_fluid_level,
            AVG(p.presion_descarga_bomba) AS avg_pdp,
            AVG(p.temperatura_tanque_aceite) AS avg_tank_temp,
            AVG(p.potencia_actual_motor) AS avg_motor_power,
            AVG(p.fluid_flow_monitor_bpd) AS avg_fluid_flow
        FROM stage.tbl_pozo_produccion p
        WHERE DATE(p.timestamp_lectura) BETWEEN v_fecha_inicio AND v_fecha_fin
        GROUP BY TO_CHAR(p.timestamp_lectura, 'YYYYMMDD')::INT, p.well_id
    )
    UPDATE reporting.fact_operaciones_diarias fd SET
        promedio_lift_efficiency_pct = COALESCE(fd.promedio_lift_efficiency_pct, ps.avg_lift_efficiency),
        promedio_bouyant_rod_weight_lb = COALESCE(fd.promedio_bouyant_rod_weight_lb, ps.avg_bouyant_rod),
        promedio_fluid_level_tvd_ft = COALESCE(fd.promedio_fluid_level_tvd_ft, ps.avg_fluid_level),
        promedio_pdp_psi = COALESCE(fd.promedio_pdp_psi, ps.avg_pdp),
        promedio_tank_fluid_temp_f = COALESCE(fd.promedio_tank_fluid_temp_f, ps.avg_tank_temp),
        promedio_motor_power_hp = COALESCE(fd.promedio_motor_power_hp, ps.avg_motor_power),
        promedio_fluid_flow_monitor_bpd = COALESCE(fd.promedio_fluid_flow_monitor_bpd, ps.avg_fluid_flow)
    FROM promedios_stage ps
    WHERE fd.fecha_id = ps.fecha_id AND fd.pozo_id = ps.pozo_id
      AND (fd.promedio_lift_efficiency_pct IS NULL 
           OR fd.promedio_bouyant_rod_weight_lb IS NULL
           OR fd.promedio_fluid_level_tvd_ft IS NULL);
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] Promedios actualizados desde stage: % filas', v_count;
    
    RAISE NOTICE '[V9] fact_operaciones_diarias completado en % ms', 
        EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start_time);
END;
$$;


-- =============================================================================
-- 2.5 SP: COMPLETAR FACT_OPERACIONES_DIARIAS (MTBF, costo)
-- =============================================================================
CREATE OR REPLACE PROCEDURE reporting.sp_completar_fact_diarias(
    p_fecha_inicio DATE DEFAULT NULL,
    p_fecha_fin DATE DEFAULT NULL
)
LANGUAGE plpgsql AS $$
DECLARE
    v_fecha_inicio DATE := COALESCE(p_fecha_inicio, CURRENT_DATE - INTERVAL '30 days');
    v_fecha_fin DATE := COALESCE(p_fecha_fin, CURRENT_DATE);
    v_tarifa_kwh DECIMAL(10,4) := 0.12;
    v_lifting_cost DECIMAL(10,4) := 2.50;
    v_count INT := 0;
BEGIN
    RAISE NOTICE '[V9] Completando fact_operaciones_diarias para % a %...', v_fecha_inicio, v_fecha_fin;
    
    -- Obtener tarifa energía y lifting cost desde config centralizada
    SELECT COALESCE(valor, 0.12) INTO v_tarifa_kwh 
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='ENERGIA' AND parametro='tarifa_kwh_usd';
    SELECT COALESCE(valor, 2.50) INTO v_lifting_cost 
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='LIFTING_COST' AND parametro='default_usd_bbl';
    
    -- MTBF: cuando hay 0 fallas, usar horas acumuladas del pozo como MTBF
    UPDATE reporting.fact_operaciones_diarias SET
        kpi_mtbf_hrs = COALESCE(
            kpi_mtbf_hrs,
            CASE 
                WHEN numero_fallas = 0 THEN 
                    COALESCE(
                        (SELECT prod.horas_operacion_acumuladas 
                         FROM stage.tbl_pozo_produccion prod 
                         WHERE prod.well_id = fact_operaciones_diarias.pozo_id 
                         ORDER BY prod.timestamp_lectura DESC
                         LIMIT 1),
                        (SELECT dp.mtbf_target FROM reporting.dim_pozo dp WHERE dp.pozo_id = fact_operaciones_diarias.pozo_id),
                        2000.00
                    )
                WHEN numero_fallas > 0 AND tiempo_operacion_hrs > 0 THEN 
                    tiempo_operacion_hrs / numero_fallas
                ELSE NULL
            END
        ),
        -- Costo operativo estimado: consumo_kwh * tarifa + lifting_cost * produccion
        costo_operativo_estimado_usd = COALESCE(
            costo_operativo_estimado_usd,
            ROUND((COALESCE(consumo_energia_kwh, 0) * v_tarifa_kwh + 
                   COALESCE(produccion_fluido_bbl, 0) * v_lifting_cost)::NUMERIC, 2)  -- lifting cost desde tbl_config_kpi
        ),
        -- EUR Arps: estimación simplificada desde reservas
        eur_modelo_arps = COALESCE(
            eur_modelo_arps,
            (SELECT res.reserva_inicial_teorica 
             FROM stage.tbl_pozo_reservas res 
             WHERE res.well_id = fact_operaciones_diarias.pozo_id 
             ORDER BY res.fecha_registro DESC
             LIMIT 1)
        )
    WHERE fecha_id IN (
        SELECT fecha_id FROM reporting.dim_tiempo WHERE fecha BETWEEN v_fecha_inicio AND v_fecha_fin
    )
    AND (kpi_mtbf_hrs IS NULL OR costo_operativo_estimado_usd IS NULL OR eur_modelo_arps IS NULL);
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] fact_diarias completado: % filas actualizadas', v_count;
END;
$$;


-- =============================================================================
-- 2.7 SP: RE-AGREGAR MENSUALES DESDE DIARIAS ACTUALIZADAS
-- =============================================================================
CREATE OR REPLACE PROCEDURE reporting.sp_reagregar_mensuales(
    p_fecha_inicio DATE DEFAULT NULL,
    p_fecha_fin DATE DEFAULT NULL
)
LANGUAGE plpgsql AS $$
DECLARE
    v_fecha_inicio DATE := COALESCE(p_fecha_inicio, CURRENT_DATE - INTERVAL '365 days');
    v_fecha_fin DATE := COALESCE(p_fecha_fin, CURRENT_DATE);
    v_count INT := 0;
BEGIN
    RAISE NOTICE '[V9] Re-agregando mensuales desde diarias para % a %...', v_fecha_inicio, v_fecha_fin;
    
    -- Re-agregar promedios que ahora existen en diarias pero no se copiaron a mensuales
    UPDATE reporting.fact_operaciones_mensuales fm SET
        promedio_lift_efficiency_pct = COALESCE(fm.promedio_lift_efficiency_pct, agg.avg_lift_eff),
        promedio_bouyant_rod_weight_lb = COALESCE(fm.promedio_bouyant_rod_weight_lb, agg.avg_bouyant),
        promedio_fluid_level_tvd_ft = COALESCE(fm.promedio_fluid_level_tvd_ft, agg.avg_fluid_level),
        promedio_pdp_psi = COALESCE(fm.promedio_pdp_psi, agg.avg_pdp),
        promedio_tank_fluid_temp_f = COALESCE(fm.promedio_tank_fluid_temp_f, agg.avg_tank_temp),
        promedio_motor_power_hp = COALESCE(fm.promedio_motor_power_hp, agg.avg_motor_power),
        promedio_fluid_flow_monitor_bpd = COALESCE(fm.promedio_fluid_flow_monitor_bpd, agg.avg_fluid_flow),
        remanent_reserves_bbl = COALESCE(fm.remanent_reserves_bbl, res.reserva_inicial_teorica),
        fecha_ultima_carga = NOW()
    FROM (
        SELECT 
            dt.anio_mes, fd.pozo_id,
            AVG(fd.promedio_lift_efficiency_pct) AS avg_lift_eff,
            AVG(fd.promedio_bouyant_rod_weight_lb) AS avg_bouyant,
            AVG(fd.promedio_fluid_level_tvd_ft) AS avg_fluid_level,
            AVG(fd.promedio_pdp_psi) AS avg_pdp,
            AVG(fd.promedio_tank_fluid_temp_f) AS avg_tank_temp,
            AVG(fd.promedio_motor_power_hp) AS avg_motor_power,
            AVG(fd.promedio_fluid_flow_monitor_bpd) AS avg_fluid_flow
        FROM reporting.fact_operaciones_diarias fd
        JOIN reporting.dim_tiempo dt ON fd.fecha_id = dt.fecha_id
        WHERE dt.fecha BETWEEN v_fecha_inicio AND v_fecha_fin
        GROUP BY dt.anio_mes, fd.pozo_id
    ) agg
    LEFT JOIN stage.tbl_pozo_reservas res ON agg.pozo_id = res.well_id
    WHERE fm.anio_mes = agg.anio_mes AND fm.pozo_id = agg.pozo_id;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] Mensuales re-agregados: % filas', v_count;
END;
$$;


-- =============================================================================
-- 3. SP: CALCULAR MTBF_DIAS EN DATASET_KPI_BUSINESS (V7-compatible)
-- =============================================================================
CREATE OR REPLACE PROCEDURE reporting.sp_calcular_kpis_business(
    p_fecha DATE DEFAULT NULL
)
LANGUAGE plpgsql AS $$
DECLARE
    v_fecha DATE := COALESCE(p_fecha, CURRENT_DATE);
    v_count INT := 0;
BEGIN
    RAISE NOTICE '[V9] Calculando KPIs business para fecha: %', v_fecha;
    
    -- Actualizar kpi_mtbf_dias desde dataset_current_values (columnas V7 WIDE)
    UPDATE reporting.dataset_kpi_business SET
        kpi_mtbf_dias = CASE 
            WHEN dcv.kpi_mtbf_hrs_act IS NOT NULL THEN dcv.kpi_mtbf_hrs_act / 24.0
            ELSE NULL
        END
    FROM (
        SELECT well_id, kpi_mtbf_hrs_act 
        FROM reporting.dataset_current_values
    ) dcv
    WHERE dataset_kpi_business.well_id = dcv.well_id
      AND dataset_kpi_business.fecha = v_fecha
      AND dataset_kpi_business.kpi_mtbf_dias IS NULL;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] KPIs business actualizados: % filas', v_count;
END;
$$;


-- =============================================================================
-- 3.5 SP: CALCULAR KPIs + SEMÁFOROS EN FACT_OPERACIONES_HORARIAS
-- Calcula: MTBF, Uptime, kWh/bbl, Vol Eff, AI Accuracy, Lift Efficiency
-- + targets/baselines (dim_pozo → tbl_config_kpi fallback)
-- + semáforos completos (variance_pct, status_color, status_level, severity_label)
-- =============================================================================
CREATE OR REPLACE PROCEDURE reporting.sp_calcular_kpis_horarios(
    p_fecha_inicio DATE DEFAULT NULL,
    p_fecha_fin DATE DEFAULT NULL
)
LANGUAGE plpgsql AS $$
DECLARE
    v_fecha_inicio DATE := COALESCE(p_fecha_inicio, CURRENT_DATE - INTERVAL '30 days');
    v_fecha_fin DATE := COALESCE(p_fecha_fin, CURRENT_DATE);
    v_count INT := 0;
    v_start_time TIMESTAMP := clock_timestamp();
    -- Fallback targets desde tbl_config_kpi
    v_mtbf_target DECIMAL(10,2);
    v_mtbf_baseline DECIMAL(10,2);
    v_uptime_target DECIMAL(5,2);
    v_uptime_baseline DECIMAL(5,2);
    v_kwh_target DECIMAL(10,4);
    v_kwh_baseline DECIMAL(10,4);
    v_vol_eff_target DECIMAL(5,2);
    v_vol_eff_baseline DECIMAL(5,2);
    v_ai_accuracy_target DECIMAL(5,2);
    v_ai_accuracy_baseline DECIMAL(5,2);
    v_lift_eff_target DECIMAL(5,2) := 85.00;
    v_lift_eff_baseline DECIMAL(5,2) := 80.00;
    v_stroke_var_warning DECIMAL(5,2) := 5.00;
    v_stroke_var_critical DECIMAL(5,2) := 15.00;
BEGIN
    RAISE NOTICE '[V9] Calculando KPIs horarios para % a %...', v_fecha_inicio, v_fecha_fin;

    -- ─── Obtener configuración fallback ─────────────────────────────────
    SELECT COALESCE(valor, 2000) INTO v_mtbf_target
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_MTBF' AND parametro='target_default_hrs';
    SELECT COALESCE(valor, 1500) INTO v_mtbf_baseline
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_MTBF' AND parametro='baseline_default_hrs';
    SELECT COALESCE(valor, 95) INTO v_uptime_target
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_UPTIME' AND parametro='target_default_pct';
    SELECT COALESCE(valor, 90) INTO v_uptime_baseline
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_UPTIME' AND parametro='baseline_default_pct';
    SELECT COALESCE(valor, 10) INTO v_kwh_target
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_KWH_BBL' AND parametro='target_default';
    SELECT COALESCE(valor, 12) INTO v_kwh_baseline
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_KWH_BBL' AND parametro='baseline_default';
    SELECT COALESCE(valor, 85) INTO v_vol_eff_target
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_VOL_EFF' AND parametro='target_default_pct';
    SELECT COALESCE(valor, 80) INTO v_vol_eff_baseline
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_VOL_EFF' AND parametro='baseline_default_pct';
    SELECT COALESCE(valor, 85) INTO v_ai_accuracy_target
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='AI_ACCURACY' AND parametro='target_default_pct';
    SELECT COALESCE(valor, 85) INTO v_ai_accuracy_baseline
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='AI_ACCURACY' AND parametro='baseline_default_pct';

    -- Lift Efficiency targets (centralizados)
    SELECT COALESCE(valor, 85) INTO v_lift_eff_target
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_VOL_EFF' AND parametro='target_default_pct';
    SELECT COALESCE(valor, 80) INTO v_lift_eff_baseline
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_VOL_EFF' AND parametro='baseline_default_pct';

    -- ═════════════════════════════════════════════════════════════════════
    -- PASO 1: Calcular KPI RAW VALUES
    -- ═════════════════════════════════════════════════════════════════════

    -- 1A: UPTIME → (tiempo_operacion_min / 60) * 100
    UPDATE reporting.fact_operaciones_horarias fh SET
        kpi_uptime_pct = ROUND((fh.tiempo_operacion_min / 60.0 * 100)::NUMERIC, 2)
    WHERE EXISTS (SELECT 1 FROM reporting.dim_tiempo dt 
                  WHERE dt.fecha_id = fh.fecha_id AND dt.fecha BETWEEN v_fecha_inicio AND v_fecha_fin)
      AND fh.kpi_uptime_pct IS NULL
      AND fh.tiempo_operacion_min IS NOT NULL;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] KPI Uptime: % filas', v_count;

    -- 1B: kWh/bbl → (motor_power_hp * 0.7457 * hrs_op) / produccion_fluido_bbl
    UPDATE reporting.fact_operaciones_horarias fh SET
        kpi_kwh_bbl = ROUND(
            (fh.motor_power_hp * 0.7457 * (fh.tiempo_operacion_min / 60.0)) 
            / fh.produccion_fluido_bbl
        , 4)
    WHERE EXISTS (SELECT 1 FROM reporting.dim_tiempo dt 
                  WHERE dt.fecha_id = fh.fecha_id AND dt.fecha BETWEEN v_fecha_inicio AND v_fecha_fin)
      AND fh.kpi_kwh_bbl IS NULL
      AND fh.produccion_fluido_bbl > 0
      AND fh.motor_power_hp IS NOT NULL
      AND fh.tiempo_operacion_min > 0;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] KPI kWh/bbl: % filas', v_count;

    -- 1C: Volumen Teórico Horario + Eficiencia Volumétrica
    --     vol_teorico = (π/4) × d² × L × SPM × tiempo_min / 9702
    UPDATE reporting.fact_operaciones_horarias fh SET
        volumen_teorico_hora_bbl = ROUND(
            (3.14159265 / 4.0) 
            * POWER(dp.diametro_embolo_bomba_in, 2) 
            * COALESCE(fh.current_stroke_length_in, dp.longitud_carrera_nominal_unidad_in)
            * fh.spm_promedio
            * fh.tiempo_operacion_min
            / 9702.0
        , 2)
    FROM reporting.dim_pozo dp
    WHERE fh.pozo_id = dp.pozo_id
      AND EXISTS (SELECT 1 FROM reporting.dim_tiempo dt 
                  WHERE dt.fecha_id = fh.fecha_id AND dt.fecha BETWEEN v_fecha_inicio AND v_fecha_fin)
      AND fh.volumen_teorico_hora_bbl IS NULL
      AND dp.diametro_embolo_bomba_in IS NOT NULL
      AND COALESCE(fh.current_stroke_length_in, dp.longitud_carrera_nominal_unidad_in) IS NOT NULL
      AND fh.spm_promedio > 0
      AND fh.tiempo_operacion_min > 0;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] Volumen teórico horario: % filas', v_count;

    UPDATE reporting.fact_operaciones_horarias fh SET
        kpi_efic_vol_pct = LEAST(
            ROUND((fh.produccion_fluido_bbl / fh.volumen_teorico_hora_bbl * 100)::NUMERIC, 2),
            150.00  -- Cap 150%: tolerancia medición; valores mayores → DQ issue
        )
    WHERE EXISTS (SELECT 1 FROM reporting.dim_tiempo dt 
                  WHERE dt.fecha_id = fh.fecha_id AND dt.fecha BETWEEN v_fecha_inicio AND v_fecha_fin)
      AND fh.kpi_efic_vol_pct IS NULL
      AND fh.volumen_teorico_hora_bbl > 0
      AND fh.produccion_fluido_bbl > 0;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] KPI Vol Eff: % filas', v_count;

    -- 1D: MTBF (tiempo acumulado entre fallas)
    --     MTBF = hrs_acumuladas / fallas_acumuladas (si hay fallas)
    --     MTBF = hrs_acumuladas (si no hay fallas → todo el tiempo sin falla)
    WITH mtbf_calcs AS (
        SELECT 
            fh.fecha_id, fh.hora_id, fh.pozo_id,
            SUM(COALESCE(fh.tiempo_operacion_min, 0) / 60.0) 
                OVER (PARTITION BY fh.pozo_id, fh.fecha_id 
                      ORDER BY fh.hora_id 
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS hrs_acum,
            SUM(COALESCE(fh.numero_fallas_hora, 0)) 
                OVER (PARTITION BY fh.pozo_id, fh.fecha_id 
                      ORDER BY fh.hora_id 
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fallas_acum
        FROM reporting.fact_operaciones_horarias fh
        WHERE EXISTS (SELECT 1 FROM reporting.dim_tiempo dt 
                      WHERE dt.fecha_id = fh.fecha_id AND dt.fecha BETWEEN v_fecha_inicio AND v_fecha_fin)
          AND fh.kpi_mtbf_hrs IS NULL
    )
    UPDATE reporting.fact_operaciones_horarias fh SET
        kpi_mtbf_hrs = CASE 
            WHEN mc.fallas_acum > 0 AND mc.hrs_acum > 0 
                THEN ROUND((mc.hrs_acum / mc.fallas_acum)::NUMERIC, 2)
            WHEN mc.hrs_acum > 0 
                THEN ROUND(mc.hrs_acum::NUMERIC, 2)
            ELSE NULL
        END
    FROM mtbf_calcs mc
    WHERE fh.fecha_id = mc.fecha_id 
      AND fh.hora_id = mc.hora_id 
      AND fh.pozo_id = mc.pozo_id;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] KPI MTBF: % filas', v_count;

    -- 1E: AI Accuracy (placeholder – sin modelo activo, queda NULL)

    -- ═════════════════════════════════════════════════════════════════════
    -- PASO 2: TARGETS y BASELINES (dim_pozo → tbl_config_kpi fallback)
    -- ═════════════════════════════════════════════════════════════════════
    UPDATE reporting.fact_operaciones_horarias fh SET
        kpi_mtbf_target     = COALESCE(fh.kpi_mtbf_target,     dp.mtbf_target,              v_mtbf_target),
        kpi_mtbf_baseline   = COALESCE(fh.kpi_mtbf_baseline,   dp.mtbf_baseline,            v_mtbf_baseline),
        kpi_uptime_target   = COALESCE(fh.kpi_uptime_target,   dp.kpi_uptime_pct_target,    v_uptime_target),
        kpi_uptime_baseline = COALESCE(fh.kpi_uptime_baseline,                               v_uptime_baseline),
        kpi_kwh_bbl_target  = COALESCE(fh.kpi_kwh_bbl_target,  dp.kpi_kwh_bbl_target,       v_kwh_target),
        kpi_kwh_bbl_baseline= COALESCE(fh.kpi_kwh_bbl_baseline,dp.kpi_kwh_bbl_baseline,     v_kwh_baseline),
        kpi_vol_eff_target  = COALESCE(fh.kpi_vol_eff_target,  dp.vol_eff_target,            v_vol_eff_target),
        kpi_vol_eff_baseline= COALESCE(fh.kpi_vol_eff_baseline,                              v_vol_eff_baseline),
        kpi_ai_accuracy_target   = COALESCE(fh.kpi_ai_accuracy_target,                       v_ai_accuracy_target),
        kpi_ai_accuracy_baseline = COALESCE(fh.kpi_ai_accuracy_baseline,                     v_ai_accuracy_baseline),
        kpi_lift_eff_target      = COALESCE(fh.kpi_lift_eff_target,  dp.lift_efficiency_target, v_lift_eff_target),
        kpi_lift_eff_baseline    = COALESCE(fh.kpi_lift_eff_baseline,                           v_lift_eff_baseline)
    FROM reporting.dim_pozo dp
    WHERE fh.pozo_id = dp.pozo_id
      AND EXISTS (SELECT 1 FROM reporting.dim_tiempo dt 
                  WHERE dt.fecha_id = fh.fecha_id AND dt.fecha BETWEEN v_fecha_inicio AND v_fecha_fin)
      AND (fh.kpi_mtbf_target IS NULL OR fh.kpi_uptime_target IS NULL 
           OR fh.kpi_kwh_bbl_target IS NULL OR fh.kpi_vol_eff_target IS NULL
           OR fh.kpi_lift_eff_target IS NULL OR fh.kpi_ai_accuracy_target IS NULL);

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] Targets/baselines horarios: % filas', v_count;

    -- ═════════════════════════════════════════════════════════════════════
    -- PASO 3: SEMÁFOROS UNIFICADOS vía fnc_evaluar_variable (V8 compatible)
    --   Escala 0-9 (tbl_catalogo_status): 0=Óptimo,1=Normal,3=Alerta,4=Crítico,5=Falla,7=SinDatos
    --   Paleta V8: #00CC66 / #99CC00 / #FFBB33 / #FF4444 / #CC0000 / #B0B0B0
    --   Umbrales: desde tbl_config_evaluacion (Zero-Hardcode)
    --   Patrón: CTE + 6 LATERAL joins + 1 UPDATE (mismo patrón que V8)
    -- ═════════════════════════════════════════════════════════════════════
    WITH src AS (
        SELECT fh2.pozo_id, fh2.fecha_id, fh2.hora_id,
               fh2.kpi_mtbf_hrs,        fh2.kpi_mtbf_target,
               fh2.kpi_uptime_pct,      fh2.kpi_uptime_target,
               fh2.kpi_kwh_bbl,         fh2.kpi_kwh_bbl_target,
               fh2.kpi_efic_vol_pct,    fh2.kpi_vol_eff_target,
               fh2.kpi_ai_accuracy_pct, fh2.kpi_ai_accuracy_target,
               fh2.lift_efficiency_pct,  fh2.kpi_lift_eff_target
        FROM reporting.fact_operaciones_horarias fh2
        WHERE EXISTS (SELECT 1 FROM reporting.dim_tiempo dt 
                      WHERE dt.fecha_id = fh2.fecha_id AND dt.fecha BETWEEN v_fecha_inicio AND v_fecha_fin)
          AND (fh2.kpi_mtbf_variance_pct IS NULL OR fh2.kpi_uptime_variance_pct IS NULL
               OR fh2.kpi_kwh_bbl_variance_pct IS NULL OR fh2.kpi_vol_eff_variance_pct IS NULL
               OR fh2.kpi_lift_eff_variance_pct IS NULL)
    )
    UPDATE reporting.fact_operaciones_horarias fh SET
        -- ── MTBF (MAYOR_MEJOR — desde tbl_config_evaluacion) ───────────
        kpi_mtbf_variance_pct   = e1.variance_pct,
        kpi_mtbf_status_color   = e1.status_color,
        kpi_mtbf_status_level   = e1.status_level,
        kpi_mtbf_severity_label = e1.severity_label,
        -- ── UPTIME (MAYOR_MEJOR) ───────────────────────────────────────
        kpi_uptime_variance_pct   = e2.variance_pct,
        kpi_uptime_status_color   = e2.status_color,
        kpi_uptime_status_level   = e2.status_level,
        kpi_uptime_severity_label = e2.severity_label,
        -- ── kWh/bbl (MENOR_MEJOR — dirección auto desde config) ────────
        kpi_kwh_bbl_variance_pct   = e3.variance_pct,
        kpi_kwh_bbl_status_color   = e3.status_color,
        kpi_kwh_bbl_status_level   = e3.status_level,
        kpi_kwh_bbl_severity_label = e3.severity_label,
        -- ── Vol Eff (MAYOR_MEJOR) ──────────────────────────────────────
        kpi_vol_eff_variance_pct   = e4.variance_pct,
        kpi_vol_eff_status_color   = e4.status_color,
        kpi_vol_eff_status_level   = e4.status_level,
        kpi_vol_eff_severity_label = e4.severity_label,
        -- ── AI Accuracy (MAYOR_MEJOR — placeholder) ────────────────────
        kpi_ai_accuracy_variance_pct   = e5.variance_pct,
        kpi_ai_accuracy_status_color   = e5.status_color,
        kpi_ai_accuracy_status_level   = e5.status_level,
        kpi_ai_accuracy_severity_label = e5.severity_label,
        -- ── Lift Efficiency (MAYOR_MEJOR) ──────────────────────────────
        kpi_lift_eff_variance_pct   = e6.variance_pct,
        kpi_lift_eff_status_color   = e6.status_color,
        kpi_lift_eff_status_level   = e6.status_level,
        kpi_lift_eff_severity_label = e6.severity_label
    FROM src
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('kpi_mtbf',       src.kpi_mtbf_hrs,        src.kpi_mtbf_target,        NULL) e1
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('kpi_uptime',     src.kpi_uptime_pct,      src.kpi_uptime_target,      NULL) e2
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('kpi_kwh_bbl',    src.kpi_kwh_bbl,         src.kpi_kwh_bbl_target,     NULL) e3
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('kpi_vol_eff',    src.kpi_efic_vol_pct,    src.kpi_vol_eff_target,     NULL) e4
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('ai_accuracy',    src.kpi_ai_accuracy_pct, src.kpi_ai_accuracy_target, NULL) e5
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable('lift_efficiency', src.lift_efficiency_pct, src.kpi_lift_eff_target,    NULL) e6
    WHERE fh.pozo_id = src.pozo_id AND fh.fecha_id = src.fecha_id AND fh.hora_id = src.hora_id;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[V9] Semáforos horarios (V8 unificado): % filas', v_count;

    RAISE NOTICE '[V9] KPIs horarios completados en % ms',
        EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start_time);
END;
$$;

COMMENT ON PROCEDURE reporting.sp_calcular_kpis_horarios IS 
'Calcula 6 KPIs + semáforos completos en fact_operaciones_horarias.
KPIs: MTBF, Uptime, kWh/bbl, Vol Eff, AI Accuracy, Lift Efficiency.
Semáforos UNIFICADOS V8: fnc_evaluar_variable() vía LATERAL join.
  Escala 0-9 (tbl_catalogo_status): 0=Óptimo,1=Normal,3=Alerta,4=Crítico,5=Falla,7=SinDatos.
  Paleta V8: #00CC66/#99CC00/#FFBB33/#FF4444/#CC0000/#B0B0B0.
  Umbrales: desde tbl_config_evaluacion (Zero-Hardcode).
Targets: dim_pozo > tbl_config_kpi fallback.
ORDEN EN PIPELINE: después de sp_calcular_derivados_horarios, antes de sp_calcular_promedios_diarios.';


-- =============================================================================
-- 4. SP MAESTRO: EJECUTAR TODOS LOS CÁLCULOS DERIVADOS
-- =============================================================================
CREATE OR REPLACE PROCEDURE reporting.sp_calcular_derivados_completos(
    p_fecha_inicio DATE DEFAULT NULL,
    p_fecha_fin DATE DEFAULT NULL,
    p_incluir_current_values BOOLEAN DEFAULT TRUE,
    p_incluir_promedios_diarios BOOLEAN DEFAULT TRUE,
    p_incluir_kpis_business BOOLEAN DEFAULT TRUE
)
LANGUAGE plpgsql AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE '[V9] INICIANDO CÁLCULOS DERIVADOS COMPLETOS';
    RAISE NOTICE '========================================';
    
    IF p_incluir_current_values THEN
        CALL reporting.sp_calcular_derivados_current_values();
    END IF;
    
    -- Derivados horarios (fluid_level, buoyant_rod, tank_temp, lift_eff)
    CALL reporting.sp_calcular_derivados_horarios(p_fecha_inicio, p_fecha_fin);
    
    -- KPIs horarios + semáforos (MTBF, Uptime, kWh/bbl, Vol Eff, AI Accuracy, Lift Eff)
    CALL reporting.sp_calcular_kpis_horarios(p_fecha_inicio, p_fecha_fin);
    
    IF p_incluir_promedios_diarios THEN
        CALL reporting.sp_calcular_promedios_diarios(p_fecha_inicio, p_fecha_fin);
    END IF;
    
    -- Completar diarias (MTBF, costo_operativo, EUR)
    CALL reporting.sp_completar_fact_diarias(p_fecha_inicio, p_fecha_fin);
    
    -- Re-agregar mensuales desde diarias actualizadas
    CALL reporting.sp_reagregar_mensuales(p_fecha_inicio, p_fecha_fin);
    
    IF p_incluir_kpis_business THEN
        CALL reporting.sp_calcular_kpis_business(COALESCE(p_fecha_fin, CURRENT_DATE));
    END IF;
    
    RAISE NOTICE '========================================';
    RAISE NOTICE '[V9] CÁLCULOS DERIVADOS COMPLETADOS EN % ms',
        EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start_time);
    RAISE NOTICE '========================================';
END;
$$;

COMMENT ON PROCEDURE reporting.sp_calcular_derivados_completos IS 
'SP maestro V9: ejecuta todos los cálculos derivados.
ORDEN EN PIPELINE: después de actualizar_current_values_v4() y sp_sync_dim_pozo_targets()';


-- =============================================================================
-- 5. VISTA DE AUDITORÍA
-- =============================================================================
CREATE OR REPLACE VIEW reporting.vw_audit_calculos_derivados AS
SELECT 
    'dataset_current_values' AS tabla,
    COUNT(*) AS total_registros,
    COUNT(fluid_level_tvd_ft) AS con_fluid_level,
    COUNT(pwf_psi_act) AS con_pwf,
    COUNT(qf_fluid_flow_monitor_bpd) AS con_qf_flow,
    COUNT(pump_stroke_length_act) AS con_stroke_length,
    COUNT(hydralift_unit_load_pct) AS con_hydralift,
    COUNT(road_load_pct_act) AS con_road_load,
    ROUND(COUNT(fluid_level_tvd_ft)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 1) AS pct_completitud
FROM reporting.dataset_current_values
UNION ALL
SELECT 
    'fact_operaciones_diarias' AS tabla,
    COUNT(*) AS total_registros,
    COUNT(promedio_lift_efficiency_pct) AS con_lift_eff,
    COUNT(promedio_fluid_level_tvd_ft) AS con_fluid_level,
    COUNT(promedio_bouyant_rod_weight_lb) AS con_bouyant,
    COUNT(promedio_pdp_psi) AS con_pdp,
    COUNT(promedio_tank_fluid_temp_f) AS con_tank_temp,
    COUNT(promedio_motor_power_hp) AS con_motor_power,
    ROUND(COUNT(promedio_lift_efficiency_pct)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 1) AS pct_completitud
FROM reporting.fact_operaciones_diarias;
