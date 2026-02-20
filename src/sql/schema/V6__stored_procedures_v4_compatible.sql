/*
--------------------------------------------------------------------------------
-- MOTOR LÓGICO V6.0 (V4 COMPATIBLE - INTEGRADO)
-- Contiene:
--   1. Tipos y Funciones de Evaluación Universal (Rescatados de V5)
--   2. Vista Pivoteada de Límites V4
--   3. Procedimiento Snapshot (Actualizar Current Values)
--------------------------------------------------------------------------------
*/

-- (Lógica Universal movida a V4__referencial_schema_redesign.sql)





-- =============================================================================
-- 3. SNAPSHOT ZERO-CALC V4 (Actualización Current Values)
-- =============================================================================
CREATE OR REPLACE PROCEDURE reporting.actualizar_current_values_v4()
LANGUAGE plpgsql AS $$
BEGIN
    -- A. UPSERT DE DATOS CRUDOS (Mapping Stage -> Reporting V4)
    INSERT INTO reporting.dataset_current_values AS target (
        well_id,
        nombre_pozo,
        cliente,
        region,
        campo,
        ultima_actualizacion,
        
        -- Mapping V4 Renamed Columns & Standard Columns
        well_head_pressure_psi_act,      -- Antes whp_psi
        casing_head_pressure_psi_act,    -- Antes chp_psi
        pump_intake_pressure_psi_act,    -- Antes pip_psi
        pump_discharge_pressure_psi_act, -- Antes pdp_psi
        
        spm_actual,                      -- MANTENER (Legacy/Display)
        pump_avg_spm_act,                -- Antes spm_actual / V4 Standard
        
        pump_fill_monitor_pct,
        gas_fill_monitor_pct_act,
        rod_weight_buoyant_lb_act,
        
        motor_power_hp_act,              -- Antes potencia_hp
        motor_current_a_act,             -- Antes amp_motor
        
        -- Nuevos campos V4
        motor_running_flag,
        produccion_fluido_bpd_act,
        produccion_petroleo_diaria_bpd_act,
        produccion_agua_diaria_bpd_act,
        
        -- [V6.1] Campos de Carga y Eficiencia (Road Load & Lift Efficiency)
        max_rod_load_lb_act,
        min_rod_load_lb_act,
        road_load_pct_act,
        lift_efficiency_pct_act,
        
        -- [V6.2] Nuevos Mapeos Directos
        kpi_kwh_bbl_act,                 -- ID:71 kwh_por_barril
        kpi_uptime_pct_act,              -- ID:106 porcentaje_operacion_diario
        tank_fluid_temperature_f,        -- ID:94 temperatura_tanque_aceite
        current_stroke_length_act_in,    -- ID:42 longitud_carrera_nominal_unidad_in
        
        -- [V6.3] Mapeos adicionales
        llenado_bomba_pct,               -- ID:64 pump_fill_monitor (alias)
        daily_downtime_act,              -- ID:114 tiempo_parada_poc_diario
        total_fluid_today_bbl,           -- Producción fluido acumulada
        oil_today_bbl,                   -- Producción petróleo acumulada
        water_today_bbl,                 -- Producción agua acumulada
        gas_today_mcf,                   -- Producción gas acumulada
        
        -- [V6.5] Campos CALCULADOS (no mapeos)
        carga_unidad_pct,                -- (carga_caja_engranajes / carga_nominal) * 100
        turno_operativo,                 -- Derivado de hora
        kpi_vol_eff_pct_act,             -- Alias de lift_efficiency
        
        -- [V6.6] KPI MTBF (Mean Time Between Failures)
        kpi_mtbf_hrs_act,                -- horas_operacion / conteo_fallas
        
        updated_at
    )
    SELECT DISTINCT ON (m.well_id)
        m.well_id,
        m.nombre_pozo,
        m.cliente,
        m.region,
        m.campo,
        p.timestamp_lectura,
        
        p.presion_cabezal,          -- well_head_pressure_psi_act
        p.presion_casing,           -- casing_head_pressure_psi_act
        p.PIP,                      -- pump_intake_pressure_psi_act
        p.presion_descarga_bomba,   -- pump_discharge_pressure_psi_act
        
        p.spm_promedio,             -- spm_actual
        p.spm_promedio,             -- pump_avg_spm_act (Mismo origen por ahora)
        
        p.pump_fill_monitor,        -- pump_fill_monitor_pct
        p.monitor_llenado_gas,      -- gas_fill_monitor_pct_act
        p.rod_weight_buoyant,       -- rod_weight_buoyant_lb_act
        
        p.potencia_actual_motor,    -- motor_power_hp_act
        p.current_amperage,         -- motor_current_a_act
        
        p.estado_motor,             -- motor_running_flag
        p.produccion_fluido_diaria, -- produccion_fluido_bpd_act
        p.produccion_petroleo_diaria, -- prod_petroleo_diaria_bpd_act
        p.produccion_agua_diaria,   -- produccion_agua_diaria_bpd_act
        
        -- [V6.1] Carga y Eficiencia calculados
        p.maximum_rod_load,         -- max_rod_load_lb_act
        p.minimum_rod_load,         -- min_rod_load_lb_act
        ROUND((p.maximum_rod_load / NULLIF(m.carga_nominal_unidad, 0)) * 100, 2),  -- road_load_pct_act
        p.eficiencia_levantamiento, -- lift_efficiency_pct_act
        
        -- [V6.2] Nuevos Mapeos Directos
        p.kwh_por_barril,                    -- kpi_kwh_bbl_act
        p.porcentaje_operacion_diario,       -- kpi_uptime_pct_act
        p.temperatura_tanque_aceite,         -- tank_fluid_temperature_f
        p.longitud_carrera_nominal_unidad_in,-- current_stroke_length_act_in
        
        -- [V6.3] Mapeos adicionales
        p.pump_fill_monitor,                 -- llenado_bomba_pct (alias de pump_fill_monitor_pct)
        p.tiempo_parada_poc_diario,          -- daily_downtime_act
        p.produccion_fluido_diaria,          -- total_fluid_today_bbl
        p.produccion_petroleo_diaria,        -- oil_today_bbl
        p.produccion_agua_diaria,            -- water_today_bbl
        p.produccion_gas_diaria,             -- gas_today_mcf
        
        -- [V6.5] Campos CALCULADOS (no mapeos)
        ROUND((p.carga_caja_engranajes / NULLIF(m.carga_nominal_unidad, 0)) * 100, 2),  -- carga_unidad_pct
        CASE 
            WHEN EXTRACT(HOUR FROM p.timestamp_lectura) BETWEEN 6 AND 13 THEN 'DIA'
            WHEN EXTRACT(HOUR FROM p.timestamp_lectura) BETWEEN 14 AND 21 THEN 'TARDE'
            ELSE 'NOCHE'
        END,                                 -- turno_operativo
        p.eficiencia_levantamiento,          -- kpi_vol_eff_pct_act (alias)
        
        -- [V6.6] KPI MTBF: MTBF = Horas operación / Número de fallas
        CASE 
            WHEN COALESCE(p.conteo_poc_medidor_acum, 0) > 0 THEN 
                ROUND(p.horas_operacion_acumuladas / p.conteo_poc_medidor_acum, 2)
            WHEN COALESCE(p.horas_operacion_acumuladas, 0) > 0 THEN 
                p.horas_operacion_acumuladas  -- Sin fallas = MTBF = todas las horas
            ELSE NULL
        END,                                 -- kpi_mtbf_hrs_act
        
        NOW()
    FROM stage.tbl_pozo_maestra m
    LEFT JOIN stage.tbl_pozo_produccion p
        ON m.well_id = p.well_id
    ORDER BY m.well_id, p.timestamp_lectura DESC
    ON CONFLICT (well_id) DO UPDATE SET 
        nombre_pozo                = EXCLUDED.nombre_pozo,
        cliente                    = EXCLUDED.cliente,
        region                     = EXCLUDED.region,
        campo                      = EXCLUDED.campo,
        
        well_head_pressure_psi_act      = EXCLUDED.well_head_pressure_psi_act,
        casing_head_pressure_psi_act    = EXCLUDED.casing_head_pressure_psi_act,
        pump_intake_pressure_psi_act    = EXCLUDED.pump_intake_pressure_psi_act,
        pump_discharge_pressure_psi_act = EXCLUDED.pump_discharge_pressure_psi_act,
        
        spm_actual                 = EXCLUDED.spm_actual,
        pump_avg_spm_act           = EXCLUDED.pump_avg_spm_act, 
        
        pump_fill_monitor_pct      = EXCLUDED.pump_fill_monitor_pct,
        gas_fill_monitor_pct_act   = EXCLUDED.gas_fill_monitor_pct_act,
        rod_weight_buoyant_lb_act  = EXCLUDED.rod_weight_buoyant_lb_act,
        
        motor_power_hp_act         = EXCLUDED.motor_power_hp_act,
        motor_current_a_act        = EXCLUDED.motor_current_a_act,
        
        motor_running_flag         = EXCLUDED.motor_running_flag,
        produccion_fluido_bpd_act       = EXCLUDED.produccion_fluido_bpd_act,
        produccion_petroleo_diaria_bpd_act    = EXCLUDED.produccion_petroleo_diaria_bpd_act,
        produccion_agua_diaria_bpd_act  = EXCLUDED.produccion_agua_diaria_bpd_act,
        
        -- [V6.1] Carga y Eficiencia
        max_rod_load_lb_act        = EXCLUDED.max_rod_load_lb_act,
        min_rod_load_lb_act        = EXCLUDED.min_rod_load_lb_act,
        road_load_pct_act          = EXCLUDED.road_load_pct_act,
        
        -- [V6.2] Nuevos Mapeos
        kpi_kwh_bbl_act            = EXCLUDED.kpi_kwh_bbl_act,
        kpi_uptime_pct_act         = EXCLUDED.kpi_uptime_pct_act,
        tank_fluid_temperature_f   = EXCLUDED.tank_fluid_temperature_f,
        current_stroke_length_act_in = EXCLUDED.current_stroke_length_act_in,
        lift_efficiency_pct_act    = EXCLUDED.lift_efficiency_pct_act,
        
        -- [V6.3] Mapeos adicionales
        llenado_bomba_pct          = EXCLUDED.llenado_bomba_pct,
        daily_downtime_act         = EXCLUDED.daily_downtime_act,
        total_fluid_today_bbl      = EXCLUDED.total_fluid_today_bbl,
        oil_today_bbl              = EXCLUDED.oil_today_bbl,
        water_today_bbl            = EXCLUDED.water_today_bbl,
        gas_today_mcf              = EXCLUDED.gas_today_mcf,
        
        -- [V6.5] Campos CALCULADOS
        carga_unidad_pct           = EXCLUDED.carga_unidad_pct,
        turno_operativo            = EXCLUDED.turno_operativo,
        kpi_vol_eff_pct_act        = EXCLUDED.kpi_vol_eff_pct_act,
        
        -- [V6.6] KPI MTBF
        kpi_mtbf_hrs_act           = EXCLUDED.kpi_mtbf_hrs_act,
        
        ultima_actualizacion       = EXCLUDED.ultima_actualizacion,
        updated_at                 = NOW();

    -- B. CAMPOS DERIVADOS SIMPLES (semáforos delegados a V8 aplicar_evaluacion_universal)
    -- ===================================================================================
    -- NOTA: Toda la lógica de semáforos (CASE WHEN hardcodeados + fnc_evaluar_universal)
    -- fue ELIMINADA de V6. V8 es el ÚNICO dueño de todos los semáforos vía fnc_evaluar_variable.
    -- V6 solo conserva campos que V8 NO maneja: comunicación, water_cut, turno.
    -- ===================================================================================
    UPDATE reporting.dataset_current_values tgt
    SET 
        -- Estado de comunicación (no es semáforo de KPI, es estado de conectividad)
        color_estado_comunicacion = CASE 
            WHEN EXTRACT(EPOCH FROM (NOW() - tgt.ultima_actualizacion))/60 > 60
                THEN '#B0B0B0' -- OFFLINE
            ELSE '#00C851'    -- NORMAL
        END,
        minutos_sin_reportar = ROUND(EXTRACT(EPOCH FROM (NOW() - tgt.ultima_actualizacion))/60)::INT,
        estado_comunicacion = CASE 
            WHEN EXTRACT(EPOCH FROM (NOW() - tgt.ultima_actualizacion))/60 > 60 THEN 'OFFLINE'
            WHEN EXTRACT(EPOCH FROM (NOW() - tgt.ultima_actualizacion))/60 > 30 THEN 'DELAYED'
            ELSE 'ONLINE'
        END,
        -- Water cut derivado
        water_cut_pct = ROUND((tgt.produccion_agua_diaria_bpd_act / NULLIF(tgt.produccion_fluido_bpd_act, 0)) * 100, 2);

    RAISE NOTICE 'Snapshot V4 actualizado (UPSERT datos crudos + campos derivados). Semáforos delegados a V8.';
END;
$$;
