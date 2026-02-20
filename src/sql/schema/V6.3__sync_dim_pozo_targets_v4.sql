/*
--------------------------------------------------------------------------------
-- SYNC DIM POZO TARGETS V6.3 (V4 COMPATIBLE) — CORREGIDO
-- Sincroniza targets y límites desde referencial + stage hacia dim_pozo
-- 
-- CORRECCIÓN: Los nombre_tecnico anteriores ('kpi_mtbf', 'pump_avg_spm_act',
--   'pump_fill_monitor_pct', 'carga_varilla_pct') NO EXISTÍAN en
--   tbl_maestra_variables, causando que todos los targets quedaran NULL.
--
-- Ahora usa variable_id directos (verificados contra tbl_limites_pozo):
--   var_id=111 → spm_promedio_diario_medidor → pump_spm_target
--   var_id=50  → llenado_bomba_minimo        → pump_fill_monitor_target
--   var_id=83  → well_head_pressure_psi_act  → whp reference
--   var_id=49  → kwh_por_barril              → kpi_kwh_bbl
--   var_id=52  → longitud_carrera_nominal     → pump_stroke_length
--
-- Además sincroniza datos de equipamiento desde stage.tbl_pozo_maestra
-- y aplica defaults de industria donde no hay datos en referencial.
--
-- Prerrequisito: V4 schema, referencial cargado
--------------------------------------------------------------------------------
*/

CREATE OR REPLACE PROCEDURE reporting.sp_sync_dim_pozo_targets()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    RAISE NOTICE 'Iniciando sincronización de Targets Referencial -> Dim Pozo...';

    -- =========================================================================
    -- PARTE 1: Datos de equipamiento desde stage.tbl_pozo_maestra
    -- =========================================================================
    UPDATE reporting.dim_pozo dp
    SET 
        rod_weight_in_air_lb = COALESCE(dp.rod_weight_in_air_lb,
            (SELECT ROUND(m.peso_sarta_aire::NUMERIC / 1000, 2) 
             FROM stage.tbl_pozo_maestra m WHERE m.well_id = dp.pozo_id)
        ),
        api_max_fluid_load_lb = COALESCE(dp.api_max_fluid_load_lb,
            (SELECT m.carga_maxima_fluido_api 
             FROM stage.tbl_pozo_maestra m WHERE m.well_id = dp.pozo_id)
        ),
        pump_depth_ft = COALESCE(dp.pump_depth_ft,
            (SELECT m.profundidad_vertical_bomba 
             FROM stage.tbl_pozo_maestra m WHERE m.well_id = dp.pozo_id)
        ),
        formation_depth_ft = COALESCE(dp.formation_depth_ft,
            (SELECT m.profundidad_vertical_yacimiento 
             FROM stage.tbl_pozo_maestra m WHERE m.well_id = dp.pozo_id)
        ),
        hydraulic_load_rated_klb = COALESCE(dp.hydraulic_load_rated_klb,
            (SELECT ROUND(m.carga_nominal_unidad::NUMERIC / 1000, 2) 
             FROM stage.tbl_pozo_maestra m WHERE m.well_id = dp.pozo_id)
        ),
        total_reserves_bbl = COALESCE(dp.total_reserves_bbl,
            (SELECT r.reserva_inicial_teorica 
             FROM stage.tbl_pozo_reservas r WHERE r.well_id = dp.pozo_id)
        );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '  Equipamiento sincronizado: % filas', v_rows;

    -- =========================================================================
    -- PARTE 2: Targets desde tbl_limites_pozo (por variable_id directo)
    -- =========================================================================
    UPDATE reporting.dim_pozo dp
    SET 
        -- SPM target: variable_id=111 (spm_promedio_diario_medidor, target=1.80)
        pump_spm_target = COALESCE(dp.pump_spm_target,
            (SELECT lim.target_value FROM referencial.tbl_limites_pozo lim 
             WHERE lim.pozo_id = dp.pozo_id AND lim.variable_id = 111),
            (SELECT lim.target_value FROM referencial.tbl_limites_pozo lim 
             WHERE lim.pozo_id = 1 AND lim.variable_id = 111),
            1.80
        ),
        -- Pump Fill target: variable_id=50 (llenado_bomba_minimo, target=80)
        pump_fill_monitor_target = COALESCE(dp.pump_fill_monitor_target,
            (SELECT lim.target_value FROM referencial.tbl_limites_pozo lim 
             WHERE lim.pozo_id = dp.pozo_id AND lim.variable_id = 50),
            (SELECT lim.target_value FROM referencial.tbl_limites_pozo lim 
             WHERE lim.pozo_id = 1 AND lim.variable_id = 50),
            80.00
        ),
        -- Pump Stroke Length target: variable_id=52 o desde maestra
        pump_stroke_length_target = COALESCE(dp.pump_stroke_length_target,
            (SELECT lim.target_value FROM referencial.tbl_limites_pozo lim 
             WHERE lim.pozo_id = dp.pozo_id AND lim.variable_id = 52),
            (SELECT m.longitud_carrera_nominal_unidad_in 
             FROM stage.tbl_pozo_maestra m WHERE m.well_id = dp.pozo_id),
            360.00
        );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '  Targets desde limites sincronizados: % filas', v_rows;

    -- =========================================================================
    -- PARTE 3: Baselines y targets con defaults de industria petrolera
    --   (estos no tienen fuente en tbl_limites_pozo)
    -- =========================================================================
    UPDATE reporting.dim_pozo dp
    SET 
        -- MTBF: baseline=1500h, target=2000h (estándar oil & gas)
        mtbf_baseline = COALESCE(dp.mtbf_baseline, 1500.00),
        mtbf_target   = COALESCE(dp.mtbf_target, 2000.00),

        -- Uptime: target=95% (industria), no hay variable en limites
        kpi_uptime_pct_target = COALESCE(dp.kpi_uptime_pct_target, 95.00),

        -- KWH/BBL: variable_id=49, baseline algo mayor que target
        kpi_kwh_bbl_baseline = COALESCE(dp.kpi_kwh_bbl_baseline, 12.000),
        kpi_kwh_bbl_target   = COALESCE(dp.kpi_kwh_bbl_target,
            (SELECT lim.target_value FROM referencial.tbl_limites_pozo lim 
             WHERE lim.pozo_id = dp.pozo_id AND lim.variable_id = 49),
            (SELECT lim.target_value FROM referencial.tbl_limites_pozo lim 
             WHERE lim.pozo_id = 1 AND lim.variable_id = 49),
            10.000
        ),

        -- Lift & Vol Efficiency targets: 85% estándar
        lift_efficiency_target = COALESCE(dp.lift_efficiency_target, 85.00),
        vol_eff_target         = COALESCE(dp.vol_eff_target, 85.00),

        -- Gas Fill Monitor: target bajo (5% = mínimo gas en bomba)
        gas_fill_monitor_target = COALESCE(dp.gas_fill_monitor_target, 5.00),

        -- Tank Fluid Temperature: 120°F operativo
        tank_fluid_temperature_f_target = COALESCE(dp.tank_fluid_temperature_f_target, 120.00),

        -- Road Load Efficiency thresholds
        road_load_status_eff_low  = COALESCE(dp.road_load_status_eff_low, 85.00),
        road_load_status_eff_high = COALESCE(dp.road_load_status_eff_high, 115.00),

        -- Hydraulic Load Efficiency thresholds
        hydraulic_load_status_eff_low  = COALESCE(dp.hydraulic_load_status_eff_low, 85.00),
        hydraulic_load_status_eff_high = COALESCE(dp.hydraulic_load_status_eff_high, 115.00);

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '  Defaults industria aplicados: % filas', v_rows;

    RAISE NOTICE 'Sincronización dimensión pozo completada (equipamiento + targets + defaults).';
END;
$$;
