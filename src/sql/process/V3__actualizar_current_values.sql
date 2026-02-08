-- =============================================================================
-- PROCEDIMIENTO: ACTUALIZAR CURRENT VALUES (V3 - Redesign)
-- LÓGICA: Upsert basado en la última lectura de Stage V4 -> Reporting V3
--         Aligned with Zero-Calc Schema (Source Fields mapping)
-- =============================================================================

CREATE OR REPLACE PROCEDURE reporting.actualizar_current_values_v3()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO reporting.dataset_current_values AS target (
        -- 1. Identificación
        well_id, cliente, region, campo, nombre_pozo, turno_operativo,
        -- 2. Vitalidad
        ultima_actualizacion, minutos_sin_reportar, estado_comunicacion, motor_running_flag,
        -- 3. Producción
        total_fluid_today_bbl, oil_today_bbl, water_today_bbl, gas_today_mcf, water_cut_pct, qf_fluid_flow_monitor_bpd,
        -- 4. Presiones
        whp_psi, chp_psi, pip_psi, pdp_psi,
        -- 5. Dinámica
        spm_actual, llenado_bomba_pct, gas_fill_monitor, 
        rod_weight_buoyant_lb, carga_unidad_pct, falla_vibracion_grados,
        -- 6. Energía
        potencia_hp, amp_motor, freq_vsd_hz,
        -- 7. [NUEVO] Source fields needed for Zero-Calc Logic
        --    These fields are raw inputs for the logic script
        ai_accuracy_score,
        pump_fill_monitor_pct,
        road_load_pct_act,
        hydralift_unit_load_pct, -- Hydraulic Load
        kpi_mtbf_hrs_act,
        kpi_uptime_pct_act,
        kpi_kwh_bbl_act,
        kpi_vol_eff_pct_act,
        pump_stroke_length_act,
        tank_fluid_temperature_f,
        
        -- 8. Calidad
        dq_status, updated_at
    )
    SELECT
        m.well_id,                  
        m.cliente,                  
        m.region,                   
        m.campo,                    
        m.nombre_pozo,              
        
        -- Cálculo de Turno
        CASE 
            WHEN EXTRACT(HOUR FROM p.timestamp_lectura) >= 0 AND EXTRACT(HOUR FROM p.timestamp_lectura) < 8 THEN 'Noche'
            WHEN EXTRACT(HOUR FROM p.timestamp_lectura) >= 8 AND EXTRACT(HOUR FROM p.timestamp_lectura) < 16 THEN 'Dia'
            ELSE 'Tarde'
        END,
        
        p.timestamp_lectura,        
        EXTRACT(EPOCH FROM (now() - p.timestamp_lectura))/60, -- Minutos sin reportar
        CASE 
            WHEN EXTRACT(EPOCH FROM (now() - p.timestamp_lectura))/60 < 15 THEN 'ONLINE'
            WHEN EXTRACT(EPOCH FROM (now() - p.timestamp_lectura))/60 < 60 THEN 'DELAYED'
            ELSE 'OFFLINE'
        END,
        p.estado_motor,             
        
        p.produccion_fluido_diaria, 
        p.produccion_petroleo_diaria, 
        p.produccion_agua_diaria,   
        p.produccion_gas_diaria,    
        p.porcentaje_agua,          
        p.fluid_flow_monitor_bpd,   
        
        p.presion_cabezal,          
        p.presion_casing,           
        p.PIP,                      
        p.presion_descarga_bomba,   
        
        p.spm_promedio,             
        p.pump_fill_monitor,        
        p.monitor_llenado_gas,      
        p.rod_weight_buoyant,       
        
        -- Carga Unidad % (Calculada o Directa)
        ROUND((p.carga_caja_engranajes / NULLIF(m.carga_nominal_unidad, 0)) * 100, 2), 
        p.falla_inclinacion_grados, 
        
        p.potencia_actual_motor,    
        p.current_amperage,         
        p.rpm_motor,
        
        -- [NUEVO] Mapeo de campos Stage -> Reporting para Zero Calc
        -- NOTA: Campos placeholder (NULL) hasta que existan en Stage
        NULL::DECIMAL(5,2),      -- ai_accuracy_score (p.ai_accuracy)
        p.pump_fill_monitor,     -- pump_fill_monitor_pct (Existe)
        NULL::DECIMAL(5,2),      -- road_load_pct_act (p.carga_varilla_pct)
        NULL::DECIMAL(5,2),      -- hydralift_unit_load_pct (p.carga_unidad_pct)
        NULL::DECIMAL(10,2),     -- kpi_mtbf_hrs_act (p.mtbf_hrs_actual)
        NULL::DECIMAL(5,2),      -- kpi_uptime_pct_act (p.uptime_pct_actual)
        NULL::DECIMAL(10,3),     -- kpi_kwh_bbl_act (p.energia_kwh_bbl)
        NULL::DECIMAL(5,2),      -- kpi_vol_eff_pct_act (p.efic_volumetrica_pct)
        NULL::DECIMAL(10,2),     -- pump_stroke_length_act (p.longitud_carrera_in)
        NULL::DECIMAL(10,2),     -- tank_fluid_temperature_f (p.temp_tanque_fluido_f)
        
        COALESCE(dq.resultado_dq, 'PASS'),
        NOW()

    FROM stage.tbl_pozo_maestra m
    -- Última Lectura (Optimizada)
    LEFT JOIN LATERAL (
        SELECT * FROM stage.tbl_pozo_produccion prod
        WHERE prod.well_id = m.well_id
        ORDER BY prod.timestamp_lectura DESC
        LIMIT 1
    ) p ON true
    
    -- Cruce con DQ
    LEFT JOIN LATERAL (
        SELECT 'FAIL' as resultado_dq 
        FROM stage.tbl_pozo_scada_dq d
        WHERE d.produccion_id = p.produccion_id
        LIMIT 1
    ) dq ON true

    -- UPSERT
    ON CONFLICT (well_id) DO UPDATE SET
        cliente = EXCLUDED.cliente,
        region = EXCLUDED.region,
        campo = EXCLUDED.campo,
        turno_operativo = EXCLUDED.turno_operativo,
        ultima_actualizacion = EXCLUDED.ultima_actualizacion,
        minutos_sin_reportar = EXCLUDED.minutos_sin_reportar,
        estado_comunicacion = EXCLUDED.estado_comunicacion,
        motor_running_flag = EXCLUDED.motor_running_flag,
        total_fluid_today_bbl = EXCLUDED.total_fluid_today_bbl,
        oil_today_bbl = EXCLUDED.oil_today_bbl,
        water_today_bbl = EXCLUDED.water_today_bbl,
        gas_today_mcf = EXCLUDED.gas_today_mcf,
        water_cut_pct = EXCLUDED.water_cut_pct,
        qf_fluid_flow_monitor_bpd = EXCLUDED.qf_fluid_flow_monitor_bpd,
        whp_psi = EXCLUDED.whp_psi,
        chp_psi = EXCLUDED.chp_psi,
        pip_psi = EXCLUDED.pip_psi,
        pdp_psi = EXCLUDED.pdp_psi,
        spm_actual = EXCLUDED.spm_actual,
        llenado_bomba_pct = EXCLUDED.llenado_bomba_pct,
        gas_fill_monitor = EXCLUDED.gas_fill_monitor,
        rod_weight_buoyant_lb = EXCLUDED.rod_weight_buoyant_lb,
        carga_unidad_pct = EXCLUDED.carga_unidad_pct,
        falla_vibracion_grados = EXCLUDED.falla_vibracion_grados,
        potencia_hp = EXCLUDED.potencia_hp,
        amp_motor = EXCLUDED.amp_motor,
        freq_vsd_hz = EXCLUDED.freq_vsd_hz,
        
        -- Updates para nuevos campos
        ai_accuracy_score = EXCLUDED.ai_accuracy_score,
        pump_fill_monitor_pct = EXCLUDED.pump_fill_monitor_pct,
        road_load_pct_act = EXCLUDED.road_load_pct_act,
        hydralift_unit_load_pct = EXCLUDED.hydralift_unit_load_pct,
        kpi_mtbf_hrs_act = EXCLUDED.kpi_mtbf_hrs_act,
        kpi_uptime_pct_act = EXCLUDED.kpi_uptime_pct_act,
        kpi_kwh_bbl_act = EXCLUDED.kpi_kwh_bbl_act,
        kpi_vol_eff_pct_act = EXCLUDED.kpi_vol_eff_pct_act,
        pump_stroke_length_act = EXCLUDED.pump_stroke_length_act,
        tank_fluid_temperature_f = EXCLUDED.tank_fluid_temperature_f,
        
        dq_status = EXCLUDED.dq_status,
        updated_at = NOW();
END;
$$;
