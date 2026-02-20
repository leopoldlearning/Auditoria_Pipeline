/*
================================================================================
V12 - VISTAS HELPER PARA FRONTEND
================================================================================
Fecha: 2026-02-20 (v3 — Sincronizado con V7 WIDE estandarizado + V8)
Propósito: Simplificar queries comunes del dashboard
Cambios v3:
  - Nombres V7 estandarizados: kpi_mtbf_hrs_*, kpi_uptime_pct_*, kpi_kwh_bbl_*,
    kpi_vol_eff_pct_*, ai_accuracy_*
  - Columnas descartadas removidas: costo_energia_usd, tiempo_operacion_hrs,
    tiempo_paro_hrs, fail_count, produccion_acumulada_bbl
  - AI Accuracy añadido a vistas daily/monthly
  - Alertas con escala V8 (0-9): >= 3 = Alerta+
================================================================================
*/

-- =============================================================================
-- VISTA 1: Dashboard Principal (Current Values + Metadata)
-- =============================================================================
CREATE OR REPLACE VIEW reporting.vw_dashboard_main AS
SELECT 
    -- Identificación
    cv.well_id,
    dp.nombre_pozo,
    dp.cliente,
    dp.campo,
    dp.region,
    dp.tipo_levantamiento,
    
    -- KPIs Actuales (Cards Superiores)
    cv.kpi_mtbf_hrs_act AS mtbf_actual,
    cv.mtbf_status_color,
    cv.mtbf_severity_label AS mtbf_status,
    cv.mtbf_target,
    cv.mtbf_baseline,
    
    cv.kpi_uptime_pct_act AS uptime_actual,
    cv.kpi_uptime_pct_status_color AS uptime_color,
    cv.kpi_uptime_pct_severity_label AS uptime_status,
    cv.kpi_uptime_pct_target AS uptime_target,
    
    cv.kpi_kwh_bbl_act AS kwh_bbl_actual,
    cv.kpi_kwh_bbl_status_color AS kwh_bbl_color,
    cv.kpi_kwh_bbl_severity_label AS kwh_bbl_status,
    cv.kpi_kwh_bbl_target AS kwh_bbl_target,
    
    cv.kpi_vol_eff_pct_act AS vol_eff_actual,
    cv.vol_eff_status_color,
    cv.vol_eff_severity_label AS vol_eff_status,
    cv.vol_eff_target,
    
    cv.ai_accuracy_act,
    cv.ai_accuracy_status_color,
    cv.ai_accuracy_severity_label AS ai_accuracy_status,
    
    -- Producción
    cv.produccion_fluido_bpd_act AS produccion_actual,
    cv.produccion_petroleo_diaria_bpd_act AS oil_production,
    cv.water_cut_pct,
    
    -- Estado Operacional
    cv.estado_comunicacion,
    cv.color_estado_comunicacion,
    cv.motor_running_flag AS motor_running,
    cv.ultima_actualizacion AS last_update,
    cv.minutos_sin_reportar,
    
    -- Presiones & Temperatura
    cv.well_head_pressure_psi_act AS whp,
    cv.casing_head_pressure_psi_act AS chp,
    cv.pump_intake_pressure_psi_act AS pip,
    cv.tank_fluid_temperature_f AS temperature,
    
    -- Cargas
    cv.road_load_pct_act AS road_load,
    cv.road_load_status_color AS road_load_color,
    cv.hydralift_unit_load_pct AS hydraulic_load,
    cv.hydraulic_load_status_color AS hydraulic_load_color
    
FROM reporting.dataset_current_values cv
JOIN reporting.dim_pozo dp ON cv.well_id = dp.pozo_id;

COMMENT ON VIEW reporting.vw_dashboard_main IS 
'Vista consolidada para dashboard principal.
Query simple: SELECT * FROM vw_dashboard_main WHERE well_id = ?
Contiene: Cards superiores (5 KPIs) + Metadata + Estado operacional.';


-- =============================================================================
-- VISTA 2: KPIs Históricos Diarios (Para Gráficas)
--   V7 WIDE estandarizado: kpi_mtbf_hrs_dia, kpi_uptime_pct_dia, etc.
--   SIN filtro periodo (V7 no tiene esa columna — diseño WIDE)
-- =============================================================================
CREATE OR REPLACE VIEW reporting.vw_kpi_daily_history AS
SELECT 
    -- Identificación
    kb.well_id,
    kb.nombre_pozo,
    kb.fecha,
    kb.region,
    
    -- MTBF
    kb.kpi_mtbf_hrs_dia       AS mtbf_actual,
    kb.kpi_mtbf_hrs_target    AS mtbf_target,
    kb.kpi_mtbf_hrs_baseline  AS mtbf_baseline,
    kb.kpi_mtbf_hrs_variance_pct  AS mtbf_variance_pct,
    kb.kpi_mtbf_hrs_status_color  AS mtbf_status_color,
    kb.kpi_mtbf_hrs_status_level  AS mtbf_status_level,
    kb.kpi_mtbf_hrs_severity_label AS mtbf_severity_label,
    kb.kpi_mtbf_days,
    
    -- Uptime
    kb.kpi_uptime_pct_dia       AS uptime_actual,
    kb.kpi_uptime_pct_target    AS uptime_target,
    kb.kpi_uptime_pct_baseline  AS uptime_baseline,
    kb.kpi_uptime_pct_variance_pct  AS uptime_variance_pct,
    kb.kpi_uptime_pct_status_color  AS uptime_status_color,
    kb.kpi_uptime_pct_status_level  AS uptime_status_level,
    kb.kpi_uptime_pct_severity_label AS uptime_severity_label,
    
    -- kWh/bbl
    kb.kpi_kwh_bbl_dia       AS kwh_bbl_actual,
    kb.kpi_kwh_bbl_target    AS kwh_bbl_target,
    kb.kpi_kwh_bbl_baseline  AS kwh_bbl_baseline,
    kb.kpi_kwh_bbl_variance_pct  AS kwh_bbl_variance_pct,
    kb.kpi_kwh_bbl_status_color  AS kwh_bbl_status_color,
    kb.kpi_kwh_bbl_status_level  AS kwh_bbl_status_level,
    kb.kpi_kwh_bbl_severity_label AS kwh_bbl_severity_label,
    kb.consumo_kwh,
    
    -- Vol Eff
    kb.kpi_vol_eff_pct_dia       AS vol_eff_actual,
    kb.kpi_vol_eff_pct_target    AS vol_eff_target,
    kb.kpi_vol_eff_pct_baseline  AS vol_eff_baseline,
    kb.kpi_vol_eff_pct_variance_pct  AS vol_eff_variance_pct,
    kb.kpi_vol_eff_pct_status_color  AS vol_eff_status_color,
    kb.kpi_vol_eff_pct_status_level  AS vol_eff_status_level,
    kb.kpi_vol_eff_pct_severity_label AS vol_eff_severity_label,
    
    -- AI Accuracy
    kb.ai_accuracy_dia,
    kb.ai_accuracy_target,
    kb.ai_accuracy_baseline,
    kb.ai_accuracy_variance_pct,
    kb.ai_accuracy_status_color,
    kb.ai_accuracy_status_level,
    kb.ai_accuracy_severity_label,
    
    -- Producción
    kb.produccion_real_bbl,
    kb.produccion_teorica_bbl,
    
    -- Calidad
    kb.calidad_datos_pct

FROM reporting.dataset_kpi_business kb
WHERE kb.kpi_mtbf_hrs_dia IS NOT NULL;  -- Solo filas con datos diarios

COMMENT ON VIEW reporting.vw_kpi_daily_history IS 
'Vista de KPIs históricos diarios (V7 WIDE estandarizado).
Columnas: kpi_mtbf_hrs_dia, kpi_uptime_pct_dia, kpi_kwh_bbl_dia, kpi_vol_eff_pct_dia, ai_accuracy_dia.
Query: SELECT * FROM vw_kpi_daily_history 
       WHERE well_id = ? AND fecha BETWEEN ? AND ?
Uso: Gráficas de evolución diaria con filtro mensual.';


-- =============================================================================
-- VISTA 3: Comparación Mensual
--   V7 WIDE estandarizado: kpi_mtbf_hrs_mes, kpi_uptime_pct_mes, etc.
-- =============================================================================
CREATE OR REPLACE VIEW reporting.vw_kpi_monthly_summary AS
SELECT 
    kb.well_id,
    kb.nombre_pozo,
    kb.fecha AS mes,
    TO_CHAR(kb.fecha, 'Mon YYYY') AS mes_label,
    
    kb.kpi_mtbf_hrs_mes      AS mtbf_actual,
    kb.kpi_mtbf_hrs_target   AS mtbf_target,
    kb.kpi_mtbf_hrs_status_color AS mtbf_status_color,
    kb.kpi_mtbf_days,
    
    kb.kpi_uptime_pct_mes      AS uptime_actual,
    kb.kpi_uptime_pct_target   AS uptime_target,
    kb.kpi_uptime_pct_status_color AS uptime_status_color,
    
    kb.kpi_kwh_bbl_mes      AS kwh_bbl_actual,
    kb.kpi_kwh_bbl_target   AS kwh_bbl_target,
    kb.kpi_kwh_bbl_status_color AS kwh_bbl_status_color,
    kb.consumo_kwh AS consumo_total_kwh,
    
    kb.kpi_vol_eff_pct_mes      AS vol_eff_actual,
    kb.kpi_vol_eff_pct_target   AS vol_eff_target,
    kb.kpi_vol_eff_pct_status_color AS vol_eff_status_color,
    
    kb.ai_accuracy_mes,
    kb.ai_accuracy_target,
    kb.ai_accuracy_status_color,
    
    kb.produccion_real_bbl AS produccion_mes_bbl,
    kb.calidad_datos_pct

FROM reporting.dataset_kpi_business kb
WHERE kb.kpi_mtbf_hrs_mes IS NOT NULL;  -- Solo filas con datos mensuales

COMMENT ON VIEW reporting.vw_kpi_monthly_summary IS 
'Vista de KPIs mensuales (V7 WIDE estandarizado).
Query: SELECT * FROM vw_kpi_monthly_summary 
       WHERE well_id = ? AND mes >= ?
Uso: Comparación de varios meses, gráficas de barras agrupadas.';


-- =============================================================================
-- VISTA 4: Lista de Pozos para Selector/Filtro
-- =============================================================================
CREATE OR REPLACE VIEW reporting.vw_well_selector AS
SELECT 
    dp.pozo_id AS well_id,
    dp.nombre_pozo AS well_name,
    dp.cliente AS operator,
    dp.campo AS field,
    dp.region,
    dp.tipo_levantamiento AS lift_type,
    cv.estado_comunicacion AS comm_status,
    cv.color_estado_comunicacion AS comm_color,
    cv.ultima_actualizacion AS last_update,
    -- Orden de prioridad: con problemas primero
    CASE 
        WHEN cv.estado_comunicacion = 'OFFLINE' THEN 1
        WHEN cv.motor_running_flag = FALSE THEN 2
        ELSE 3
    END AS priority_order
FROM reporting.dim_pozo dp
LEFT JOIN reporting.dataset_current_values cv ON dp.pozo_id = cv.well_id
ORDER BY priority_order, dp.nombre_pozo;

COMMENT ON VIEW reporting.vw_well_selector IS 
'Vista para dropdown/selector de pozos en dashboard.
Query: SELECT * FROM vw_well_selector WHERE operator = ?
Incluye: Estado comunicación, última actualización, orden por prioridad.';


-- =============================================================================
-- VISTA 5: Panel de Alarmas/Eventos
--   Escala V8 unificada (0-9): >= 3 = Alerta o peor
--   0=Óptimo, 1=Normal, 3=Alerta, 4=Crítico, 5=Falla, 7=Sin Datos
-- =============================================================================
CREATE OR REPLACE VIEW reporting.vw_well_alerts AS
SELECT 
    cv.well_id,
    cv.nombre_pozo,
    cv.ultima_actualizacion,
    
    -- Alarmas de KPIs (V8: status_level >= 3 = Alerta+)
    CASE WHEN cv.mtbf_status_level >= 3 THEN TRUE ELSE FALSE END AS alert_mtbf,
    cv.kpi_mtbf_hrs_act AS mtbf_value,
    cv.mtbf_severity_label AS mtbf_severity,
    
    CASE WHEN cv.kpi_uptime_pct_status_level >= 3 THEN TRUE ELSE FALSE END AS alert_uptime,
    cv.kpi_uptime_pct_act AS uptime_value,
    cv.kpi_uptime_pct_severity_label AS uptime_severity,
    
    CASE WHEN cv.kpi_kwh_bbl_status_level >= 3 THEN TRUE ELSE FALSE END AS alert_kwh,
    cv.kpi_kwh_bbl_act AS kwh_value,
    cv.kpi_kwh_bbl_severity_label AS kwh_severity,
    
    CASE WHEN cv.vol_eff_status_level >= 3 THEN TRUE ELSE FALSE END AS alert_vol_eff,
    cv.kpi_vol_eff_pct_act AS vol_eff_value,
    cv.vol_eff_severity_label AS vol_eff_severity,
    
    -- Alarmas de Cargas (V8: >= 3 = Alerta+)
    CASE WHEN cv.road_load_status_level >= 3 THEN TRUE ELSE FALSE END AS alert_road_load,
    cv.road_load_pct_act AS road_load_value,
    cv.road_load_severity_label AS road_load_severity,
    
    -- Estado general
    cv.estado_comunicacion,
    cv.motor_running_flag,
    cv.minutos_sin_reportar,
    
    -- Conteo de alarmas
    (CASE WHEN cv.mtbf_status_level >= 3 THEN 1 ELSE 0 END +
     CASE WHEN cv.kpi_uptime_pct_status_level >= 3 THEN 1 ELSE 0 END +
     CASE WHEN cv.kpi_kwh_bbl_status_level >= 3 THEN 1 ELSE 0 END +
     CASE WHEN cv.vol_eff_status_level >= 3 THEN 1 ELSE 0 END +
     CASE WHEN cv.road_load_status_level >= 3 THEN 1 ELSE 0 END) AS total_alerts

FROM reporting.dataset_current_values cv
WHERE cv.mtbf_status_level >= 3 
   OR cv.kpi_uptime_pct_status_level >= 3
   OR cv.kpi_kwh_bbl_status_level >= 3
   OR cv.vol_eff_status_level >= 3
   OR cv.road_load_status_level >= 3
   OR cv.estado_comunicacion = 'OFFLINE'
   OR cv.motor_running_flag = FALSE;

COMMENT ON VIEW reporting.vw_well_alerts IS 
'Vista de pozos con alarmas activas.
Escala V8 unificada (0-9): >= 3 = Alerta, >= 4 = Crítico, >= 5 = Falla.
Query: SELECT * FROM vw_well_alerts ORDER BY total_alerts DESC
Uso: Panel de alertas, notificaciones, priorización de pozos.';


-- =============================================================================
-- FIN
-- =============================================================================
