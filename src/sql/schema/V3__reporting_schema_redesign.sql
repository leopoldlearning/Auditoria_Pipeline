/*
--------------------------------------------------------------------------------
-- ESQUEMA REPORTING DEFINITIVO (V3) - STANDARD VERSION
-- Basado en: Requerimientos CSV 114 items + Naming Convention snake_case
-- Estrategia: Nuevo archivo versión 3.0.
--------------------------------------------------------------------------------
*/

-- Asegurar que el esquema existe
CREATE SCHEMA IF NOT EXISTS reporting;

-- =============================================================================
-- 1. DIMENSIONES (Mantenimiento y Enriquecimiento)
-- =============================================================================

-- 1.1 dim_tiempo
CREATE TABLE IF NOT EXISTS reporting.dim_tiempo (
    fecha_id INT PRIMARY KEY,
    fecha DATE NOT NULL,
    anio INT NOT NULL,
    mes INT NOT NULL,
    dia INT NOT NULL,
    mes_nombre VARCHAR(20),
    dia_semana VARCHAR(15),
    anio_mes VARCHAR(7),
    trimestre INT,
    semestre INT
);

-- 1.2 dim_hora
CREATE TABLE IF NOT EXISTS reporting.dim_hora (
    hora_id INT PRIMARY KEY,      -- 0 a 23
    hora_etiqueta VARCHAR(10),    -- "00:00"
    turno_operativo VARCHAR(20)   -- "Dia" / "Noche"
);

-- 1.3 dim_pozo (Enriquecida)
CREATE TABLE IF NOT EXISTS reporting.dim_pozo (
    pozo_id INT PRIMARY KEY,
    nombre_pozo VARCHAR(100) NOT NULL,
    cliente VARCHAR(100),
    pais VARCHAR(50),
    region VARCHAR(50),
    campo VARCHAR(100),
    api_number VARCHAR(50),
    coordenadas_pozo VARCHAR(100),
    tipo_pozo VARCHAR(50),
    tipo_levantamiento VARCHAR(50),
    
    -- Parámetros de Diseño (Críticos para KPIs)
    profundidad_completacion_ft DECIMAL(10, 2),
    diametro_embolo_bomba_in DECIMAL(5, 2),  
    longitud_carrera_nominal_unidad DECIMAL(5, 2),
    potencia_nominal_motor_hp DECIMAL(10, 2),
    
    -- [NUEVO ZERO-CALC] Variables Estáticas y Targets Fijos
    nombre_yacimiento VARCHAR(100),
    rod_weight_in_air_lb DECIMAL(10, 2),      -- ID 72
    api_max_fluid_load_lb DECIMAL(10, 2),     -- ID 75
    pump_depth_ft DECIMAL(10, 2),             -- ID 39
    formation_depth_ft DECIMAL(10, 2),        -- ID 38
    hydraulic_load_rated_klb DECIMAL(10, 2),  -- ID 46
    total_reserves_bbl DECIMAL(14, 2),        -- ID 128
    
    -- [NUEVO ZERO-CALC] Targets y Baselines Específicos
    mtbf_baseline DECIMAL(10, 2),
    mtbf_target DECIMAL(10, 2),
    kpi_uptime_pct_target DECIMAL(5, 2),
    kpi_kwh_bbl_baseline DECIMAL(10, 3),
    kpi_kwh_bbl_target DECIMAL(10, 3),
    lift_efficiency_target DECIMAL(5, 2),
    vol_eff_target DECIMAL(5, 2),
    pump_spm_target DECIMAL(5, 2),
    pump_stroke_length_target DECIMAL(10, 2),
    gas_fill_monitor_target DECIMAL(5, 2),
    pump_fill_monitor_target DECIMAL(5, 2),
    
    -- [NUEVO ZERO-CALC] Umbrales de Visualización Estáticos
    tank_fluid_temperature_f_target DECIMAL(10, 2),
    road_load_status_eff_low DECIMAL(5, 2),
    road_load_status_eff_high DECIMAL(5, 2),
    hydraulic_load_status_eff_low DECIMAL(5, 2),
    hydraulic_load_status_eff_high DECIMAL(5, 2),
    
    fecha_ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 1.4 dim_rls_usuario
CREATE TABLE IF NOT EXISTS reporting.dim_rls_usuario (
    user_email VARCHAR(100) NOT NULL,
    pozo_id INT NOT NULL, 
    nivel_acceso VARCHAR(20), 
    PRIMARY KEY (user_email, pozo_id)
);

-- 1.5 vw_dim_mes
CREATE OR REPLACE VIEW reporting.vw_dim_mes AS
SELECT DISTINCT anio_mes, anio, mes, mes_nombre, semestre, trimestre
FROM reporting.dim_tiempo ORDER BY anio, mes;

-- =============================================================================
-- 2. CAPA TRANSACCIONAL HISTÓRICA (Facts Actualizadas)
-- =============================================================================

-- 2.1 fact_operaciones_horarias
CREATE TABLE IF NOT EXISTS reporting.fact_operaciones_horarias (
    fact_hora_id BIGINT GENERATED ALWAYS AS IDENTITY,
    fecha_id INT NOT NULL,
    hora_id INT NOT NULL,
    pozo_id INT NOT NULL,
    fecha_hora TIMESTAMP NOT NULL,
    
    -- Producción
    prod_petroleo_bbl DECIMAL(10, 2),
    prod_agua_bbl DECIMAL(10, 2),
    prod_gas_mcf DECIMAL(10, 2),
    prod_acumulada_dia_bbl DECIMAL(10, 2),
    fluid_flow_monitor_bpd DECIMAL(10, 2),    -- ID 65
    
    -- Dinámica Promedio
    spm_promedio DECIMAL(5, 2),
    presion_cabezal_psi DECIMAL(10, 2),
    presion_casing_psi DECIMAL(10, 2),
    pip_psi DECIMAL(10, 2),
    temperatura_motor_f DECIMAL(10, 2),
    amperaje_motor_a DECIMAL(10, 2), -- motor_current_a_avg_hr
    lift_efficiency_pct DECIMAL(5, 2),        -- ID 118
    bouyant_rod_weight_lb DECIMAL(10, 2),     -- ID 73
    fluid_level_tvd_ft DECIMAL(10, 2),        -- ID 59
    pdp_psi DECIMAL(10, 2),                   -- ID 62
    tank_fluid_temp_f DECIMAL(10, 2),         -- ID 94
    motor_power_hp DECIMAL(10, 2),            -- ID 66  
    current_stroke_length_in DECIMAL(10, 2),  -- ID 68
    
    -- Tiempos y Estado
    tiempo_operacion_min DECIMAL(5, 2),
    estado_motor_fin_hora BOOLEAN,
    numero_fallas_hora INT,

    -- [NUEVO] Integración Universal
    ipr_qmax_teorico DECIMAL(10,2),
    
    PRIMARY KEY (fecha_id, hora_id, pozo_id),
    CONSTRAINT fk_h_tiempo FOREIGN KEY (fecha_id) REFERENCES reporting.dim_tiempo(fecha_id),
    CONSTRAINT fk_h_hora FOREIGN KEY (hora_id) REFERENCES reporting.dim_hora(hora_id),
    CONSTRAINT fk_h_pozo FOREIGN KEY (pozo_id) REFERENCES reporting.dim_pozo(pozo_id)
);

-- 2.2 fact_operaciones_diarias
CREATE TABLE IF NOT EXISTS reporting.fact_operaciones_diarias (
    fact_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fecha_id INT NOT NULL,
    pozo_id INT NOT NULL,
    periodo_comparacion VARCHAR(20) DEFAULT 'DIARIO',
    
    -- Producción
    produccion_fluido_bbl DECIMAL(12, 2),
    produccion_petroleo_bbl DECIMAL(12, 2),
    produccion_agua_bbl DECIMAL(12, 2),
    produccion_gas_mcf DECIMAL(12, 2),
    water_cut_pct DECIMAL(5, 2),
    
    -- Operación
    spm_promedio DECIMAL(5, 2),
    spm_maximo DECIMAL(5, 2),
    emboladas_totales INT,
    tiempo_operacion_hrs DECIMAL(5, 2),
    tiempo_paro_noprog_hrs DECIMAL(5, 2),
    promedio_lift_efficiency_pct DECIMAL(5, 2),
    promedio_bouyant_rod_weight_lb DECIMAL(10, 2),
    promedio_fluid_level_tvd_ft DECIMAL(10, 2),
    promedio_pdp_psi DECIMAL(10, 2),
    promedio_tank_fluid_temp_f DECIMAL(10, 2),
    promedio_motor_power_hp DECIMAL(10, 2),
    promedio_fluid_flow_monitor_bpd DECIMAL(10, 2),
    
    -- Energía y Dinámica
    consumo_energia_kwh DECIMAL(12, 2),
    potencia_promedio_kw DECIMAL(10, 2),
    presion_cabezal_psi DECIMAL(10, 2),
    presion_casing_psi DECIMAL(10, 2),
    pip_psi DECIMAL(10, 2),
    carga_max_rod_lb DECIMAL(10, 2),
    carga_min_rod_lb DECIMAL(10, 2),
    llenado_bomba_pct DECIMAL(5, 2),
    numero_fallas INT DEFAULT 0,
    flag_falla BOOLEAN DEFAULT FALSE,
    
    -- KPIs Avanzados
    volumen_teorico_bbl DECIMAL(12, 2),
    kpi_efic_vol_pct DECIMAL(10, 2),
    kpi_dop_pct DECIMAL(10, 2),
    kpi_kwh_bbl DECIMAL(10, 3),
    kpi_mtbf_hrs DECIMAL(10, 2),
    kpi_uptime_pct DECIMAL(10, 2),
    kpi_fill_efficiency_pct DECIMAL(10, 2),
    
    -- [NUEVO] Costos y Reservas
    costo_operativo_estimado_usd DECIMAL(12,2),
    eur_modelo_arps DECIMAL(14,2),

    -- Metadatos
    calidad_datos_estado VARCHAR(20),
    completitud_datos_pct DECIMAL(5, 2),
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_fact_diaria UNIQUE (fecha_id, pozo_id, periodo_comparacion),
    CONSTRAINT fk_d_tiempo FOREIGN KEY (fecha_id) REFERENCES reporting.dim_tiempo(fecha_id),
    CONSTRAINT fk_d_pozo FOREIGN KEY (pozo_id) REFERENCES reporting.dim_pozo(pozo_id)
);

-- 2.3 fact_operaciones_mensuales
CREATE TABLE IF NOT EXISTS reporting.fact_operaciones_mensuales (
    fact_mes_id BIGINT GENERATED ALWAYS AS IDENTITY,
    anio_mes VARCHAR(7) NOT NULL,
    pozo_id INT NOT NULL,
    
    total_petroleo_bbl DECIMAL(14, 2),
    total_agua_bbl DECIMAL(14, 2),
    total_gas_mcf DECIMAL(14, 2),
    total_fluido_bbl DECIMAL(14, 2),
    
    promedio_spm DECIMAL(5, 2),
    promedio_whp_psi DECIMAL(10, 2),
    promedio_chp_psi DECIMAL(10, 2),
    promedio_water_cut_pct DECIMAL(5, 2),
    promedio_lift_efficiency_pct DECIMAL(5, 2),
    promedio_bouyant_rod_weight_lb DECIMAL(10, 2),
    promedio_fluid_level_tvd_ft DECIMAL(10, 2),
    promedio_pdp_psi DECIMAL(10, 2),
    promedio_tank_fluid_temp_f DECIMAL(10, 2),
    promedio_motor_power_hp DECIMAL(10, 2),
    promedio_fluid_flow_monitor_bpd DECIMAL(10, 2),
    
    total_fallas_mes INT,
    dias_operando INT,
    tiempo_operacion_hrs DECIMAL(10, 2),
    tiempo_paro_hrs DECIMAL(10, 2),
    eficiencia_uptime_pct DECIMAL(5, 2),
    promedio_efic_vol_pct DECIMAL(5, 2),
    consumo_energia_total_kwh DECIMAL(14, 2),
    kpi_kwh_bbl_mes DECIMAL(10, 3),
    
    fecha_ultima_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (anio_mes, pozo_id),
    CONSTRAINT fk_m_pozo FOREIGN KEY (pozo_id) REFERENCES reporting.dim_pozo(pozo_id)
);

-- =============================================================================
-- 3. NUEVOS DATASETS PLANOS
-- =============================================================================

-- 3.1 dataset_current_values (EXPANDIDO PARA ZERO-CALC)
CREATE TABLE IF NOT EXISTS reporting.dataset_current_values (
    well_id INT PRIMARY KEY,
    nombre_pozo VARCHAR(100),
    cliente VARCHAR(100),
    region VARCHAR(100),
    campo VARCHAR(100),
    turno_operativo VARCHAR(20),

    -- VITALIDAD & ESTADO
    ultima_actualizacion TIMESTAMP,
    minutos_sin_reportar INT, 
    estado_comunicacion VARCHAR(20),
    color_estado_comunicacion VARCHAR(7),
    motor_running_flag BOOLEAN,
    
    -- PANEL 1: DINÁMICA DE SUPERFICIE
    whp_psi DECIMAL(10,2),
    whp_status_color VARCHAR(7),
    chp_psi DECIMAL(10,2),
    pip_psi DECIMAL(10,2),
    pdp_psi DECIMAL(10,2),
    
    spm_actual DECIMAL(5,2),
    spm_target DECIMAL(5,2),
    spm_status_color VARCHAR(7),
    -- [NUEVO] Variaciones y Labels SPM
    pump_spm_var_pct DECIMAL(5,2),
    
    -- Bombeo
    freq_vsd_hz DECIMAL(5,2),
    amp_motor DECIMAL(10,2),
    potencia_hp DECIMAL(10,2),
    llenado_bomba_pct DECIMAL(5,2),
    
    -- [NUEVO] Llenado Bomba Monitor
    pump_fill_monitor_pct DECIMAL(5,2),
    pump_fill_monitor_target DECIMAL(5,2),
    pump_fill_monitor_var DECIMAL(5,2), -- [Standardized per request]
    pump_fill_monitor_status_color VARCHAR(7),
    
    -- PANEL 2: PRODUCCIÓN
    total_fluid_today_bbl DECIMAL(10,2),
    oil_today_bbl DECIMAL(10,2),
    water_today_bbl DECIMAL(10,2),
    gas_today_mcf DECIMAL(10,2),
    water_cut_pct DECIMAL(5,2),
    
    -- [NUEVO] Tasas Diarias (BPD)
    produccion_fluido_bpd_act DECIMAL(10,2),
    produccion_petroleo_diaria_bpd_act DECIMAL(10,2),
    produccion_agua_diaria_bpd_act DECIMAL(10,2),
    
    qf_fluid_flow_monitor_bpd DECIMAL(10,2),
    ipr_qmax_bpd DECIMAL(10,2),
    ipr_eficiencia_flujo_pct DECIMAL(5,2),
    
    -- PANEL 3: ESTADO DE SALUD & ALERTAS
    ai_accuracy_score DECIMAL(5,2),
    ai_accuracy_status_color VARCHAR(7),
    ai_accuracy_severity_label VARCHAR(20), -- [NUEVO]
    
    gas_fill_monitor DECIMAL(5,2),
    gas_fill_status_color VARCHAR(7),    -- [NUEVO]
    gas_fill_severity_label VARCHAR(20), -- [NUEVO]
    
    rod_weight_buoyant_lb DECIMAL(10,2),
    carga_unidad_pct DECIMAL(5,2),
    falla_vibracion_grados DECIMAL(5,2),
    
    -- [NUEVO] Cargas y Pesos
    max_rod_load_lb_act DECIMAL(10,2),
    min_rod_load_lb_act DECIMAL(10,2),
    max_pump_load_lb_act DECIMAL(10,2),
    min_pump_load_lb_act DECIMAL(10,2),
    
    -- [NUEVO] Road Load (Carga Varillas) - Zero Calc
    road_load_pct_act DECIMAL(5,2),
    road_load_status_level INT,
    road_load_status_color VARCHAR(7),
    road_load_status_label VARCHAR(50),
    road_load_status_legend_text VARCHAR(50),
    road_load_status_threshold_red DECIMAL(5,2),
    
    -- [NUEVO] Hydraulic Load (Carga Unidad) - Zero Calc
    hydralift_unit_load_pct DECIMAL(5,2),
    hydraulic_load_status_level INT,
    hydraulic_load_status_color VARCHAR(7),
    hydraulic_load_status_label VARCHAR(50),
    hydraulic_load_status_legend_text VARCHAR(50),
    hydraulic_load_status_threshold_red DECIMAL(5,2),
    
    -- [NUEVO] KPIs Negocio & Status
    kpi_uptime_pct_act DECIMAL(5,2),
    kpi_uptime_pct_status_color VARCHAR(7),
    kpi_uptime_pct_severity_label VARCHAR(20),
    
    kpi_kwh_bbl_act DECIMAL(10,3),
    kpi_kwh_bbl_status_color VARCHAR(7),
    kpi_kwh_bbl_severity_label VARCHAR(20),
    
    kpi_mtbf_hrs_act DECIMAL(10,2),
    kpi_mtbf_variance_pct DECIMAL(5,2), -- [Standardized per request]
    mtbf_status_color VARCHAR(7),
    
    kpi_vol_eff_pct_act DECIMAL(5,2),
    vol_eff_status_color VARCHAR(7),
    vol_eff_severity_label VARCHAR(20),
    
    lift_efficiency_pct_act DECIMAL(5,2),
    lift_efficiency_severity_label VARCHAR(20),
    
    -- [NUEVO] Diagnósticos Adicionales
    pump_stroke_length_act DECIMAL(10,2),
    pump_stroke_length_var_pct DECIMAL(5,2),
    pump_stroke_length_status_color VARCHAR(7),
    
    daily_downtime_act DECIMAL(5,2),
    daily_downtime_status_color VARCHAR(7),
    daily_downtime_severity_label VARCHAR(20),
    
    tank_fluid_temperature_f DECIMAL(10,2),
    tank_fluid_temp_status_color VARCHAR(7),
    tank_fluid_temperature_f_severity_label VARCHAR(20),
    
    dq_status VARCHAR(10),
    
    -- Inclinometría (Nuevos Flags)
    inclinacion_severidad_flag VARCHAR(20),
    inclinacion_cilindro_x_act DECIMAL(5,2),
    inclinacion_cilindro_y_act DECIMAL(5,2),
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_curr_campo ON reporting.dataset_current_values(campo);
CREATE INDEX IF NOT EXISTS idx_curr_region ON reporting.dataset_current_values(region);

-- 3.2 dataset_latest_dynacard
CREATE TABLE IF NOT EXISTS reporting.dataset_latest_dynacard (
    well_id INT PRIMARY KEY,
    timestamp_carta TIMESTAMP,
    superficie_json JSONB, 
    fondo_json JSONB,
    carga_min_superficie DECIMAL(10,2),
    carga_max_superficie DECIMAL(10,2),
    diagnostico_ia VARCHAR(100), 
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3.3 dataset_kpi_business
CREATE TABLE IF NOT EXISTS reporting.dataset_kpi_business (
    kpi_id BIGINT GENERATED ALWAYS AS IDENTITY,
    fecha DATE,
    well_id INT,
    nombre_pozo VARCHAR(100),
    campo VARCHAR(100),
    uptime_pct DECIMAL(5,2),           
    tiempo_operacion_hrs DECIMAL(4,2),
    mtbf_dias DECIMAL(10,2),
    fail_count INT,
    costo_energia_usd DECIMAL(12,2),
    kwh_por_barril DECIMAL(10,4),
    lifting_cost_usd_bbl DECIMAL(10,2),
    eur_remanente_bbl DECIMAL(14,2),
    vida_util_estimada_dias INT,
    PRIMARY KEY (fecha, well_id)
);

-- =============================================================================
-- 4. FUNCIONES DE UTILIDAD
-- =============================================================================

CREATE OR REPLACE FUNCTION reporting.poblar_dim_tiempo(fecha_inicio DATE, fecha_fin DATE)
RETURNS INT AS $$
DECLARE
    filas_insertadas INT;
BEGIN
    INSERT INTO reporting.dim_tiempo (
        fecha_id, fecha, anio, mes, dia, 
        mes_nombre, dia_semana, anio_mes, trimestre, semestre
    )
    SELECT
        TO_CHAR(datum, 'YYYYMMDD')::INT AS fecha_id,
        datum::DATE AS fecha,
        EXTRACT(YEAR FROM datum)::INT AS anio,
        EXTRACT(MONTH FROM datum)::INT AS mes,
        EXTRACT(DAY FROM datum)::INT AS dia,
        TO_CHAR(datum, 'TMMonth') AS mes_nombre,
        TO_CHAR(datum, 'TMDay') AS dia_semana,
        TO_CHAR(datum, 'YYYY-MM') AS anio_mes,
        EXTRACT(QUARTER FROM datum)::INT AS trimestre,
        CASE WHEN EXTRACT(MONTH FROM datum) <= 6 THEN 1 ELSE 2 END AS semestre
    FROM generate_series(fecha_inicio, fecha_fin, '1 day'::interval) AS datum
    ON CONFLICT (fecha_id) DO NOTHING;

    GET DIAGNOSTICS filas_insertadas = ROW_COUNT;
    RETURN filas_insertadas;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION reporting.poblar_dim_hora()
RETURNS INT AS $$
DECLARE
    filas_insertadas INT;
BEGIN
    INSERT INTO reporting.dim_hora (hora_id, hora_etiqueta, turno_operativo)
    SELECT 
        h, 
        TO_CHAR(make_time(h, 0, 0), 'HH24:MI'), 
        CASE WHEN h BETWEEN 6 AND 18 THEN 'Dia' ELSE 'Noche' END
    FROM generate_series(0, 23) h
    ON CONFLICT (hora_id) DO NOTHING;
    
    GET DIAGNOSTICS filas_insertadas = ROW_COUNT;
    RETURN filas_insertadas;
END;
$$ LANGUAGE plpgsql;

CREATE INDEX IF NOT EXISTS idx_hora_fechahora ON reporting.fact_operaciones_horarias (fecha_hora);
CREATE INDEX IF NOT EXISTS idx_diaria_fecha ON reporting.fact_operaciones_diarias (fecha_id);
CREATE INDEX IF NOT EXISTS idx_mensual_pozo ON reporting.fact_operaciones_mensuales (pozo_id, anio_mes);
