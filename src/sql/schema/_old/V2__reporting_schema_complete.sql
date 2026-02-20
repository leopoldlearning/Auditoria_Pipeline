/*
--------------------------------------------------------------------------------
-- ESQUEMA REPORTING DEFINITIVO (V2) - ACTUALIZACIÓN E INTEGRACIÓN
-- Basado en: Esquema Actual + Hoja de Validación (CSV 114 items)
-- Estrategia: Mantener existente, adicionar faltantes, crear datasets planos.
--------------------------------------------------------------------------------
*/

-- Asegurar que el esquema existe
CREATE SCHEMA IF NOT EXISTS reporting;

-- =============================================================================
-- 1. DIMENSIONES (Mantenimiento y Enriquecimiento)
-- =============================================================================

-- 1.1 DIM_TIEMPO (Se mantiene igual, estructura estándar)
CREATE TABLE IF NOT EXISTS reporting.DIM_TIEMPO (
    Fecha_ID INT PRIMARY KEY,
    Fecha DATE NOT NULL,
    Anio INT NOT NULL,
    Mes INT NOT NULL,
    Dia INT NOT NULL,
    Mes_Nombre VARCHAR(20),
    Dia_Semana VARCHAR(15),
    Anio_Mes VARCHAR(7),
    Trimestre INT,
    Semestre INT
);

-- 1.2 DIM_HORA (Se mantiene igual)
CREATE TABLE IF NOT EXISTS reporting.DIM_HORA (
    Hora_ID INT PRIMARY KEY,      -- 0 a 23
    Hora_Etiqueta VARCHAR(10),    -- "00:00"
    Turno_Operativo VARCHAR(20)   -- "Dia" / "Noche"
);

-- 1.3 DIM_POZO (Enriquecida con parámetros de diseño faltantes del CSV)
CREATE TABLE IF NOT EXISTS reporting.DIM_POZO (
    Pozo_ID INT PRIMARY KEY,
    Nombre_Pozo VARCHAR(100) NOT NULL,
    Cliente VARCHAR(100),
    Pais VARCHAR(50),
    Region VARCHAR(50),
    Campo VARCHAR(100),
    API_Number VARCHAR(50),
    Coordenadas_Pozo VARCHAR(100),
    Tipo_Pozo VARCHAR(50),
    Tipo_Levantamiento VARCHAR(50),
    
    -- Parámetros de Diseño (Críticos para KPIs)
    Profundidad_Completacion_ft DECIMAL(10, 2),
    Diametro_Embolo_Bomba_in DECIMAL(5, 2),  
    Longitud_Carrera_Nominal_in DECIMAL(5, 2),
    Potencia_Nominal_Motor_hp DECIMAL(10, 2),
    Nombre_Yacimiento VARCHAR(100),

    Rod_Weight_In_Air_lb DECIMAL(10, 2),      -- ID 72
    API_Max_Fluid_Load_lb DECIMAL(10, 2),     -- ID 75
    Pump_Depth_ft DECIMAL(10, 2),             -- ID 39
    Formation_Depth_ft DECIMAL(10, 2),        -- ID 38
    Hydraulic_Load_Rated_klb DECIMAL(10, 2),  -- ID 46
    Total_Reserves_bbl DECIMAL(14, 2),        -- ID 128
    
    Fecha_Ultima_Actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 1.4 DIM_RLS_USUARIO (Se mantiene igual)
CREATE TABLE IF NOT EXISTS reporting.DIM_RLS_USUARIO (
    User_Email VARCHAR(100) NOT NULL,
    Pozo_ID INT NOT NULL, 
    Nivel_Acceso VARCHAR(20), 
    PRIMARY KEY (User_Email, Pozo_ID)
);

-- 1.5 VW_DIM_MES (Vista de ayuda)
CREATE OR REPLACE VIEW reporting.VW_DIM_MES AS
SELECT DISTINCT Anio_Mes, Anio, Mes, Mes_Nombre, Semestre, Trimestre
FROM reporting.DIM_TIEMPO ORDER BY Anio, Mes;

-- =============================================================================
-- 2. CAPA TRANSACCIONAL HISTÓRICA (Facts Actualizadas)
-- =============================================================================

-- 2.1 FACT_OPERACIONES_HORARIAS (Añadimos columnas de Universal/IA si faltan)
CREATE TABLE IF NOT EXISTS reporting.FACT_OPERACIONES_HORARIAS (
    Fact_Hora_ID BIGINT GENERATED ALWAYS AS IDENTITY,
    Fecha_ID INT NOT NULL,
    Hora_ID INT NOT NULL,
    Pozo_ID INT NOT NULL,
    Fecha_Hora TIMESTAMP NOT NULL,
    
    -- Producción
    Prod_Petroleo_bbl DECIMAL(10, 2),
    Prod_Agua_bbl DECIMAL(10, 2),
    Prod_Gas_mcf DECIMAL(10, 2),
    Prod_Acumulada_Dia_bbl DECIMAL(10, 2),
    Fluid_Flow_Monitor_bpd DECIMAL(10, 2),    -- ID 65
    
    -- Dinámica Promedio
    SPM_Promedio DECIMAL(5, 2),
    Presion_Cabezal_psi DECIMAL(10, 2),
    Presion_Casing_psi DECIMAL(10, 2),
    PIP_psi DECIMAL(10, 2),
    Temperatura_Motor_F DECIMAL(10, 2),
    Amperaje_Motor_A DECIMAL(10, 2),
    Lift_Efficiency_pct DECIMAL(5, 2),        -- ID 118
    Bouyant_Rod_Weight_lb DECIMAL(10, 2),     -- ID 73
    Fluid_Level_TVD_ft DECIMAL(10, 2),        -- ID 59
    PDP_psi DECIMAL(10, 2),                   -- ID 62
    Tank_Fluid_Temp_F DECIMAL(10, 2),         -- ID 94
    Motor_Power_Hp DECIMAL(10, 2),            -- ID 66  
    Current_Stroke_Length_in DECIMAL(10, 2),  -- ID 68
    
    -- Tiempos y Estado
    Tiempo_Operacion_min DECIMAL(5, 2),
    Estado_Motor_Fin_Hora BOOLEAN,
    Numero_Fallas_Hora INT,

    -- [NUEVO] Integración Universal (Si se requiere histórico horario de IPR)
    IPR_Qmax_Teorico DECIMAL(10,2),
    
    PRIMARY KEY (Fecha_ID, Hora_ID, Pozo_ID),
    CONSTRAINT fk_h_tiempo FOREIGN KEY (Fecha_ID) REFERENCES reporting.DIM_TIEMPO(Fecha_ID),
    CONSTRAINT fk_h_hora FOREIGN KEY (Hora_ID) REFERENCES reporting.DIM_HORA(Hora_ID),
    CONSTRAINT fk_h_pozo FOREIGN KEY (Pozo_ID) REFERENCES reporting.DIM_POZO(Pozo_ID)
);

-- 2.2 FACT_OPERACIONES_DIARIAS (Enriquecida para KPIs de Negocio)
CREATE TABLE IF NOT EXISTS reporting.FACT_OPERACIONES_DIARIAS (
    Fact_ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    Fecha_ID INT NOT NULL,
    Pozo_ID INT NOT NULL,
    Periodo_Comparacion VARCHAR(20) DEFAULT 'DIARIO',
    
    -- Producción
    Produccion_Fluido_bbl DECIMAL(12, 2),
    Produccion_Petroleo_bbl DECIMAL(12, 2),
    Produccion_Agua_bbl DECIMAL(12, 2),
    Produccion_Gas_mcf DECIMAL(12, 2),
    Water_Cut_pct DECIMAL(5, 2),
    
    -- Operación
    SPM_Promedio DECIMAL(5, 2),
    SPM_Maximo DECIMAL(5, 2),
    Emboladas_Totales INT,
    Tiempo_Operacion_hrs DECIMAL(5, 2),
    Tiempo_Paro_NoProg_hrs DECIMAL(5, 2),
    Promedio_Lift_Efficiency_pct DECIMAL(5, 2),
    Promedio_Bouyant_Rod_Weight_lb DECIMAL(10, 2),
    Promedio_Fluid_Level_TVD_ft DECIMAL(10, 2),
    Promedio_PDP_psi DECIMAL(10, 2),
    Promedio_Tank_Fluid_Temp_F DECIMAL(10, 2),
    Promedio_Motor_Power_Hp DECIMAL(10, 2),
    Promedio_Fluid_Flow_Monitor_bpd DECIMAL(10, 2),
    
    -- Energía y Dinámica
    Consumo_Energia_kwh DECIMAL(12, 2),
    Potencia_Promedio_kw DECIMAL(10, 2),
    Presion_Cabezal_psi DECIMAL(10, 2),
    Presion_Casing_psi DECIMAL(10, 2),
    PIP_psi DECIMAL(10, 2),
    Carga_Max_Rod_lb DECIMAL(10, 2),
    Carga_Min_Rod_lb DECIMAL(10, 2),
    Llenado_Bomba_pct DECIMAL(5, 2),
    Numero_Fallas INT DEFAULT 0,
    Flag_Falla BOOLEAN DEFAULT FALSE,
    
    -- KPIs Avanzados
    Volumen_Teorico_bbl DECIMAL(12, 2),
    KPI_Efic_Vol_pct DECIMAL(10, 2),
    KPI_DOP_pct DECIMAL(10, 2),
    KPI_KWH_BBL DECIMAL(10, 3),
    KPI_MTBF_hrs DECIMAL(10, 2),
    KPI_Uptime_pct DECIMAL(10, 2),
    KPI_Fill_Efficiency_pct DECIMAL(10, 2),
    
    -- [NUEVO] Costos y Reservas (Integración Universal ARPS / Finanzas)
    Costo_Operativo_Estimado_usd DECIMAL(12,2),
    EUR_Modelo_Arps DECIMAL(14,2),

    -- Metadatos
    Calidad_Datos_Estado VARCHAR(20),
    Completitud_Datos_pct DECIMAL(5, 2),
    Fecha_Carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_fact_diaria UNIQUE (Fecha_ID, Pozo_ID, Periodo_Comparacion),
    CONSTRAINT fk_d_tiempo FOREIGN KEY (Fecha_ID) REFERENCES reporting.DIM_TIEMPO(Fecha_ID),
    CONSTRAINT fk_d_pozo FOREIGN KEY (Pozo_ID) REFERENCES reporting.DIM_POZO(Pozo_ID)
);

-- 2.3 FACT_OPERACIONES_MENSUALES (Se mantiene igual)
CREATE TABLE IF NOT EXISTS reporting.FACT_OPERACIONES_MENSUALES (
    Fact_Mes_ID BIGINT GENERATED ALWAYS AS IDENTITY,
    Anio_Mes VARCHAR(7) NOT NULL,
    Pozo_ID INT NOT NULL,
    
    Total_Petroleo_bbl DECIMAL(14, 2),
    Total_Agua_bbl DECIMAL(14, 2),
    Total_Gas_mcf DECIMAL(14, 2),
    Total_Fluido_bbl DECIMAL(14, 2),
    
    Promedio_SPM DECIMAL(5, 2),
    Promedio_WHP_psi DECIMAL(10, 2),
    Promedio_CHP_psi DECIMAL(10, 2),
    Promedio_Water_Cut_pct DECIMAL(5, 2),
    Promedio_Lift_Efficiency_pct DECIMAL(5, 2),
    Promedio_Bouyant_Rod_Weight_lb DECIMAL(10, 2),
    Promedio_Fluid_Level_TVD_ft DECIMAL(10, 2),
    Promedio_PDP_psi DECIMAL(10, 2),
    Promedio_Tank_Fluid_Temp_F DECIMAL(10, 2),
    Promedio_Motor_Power_Hp DECIMAL(10, 2),
    Promedio_Fluid_Flow_Monitor_bpd DECIMAL(10, 2),
    
    Total_Fallas_Mes INT,
    Dias_Operando INT,
    Tiempo_Operacion_hrs DECIMAL(10, 2),
    Tiempo_Paro_hrs DECIMAL(10, 2),
    Eficiencia_Uptime_pct DECIMAL(5, 2),
    Promedio_Efic_Vol_pct DECIMAL(5, 2),
    Consumo_Energia_Total_kwh DECIMAL(14, 2),
    KPI_KWH_BBL_Mes DECIMAL(10, 3),
    
    Fecha_Ultima_Carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (Anio_Mes, Pozo_ID),
    CONSTRAINT fk_m_pozo FOREIGN KEY (Pozo_ID) REFERENCES reporting.DIM_POZO(Pozo_ID)
);

-- =============================================================================
-- 3. NUEVOS DATASETS PLANOS (Para QuickSight - Zero Calculation)
-- Estas tablas NO existían y se crean para cumplir los requisitos visuales.
-- =============================================================================

-- 3.1 DATASET CURRENT VALUES (Tiempo Real - Paneles 1 y 2)
CREATE TABLE IF NOT EXISTS reporting.dataset_current_values (
    -- IDENTIFICACIÓN
    well_id INT PRIMARY KEY,
    nombre_pozo VARCHAR(100),
    cliente VARCHAR(100),
    region VARCHAR(100),
    campo VARCHAR(100),
    turno_operativo VARCHAR(20),

    -- VITALIDAD & ESTADO
    ultima_actualizacion TIMESTAMP, -- ID 50
    minutos_sin_reportar INT,       -- KPI Latencia
    estado_comunicacion VARCHAR(20), -- 'ONLINE', 'OFFLINE'
    color_estado_comunicacion VARCHAR(7), -- HEX Code (Migrado a Ref)
    motor_running_flag BOOLEAN,     -- ID 120
    
    -- PANEL 1: SURFACE OPERATIONS (Variables Críticas con Semáforos)
    whp_psi DECIMAL(10,2),          -- ID 54
    whp_status_color VARCHAR(7),    -- HEX Calculado en ETL
    
    chp_psi DECIMAL(10,2),          -- ID 55
    pip_psi DECIMAL(10,2),          -- ID 61
    pdp_psi DECIMAL(10,2),          -- ID 62
    
    spm_actual DECIMAL(5,2),        -- ID 51
    spm_target DECIMAL(5,2),        -- Target (desde Referencial)
    spm_status_color VARCHAR(7),    -- HEX Calculado
    
    freq_vsd_hz DECIMAL(5,2),       -- ID 85
    amp_motor DECIMAL(10,2),        -- ID 67
    potencia_hp DECIMAL(10,2),      -- ID 66
    
    -- PANEL 2: PRODUCTION (Integración Universal + Stage)
    total_fluid_today_bbl DECIMAL(10,2), -- ID 107 (Qact)
    oil_today_bbl DECIMAL(10,2),         -- ID 108 (Qo)
    water_today_bbl DECIMAL(10,2),       -- ID 109 (Qw)
    gas_today_mcf DECIMAL(10,2),         -- ID 110 (Qg)
    water_cut_pct DECIMAL(5,2),          -- ID 57
    
    qf_fluid_flow_monitor_bpd DECIMAL(10,2), -- ID 65
    
    -- Datos IPR (Desde Universal)
    ipr_qmax_bpd DECIMAL(10,2),
    ipr_eficiencia_flujo_pct DECIMAL(5,2), -- (Qact / Qmax)%
    
    -- PANEL 3: BUSINESS & AI (Requerimientos CSV)
    ai_accuracy_score DECIMAL(5,2),       -- Score del modelo
    ai_accuracy_status_color VARCHAR(7),  -- Semáforo (Verde >95%)
    
    -- DINÁMICA DE SARTA
    llenado_bomba_pct DECIMAL(5,2),       -- ID 64
    gas_fill_monitor DECIMAL(5,2),        -- ID 96
    rod_weight_buoyant_lb DECIMAL(10,2),  -- ID 73
    carga_unidad_pct DECIMAL(5,2),        -- ID 80
    falla_vibracion_grados DECIMAL(5,2),  -- ID 91
    
    -- METADATOS
    dq_status VARCHAR(10),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para Filtros Rápidos en Dataset Current Values
CREATE INDEX IF NOT EXISTS idx_curr_campo ON reporting.dataset_current_values(campo);
CREATE INDEX IF NOT EXISTS idx_curr_region ON reporting.dataset_current_values(region);


-- 3.2 DATASET LATEST DYNACARD (Visualización Avanzada)
-- Almacena la última carta para graficar. Sustituye la necesidad de leer textos largos en Facts.
CREATE TABLE IF NOT EXISTS reporting.dataset_latest_dynacard (
    well_id INT PRIMARY KEY,
    timestamp_carta TIMESTAMP,
    
    -- Arrays JSON optimizados para gráficos de dispersión
    -- (QuickSight puede leer JSON o requerir transformación plana en ETL)
    superficie_json JSONB, 
    fondo_json JSONB,
    
    -- KPIs calculados de la Carta
    carga_min_superficie DECIMAL(10,2),
    carga_max_superficie DECIMAL(10,2),
    diagnostico_ia VARCHAR(100), 
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3.3 DATASET KPI BUSINESS (Histórico Agregado)
-- Tabla plana para el Panel 3, consolidando financiero y operativo.
CREATE TABLE IF NOT EXISTS reporting.dataset_kpi_business (
    kpi_id BIGINT GENERATED ALWAYS AS IDENTITY,
    fecha DATE,
    well_id INT,
    nombre_pozo VARCHAR(100),
    campo VARCHAR(100),
    
    -- Operativos (CSV Fila 113)
    uptime_pct DECIMAL(5,2),           
    tiempo_operacion_hrs DECIMAL(4,2),
    mtbf_dias DECIMAL(10,2),
    fail_count INT,
    
    -- Financieros / Energía
    costo_energia_usd DECIMAL(12,2),
    kwh_por_barril DECIMAL(10,4),
    lifting_cost_usd_bbl DECIMAL(10,2),
    
    -- Reservas (Universal ARPS)
    eur_remanente_bbl DECIMAL(14,2),
    vida_util_estimada_dias INT,
    
    PRIMARY KEY (fecha, well_id)
);

-- =============================================================================
-- 4. FUNCIONES DE UTILIDAD (Se mantienen las existentes)
-- =============================================================================

CREATE OR REPLACE FUNCTION reporting.poblar_dim_tiempo(fecha_inicio DATE, fecha_fin DATE)
RETURNS INT AS $$
DECLARE
    filas_insertadas INT;
BEGIN
    INSERT INTO reporting.dim_tiempo (
        Fecha_ID, Fecha, Anio, Mes, Dia, 
        Mes_Nombre, Dia_Semana, Anio_Mes, Trimestre, Semestre
    )
    SELECT
        TO_CHAR(datum, 'YYYYMMDD')::INT AS Fecha_ID,
        datum::DATE AS Fecha,
        EXTRACT(YEAR FROM datum)::INT AS Anio,
        EXTRACT(MONTH FROM datum)::INT AS Mes,
        EXTRACT(DAY FROM datum)::INT AS Dia,
        TO_CHAR(datum, 'TMMonth') AS Mes_Nombre,
        TO_CHAR(datum, 'TMDay') AS Dia_Semana,
        TO_CHAR(datum, 'YYYY-MM') AS Anio_Mes,
        EXTRACT(QUARTER FROM datum)::INT AS Trimestre,
        CASE WHEN EXTRACT(MONTH FROM datum) <= 6 THEN 1 ELSE 2 END AS Semestre
    FROM generate_series(fecha_inicio, fecha_fin, '1 day'::interval) AS datum
    ON CONFLICT (Fecha_ID) DO NOTHING;

    GET DIAGNOSTICS filas_insertadas = ROW_COUNT;
    RETURN filas_insertadas;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION reporting.poblar_dim_hora()
RETURNS INT AS $$
DECLARE
    filas_insertadas INT;
BEGIN
    INSERT INTO reporting.dim_hora (Hora_ID, Hora_Etiqueta, Turno_Operativo)
    SELECT 
        h, 
        TO_CHAR(make_time(h, 0, 0), 'HH24:MI'), 
        CASE WHEN h BETWEEN 6 AND 18 THEN 'Dia' ELSE 'Noche' END
    FROM generate_series(0, 23) h
    ON CONFLICT (Hora_ID) DO NOTHING;
    
    GET DIAGNOSTICS filas_insertadas = ROW_COUNT;
    RETURN filas_insertadas;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------
-- Creación de Índices para optimización de consultas
-- -----------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_hora_fechahora ON reporting.FACT_OPERACIONES_HORARIAS (Fecha_Hora);
CREATE INDEX IF NOT EXISTS idx_diaria_fecha ON reporting.FACT_OPERACIONES_DIARIAS (Fecha_ID);
CREATE INDEX IF NOT EXISTS idx_mensual_pozo ON reporting.FACT_OPERACIONES_MENSUALES (Pozo_ID, Anio_Mes);