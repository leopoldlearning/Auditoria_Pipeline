/*
------------------------------------------------------------------------------------------------------------------------
-- SCRIPT INFORMATION BLOCK
------------------------------------------------------------------------------------------------------------------------

-- FILE NAME:                 reporting_schema
-- DESCRIPTION:               Creación de esquema de base de datos para proceso de limpieza de datos, transformaciones y limpieza desde capa STAGE
-- DATABASE PLATFORM:         PostgreSQL
-- DATABASE VERSION:          PostgreSQL 17

-- AUTHOR:                    ITMEET
-- CREATION DATE:             2025-11-20

------------------------------------------------------------------------------------------------------------------------
-- VERSION CONTROL & HISTORY
------------------------------------------------------------------------------------------------------------------------

-- VERSION:                   1.0.0
-- LAST MODIFIED BY:          ITMEET
-- LAST MODIFIED DATE:        2025-11-20
-- CHANGE LOG:                - Creación inicial del script para establecer el esquema de la base de datos de Reporting.

------------------------------------------------------------------------------------------------------------------------
-- EXECUTION & DEPENDENCIES
------------------------------------------------------------------------------------------------------------------------

-- TARGET OBJECT(S):          CAPA DE DIMENSIONES, CAPA TRANSACCIONAL, FUNCIONES, RLS
-- PRE-REQUISITES:            Capa Stage debe estar creada y poblada con datos iniciales. 
-- TRANSACTION HANDLING:      
-- PERFORMANCE NOTE:          


-- PANEL 1: Surface operations 
-- PANEL 2: Production
-- PANEL 3: KPI Business (Key Perfomance Indicators)

------------------------------------------------------------------------------------------------------------------------
*/

-- -------------------------------------------------------------------------
-- 1. CAPA DE DIMENSIONES
-- -------------------------------------------------------------------------

-- -----------------------------------------------------
-- Tabla DIM_TIEMPO
-- PK: Fecha_ID
-- -----------------------------------------------------
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

-- -----------------------------------------------------
-- Tabla DIM_HORA
-- PK: Hora_ID
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS reporting.DIM_HORA (
    Hora_ID INT PRIMARY KEY,      -- 0 a 23
    Hora_Etiqueta VARCHAR(10),    -- "00:00"
    Turno_Operativo VARCHAR(20)   -- "Dia" / "Noche"
);

-- -----------------------------------------------------
-- Tabla DIM_POZO
-- PK: Pozo_ID
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS reporting.DIM_POZO (
    Pozo_ID INT PRIMARY KEY, -- NOTA: Solo un registro por pozo_ID, revisar si se actualiza con último registro de capa Stage, tbl_pozo_maestra
    Nombre_Pozo VARCHAR(100) NOT NULL,
    Cliente VARCHAR(100),
    Pais VARCHAR(50),           -- Habilitado para Gobernanza (RLS)
    Region VARCHAR(50),
    Campo VARCHAR(100),         -- Habilitado para Gobernanza (RLS)
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
    Total_Reserves_bbl DECIMAL(14, 2),       -- ID 128
    
    Fecha_Ultima_Actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------
-- Tabla DIM_RLS_USUARIO
-- PK: User_Email, Pozo_ID
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS reporting.DIM_RLS_USUARIO (
    User_Email VARCHAR(100) NOT NULL,
    Pozo_ID INT NOT NULL, 
    Nivel_Acceso VARCHAR(20), -- Validar nivel de acceso, para escalamiento.
    PRIMARY KEY (User_Email, Pozo_ID)
);

-- -----------------------------------------------------
-- Vista VW_DIM_MES
-- -----------------------------------------------------
CREATE OR REPLACE VIEW reporting.VW_DIM_MES AS
SELECT DISTINCT Anio_Mes, 
    Anio, 
    Mes, 
    Mes_Nombre, 
    Semestre, 
    Trimestre
FROM reporting.DIM_TIEMPO ORDER BY Anio, Mes;

-- -------------------------------------------------------------------------
-- 2. CAPA TRANSACCIONAL
-- -------------------------------------------------------------------------

-- -----------------------------------------------------
-- Tabla FACT_OPERACIONES_HORARIAS
-- PK: Fecha_ID, Hora_ID, Pozo_ID
-- -----------------------------------------------------
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
    Current_Stroke_Length_in DECIMAL(10, 2),  -- ID 68 (Solo Horaria)

    
    -- Tiempos y Estado
    Tiempo_Operacion_min DECIMAL(5, 2),
    Estado_Motor_Fin_Hora BOOLEAN,
    Numero_Fallas_Hora INT,
    
    PRIMARY KEY (Fecha_ID, Hora_ID, Pozo_ID),
    CONSTRAINT fk_h_tiempo FOREIGN KEY (Fecha_ID) REFERENCES reporting.DIM_TIEMPO(Fecha_ID),
    CONSTRAINT fk_h_hora FOREIGN KEY (Hora_ID) REFERENCES reporting.DIM_HORA(Hora_ID),
    CONSTRAINT fk_h_pozo FOREIGN KEY (Pozo_ID) REFERENCES reporting.DIM_POZO(Pozo_ID)
);

-- -----------------------------------------------------
-- Tabla FACT_OPERACIONES_DIARIAS
-- PK: Fact_ID, 
-- FK: Fecha_ID, Pozo_ID
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS reporting.FACT_OPERACIONES_DIARIAS (
    Fact_ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    Fecha_ID INT NOT NULL,
    Pozo_ID INT NOT NULL,
    Periodo_Comparacion VARCHAR(20) DEFAULT 'DIARIO',
    
    -- Producción (Panel 2, Indicador 7)
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
    
    -- Metadatos
    Calidad_Datos_Estado VARCHAR(20),
    Completitud_Datos_pct DECIMAL(5, 2),
    Fecha_Carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_fact_diaria UNIQUE (Fecha_ID, Pozo_ID, Periodo_Comparacion),
    CONSTRAINT fk_d_tiempo FOREIGN KEY (Fecha_ID) REFERENCES reporting.DIM_TIEMPO(Fecha_ID),
    CONSTRAINT fk_d_pozo FOREIGN KEY (Pozo_ID) REFERENCES reporting.DIM_POZO(Pozo_ID)
);

-- -----------------------------------------------------
-- Tabla FACT_OPERACIONES_MENSUALES
-- PK: Anio_Mes, Pozo_ID, 
-- FK: Pozo_ID
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS reporting.FACT_OPERACIONES_MENSUALES (
    Fact_Mes_ID BIGINT GENERATED ALWAYS AS IDENTITY,
    Anio_Mes VARCHAR(7) NOT NULL, -- "2025-11"
    Pozo_ID INT NOT NULL,
    
    -- Totales
    Total_Petroleo_bbl DECIMAL(14, 2),
    Total_Agua_bbl DECIMAL(14, 2),
    Total_Gas_mcf DECIMAL(14, 2),
    Total_Fluido_bbl DECIMAL(14, 2),
    
    -- Promedios
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
    
    -- Eficiencia
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

-- -------------------------------------------------------------------------
-- 3. FUNCIONES
-- -------------------------------------------------------------------------

-- Función Optimizada para Poblar Tiempo
-- Función para poblar dim_tiempo con un rango de fechas
CREATE OR REPLACE FUNCTION reporting.poblar_dim_tiempo(
    fecha_inicio DATE,
    fecha_fin DATE
)
RETURNS INT AS $$
DECLARE
    filas_insertadas INT;
BEGIN
    -- Uso de generate_series para inserción masiva eficiente
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
        TO_CHAR(datum, 'TMMonth') AS Mes_Nombre, -- Configurar lc_time en español si se requiere
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

-- Función para poblar dim_hora
-- Pobla la dimensión de horas (0-23) con etiquetas y turnos operativos
CREATE OR REPLACE FUNCTION reporting.poblar_dim_hora()
RETURNS INT AS $$
DECLARE
    filas_insertadas INT;
BEGIN
    INSERT INTO reporting.dim_hora (Hora_ID, Hora_Etiqueta, Turno_Operativo)
    SELECT 
        h, 
        TO_CHAR(make_time(h, 0, 0), 'HH24:MI'), -- Generates "00:00" through "23:00" natively
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
