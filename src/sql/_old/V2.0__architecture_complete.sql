-- =========================================================================
-- ARQUITECTURA V2.0: PLATAFORMA DE DATOS UNIFICADA (HORA-DIA-MES)
-- Descripción: Esquema consolidado para Reporting de Alta Performance.
-- Incluye: Dimensiones, RLS, Tablas de Hechos y Vistas.
-- =========================================================================

-- -------------------------------------------------------------------------
-- 1. CAPA DE DIMENSIONES (Contexto)
-- -------------------------------------------------------------------------

-- 1.1. Dimensión Tiempo (Calendario Maestro)
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

-- 1.2. Dimensión Hora (Para filtros rápidos en BI)
CREATE TABLE IF NOT EXISTS reporting.DIM_HORA (
    Hora_ID INT PRIMARY KEY,      -- 0 a 23
    Hora_Etiqueta VARCHAR(10),    -- "00:00"
    Turno_Operativo VARCHAR(20)   -- "Dia" / "Noche"
);

-- 1.3. Dimensión Pozo (Con Atributos de Gobierno y Geografía)
DROP TABLE IF EXISTS reporting.DIM_POZO CASCADE;
CREATE TABLE reporting.DIM_POZO (
    Pozo_ID INT PRIMARY KEY,
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
    
    Fecha_Ultima_Actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 1.4. Dimensión de Seguridad (Row-Level Security)
CREATE TABLE IF NOT EXISTS reporting.DIM_RLS_USUARIO (
    User_Email VARCHAR(100) NOT NULL,
    Pozo_ID INT NOT NULL, 
    Nivel_Acceso VARCHAR(20),
    PRIMARY KEY (User_Email, Pozo_ID)
);

-- 1.5. Vista de Tiempo Mensual (Helper para BI)
CREATE OR REPLACE VIEW reporting.VW_DIM_MES AS
SELECT DISTINCT Anio_Mes, Anio, Mes, Mes_Nombre, Semestre, Trimestre
FROM reporting.DIM_TIEMPO ORDER BY Anio, Mes;

-- -------------------------------------------------------------------------
-- 2. CAPA TRANSACCIONAL (Tablas de Hechos)
-- -------------------------------------------------------------------------

-- 2.1. Fact Table: OPERACIONES HORARIAS (Alta Frecuencia)
CREATE TABLE IF NOT EXISTS reporting.FACT_OPERACIONES_HORARIAS (
    Fact_Hora_ID BIGINT GENERATED ALWAYS AS IDENTITY,
    Fecha_ID INT NOT NULL,
    Hora_ID INT NOT NULL,
    Pozo_ID INT NOT NULL,
    Fecha_Hora TIMESTAMP NOT NULL,
    
    -- Deltas de Producción
    Prod_Petroleo_bbl DECIMAL(10, 2),
    Prod_Agua_bbl DECIMAL(10, 2),
    Prod_Gas_mcf DECIMAL(10, 2),
    Prod_Acumulada_Dia_bbl DECIMAL(10, 2),
    
    -- Dinámica Promedio
    SPM_Promedio DECIMAL(5, 2),
    Presion_Cabezal_psi DECIMAL(10, 2),
    Presion_Casing_psi DECIMAL(10, 2),
    PIP_psi DECIMAL(10, 2),
    Temperatura_Motor_F DECIMAL(10, 2),
    Amperaje_Motor_A DECIMAL(10, 2),
    
    -- Tiempos y Estado
    Tiempo_Operacion_min DECIMAL(5, 2),
    Estado_Motor_Fin_Hora BOOLEAN,
    Numero_Fallas_Hora INT,
    
    PRIMARY KEY (Fecha_ID, Hora_ID, Pozo_ID),
    CONSTRAINT fk_h_tiempo FOREIGN KEY (Fecha_ID) REFERENCES reporting.DIM_TIEMPO(Fecha_ID),
    CONSTRAINT fk_h_hora FOREIGN KEY (Hora_ID) REFERENCES reporting.DIM_HORA(Hora_ID),
    CONSTRAINT fk_h_pozo FOREIGN KEY (Pozo_ID) REFERENCES reporting.DIM_POZO(Pozo_ID)
);

-- 2.2. Fact Table: OPERACIONES DIARIAS (Reporte Oficial)
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

-- 2.3. Fact Table: OPERACIONES MENSUALES (Agregada)
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
-- 3. FUNCIONES DE SOPORTE
-- -------------------------------------------------------------------------

-- Función Optimizada para Poblar Tiempo
CREATE OR REPLACE FUNCTION reporting.poblar_dim_tiempo(f_inicio DATE, f_fin DATE)
RETURNS INT AS $$
DECLARE filas INT;
BEGIN
    INSERT INTO reporting.dim_tiempo (
        Fecha_ID, Fecha, Anio, Mes, Dia, Mes_Nombre, Dia_Semana, Anio_Mes, Trimestre, Semestre
    )
    SELECT
        TO_CHAR(d, 'YYYYMMDD')::INT, d::DATE,
        EXTRACT(YEAR FROM d), EXTRACT(MONTH FROM d), EXTRACT(DAY FROM d),
        TO_CHAR(d, 'TMMonth'), TO_CHAR(d, 'TMDay'), TO_CHAR(d, 'YYYY-MM'),
        EXTRACT(QUARTER FROM d), CASE WHEN EXTRACT(MONTH FROM d) <= 6 THEN 1 ELSE 2 END
    FROM generate_series(f_inicio, f_fin, '1 day'::interval) AS d
    ON CONFLICT (Fecha_ID) DO NOTHING;
    GET DIAGNOSTICS filas = ROW_COUNT;
    RETURN filas;
END;
$$ LANGUAGE plpgsql;

-- Índices Clave
CREATE INDEX IF NOT EXISTS idx_hora_fechahora ON reporting.FACT_OPERACIONES_HORARIAS (Fecha_Hora);
CREATE INDEX IF NOT EXISTS idx_diaria_fecha ON reporting.FACT_OPERACIONES_DIARIAS (Fecha_ID);
CREATE INDEX IF NOT EXISTS idx_mensual_pozo ON reporting.FACT_OPERACIONES_MENSUALES (Pozo_ID, Anio_Mes);