-- =========================================================================
-- MIGRACIÓN V1.1: REPORTING LAYER REFACTOR
-- Arquitecto: Data Engineer
-- Fecha: 2025-11-19
-- Descripción: Ampliación de Fact Table y Dimensión Pozo para integridad.
-- =========================================================================

-- 1. DIMENSIÓN POZO (Ampliación de atributos de Gobierno)
DROP TABLE IF EXISTS reporting.DIM_POZO CASCADE;

CREATE TABLE reporting.DIM_POZO (
    Pozo_ID INT PRIMARY KEY,
    Nombre_Pozo VARCHAR(100) NOT NULL,
    Cliente VARCHAR(100),
    Pais VARCHAR(50),           -- Habilitado para Gobernanza/RLS
    Region VARCHAR(50),
    Campo VARCHAR(100),         -- Habilitado para Gobernanza/RLS
    API_Number VARCHAR(50),
    Coordenadas_Pozo VARCHAR(100),
    Tipo_Pozo VARCHAR(50),
    Tipo_Levantamiento VARCHAR(50),
    
    -- Parámetros Técnicos de Diseño
    Profundidad_Completacion_ft DECIMAL(10, 2),
    Diametro_Embolo_Bomba_in DECIMAL(5, 2),
    Longitud_Carrera_Nominal_in DECIMAL(5, 2),
    Potencia_Nominal_Motor_hp DECIMAL(10, 2),
    Nombre_Yacimiento VARCHAR(100),
    
    -- Auditoría
    Fecha_Ultima_Actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE reporting.DIM_POZO IS 'Maestra de Pozos enriquecida con atributos geográficos para gobierno de datos.';

-- 2. DIMENSIÓN TIEMPO (Sin cambios estructurales, se asegura su existencia)
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

-- 3. TABLA DE HECHOS (Refactorizada a "Wide Table" para Analítica Avanzada)
DROP TABLE IF EXISTS reporting.FACT_OPERACIONES_DIARIAS CASCADE;

CREATE TABLE reporting.FACT_OPERACIONES_DIARIAS (
    -- Claves
    Fact_ID BIGINT GENERATED ALWAYS AS IDENTITY, -- Eficiencia: Secuencia automática
    Fecha_ID INT NOT NULL,
    Pozo_ID INT NOT NULL,
    Periodo_Comparacion VARCHAR(20) NOT NULL DEFAULT 'DIARIO',
    
    -- Métricas de Producción (Raw)
    Produccion_Fluido_bbl DECIMAL(12, 2),
    Produccion_Petroleo_bbl DECIMAL(12, 2),
    Produccion_Agua_bbl DECIMAL(12, 2),
    Produccion_Gas_mcf DECIMAL(12, 2),
    Water_Cut_pct DECIMAL(5, 2),
    
    -- Métricas Operativas (Raw & Aggregated)
    SPM_Promedio DECIMAL(5, 2),
    SPM_Maximo DECIMAL(5, 2),
    Emboladas_Totales INT,
    Tiempo_Operacion_hrs DECIMAL(5, 2),
    Tiempo_Paro_NoProg_hrs DECIMAL(5, 2),
    
    -- Energía (Estandarizado a KW)
    Consumo_Energia_kwh DECIMAL(12, 2),
    Potencia_Promedio_kw DECIMAL(10, 2), 
    
    -- Dinámica y Cargas
    Presion_Cabezal_psi DECIMAL(10, 2),
    Presion_Casing_psi DECIMAL(10, 2),
    PIP_psi DECIMAL(10, 2),
    Carga_Max_Rod_lb DECIMAL(10, 2),
    Carga_Min_Rod_lb DECIMAL(10, 2),
    Llenado_Bomba_pct DECIMAL(5, 2),
    
    -- Fallas y Alertas
    Numero_Fallas INT DEFAULT 0,
    Flag_Falla BOOLEAN DEFAULT FALSE,
    
    -- KPIs Calculados (Nullable para resiliencia del pipeline)
    Volumen_Teorico_bbl DECIMAL(12, 2),
    KPI_Efic_Vol_pct DECIMAL(10, 2),
    KPI_DOP_pct DECIMAL(10, 2),
    KPI_KWH_BBL DECIMAL(10, 3),
    KPI_MTBF_hrs DECIMAL(10, 2),
    KPI_Uptime_pct DECIMAL(10, 2),
    KPI_SNE_pct DECIMAL(10, 2),
    KPI_HSS_Desv_Presion_pct DECIMAL(10, 2),
    KPI_Fill_Efficiency_pct DECIMAL(10, 2),
    
    -- Metadatos de Calidad
    Calidad_Datos_Estado VARCHAR(20),
    Completitud_Datos_pct DECIMAL(5, 2),
    Fecha_Carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Definición de Clave Primaria y Constraints
    PRIMARY KEY (Fact_ID),
    CONSTRAINT uq_fact_diaria UNIQUE (Fecha_ID, Pozo_ID, Periodo_Comparacion), -- Garantiza Idempotencia
    
    CONSTRAINT fk_fact_tiempo FOREIGN KEY (Fecha_ID) REFERENCES reporting.DIM_TIEMPO(Fecha_ID),
    CONSTRAINT fk_fact_pozo FOREIGN KEY (Pozo_ID) REFERENCES reporting.DIM_POZO(Pozo_ID)
);

-- Índices de Rendimiento (Performance Tuning)
CREATE INDEX idx_fact_fecha ON reporting.FACT_OPERACIONES_DIARIAS (Fecha_ID);
CREATE INDEX idx_fact_pozo ON reporting.FACT_OPERACIONES_DIARIAS (Pozo_ID);
CREATE INDEX idx_fact_calidad ON reporting.FACT_OPERACIONES_DIARIAS (Calidad_Datos_Estado); -- Para monitoreo de DQ

COMMENT ON TABLE reporting.FACT_OPERACIONES_DIARIAS IS 'Tabla central de hechos optimizada. Contiene métricas crudas y KPIs calculados.';