-- =========================================================================
-- ARQUITECTURA V1.4: CAPA DE AGREGACIÓN MENSUAL
-- Propósito: Tendencias de largo plazo y KPIs Ejecutivos con carga mínima.
-- =========================================================================

-- 1. DIMENSIÓN TIEMPO MENSUAL (Opcional, vista simplificada)
-- Se puede reusar DIM_TIEMPO filtrando por Dia=1, pero una vista ayuda a BI
CREATE OR REPLACE VIEW reporting.VW_DIM_MES AS
SELECT DISTINCT
    Anio_Mes,
    Anio,
    Mes,
    Mes_Nombre,
    Semestre,
    Trimestre
FROM reporting.DIM_TIEMPO
ORDER BY Anio, Mes;

-- 2. TABLA DE HECHOS MENSUAL
CREATE TABLE reporting.FACT_OPERACIONES_MENSUALES (
    Fact_Mes_ID BIGINT GENERATED ALWAYS AS IDENTITY,
    Anio_Mes VARCHAR(7) NOT NULL, -- "2025-11" (Partition Key natural)
    Pozo_ID INT NOT NULL,
    
    -- TOTALES DE PRODUCCIÓN (Sumas)
    Total_Petroleo_bbl DECIMAL(14, 2),
    Total_Agua_bbl DECIMAL(14, 2),
    Total_Gas_mcf DECIMAL(14, 2),
    Total_Fluido_bbl DECIMAL(14, 2),
    
    -- PROMEDIOS OPERATIVOS (Ponderados si es posible, o simples)
    Promedio_SPM DECIMAL(5, 2),
    Promedio_WHP_psi DECIMAL(10, 2),
    Promedio_CHP_psi DECIMAL(10, 2),
    Promedio_Water_Cut_pct DECIMAL(5, 2),
    
    -- EFICIENCIA Y FALLAS
    Total_Fallas_Mes INT,
    Dias_Operando INT,        -- Días con producción > 0
    Tiempo_Operacion_hrs DECIMAL(10, 2),
    Tiempo_Paro_hrs DECIMAL(10, 2),
    
    -- KPIs AGREGADOS
    Eficiencia_Uptime_pct DECIMAL(5, 2), -- (Horas Op / Horas Totales Mes)
    Promedio_Efic_Vol_pct DECIMAL(5, 2),
    Consumo_Energia_Total_kwh DECIMAL(14, 2),
    KPI_KWH_BBL_Mes DECIMAL(10, 3), -- Recalculado a nivel mes (Total Kwh / Total Oil)
    
    -- CONTROL
    Fecha_Ultima_Carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (Anio_Mes, Pozo_ID),
    CONSTRAINT fk_mensual_pozo FOREIGN KEY (Pozo_ID) REFERENCES reporting.DIM_POZO(Pozo_ID)
);

-- Índice para Dashboards Ejecutivos (Instantáneos)
CREATE INDEX idx_fact_mes_pozo ON reporting.FACT_OPERACIONES_MENSUALES (Pozo_ID, Anio_Mes);

COMMENT ON TABLE reporting.FACT_OPERACIONES_MENSUALES IS 'Agregación Mensual: < 500 filas/año por pozo. Carga insignificante para BI.';