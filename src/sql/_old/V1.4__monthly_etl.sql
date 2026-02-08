-- =========================================================================
-- ETL V1.4: DIARIO → MENSUAL (CASCADING AGGREGATION)
-- Estrategia: Calcular el mes sumando los días ya procesados.
-- Carga DB: Extremadamente Baja.
-- =========================================================================

BEGIN;

-- Parámetros de ejecución (Normalmente mes actual)
DO $$
DECLARE
    v_anio_mes VARCHAR(7) := TO_CHAR(CURRENT_DATE, 'YYYY-MM'); -- '2025-11'
BEGIN

    -- Lógica de Upsert (Insertar o Actualizar el mes actual)
    INSERT INTO reporting.FACT_OPERACIONES_MENSUALES (
        Anio_Mes, Pozo_ID,
        Total_Petroleo_bbl, Total_Agua_bbl, Total_Gas_mcf, Total_Fluido_bbl,
        Promedio_SPM, Promedio_WHP_psi, Promedio_CHP_psi, Promedio_Water_Cut_pct,
        Total_Fallas_Mes, Dias_Operando, Tiempo_Operacion_hrs, Tiempo_Paro_hrs,
        Eficiencia_Uptime_pct, Promedio_Efic_Vol_pct, 
        Consumo_Energia_Total_kwh, KPI_KWH_BBL_Mes,
        Fecha_Ultima_Carga
    )
    SELECT
        dt.Anio_Mes,
        f.Pozo_ID,
        
        -- Sumas Simples
        SUM(f.Produccion_Petroleo_bbl),
        SUM(f.Produccion_Agua_bbl),
        SUM(f.Produccion_Gas_mcf),
        SUM(f.Produccion_Fluido_bbl),
        
        -- Promedios
        AVG(f.SPM_Promedio),
        AVG(f.Presion_Cabezal_psi),
        AVG(f.Presion_Casing_psi),
        AVG(f.Water_Cut_pct),
        
        -- Conteos
        SUM(f.Numero_Fallas),
        COUNT(CASE WHEN f.Tiempo_Operacion_hrs > 0 THEN 1 END) as Dias_Op,
        SUM(f.Tiempo_Operacion_hrs),
        SUM(f.Tiempo_Paro_NoProg_hrs),
        
        -- KPIs Recalculados (Matemáticamente correctos para el mes)
        -- Uptime = Horas Op / (Horas Op + Horas Paro)
        CASE 
            WHEN SUM(f.Tiempo_Operacion_hrs + f.Tiempo_Paro_NoProg_hrs) > 0 
            THEN (SUM(f.Tiempo_Operacion_hrs) / SUM(f.Tiempo_Operacion_hrs + f.Tiempo_Paro_NoProg_hrs)) * 100.0
            ELSE 0 
        END,
        
        AVG(f.KPI_Efic_Vol_pct), -- Promedio de eficiencia diaria
        
        SUM(f.Consumo_Energia_kwh),
        
        -- KWH por Barril (Suma Energia / Suma Petroleo) - Más preciso que promedio de promedios
        CASE 
            WHEN SUM(f.Produccion_Petroleo_bbl) > 0 
            THEN SUM(f.Consumo_Energia_kwh) / SUM(f.Produccion_Petroleo_bbl)
            ELSE 0 
        END,
        
        CURRENT_TIMESTAMP

    FROM reporting.FACT_OPERACIONES_DIARIAS f
    JOIN reporting.DIM_TIEMPO dt ON f.Fecha_ID = dt.Fecha_ID
    WHERE dt.Anio_Mes = v_anio_mes -- Solo procesamos el mes en curso o solicitado
    GROUP BY dt.Anio_Mes, f.Pozo_ID
    
    ON CONFLICT (Anio_Mes, Pozo_ID) DO UPDATE SET
        Total_Petroleo_bbl = EXCLUDED.Total_Petroleo_bbl,
        Total_Agua_bbl = EXCLUDED.Total_Agua_bbl,
        Eficiencia_Uptime_pct = EXCLUDED.Eficiencia_Uptime_pct,
        KPI_KWH_BBL_Mes = EXCLUDED.KPI_KWH_BBL_Mes,
        Fecha_Ultima_Carga = CURRENT_TIMESTAMP;

END $$;

COMMIT;