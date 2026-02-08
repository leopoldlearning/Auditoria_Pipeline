-- =========================================================================
-- ETL V1.1: STAGE → REPORTING (OPTIMIZADO & AUTOCONTENIDO)
-- Descripción: Carga diaria con conversión de unidades y mapeo geográfico.
--              Incluye función optimizada de tiempo.
-- Frecuencia: Diaria (T-1)
-- =========================================================================

BEGIN;

-- =========================================================================
-- PASO 1: DEFINICIÓN Y EJECUCIÓN DE POBLADO DE TIEMPO (Optimizado)
-- =========================================================================

-- Función Vectorizada (Set-Based) - Reemplaza cursor iterativo por performance
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

-- Ejecución idempotente para asegurar cobertura del mes actual y próximo
SELECT reporting.poblar_dim_tiempo(
    (CURRENT_DATE - INTERVAL '1 month')::DATE, 
    (CURRENT_DATE + INTERVAL '1 month')::DATE
);

-- =========================================================================
-- PASO 2: ACTUALIZAR DIMENSIÓN DE POZO (Con Datos Geográficos)
-- =========================================================================
INSERT INTO reporting.dim_pozo (
    pozo_id, nombre_pozo, cliente, 
    pais, region, campo, -- GOBERNANZA: Campos críticos agregados
    api_number, coordenadas_pozo, tipo_pozo, tipo_levantamiento,
    profundidad_completacion_ft, diametro_embolo_bomba_in, longitud_carrera_nominal_in,
    potencia_nominal_motor_hp, nombre_yacimiento
)
SELECT 
    m.well_id,
    m.nombre_pozo,
    m.cliente,
    m.pais,    
    m.region,
    m.campo,   
    m.api_number,
    m.coordenadas_pozo,
    m.tipo_pozo,
    m.tipo_levantamiento,
    m.profundidad_completacion,
    m.diametro_embolo_bomba,
    m.longitud_carrera_nominal,
    m.potencia_nominal_motor,
    m.nombre_yacimiento
FROM stage.tbl_pozo_maestra m
ON CONFLICT (pozo_id) DO UPDATE SET
    nombre_pozo = EXCLUDED.nombre_pozo,
    pais = EXCLUDED.pais,
    campo = EXCLUDED.campo,
    tipo_levantamiento = EXCLUDED.tipo_levantamiento,
    potencia_nominal_motor_hp = EXCLUDED.potencia_nominal_motor_hp,
    fecha_ultima_actualizacion = CURRENT_TIMESTAMP;

-- =========================================================================
-- PASO 3: CÁLCULO DE MÉTRICAS Y KPIs (Lógica de Negocio)
-- =========================================================================

WITH datos_diarios AS (
    SELECT 
        p.well_id,
        DATE(p.timestamp_lectura) as fecha,
        
        -- Métricas Brutas (Agregaciones Robustas)
        AVG(p.spm_promedio) as spm_promedio,
        MAX(p.spm_promedio) as spm_maximo,
        MAX(p.emboladas_diarias) as emboladas_totales,
        
        -- Tiempos
        (MAX(p.horas_operacion_acumuladas) - COALESCE(MIN(p.horas_operacion_acumuladas), 0)) as tiempo_op_raw,
        MAX(p.tiempo_parada_poc_diario) as tiempo_paro_noprog,
        
        -- Producción
        MAX(p.produccion_fluido_diaria) as prod_fluido,
        MAX(p.produccion_petroleo_diaria) as prod_petroleo,
        MAX(p.produccion_agua_diaria) as prod_agua,
        MAX(p.produccion_gas_diaria) as prod_gas,
        AVG(p.porcentaje_agua) as water_cut,
        
        -- Energía (CONVERSIÓN HP -> KW)
        (MAX(p.energia_medidor_acumulada) - COALESCE(MIN(p.energia_medidor_acumulada), 0)) as consumo_kwh,
        (AVG(p.potencia_actual_motor) * 0.7457) as potencia_prom_kw, -- 1 HP = 0.7457 KW
        
        -- Presiones y Dinámica (Nuevos campos para Wide Table)
        AVG(p.presion_cabezal) as whp,
        AVG(p.presion_casing) as chp,
        AVG(p.pip) as pip,
        MAX(p.maximum_rod_load) as rod_max,
        MIN(p.minimum_rod_load) as rod_min,
        MAX(p.llenado_promedio_diario) as pump_fill,
        
        -- Fallas
        MAX(p.conteo_poc_diario) as fallas,
        BOOL_OR(NOT p.estado_motor) as flag_falla,
        
        -- Calidad
        COUNT(*) as num_registros,
        COUNT(p.spm_promedio) as registros_validos
        
    FROM stage.tbl_pozo_produccion p
    WHERE DATE(p.timestamp_lectura) = CURRENT_DATE - INTERVAL '1 day' -- Delta Load
    GROUP BY p.well_id, DATE(p.timestamp_lectura)
),

parametros_diseno AS (
    SELECT pozo_id, diametro_embolo_bomba_in, longitud_carrera_nominal_in FROM reporting.dim_pozo
),

kpis_calculados AS (
    SELECT 
        dd.*,
        pd.diametro_embolo_bomba_in,
        pd.longitud_carrera_nominal_in,
        
        -- Limpieza de Tiempos
        LEAST(dd.tiempo_op_raw, 24.0) as tiempo_op_clean,
        
        -- KPI: Volumen Teórico (BBL)
        (0.000971 * POWER(pd.diametro_embolo_bomba_in, 2) * pd.longitud_carrera_nominal_in * dd.spm_promedio * 1440) as vol_teorico
        
    FROM datos_diarios dd
    LEFT JOIN parametros_diseno pd ON dd.well_id = pd.pozo_id
)

INSERT INTO reporting.fact_operaciones_diarias (
    fecha_id, pozo_id, periodo_comparacion,
    
    -- Métricas Brutas
    produccion_fluido_bbl, produccion_petroleo_bbl, produccion_agua_bbl, produccion_gas_mcf,
    water_cut_pct, spm_promedio, spm_maximo, emboladas_totales, 
    tiempo_operacion_hrs, tiempo_paro_noprog_hrs, 
    consumo_energia_kwh, potencia_promedio_kw,
    presion_cabezal_psi, presion_casing_psi, pip_psi, 
    carga_max_rod_lb, carga_min_rod_lb, llenado_bomba_pct,
    numero_fallas, flag_falla,
    
    -- KPIs
    volumen_teorico_bbl, kpi_efic_vol_pct, kpi_dop_pct, kpi_kwh_bbl, 
    kpi_mtbf_hrs, kpi_uptime_pct, kpi_fill_efficiency_pct,
    
    -- Metadatos
    completitud_datos_pct, calidad_datos_estado
)
SELECT
    TO_CHAR(k.fecha, 'YYYYMMDD')::INT,
    k.well_id,
    'DIARIO',
    
    -- Mapping Raw
    k.prod_fluido, k.prod_petroleo, k.prod_agua, k.prod_gas,
    k.water_cut, k.spm_promedio, k.spm_maximo, k.emboladas_totales,
    k.tiempo_op_clean, k.tiempo_paro_noprog,
    k.consumo_kwh, k.potencia_prom_kw,
    k.whp, k.chp, k.pip,
    k.rod_max, k.rod_min, k.pump_fill,
    k.fallas, k.flag_falla,
    
    -- KPI Calculos
    k.vol_teorico,
    
    -- Eficiencia Volumétrica
    CASE WHEN k.vol_teorico > 0 THEN (k.prod_fluido / k.vol_teorico) * 100.0 ELSE 0 END,
    
    -- DOP (Disponibilidad Operativa)
    (k.tiempo_op_clean / 24.0) * 100.0,
    
    -- KWH por Barril
    CASE WHEN k.prod_petroleo > 0 THEN k.consumo_kwh / k.prod_petroleo ELSE NULL END,
    
    -- MTBF
    CASE WHEN k.fallas > 0 THEN k.tiempo_op_clean / k.fallas ELSE NULL END,
    
    -- Uptime (vs Tiempo No Programado)
    CASE WHEN (k.tiempo_op_clean + k.tiempo_paro_noprog) > 0 
         THEN (k.tiempo_op_clean / (k.tiempo_op_clean + k.tiempo_paro_noprog)) * 100.0 
         ELSE 0 END,
         
    -- Fill Eff
    k.pump_fill,
    
    -- Calidad
    (k.registros_validos::DECIMAL / NULLIF(k.num_registros, 0)) * 100.0,
    CASE WHEN (k.registros_validos::DECIMAL / NULLIF(k.num_registros, 0)) >= 0.9 THEN 'OK' ELSE 'WARNING' END

FROM kpis_calculados k
-- UPSERT basado en la Unique Constraint (Idempotencia)
ON CONFLICT (fecha_id, pozo_id, periodo_comparacion) DO UPDATE SET
    produccion_petroleo_bbl = EXCLUDED.produccion_petroleo_bbl,
    kpi_efic_vol_pct = EXCLUDED.kpi_efic_vol_pct,
    kpi_dop_pct = EXCLUDED.kpi_dop_pct,
    potencia_promedio_kw = EXCLUDED.potencia_promedio_kw,
    fecha_carga = CURRENT_TIMESTAMP;

COMMIT;