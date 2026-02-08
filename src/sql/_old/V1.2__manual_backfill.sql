-- =========================================================================
-- UTILIDAD: BACKFILL MANUAL / REPROCESAMIENTO HISTÓRICO
-- Descripción: Permite cargar un rango de fechas arbitrario, ignorando
--              la regla de "solo ayer" (T-1). Útil para pruebas y cargas iniciales.
-- =========================================================================

BEGIN;

-- 1. CONFIGURACIÓN DEL RANGO DE FECHAS A PROCESAR
--    Ajusta estas variables según tus datos de prueba.
DO $$
DECLARE
    v_fecha_inicio DATE := '2025-11-01'; -- Inicio del rango
    v_fecha_fin    DATE := '2025-11-30'; -- Fin del rango (incluye tus datos del 19)
BEGIN
    
    -- A) Asegurar que DIM_TIEMPO cubra el rango
    PERFORM reporting.poblar_dim_tiempo(v_fecha_inicio, v_fecha_fin);

    -- B) Lógica de Ingesta Masiva (Copia exacta de la lógica V1.1 pero con filtro de rango)
    WITH datos_diarios AS (
        SELECT 
            p.well_id,
            DATE(p.timestamp_lectura) as fecha,
            
            -- Métricas Brutas
            AVG(p.spm_promedio) as spm_promedio,
            MAX(p.spm_promedio) as spm_maximo,
            MAX(p.emboladas_diarias) as emboladas_totales,
            (MAX(p.horas_operacion_acumuladas) - COALESCE(MIN(p.horas_operacion_acumuladas), 0)) as tiempo_op_raw,
            MAX(p.tiempo_parada_poc_diario) as tiempo_paro_noprog,
            MAX(p.produccion_fluido_diaria) as prod_fluido,
            MAX(p.produccion_petroleo_diaria) as prod_petroleo,
            MAX(p.produccion_agua_diaria) as prod_agua,
            MAX(p.produccion_gas_diaria) as prod_gas,
            AVG(p.porcentaje_agua) as water_cut,
            (MAX(p.energia_medidor_acumulada) - COALESCE(MIN(p.energia_medidor_acumulada), 0)) as consumo_kwh,
            (AVG(p.potencia_actual_motor) * 0.7457) as potencia_prom_kw,
            AVG(p.presion_cabezal) as whp,
            AVG(p.presion_casing) as chp,
            AVG(p.pip) as pip,
            MAX(p.maximum_rod_load) as rod_max,
            MIN(p.minimum_rod_load) as rod_min,
            MAX(p.llenado_promedio_diario) as pump_fill,
            MAX(p.conteo_poc_diario) as fallas,
            BOOL_OR(NOT p.estado_motor) as flag_falla,
            COUNT(*) as num_registros,
            COUNT(p.spm_promedio) as registros_validos
            
        FROM stage.tbl_pozo_produccion p
        -- CAMBIO CRÍTICO: Usamos el rango de variables en lugar de CURRENT_DATE
        WHERE DATE(p.timestamp_lectura) BETWEEN v_fecha_inicio AND v_fecha_fin
        GROUP BY p.well_id, DATE(p.timestamp_lectura)
    ),

    parametros_diseno AS (
        SELECT pozo_id, diametro_embolo_bomba_in, longitud_carrera_nominal_in 
        FROM reporting.dim_pozo
    ),

    kpis_calculados AS (
        SELECT 
            dd.*,
            pd.diametro_embolo_bomba_in,
            pd.longitud_carrera_nominal_in,
            LEAST(dd.tiempo_op_raw, 24.0) as tiempo_op_clean,
            (0.000971 * POWER(pd.diametro_embolo_bomba_in, 2) * pd.longitud_carrera_nominal_in * dd.spm_promedio * 1440) as vol_teorico
        FROM datos_diarios dd
        LEFT JOIN parametros_diseno pd ON dd.well_id = pd.pozo_id
    )

    INSERT INTO reporting.fact_operaciones_diarias (
        fecha_id, pozo_id, periodo_comparacion,
        produccion_fluido_bbl, produccion_petroleo_bbl, produccion_agua_bbl, produccion_gas_mcf,
        water_cut_pct, spm_promedio, spm_maximo, emboladas_totales, 
        tiempo_operacion_hrs, tiempo_paro_noprog_hrs, 
        consumo_energia_kwh, potencia_promedio_kw,
        presion_cabezal_psi, presion_casing_psi, pip_psi, 
        carga_max_rod_lb, carga_min_rod_lb, llenado_bomba_pct,
        numero_fallas, flag_falla,
        volumen_teorico_bbl, kpi_efic_vol_pct, kpi_dop_pct, kpi_kwh_bbl, 
        kpi_mtbf_hrs, kpi_uptime_pct, kpi_fill_efficiency_pct,
        completitud_datos_pct, calidad_datos_estado
    )
    SELECT
        TO_CHAR(k.fecha, 'YYYYMMDD')::INT,
        k.well_id,
        'DIARIO',
        k.prod_fluido, k.prod_petroleo, k.prod_agua, k.prod_gas,
        k.water_cut, k.spm_promedio, k.spm_maximo, k.emboladas_totales,
        k.tiempo_op_clean, k.tiempo_paro_noprog,
        k.consumo_kwh, k.potencia_prom_kw,
        k.whp, k.chp, k.pip,
        k.rod_max, k.rod_min, k.pump_fill,
        k.fallas, k.flag_falla,
        k.vol_teorico,
        CASE WHEN k.vol_teorico > 0 THEN (k.prod_fluido / k.vol_teorico) * 100.0 ELSE 0 END,
        (k.tiempo_op_clean / 24.0) * 100.0,
        CASE WHEN k.prod_petroleo > 0 THEN k.consumo_kwh / k.prod_petroleo ELSE NULL END,
        CASE WHEN k.fallas > 0 THEN k.tiempo_op_clean / k.fallas ELSE NULL END,
        CASE WHEN (k.tiempo_op_clean + k.tiempo_paro_noprog) > 0 THEN (k.tiempo_op_clean / (k.tiempo_op_clean + k.tiempo_paro_noprog)) * 100.0 ELSE 0 END,
        k.pump_fill,
        (k.registros_validos::DECIMAL / NULLIF(k.num_registros, 0)) * 100.0,
        CASE WHEN (k.registros_validos::DECIMAL / NULLIF(k.num_registros, 0)) >= 0.9 THEN 'OK' ELSE 'WARNING' END
    FROM kpis_calculados k
    ON CONFLICT (fecha_id, pozo_id, periodo_comparacion) DO UPDATE SET
        produccion_petroleo_bbl = EXCLUDED.produccion_petroleo_bbl,
        kpi_efic_vol_pct = EXCLUDED.kpi_efic_vol_pct,
        kpi_dop_pct = EXCLUDED.kpi_dop_pct,
        potencia_promedio_kw = EXCLUDED.potencia_promedio_kw,
        fecha_carga = CURRENT_TIMESTAMP;

END $$;

COMMIT;