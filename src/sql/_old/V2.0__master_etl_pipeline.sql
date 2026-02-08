-- =========================================================================
-- PIPELINE MAESTRO V2.0: PROCEDIMIENTO ALMACENADO (LAMBDA READY)
-- Descripción: Lógica encapsulada para ejecución remota con parámetros.
-- Ejecución desde Lambda: CALL reporting.sp_master_etl('2025-11-01', '2025-11-20');
-- Ejecución Diaria (Auto): CALL reporting.sp_master_etl(NULL, NULL);
-- =========================================================================

-- Creamos (o reemplazamos) el procedimiento para que sea invocable externamente
CREATE OR REPLACE PROCEDURE reporting.sp_master_etl(
    p_fecha_inicio DATE DEFAULT NULL,
    p_fecha_fin DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    -- Variables de control local
    v_inicio DATE;
    v_fin    DATE;
BEGIN
    -- ---------------------------------------------------------------------
    -- 1. LÓGICA DE FECHAS DINÁMICAS
    -- Si los parámetros son NULL (ejecución automática), usamos "Ayer".
    -- Si se envían fechas (backfill manual), usamos esas.
    -- ---------------------------------------------------------------------
    v_inicio := COALESCE(p_fecha_inicio, (CURRENT_DATE - INTERVAL '1 day')::DATE);
    v_fin    := COALESCE(p_fecha_fin, (CURRENT_DATE - INTERVAL '1 day')::DATE);

    RAISE NOTICE 'Iniciando Pipeline Master ETL desde % hasta %', v_inicio, v_fin;

    -- =====================================================================
    -- PASO 0: ACTUALIZACIÓN DE DIMENSIONES
    -- =====================================================================
    
    -- 0.1. Tiempo y Horas
    PERFORM reporting.poblar_dim_tiempo(v_inicio, v_fin);
    
    INSERT INTO reporting.DIM_HORA (Hora_ID, Hora_Etiqueta, Turno_Operativo)
    SELECT h, TO_CHAR(h, 'FM00')||':00', CASE WHEN h BETWEEN 6 AND 18 THEN 'Dia' ELSE 'Noche' END
    FROM generate_series(0, 23) h ON CONFLICT DO NOTHING;

    -- 0.2. Pozos (Sincronización con Stage)
    INSERT INTO reporting.dim_pozo (
        pozo_id, nombre_pozo, cliente, pais, region, campo, 
        api_number, coordenadas_pozo, tipo_pozo, tipo_levantamiento,
        profundidad_completacion_ft, diametro_embolo_bomba_in, longitud_carrera_nominal_in,
        potencia_nominal_motor_hp, nombre_yacimiento
    )
    SELECT 
        well_id, nombre_pozo, cliente, pais, region, campo,
        api_number, coordenadas_pozo, tipo_pozo, tipo_levantamiento,
        profundidad_completacion, diametro_embolo_bomba, longitud_carrera_nominal,
        potencia_nominal_motor, nombre_yacimiento
    FROM stage.tbl_pozo_maestra
    ON CONFLICT (pozo_id) DO UPDATE SET
        pais = EXCLUDED.pais, campo = EXCLUDED.campo, fecha_ultima_actualizacion = CURRENT_TIMESTAMP;

    -- =====================================================================
    -- PASO 1: NIVEL HORARIO (STAGE -> FACT_HORARIA)
    -- Lógica: Cálculo de Deltas a partir de acumuladores
    -- =====================================================================
    RAISE NOTICE 'Procesando Capa Horaria...';
    
    WITH base_horaria AS (
        SELECT 
            p.well_id,
            DATE(p.timestamp_lectura) as fecha_real,
            EXTRACT(HOUR FROM p.timestamp_lectura)::INT as hora_real,
            DATE_TRUNC('hour', p.timestamp_lectura) as fecha_hora,
            AVG(p.spm_promedio) as spm, AVG(p.presion_cabezal) as whp, AVG(p.presion_casing) as chp,
            AVG(p.pip) as pip, AVG(p.temperatura_motor) as temp_motor, AVG(p.current_amperage) as amperaje,
            MAX(p.produccion_petroleo_diaria) as acum_oil,
            MAX(p.produccion_agua_diaria) as acum_water,
            MAX(p.produccion_gas_diaria) as acum_gas,
            (MAX(p.horas_operacion_acumuladas) - MIN(p.horas_operacion_acumuladas)) * 60.0 as run_min,
            MAX(p.conteo_poc_diario) as fallas_dia,
            BOOL_OR(p.estado_motor) as motor_on
        FROM stage.tbl_pozo_produccion p
        WHERE DATE(p.timestamp_lectura) BETWEEN v_inicio AND v_fin
        GROUP BY 1, 2, 3, 4
    ),
    deltas AS (
        SELECT b.*,
            CASE WHEN b.hora_real = 0 OR (b.acum_oil - LAG(b.acum_oil) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real)) < 0 THEN b.acum_oil
                 ELSE COALESCE(b.acum_oil - LAG(b.acum_oil) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real), 0) END as d_oil,
            CASE WHEN b.hora_real = 0 THEN b.acum_water ELSE GREATEST(0, COALESCE(b.acum_water - LAG(b.acum_water) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real), 0)) END as d_water,
            CASE WHEN b.hora_real = 0 THEN b.acum_gas ELSE GREATEST(0, COALESCE(b.acum_gas - LAG(b.acum_gas) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real), 0)) END as d_gas,
            CASE WHEN b.hora_real = 0 THEN b.fallas_dia ELSE GREATEST(0, b.fallas_dia - LAG(b.fallas_dia) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real)) END as d_fallas
        FROM base_horaria b
    )
    INSERT INTO reporting.FACT_OPERACIONES_HORARIAS (
        Fecha_ID, Hora_ID, Pozo_ID, Fecha_Hora,
        Prod_Petroleo_bbl, Prod_Agua_bbl, Prod_Gas_mcf, Prod_Acumulada_Dia_bbl,
        SPM_Promedio, Presion_Cabezal_psi, Presion_Casing_psi, PIP_psi,
        Temperatura_Motor_F, Amperaje_Motor_A, Tiempo_Operacion_min, Estado_Motor_Fin_Hora, Numero_Fallas_Hora
    )
    SELECT
        TO_CHAR(d.fecha_real, 'YYYYMMDD')::INT, d.hora_real, d.well_id, d.fecha_hora,
        ROUND(d.d_oil::NUMERIC, 2), ROUND(d.d_water::NUMERIC, 2), ROUND(d.d_gas::NUMERIC, 2), d.acum_oil,
        d.spm, d.whp, d.chp, d.pip, d.temp_motor, d.amperaje, LEAST(d.run_min, 60.0), d.motor_on, d.d_fallas
    FROM deltas d
    ON CONFLICT (Fecha_ID, Hora_ID, Pozo_ID) DO UPDATE SET
        Prod_Petroleo_bbl = EXCLUDED.Prod_Petroleo_bbl, SPM_Promedio = EXCLUDED.SPM_Promedio;

    -- =====================================================================
    -- PASO 2: NIVEL DIARIO (STAGE -> FACT_DIARIA)
    -- Lógica: Snapshot Diario de Acumuladores (Fuente Oficial)
    -- =====================================================================
    RAISE NOTICE 'Procesando Capa Diaria...';

    WITH datos_diarios AS (
        SELECT 
            p.well_id, DATE(p.timestamp_lectura) as fecha,
            AVG(p.spm_promedio) as spm_avg, MAX(p.spm_promedio) as spm_max, MAX(p.emboladas_diarias) as emb_tot,
            (MAX(p.horas_operacion_acumuladas) - COALESCE(MIN(p.horas_operacion_acumuladas), 0)) as t_op,
            MAX(p.tiempo_parada_poc_diario) as t_paro,
            MAX(p.produccion_fluido_diaria) as prod_f, MAX(p.produccion_petroleo_diaria) as prod_o,
            MAX(p.produccion_agua_diaria) as prod_w, MAX(p.produccion_gas_diaria) as prod_g,
            AVG(p.porcentaje_agua) as wc,
            (MAX(p.energia_medidor_acumulada) - COALESCE(MIN(p.energia_medidor_acumulada), 0)) as kwh,
            (AVG(p.potencia_actual_motor) * 0.7457) as kw_avg,
            AVG(p.presion_cabezal) as whp, AVG(p.presion_casing) as chp, AVG(p.pip) as pip,
            MAX(p.maximum_rod_load) as rod_max, MIN(p.minimum_rod_load) as rod_min, MAX(p.llenado_promedio_diario) as fill,
            MAX(p.conteo_poc_diario) as fallas, BOOL_OR(NOT p.estado_motor) as flag_falla,
            COUNT(p.spm_promedio) as validos, COUNT(*) as total
        FROM stage.tbl_pozo_produccion p
        WHERE DATE(p.timestamp_lectura) BETWEEN v_inicio AND v_fin
        GROUP BY 1, 2
    ),
    kpis AS (
        SELECT dd.*, pd.diametro_embolo_bomba_in as d, pd.longitud_carrera_nominal_in as l,
            LEAST(dd.t_op, 24.0) as t_op_clean,
            (0.000971 * POWER(pd.diametro_embolo_bomba_in, 2) * pd.longitud_carrera_nominal_in * dd.spm_avg * 1440) as vol_teo
        FROM datos_diarios dd LEFT JOIN reporting.dim_pozo pd ON dd.well_id = pd.pozo_id
    )
    INSERT INTO reporting.FACT_OPERACIONES_DIARIAS (
        fecha_id, pozo_id, produccion_fluido_bbl, produccion_petroleo_bbl, produccion_agua_bbl, produccion_gas_mcf,
        water_cut_pct, spm_promedio, spm_maximo, emboladas_totales, tiempo_operacion_hrs, tiempo_paro_noprog_hrs,
        consumo_energia_kwh, potencia_promedio_kw, presion_cabezal_psi, presion_casing_psi, pip_psi,
        carga_max_rod_lb, carga_min_rod_lb, llenado_bomba_pct, numero_fallas, flag_falla,
        volumen_teorico_bbl, kpi_efic_vol_pct, kpi_dop_pct, kpi_kwh_bbl, kpi_mtbf_hrs, kpi_uptime_pct, kpi_fill_efficiency_pct,
        completitud_datos_pct, calidad_datos_estado
    )
    SELECT
        TO_CHAR(k.fecha, 'YYYYMMDD')::INT, k.well_id,
        k.prod_f, k.prod_o, k.prod_w, k.prod_g, k.wc, k.spm_avg, k.spm_max, k.emb_tot, k.t_op_clean, k.t_paro,
        k.kwh, k.kw_avg, k.whp, k.chp, k.pip, k.rod_max, k.rod_min, k.fill, k.fallas, k.flag_falla,
        k.vol_teo,
        CASE WHEN k.vol_teo > 0 THEN (k.prod_f / k.vol_teo) * 100.0 ELSE 0 END,
        (k.t_op_clean / 24.0) * 100.0,
        CASE WHEN k.prod_o > 0 THEN k.kwh / k.prod_o ELSE NULL END,
        CASE WHEN k.fallas > 0 THEN k.t_op_clean / k.fallas ELSE NULL END,
        CASE WHEN (k.t_op_clean + k.t_paro) > 0 THEN (k.t_op_clean / (k.t_op_clean + k.t_paro)) * 100.0 ELSE 0 END,
        k.fill,
        (k.validos::DECIMAL / NULLIF(k.total, 0)) * 100.0,
        CASE WHEN (k.validos::DECIMAL / NULLIF(k.total, 0)) >= 0.9 THEN 'OK' ELSE 'WARNING' END
    FROM kpis k
    ON CONFLICT (fecha_id, pozo_id, periodo_comparacion) DO UPDATE SET
        produccion_petroleo_bbl = EXCLUDED.produccion_petroleo_bbl, kpi_efic_vol_pct = EXCLUDED.kpi_efic_vol_pct, fecha_carga = CURRENT_TIMESTAMP;

    -- =====================================================================
    -- PASO 3: NIVEL MENSUAL (FACT_DIARIA -> FACT_MENSUAL)
    -- Lógica: Agregación en Cascada (Costo computacional mínimo)
    -- =====================================================================
    RAISE NOTICE 'Procesando Capa Mensual...';

    INSERT INTO reporting.FACT_OPERACIONES_MENSUALES (
        Anio_Mes, Pozo_ID, Total_Petroleo_bbl, Total_Agua_bbl, Total_Gas_mcf, Total_Fluido_bbl,
        Promedio_SPM, Promedio_WHP_psi, Promedio_CHP_psi, Promedio_Water_Cut_pct,
        Total_Fallas_Mes, Dias_Operando, Tiempo_Operacion_hrs, Tiempo_Paro_hrs,
        Eficiencia_Uptime_pct, Promedio_Efic_Vol_pct, Consumo_Energia_Total_kwh, KPI_KWH_BBL_Mes, Fecha_Ultima_Carga
    )
    SELECT
        dt.Anio_Mes, f.Pozo_ID,
        SUM(f.Produccion_Petroleo_bbl), SUM(f.Produccion_Agua_bbl), SUM(f.Produccion_Gas_mcf), SUM(f.Produccion_Fluido_bbl),
        AVG(f.SPM_Promedio), AVG(f.Presion_Cabezal_psi), AVG(f.Presion_Casing_psi), AVG(f.Water_Cut_pct),
        SUM(f.Numero_Fallas), COUNT(CASE WHEN f.Tiempo_Operacion_hrs > 0 THEN 1 END), SUM(f.Tiempo_Operacion_hrs), SUM(f.Tiempo_Paro_NoProg_hrs),
        CASE WHEN SUM(f.Tiempo_Operacion_hrs + f.Tiempo_Paro_NoProg_hrs) > 0 THEN (SUM(f.Tiempo_Operacion_hrs) / SUM(f.Tiempo_Operacion_hrs + f.Tiempo_Paro_NoProg_hrs)) * 100.0 ELSE 0 END,
        AVG(f.KPI_Efic_Vol_pct), SUM(f.Consumo_Energia_kwh),
        CASE WHEN SUM(f.Produccion_Petroleo_bbl) > 0 THEN SUM(f.Consumo_Energia_kwh) / SUM(f.Produccion_Petroleo_bbl) ELSE 0 END,
        CURRENT_TIMESTAMP
    FROM reporting.FACT_OPERACIONES_DIARIAS f
    JOIN reporting.DIM_TIEMPO dt ON f.Fecha_ID = dt.Fecha_ID
    WHERE dt.Fecha BETWEEN v_inicio AND v_fin
    GROUP BY dt.Anio_Mes, f.Pozo_ID
    ON CONFLICT (Anio_Mes, Pozo_ID) DO UPDATE SET
        Total_Petroleo_bbl = EXCLUDED.Total_Petroleo_bbl, KPI_KWH_BBL_Mes = EXCLUDED.KPI_KWH_BBL_Mes, Fecha_Ultima_Carga = CURRENT_TIMESTAMP;

    RAISE NOTICE 'Pipeline Completado Exitosamente.';
END;
$$;

COMMENT ON PROCEDURE reporting.sp_master_etl IS 'Orquestador Maestro. Params: (fecha_inicio, fecha_fin). Si NULL, procesa ayer.';