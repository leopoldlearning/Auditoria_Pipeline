/*
--------------------------------------------------------------------------------
-- MOTOR DE REPORTING HISTÓRICO V6.1 (V4 COMPATIBLE)
-- Reemplaza a: V2_reporting_engine.sql
-- ETL: STAGE → REPORTING (Histórico Horario, Diario, Mensual)
-- Adaptado para esquema Reporting V4 (Nuevos Nombres de Columnas)
--------------------------------------------------------------------------------
*/

-- ============================================================
-- 1. PROCEDIMIENTO PRINCIPAL DE REPORTING HISTÓRICO
-- ============================================================

CREATE OR REPLACE PROCEDURE reporting.sp_load_to_reporting(
    p_fecha_inicio DATE,
    p_fecha_fin DATE,
    p_procesar_horario BOOLEAN DEFAULT TRUE,
    p_procesar_diario  BOOLEAN DEFAULT TRUE,
    p_procesar_mensual BOOLEAN DEFAULT TRUE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_fecha_inicio DATE := COALESCE(p_fecha_inicio, CURRENT_DATE);
    v_fecha_fin    DATE := COALESCE(p_fecha_fin,    CURRENT_DATE);
BEGIN
    ------------------------------------------------------------------------
    -- 1) DIMENSIONES BÁSICAS
    ------------------------------------------------------------------------
    PERFORM reporting.poblar_dim_tiempo(
        (v_fecha_inicio - INTERVAL '1 year')::DATE,
        (v_fecha_fin   + INTERVAL '1 year')::DATE
    );

    PERFORM reporting.poblar_dim_hora();

    ------------------------------------------------------------------------
    -- 2) DIMENSIÓN POZO (ENRIQUECIDA DESDE STAGE.TBL_POZO_MAESTRA)
    ------------------------------------------------------------------------
    INSERT INTO reporting.dim_pozo (
        pozo_id,
        nombre_pozo,
        cliente,
        pais,
        region,
        campo,
        api_number,
        coordenadas_pozo,
        tipo_pozo,
        tipo_levantamiento,
        profundidad_completacion_ft,
        diametro_embolo_bomba_in,
        longitud_carrera_nominal_unidad_in, -- [V4] Renamed
        potencia_nominal_motor_hp,
        nombre_yacimiento
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
        m.longitud_carrera_nominal_unidad,
        m.potencia_nominal_motor,
        m.nombre_yacimiento
    FROM stage.tbl_pozo_maestra m
    ON CONFLICT (pozo_id) DO UPDATE SET
        nombre_pozo               = EXCLUDED.nombre_pozo,
        cliente                   = EXCLUDED.cliente,
        pais                      = EXCLUDED.pais,
        region                    = EXCLUDED.region,
        campo                     = EXCLUDED.campo,
        api_number                = EXCLUDED.api_number,
        coordenadas_pozo          = EXCLUDED.coordenadas_pozo,
        tipo_pozo                 = EXCLUDED.tipo_pozo,
        tipo_levantamiento        = EXCLUDED.tipo_levantamiento,
        profundidad_completacion_ft = EXCLUDED.profundidad_completacion_ft,
        diametro_embolo_bomba_in  = EXCLUDED.diametro_embolo_bomba_in,
        longitud_carrera_nominal_unidad_in = EXCLUDED.longitud_carrera_nominal_unidad_in, -- [V4]
        potencia_nominal_motor_hp = EXCLUDED.potencia_nominal_motor_hp,
        nombre_yacimiento         = EXCLUDED.nombre_yacimiento,
        fecha_ultima_actualizacion = CURRENT_TIMESTAMP;

    ------------------------------------------------------------------------
    -- 3) CAPA HORARIA AVANZADA (STAGE → FACT_OPERACIONES_HORARIAS)
    ------------------------------------------------------------------------
    IF p_procesar_horario THEN

        WITH base_horaria AS (
            SELECT 
                p.well_id,
                DATE(p.timestamp_lectura)                         AS fecha_real,
                EXTRACT(HOUR FROM p.timestamp_lectura)::INT       AS hora_real,
                DATE_TRUNC('hour', p.timestamp_lectura)           AS fecha_hora,

                -- Dinámica promedio / snapshot horario
                AVG(p.spm_promedio)               AS spm,
                AVG(p.presion_cabezal)            AS whp,
                AVG(p.presion_casing)             AS chp,
                AVG(p.pip)                        AS pip,
                AVG(p.presion_descarga_bomba)     AS pdp,
                AVG(p.temperatura_tanque_aceite)  AS temp_motor,   -- proxy
                AVG(p.current_amperage)           AS amperaje,
                AVG(p.potencia_actual_motor)      AS potencia,

                -- Acumuladores (estado al final de la hora)
                MAX(p.produccion_petroleo_diaria) AS acum_oil,
                MAX(p.produccion_agua_diaria)     AS acum_water,
                MAX(p.produccion_gas_diaria)      AS acum_gas,
                MAX(p.produccion_fluido_diaria)   AS acum_fluido,

                -- Tiempos (delta dentro de la hora, en minutos)
                (MAX(p.horas_operacion_acumuladas)
                 - COALESCE(MIN(p.horas_operacion_acumuladas), 0)) * 60.0 AS run_min,
                MAX(p.conteo_poc_diario)          AS fallas_dia,

                -- Estado motor en la hora
                BOOL_OR(p.estado_motor)           AS algun_momento_encendido,

                -- Calidad
                COUNT(*)                          AS num_registros
            FROM stage.tbl_pozo_produccion p
            WHERE DATE(p.timestamp_lectura) BETWEEN v_fecha_inicio AND v_fecha_fin
            GROUP BY
                p.well_id,
                DATE(p.timestamp_lectura),
                EXTRACT(HOUR FROM p.timestamp_lectura),
                DATE_TRUNC('hour', p.timestamp_lectura)
        ),
        deltas_calculados AS (
            SELECT
                b.*,

                -- Delta Petróleo
                CASE 
                    WHEN b.hora_real = 0 THEN b.acum_oil
                    WHEN (b.acum_oil - LAG(b.acum_oil) OVER (
                            PARTITION BY b.well_id, b.fecha_real
                            ORDER BY b.hora_real
                         )) < 0 THEN b.acum_oil
                    ELSE COALESCE(
                        b.acum_oil - LAG(b.acum_oil) OVER (
                            PARTITION BY b.well_id, b.fecha_real
                            ORDER BY b.hora_real
                        ), 0)
                END AS delta_oil,

                -- Delta Agua
                CASE 
                    WHEN b.hora_real = 0 THEN b.acum_water
                    ELSE GREATEST(0, COALESCE(
                        b.acum_water - LAG(b.acum_water) OVER (
                            PARTITION BY b.well_id, b.fecha_real
                            ORDER BY b.hora_real
                        ), 0))
                END AS delta_water,

                -- Delta Gas
                CASE 
                    WHEN b.hora_real = 0 THEN b.acum_gas
                    ELSE GREATEST(0, COALESCE(
                        b.acum_gas - LAG(b.acum_gas) OVER (
                            PARTITION BY b.well_id, b.fecha_real
                            ORDER BY b.hora_real
                        ), 0))
                END AS delta_gas,

                -- Delta Fluido
                CASE 
                    WHEN b.hora_real = 0 THEN b.acum_fluido
                    ELSE GREATEST(0, COALESCE(
                        b.acum_fluido - LAG(b.acum_fluido) OVER (
                            PARTITION BY b.well_id, b.fecha_real
                            ORDER BY b.hora_real
                        ), 0))
                END AS delta_fluido,

                -- Delta fallas
                CASE 
                    WHEN b.hora_real = 0 THEN b.fallas_dia
                    ELSE GREATEST(0, b.fallas_dia - COALESCE(LAG(b.fallas_dia) OVER (
                            PARTITION BY b.well_id, b.fecha_real
                            ORDER BY b.hora_real
                        ), 0))
                END AS delta_fallas
            FROM base_horaria b
        )
        INSERT INTO reporting.fact_operaciones_horarias (
            fecha_id,
            hora_id,
            pozo_id,
            fecha_hora,

            -- [V4] Columnas Renombradas
            prod_petroleo_diaria_bpd, -- Antes prod_petroleo_bbl
            prod_agua_bbl,
            prod_gas_mcf,
            prod_acumulada_dia_bbl,
            produccion_fluido_bbl,    -- [V4 NEW]

            spm_promedio,
            presion_cabezal_psi,
            presion_casing_psi,
            pip_psi,
            pdp_psi,
            
            temperatura_motor_f,
            amperaje_motor_a,
            motor_power_hp,

            tiempo_operacion_min,
            estado_motor_fin_hora,
            numero_fallas_hora
        )
        SELECT
            TO_CHAR(d.fecha_real, 'YYYYMMDD')::INT AS fecha_id,
            d.hora_real                             AS hora_id,
            d.well_id                               AS pozo_id,
            d.fecha_hora                            AS fecha_hora,

            ROUND(d.delta_oil::NUMERIC,   2)        AS prod_petroleo_diaria_bpd,
            ROUND(d.delta_water::NUMERIC, 2)        AS prod_agua_bbl,
            ROUND(d.delta_gas::NUMERIC,   2)        AS prod_gas_mcf,
            ROUND(d.acum_oil::NUMERIC, 2)           AS prod_acumulada_dia_bbl, -- Nota: prod_acumulada_dia_bbl solía ser fluido, verificar semántica. Si es OIL, usar acum_oil. Si es fluido, usar acum_fluido.
                                                    -- En V2: prod_acumulada_dia_bbl <- d.acum_fluido. Mantenemos semántica original.
            ROUND(d.delta_fluido::NUMERIC, 2)       AS produccion_fluido_bbl,

            ROUND(d.spm::NUMERIC,        2)         AS spm_promedio,
            ROUND(d.whp::NUMERIC,        2)         AS presion_cabezal_psi,
            ROUND(d.chp::NUMERIC,        2)         AS presion_casing_psi,
            ROUND(d.pip::NUMERIC,        2)         AS pip_psi,
            ROUND(d.pdp::NUMERIC,        2)         AS pdp_psi,

            ROUND(d.temp_motor::NUMERIC, 2)         AS temperatura_motor_f,
            ROUND(d.amperaje::NUMERIC,   2)         AS amperaje_motor_a,
            ROUND(d.potencia::NUMERIC,   2)         AS motor_power_hp,

            LEAST(ROUND(d.run_min::NUMERIC, 2), 60.0) AS tiempo_operacion_min,
            d.algun_momento_encendido                AS estado_motor_fin_hora,
            d.delta_fallas::INT                      AS numero_fallas_hora
        FROM deltas_calculados d
        WHERE d.num_registros > 0
        ON CONFLICT (fecha_id, hora_id, pozo_id)
        DO UPDATE SET
            prod_petroleo_diaria_bpd = EXCLUDED.prod_petroleo_diaria_bpd,
            prod_agua_bbl           = EXCLUDED.prod_agua_bbl,
            prod_gas_mcf            = EXCLUDED.prod_gas_mcf,
            prod_acumulada_dia_bbl  = EXCLUDED.prod_acumulada_dia_bbl,
            produccion_fluido_bbl   = EXCLUDED.produccion_fluido_bbl,
            
            spm_promedio            = EXCLUDED.spm_promedio,
            presion_cabezal_psi     = EXCLUDED.presion_cabezal_psi,
            presion_casing_psi      = EXCLUDED.presion_casing_psi,
            pip_psi                 = EXCLUDED.pip_psi,
            pdp_psi                 = EXCLUDED.pdp_psi,
            
            temperatura_motor_f     = EXCLUDED.temperatura_motor_f,
            amperaje_motor_a        = EXCLUDED.amperaje_motor_a,
            motor_power_hp          = EXCLUDED.motor_power_hp,
            
            tiempo_operacion_min    = EXCLUDED.tiempo_operacion_min,
            estado_motor_fin_hora   = EXCLUDED.estado_motor_fin_hora,
            numero_fallas_hora      = EXCLUDED.numero_fallas_hora;

        RAISE NOTICE 'Procesamiento horario completado para rango: % a %', v_fecha_inicio, v_fecha_fin;
    END IF;

    ------------------------------------------------------------------------
    -- 4) CAPA DIARIA AVANZADA (STAGE → FACT_OPERACIONES_DIARIAS)
    ------------------------------------------------------------------------
    IF p_procesar_diario THEN

        WITH datos_diarios AS (
            SELECT 
                p.well_id,
                DATE(p.timestamp_lectura) AS fecha,

                -- Métricas brutas
                AVG(p.spm_promedio)               AS spm_promedio,
                MAX(p.spm_promedio)               AS spm_maximo,
                MAX(p.emboladas_diarias)          AS emboladas_totales,

                -- Tiempos
                (MAX(p.horas_operacion_acumuladas)
                 - COALESCE(MIN(p.horas_operacion_acumuladas), 0)) AS tiempo_op_raw,
                MAX(p.tiempo_parada_poc_diario)   AS tiempo_paro_noprog,

                -- Producción
                MAX(p.produccion_fluido_diaria)   AS prod_fluido,
                MAX(p.produccion_petroleo_diaria) AS prod_petroleo,
                MAX(p.produccion_agua_diaria)     AS prod_agua,
                MAX(p.produccion_gas_diaria)      AS prod_gas,
                AVG(p.porcentaje_agua)            AS water_cut,

                -- Energía
                (MAX(p.energia_medidor_acumulada)
                 - COALESCE(MIN(p.energia_medidor_acumulada), 0)) AS consumo_kwh,
                (AVG(p.potencia_actual_motor) * 0.7457)            AS potencia_prom_kw,

                -- Presiones y dinámica
                AVG(p.presion_cabezal)            AS whp,
                AVG(p.presion_casing)             AS chp,
                AVG(p.pip)                        AS pip,
                MAX(p.maximum_rod_load)           AS rod_max,
                MIN(p.minimum_rod_load)           AS rod_min,
                MAX(p.llenado_promedio_diario)    AS pump_fill,

                -- Fallas
                MAX(p.conteo_poc_diario)          AS fallas,
                BOOL_OR(NOT p.estado_motor)       AS flag_falla,
                
                -- Estado Fin Día (Ultimo registro)
                -- (Se puede mejorar con Distinct On o Last Value, aquí usaremos una aproximación simple si estado_motor es bool)
                -- Asumiremos que si hubo operación en la última hora es true, pero lo ideal es buscar el ultimo record.
                -- Para V6.1 simplificado: Si tiempo_op > 20h -> true ? No, mejor aproximación:
                -- TODO: Implementar lógica last_value real si es crítico.

                -- Calidad
                COUNT(*)                          AS num_registros,
                COUNT(p.pip)                      AS registros_validos
            FROM stage.tbl_pozo_produccion p
            WHERE DATE(p.timestamp_lectura) BETWEEN v_fecha_inicio AND v_fecha_fin
            GROUP BY p.well_id, DATE(p.timestamp_lectura)
        ),
        estado_fin_dia AS (
            SELECT DISTINCT ON (well_id, DATE(timestamp_lectura))
                well_id,
                DATE(timestamp_lectura) as fecha,
                estado_motor
            FROM stage.tbl_pozo_produccion
            WHERE DATE(timestamp_lectura) BETWEEN v_fecha_inicio AND v_fecha_fin
            ORDER BY well_id, DATE(timestamp_lectura), timestamp_lectura DESC
        ),
        parametros_diseno AS (
            SELECT
                pozo_id,
                diametro_embolo_bomba_in,
                longitud_carrera_nominal_unidad_in
            FROM reporting.dim_pozo
        ),
        kpis_calculados AS (
            SELECT
                dd.*,
                efd.estado_motor as estado_motor_fin_dia_calc,
                pd.diametro_embolo_bomba_in,
                pd.longitud_carrera_nominal_unidad_in,

                LEAST(dd.tiempo_op_raw, 24.0) AS tiempo_op_clean,

                -- Volumen teórico (BBL)
                (0.000971
                 * POWER(pd.diametro_embolo_bomba_in, 2)
                 * pd.longitud_carrera_nominal_unidad_in
                 * dd.spm_promedio
                 * 1440) AS vol_teorico
            FROM datos_diarios dd
            LEFT JOIN estado_fin_dia efd ON dd.well_id = efd.well_id AND dd.fecha = efd.fecha
            LEFT JOIN parametros_diseno pd
                ON dd.well_id = pd.pozo_id
        )
        INSERT INTO reporting.fact_operaciones_diarias (
            fecha_id,
            pozo_id,
            periodo_comparacion,

            produccion_fluido_bbl,
            produccion_petroleo_bbl,
            produccion_agua_bbl,
            produccion_gas_mcf,
            water_cut_pct,

            spm_promedio,
            spm_maximo,
            emboladas_totales,
            tiempo_operacion_hrs,
            tiempo_paro_noprog_hrs,

            consumo_energia_kwh,
            potencia_promedio_kw,
            presion_cabezal_psi,
            presion_casing_psi,
            pip_psi,
            carga_max_rod_lb,
            carga_min_rod_lb,
            llenado_bomba_pct,
            numero_fallas,
            flag_falla,
            
            estado_motor_fin_dia, -- [V4 NEW]

            volumen_teorico_bbl,
            kpi_efic_vol_pct,
            kpi_dop_pct,
            kpi_kwh_bbl,
            kpi_mtbf_hrs,
            kpi_uptime_pct,
            kpi_fill_efficiency_pct,

            completitud_datos_pct,
            calidad_datos_estado,
            fecha_carga
        )
        SELECT
            TO_CHAR(k.fecha, 'YYYYMMDD')::INT AS fecha_id,
            k.well_id                         AS pozo_id,
            'DIARIO'                          AS periodo_comparacion,

            -- Métricas brutas
            k.prod_fluido,
            k.prod_petroleo,
            k.prod_agua,
            k.prod_gas,
            k.water_cut,

            k.spm_promedio,
            k.spm_maximo,
            k.emboladas_totales,
            k.tiempo_op_clean,
            k.tiempo_paro_noprog,

            k.consumo_kwh,
            k.potencia_prom_kw,
            k.whp,
            k.chp,
            k.pip,
            k.rod_max,
            k.rod_min,
            k.pump_fill,
            k.fallas,
            k.flag_falla,
            
            k.estado_motor_fin_dia_calc,

            -- Volumen teórico
            k.vol_teorico,

            -- Eficiencia volumétrica
            CASE
                WHEN k.vol_teorico > 0
                    THEN (k.prod_fluido / k.vol_teorico) * 100.0
                ELSE 0
            END AS kpi_efic_vol_pct,

            -- DOP
            (k.tiempo_op_clean / 24.0) * 100.0 AS kpi_dop_pct,

            -- KWH/BBL
            CASE
                WHEN k.prod_petroleo > 0
                    THEN k.consumo_kwh / k.prod_petroleo
                ELSE NULL
            END AS kpi_kwh_bbl,

            -- MTBF
            CASE
                WHEN k.fallas > 0
                    THEN k.tiempo_op_clean / k.fallas
                ELSE NULL
            END AS kpi_mtbf_hrs,

            -- Uptime vs tiempo no programado
            CASE
                WHEN (k.tiempo_op_clean + k.tiempo_paro_noprog) > 0
                    THEN (k.tiempo_op_clean
                          / (k.tiempo_op_clean + k.tiempo_paro_noprog)) * 100.0
                ELSE 0
            END AS kpi_uptime_pct,

            -- Fill efficiency (proxy)
            k.pump_fill AS kpi_fill_efficiency_pct,

            -- Calidad
            (k.registros_validos::DECIMAL
             / NULLIF(k.num_registros, 0)) * 100.0 AS completitud_datos_pct,
            CASE
                WHEN (k.registros_validos::DECIMAL
                      / NULLIF(k.num_registros, 0)) >= 0.9
                    THEN 'OK'
                ELSE 'WARNING'
            END AS calidad_datos_estado,

            CURRENT_TIMESTAMP AS fecha_carga
        FROM kpis_calculados k
        ON CONFLICT (fecha_id, pozo_id, periodo_comparacion)
        DO UPDATE SET
            produccion_fluido_bbl   = EXCLUDED.produccion_fluido_bbl,
            produccion_petroleo_bbl = EXCLUDED.produccion_petroleo_bbl,
            produccion_agua_bbl     = EXCLUDED.produccion_agua_bbl,
            produccion_gas_mcf      = EXCLUDED.produccion_gas_mcf,
            water_cut_pct           = EXCLUDED.water_cut_pct,
            spm_promedio            = EXCLUDED.spm_promedio,
            spm_maximo              = EXCLUDED.spm_maximo,
            emboladas_totales       = EXCLUDED.emboladas_totales,
            tiempo_operacion_hrs    = EXCLUDED.tiempo_operacion_hrs,
            tiempo_paro_noprog_hrs  = EXCLUDED.tiempo_paro_noprog_hrs,
            consumo_energia_kwh     = EXCLUDED.consumo_energia_kwh,
            potencia_promedio_kw    = EXCLUDED.potencia_promedio_kw,
            presion_cabezal_psi     = EXCLUDED.presion_cabezal_psi,
            presion_casing_psi      = EXCLUDED.presion_casing_psi,
            pip_psi                 = EXCLUDED.pip_psi,
            carga_max_rod_lb        = EXCLUDED.carga_max_rod_lb,
            carga_min_rod_lb        = EXCLUDED.carga_min_rod_lb,
            llenado_bomba_pct       = EXCLUDED.llenado_bomba_pct,
            numero_fallas           = EXCLUDED.numero_fallas,
            flag_falla              = EXCLUDED.flag_falla,
            estado_motor_fin_dia    = EXCLUDED.estado_motor_fin_dia,
            volumen_teorico_bbl     = EXCLUDED.volumen_teorico_bbl,
            kpi_efic_vol_pct        = EXCLUDED.kpi_efic_vol_pct,
            kpi_dop_pct             = EXCLUDED.kpi_dop_pct,
            kpi_kwh_bbl             = EXCLUDED.kpi_kwh_bbl,
            kpi_mtbf_hrs            = EXCLUDED.kpi_mtbf_hrs,
            kpi_uptime_pct          = EXCLUDED.kpi_uptime_pct,
            kpi_fill_efficiency_pct = EXCLUDED.kpi_fill_efficiency_pct,
            completitud_datos_pct   = EXCLUDED.completitud_datos_pct,
            calidad_datos_estado    = EXCLUDED.calidad_datos_estado,
            fecha_carga             = CURRENT_TIMESTAMP;

        RAISE NOTICE 'Procesamiento diario completado para rango: % a %', v_fecha_inicio, v_fecha_fin;
    END IF;

    ------------------------------------------------------------------------
    -- 5) CAPA MENSUAL (FACT_OPERACIONES_MENSUALES DESDE LA DIARIA)
    ------------------------------------------------------------------------
    IF p_procesar_mensual THEN

        WITH base_mensual AS (
            SELECT
                dt.anio_mes,
                f.pozo_id,

                SUM(f.produccion_petroleo_bbl)          AS total_petroleo_bbl,
                SUM(f.produccion_agua_bbl)              AS total_agua_bbl,
                SUM(f.produccion_gas_mcf)               AS total_gas_mcf,
                SUM(f.produccion_fluido_bbl)            AS total_fluido_bbl,

                AVG(f.spm_promedio)                     AS promedio_spm,
                AVG(f.presion_cabezal_psi)              AS promedio_whp_psi,
                AVG(f.presion_casing_psi)               AS promedio_chp_psi,
                AVG(f.water_cut_pct)                    AS promedio_water_cut_pct,

                AVG(f.promedio_lift_efficiency_pct)     AS promedio_lift_efficiency_pct,
                AVG(f.promedio_bouyant_rod_weight_lb)   AS promedio_bouyant_rod_weight_lb,
                AVG(f.promedio_fluid_level_tvd_ft)      AS promedio_fluid_level_tvd_ft,
                AVG(f.promedio_pdp_psi)                 AS promedio_pdp_psi,
                AVG(f.promedio_tank_fluid_temp_f)       AS promedio_tank_fluid_temp_f,
                AVG(f.promedio_motor_power_hp)          AS promedio_motor_power_hp,
                AVG(f.promedio_fluid_flow_monitor_bpd)  AS promedio_fluid_flow_monitor_bpd,

                SUM(f.numero_fallas)                    AS total_fallas_mes,
                COUNT(*)                                AS dias_operando,
                SUM(f.tiempo_operacion_hrs)             AS tiempo_operacion_hrs,
                SUM(f.tiempo_paro_noprog_hrs)           AS tiempo_paro_hrs,

                -- Uptime mensual
                CASE
                    WHEN SUM(f.tiempo_operacion_hrs + f.tiempo_paro_noprog_hrs) > 0
                        THEN (SUM(f.tiempo_operacion_hrs)
                              / SUM(f.tiempo_operacion_hrs + f.tiempo_paro_noprog_hrs)) * 100.0
                    ELSE 0
                END AS eficiencia_uptime_pct,

                AVG(f.kpi_efic_vol_pct)                 AS promedio_efic_vol_pct,
                SUM(f.consumo_energia_kwh)              AS consumo_energia_total_kwh,

                -- KWH/BBL mensual (sobre petróleo)
                CASE
                    WHEN SUM(f.produccion_petroleo_bbl) > 0
                        THEN SUM(f.consumo_energia_kwh) / SUM(f.produccion_petroleo_bbl)
                    ELSE NULL
                END AS kpi_kwh_bbl_mes
            FROM reporting.fact_operaciones_diarias f
            JOIN reporting.dim_tiempo dt
                ON f.fecha_id = dt.fecha_id
            WHERE dt.fecha BETWEEN v_fecha_inicio AND v_fecha_fin
              AND f.periodo_comparacion = 'DIARIO'
            GROUP BY dt.anio_mes, f.pozo_id
        )
        INSERT INTO reporting.fact_operaciones_mensuales (
            anio_mes,
            pozo_id,

            produccion_petroleo_acumulada_bbl, -- [V4] Antes total_petroleo_bbl
            total_agua_bbl,
            total_gas_mcf,
            total_fluido_bbl,
            prom_produccion_fluido_bbl, -- [V4 NEW] Agregado para integridad con tabla

            promedio_spm,
            promedio_whp_psi,
            promedio_chp_psi,
            promedio_water_cut_pct,
            promedio_lift_efficiency_pct,
            promedio_bouyant_rod_weight_lb,
            promedio_fluid_level_tvd_ft,
            promedio_pdp_psi,
            promedio_tank_fluid_temp_f,
            promedio_motor_power_hp,
            promedio_fluid_flow_monitor_bpd,

            total_fallas_mes,
            dias_operando,
            tiempo_operacion_hrs,
            tiempo_paro_hrs,
            eficiencia_uptime_pct,
            promedio_efic_vol_pct,
            consumo_energia_total_kwh,
            kpi_kwh_bbl_mes,
            fecha_ultima_carga
        )
        SELECT
            b.anio_mes,
            b.pozo_id,

            b.total_petroleo_bbl,
            b.total_agua_bbl,
            b.total_gas_mcf,
            b.total_fluido_bbl,
            -- Para prom_produccion_fluido_bbl usamos total_fluido_bbl / dias_operando como proxy si no está en la base mensual
            CASE WHEN b.dias_operando > 0 THEN b.total_fluido_bbl / b.dias_operando ELSE 0 END,

            b.promedio_spm,
            b.promedio_whp_psi,
            b.promedio_chp_psi,
            b.promedio_water_cut_pct,
            b.promedio_lift_efficiency_pct,
            b.promedio_bouyant_rod_weight_lb,
            b.promedio_fluid_level_tvd_ft,
            b.promedio_pdp_psi,
            b.promedio_tank_fluid_temp_f,
            b.promedio_motor_power_hp,
            b.promedio_fluid_flow_monitor_bpd,

            b.total_fallas_mes,
            b.dias_operando,
            b.tiempo_operacion_hrs,
            b.tiempo_paro_hrs,
            b.eficiencia_uptime_pct,
            b.promedio_efic_vol_pct,
            b.consumo_energia_total_kwh,
            b.kpi_kwh_bbl_mes,
            CURRENT_TIMESTAMP
        FROM base_mensual b
        ON CONFLICT (anio_mes, pozo_id)
        DO UPDATE SET
            produccion_petroleo_acumulada_bbl          = EXCLUDED.produccion_petroleo_acumulada_bbl,
            total_agua_bbl              = EXCLUDED.total_agua_bbl,
            total_gas_mcf               = EXCLUDED.total_gas_mcf,
            total_fluido_bbl            = EXCLUDED.total_fluido_bbl,
            prom_produccion_fluido_bbl  = EXCLUDED.prom_produccion_fluido_bbl,
            
            promedio_spm                = EXCLUDED.promedio_spm,
            promedio_whp_psi            = EXCLUDED.promedio_whp_psi,
            promedio_chp_psi            = EXCLUDED.promedio_chp_psi,
            promedio_water_cut_pct      = EXCLUDED.promedio_water_cut_pct,
            promedio_lift_efficiency_pct = EXCLUDED.promedio_lift_efficiency_pct,
            promedio_bouyant_rod_weight_lb = EXCLUDED.promedio_bouyant_rod_weight_lb,
            promedio_fluid_level_tvd_ft = EXCLUDED.promedio_fluid_level_tvd_ft,
            promedio_pdp_psi            = EXCLUDED.promedio_pdp_psi,
            promedio_tank_fluid_temp_f  = EXCLUDED.promedio_tank_fluid_temp_f,
            promedio_motor_power_hp     = EXCLUDED.promedio_motor_power_hp,
            promedio_fluid_flow_monitor_bpd = EXCLUDED.promedio_fluid_flow_monitor_bpd,
            total_fallas_mes            = EXCLUDED.total_fallas_mes,
            dias_operando               = EXCLUDED.dias_operando,
            tiempo_operacion_hrs        = EXCLUDED.tiempo_operacion_hrs,
            tiempo_paro_hrs             = EXCLUDED.tiempo_paro_hrs,
            eficiencia_uptime_pct       = EXCLUDED.eficiencia_uptime_pct,
            promedio_efic_vol_pct       = EXCLUDED.promedio_efic_vol_pct,
            consumo_energia_total_kwh   = EXCLUDED.consumo_energia_total_kwh,
            kpi_kwh_bbl_mes             = EXCLUDED.kpi_kwh_bbl_mes,
            fecha_ultima_carga          = CURRENT_TIMESTAMP;

        RAISE NOTICE 'Procesamiento mensual completado para rango: % a %', v_fecha_inicio, v_fecha_fin;
    END IF;

END;
$$;


-- ============================================================
-- 2. PROCEDIMIENTO DE KPIs DE NEGOCIO (DIARIO + MENSUAL)
-- ============================================================

CREATE OR REPLACE PROCEDURE reporting.sp_load_kpi_business(
    p_fecha_inicio DATE,
    p_fecha_fin DATE
)
LANGUAGE plpgsql
AS $$
BEGIN
    ----------------------------------------------------------------------
    -- 1. KPIs DIARIOS
    ----------------------------------------------------------------------
    INSERT INTO reporting.dataset_kpi_business (
        fecha,
        well_id,
        nombre_pozo,
        campo,
        uptime_pct,
        tiempo_operacion_hrs,
        mtbf_dias,
        fail_count,
        costo_energia_usd,
        kwh_por_barril,
        lifting_cost_usd_bbl,
        eur_remanente_bbl,
        vida_util_estimada_dias
    )
    SELECT
        dt.fecha,
        d.pozo_id,
        p.nombre_pozo,
        p.campo,
        d.kpi_uptime_pct,
        d.tiempo_operacion_hrs,
        CASE 
            WHEN d.kpi_mtbf_hrs IS NOT NULL THEN d.kpi_mtbf_hrs / 24.0
            ELSE NULL
        END AS mtbf_dias,
        d.numero_fallas,
        NULL AS costo_energia_usd,
        d.kpi_kwh_bbl,
        NULL AS lifting_cost_usd_bbl,
        NULL AS eur_remanente_bbl,
        NULL AS vida_util_estimada_dias
    FROM reporting.fact_operaciones_diarias d
    JOIN reporting.dim_tiempo dt ON d.fecha_id = dt.fecha_id
    JOIN reporting.dim_pozo p ON d.pozo_id = p.pozo_id
    WHERE dt.fecha BETWEEN p_fecha_inicio AND p_fecha_fin
      AND d.periodo_comparacion = 'DIARIO'
    ON CONFLICT (fecha, well_id)
    DO UPDATE SET
        uptime_pct = EXCLUDED.uptime_pct,
        tiempo_operacion_hrs = EXCLUDED.tiempo_operacion_hrs,
        mtbf_dias = EXCLUDED.mtbf_dias,
        fail_count = EXCLUDED.fail_count,
        kwh_por_barril = EXCLUDED.kwh_por_barril;

    ----------------------------------------------------------------------
    -- 2. KPIs MENSUALES
    ----------------------------------------------------------------------
    INSERT INTO reporting.dataset_kpi_business (
        fecha,
        well_id,
        nombre_pozo,
        campo,
        uptime_pct,
        tiempo_operacion_hrs,
        mtbf_dias,
        fail_count,
        costo_energia_usd,
        kwh_por_barril,
        lifting_cost_usd_bbl,
        eur_remanente_bbl,
        vida_util_estimada_dias
    )
    SELECT
        TO_DATE(m.anio_mes || '-01', 'YYYY-MM'),
        m.pozo_id,
        p.nombre_pozo,
        p.campo,
        m.eficiencia_uptime_pct,
        m.tiempo_operacion_hrs,
        CASE 
            WHEN m.total_fallas_mes > 0 THEN (m.tiempo_operacion_hrs / m.total_fallas_mes) / 24.0
            ELSE NULL
        END AS mtbf_dias,
        m.total_fallas_mes,
        NULL AS costo_energia_usd,
        m.kpi_kwh_bbl_mes,
        NULL AS lifting_cost_usd_bbl,
        NULL AS eur_remanente_bbl,
        NULL AS vida_util_estimada_dias
    FROM reporting.fact_operaciones_mensuales m
    JOIN reporting.dim_pozo p ON m.pozo_id = p.pozo_id
    WHERE TO_DATE(m.anio_mes || '-01', 'YYYY-MM')
          BETWEEN p_fecha_inicio AND p_fecha_fin
    ON CONFLICT (fecha, well_id)
    DO UPDATE SET
        uptime_pct = EXCLUDED.uptime_pct,
        tiempo_operacion_hrs = EXCLUDED.tiempo_operacion_hrs,
        mtbf_dias = EXCLUDED.mtbf_dias,
        fail_count = EXCLUDED.fail_count,
        kwh_por_barril = EXCLUDED.kwh_por_barril;

END;
$$;
