-- =========================================================================
-- PROCEDIMIENTO MAESTRO: CARGA A REPORTING (UNIFICADO)
-- Descripción: Orquestador unificado para procesamiento horario, diario y mensual
-- 
-- Proyecto: HRP Hydrog - Sprint 2
-- Cliente: HYDROG, INC.
-- Equipo: ITMEET GIA Team
-- Fecha: 2025-12-04
-- Versión: 2.0.0
-- =========================================================================

-- Este procedimiento almacenado permite ejecutar el proceso de carga a reporting
-- con diferentes granularidades y rangos de fechas configurables.

-- USO:
-- Ejecución diaria automática (solo diario, fecha actual):
--   CALL reporting.sp_load_to_reporting();
--
-- Procesar rango de fechas (solo diario):
--   CALL reporting.sp_load_to_reporting('2025-11-01'::DATE, '2025-11-30'::DATE);
--
-- Procesar con horario y diario:
--   CALL reporting.sp_load_to_reporting(
--       '2025-12-01'::DATE, 
--       '2025-12-01'::DATE,
--       TRUE,  -- procesar_horario
--       TRUE,  -- procesar_diario
--       FALSE  -- procesar_mensual
--   );
--
-- Procesar todo (hora, día, mes):
--   CALL reporting.sp_load_to_reporting(
--       '2025-11-01'::DATE,
--       '2025-11-30'::DATE,
--       TRUE, TRUE, TRUE
--   );

CREATE OR REPLACE PROCEDURE reporting.sp_load_to_reporting(
    p_fecha_inicio DATE DEFAULT NULL,
    p_fecha_fin DATE DEFAULT NULL,
    p_procesar_horario BOOLEAN DEFAULT TRUE,
    p_procesar_diario BOOLEAN DEFAULT TRUE,
    p_procesar_mensual BOOLEAN DEFAULT TRUE
)
LANGUAGE plpgsql
AS $$
DECLARE
    -- Variables de control local
    v_inicio DATE;
    v_fin DATE;
    v_fecha_actual DATE := CURRENT_DATE;
BEGIN
    -- =====================================================================
    -- 1. LÓGICA DE FECHAS DINÁMICAS
    -- =====================================================================
    -- Si los parámetros son NULL (ejecución automática), usamos CURRENT_DATE.
    -- Si se envían fechas (backfill manual), usamos esas.
    
    v_inicio := COALESCE(p_fecha_inicio, v_fecha_actual);
    v_fin := COALESCE(p_fecha_fin, v_fecha_actual);
    
    -- Validación básica
    IF v_inicio > v_fin THEN
        RAISE EXCEPTION 'fecha_inicio (%) no puede ser mayor que fecha_fin (%)', v_inicio, v_fin;
    END IF;
    
    RAISE NOTICE 'Iniciando Pipeline de Carga a Reporting';
    RAISE NOTICE 'Rango de fechas: % a %', v_inicio, v_fin;
    RAISE NOTICE 'Procesar Horario: %, Diario: %, Mensual: %', 
        p_procesar_horario, p_procesar_diario, p_procesar_mensual;
    
    -- =====================================================================
    -- 2. INICIALIZACIÓN DE DIMENSIONES
    -- =====================================================================
    
    RAISE NOTICE 'Inicializando dimensiones...';
    
    -- Poblar dim_tiempo para el rango necesario + margen
    PERFORM reporting.poblar_dim_tiempo(
        (v_inicio - INTERVAL '1 years')::DATE,
        (v_fin + INTERVAL '1 years')::DATE
    );
    
    -- Poblar dim_hora si no está poblada
    PERFORM reporting.poblar_dim_hora();
    
    -- Sincronizar dim_pozo desde stage
    INSERT INTO reporting.dim_pozo (
        pozo_id, nombre_pozo, cliente, pais, region, campo,
        api_number, coordenadas_pozo, tipo_pozo, tipo_levantamiento,
        profundidad_completacion_ft, diametro_embolo_bomba_in, 
        longitud_carrera_nominal_in, potencia_nominal_motor_hp, nombre_yacimiento
    )
    SELECT 
        well_id, nombre_pozo, cliente, pais, region, campo,
        api_number, coordenadas_pozo, tipo_pozo, tipo_levantamiento,
        profundidad_completacion, diametro_embolo_bomba, longitud_carrera_nominal,
        potencia_nominal_motor, nombre_yacimiento
    FROM stage.tbl_pozo_maestra
    ON CONFLICT (pozo_id) DO UPDATE SET
        nombre_pozo = EXCLUDED.nombre_pozo,
        pais = EXCLUDED.pais,
        campo = EXCLUDED.campo,
        tipo_levantamiento = EXCLUDED.tipo_levantamiento,
        potencia_nominal_motor_hp = EXCLUDED.potencia_nominal_motor_hp,
        fecha_ultima_actualizacion = CURRENT_TIMESTAMP;
    
    -- =====================================================================
    -- 3. PROCESAMIENTO HORARIO (Opcional)
    -- =====================================================================
    
    IF p_procesar_horario THEN
        RAISE NOTICE 'Procesando capa horaria...';
        
        -- Agrupación base por hora con deltas horarios
        WITH base_horaria AS (
            SELECT 
                p.well_id,
                DATE(p.timestamp_lectura) as fecha_real,
                EXTRACT(HOUR FROM p.timestamp_lectura)::INT as hora_real,
                DATE_TRUNC('hour', p.timestamp_lectura) as fecha_hora,
                
                -- Métricas Promedio/Snapshot
                AVG(p.spm_promedio) as spm,
                AVG(p.presion_cabezal) as whp,
                AVG(p.presion_casing) as chp,
                AVG(p.pip) as pip,
                AVG(p.temperatura_tanque_aceite) as temp_motor, -- Sustituto de temperatura_motor
                AVG(p.current_amperage) as amperaje,
                
                -- Acumuladores (MAX = valor al final de la hora)
                MAX(p.produccion_petroleo_diaria) as acum_oil,
                MAX(p.produccion_agua_diaria) as acum_water,
                MAX(p.produccion_gas_diaria) as acum_gas,
                MAX(p.produccion_fluido_diaria) as acum_fluido,
                
                -- Tiempos (en minutos)
                (MAX(p.horas_operacion_acumuladas) - COALESCE(MIN(p.horas_operacion_acumuladas), 0)) * 60.0 as run_min,
                MAX(p.conteo_poc_diario) as fallas_dia,
                
                BOOL_OR(p.estado_motor) as algun_momento_encendido,
                COUNT(*) as num_registros
                
            FROM stage.tbl_pozo_produccion p
            WHERE DATE(p.timestamp_lectura) BETWEEN v_inicio AND v_fin
            GROUP BY p.well_id, DATE(p.timestamp_lectura), EXTRACT(HOUR FROM p.timestamp_lectura), DATE_TRUNC('hour', p.timestamp_lectura)
        ),
        
        -- Cálculo de deltas horarios (maneja reinicios automáticamente)
        deltas_calculados AS (
            SELECT 
                b.*,
                
                -- Delta Petróleo (con manejo de reinicios)
                CASE 
                    WHEN b.hora_real = 0 THEN b.acum_oil
                    WHEN (b.acum_oil - LAG(b.acum_oil) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real)) < 0 
                        THEN b.acum_oil
                    ELSE COALESCE(b.acum_oil - LAG(b.acum_oil) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real), 0)
                END as delta_oil,
                
                -- Delta Agua
                CASE 
                    WHEN b.hora_real = 0 THEN b.acum_water
                    ELSE GREATEST(0, COALESCE(b.acum_water - LAG(b.acum_water) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real), 0))
                END as delta_water,
                
                -- Delta Gas
                CASE 
                    WHEN b.hora_real = 0 THEN b.acum_gas
                    ELSE GREATEST(0, COALESCE(b.acum_gas - LAG(b.acum_gas) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real), 0))
                END as delta_gas,
                
                -- Delta Fluido
                CASE 
                    WHEN b.hora_real = 0 THEN b.acum_fluido
                    ELSE GREATEST(0, COALESCE(b.acum_fluido - LAG(b.acum_fluido) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real), 0))
                END as delta_fluido,
                
                -- Fallas en esta hora
                CASE 
                    WHEN b.hora_real = 0 THEN b.fallas_dia
                    ELSE GREATEST(0, b.fallas_dia - COALESCE(LAG(b.fallas_dia) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real), 0))
                END as delta_fallas
                
            FROM base_horaria b
        )
        
        -- Inserción en fact_operaciones_horarias
        INSERT INTO reporting.FACT_OPERACIONES_HORARIAS (
            Fecha_ID, Hora_ID, Pozo_ID, Fecha_Hora,
            Prod_Petroleo_bbl, Prod_Agua_bbl, Prod_Gas_mcf, Prod_Acumulada_Dia_bbl,
            SPM_Promedio, Presion_Cabezal_psi, Presion_Casing_psi, PIP_psi,
            Temperatura_Motor_F, Amperaje_Motor_A,
            Tiempo_Operacion_min, Estado_Motor_Fin_Hora, Numero_Fallas_Hora
        )
        SELECT
            TO_CHAR(d.fecha_real, 'YYYYMMDD')::INT as Fecha_ID,
            d.hora_real as Hora_ID,
            d.well_id as Pozo_ID,
            d.fecha_hora as Fecha_Hora,
            
            ROUND(d.delta_oil::NUMERIC, 2) as Prod_Petroleo_bbl,
            ROUND(d.delta_water::NUMERIC, 2) as Prod_Agua_bbl,
            ROUND(d.delta_gas::NUMERIC, 2) as Prod_Gas_mcf,
            ROUND(d.acum_fluido::NUMERIC, 2) as Prod_Acumulada_Dia_bbl,
            
            ROUND(d.spm::NUMERIC, 2) as SPM_Promedio,
            ROUND(d.whp::NUMERIC, 2) as Presion_Cabezal_psi,
            ROUND(d.chp::NUMERIC, 2) as Presion_Casing_psi,
            ROUND(d.pip::NUMERIC, 2) as PIP_psi,
            ROUND(d.temp_motor::NUMERIC, 2) as Temperatura_Motor_F,
            ROUND(d.amperaje::NUMERIC, 2) as Amperaje_Motor_A,
            
            LEAST(ROUND(d.run_min::NUMERIC, 2), 60.0) as Tiempo_Operacion_min,
            d.algun_momento_encendido as Estado_Motor_Fin_Hora,
            d.delta_fallas::INT as Numero_Fallas_Hora
            
        FROM deltas_calculados d
        WHERE d.num_registros > 0
        
        ON CONFLICT (Fecha_ID, Hora_ID, Pozo_ID) 
        DO UPDATE SET
            Prod_Petroleo_bbl = EXCLUDED.Prod_Petroleo_bbl,
            Prod_Agua_bbl = EXCLUDED.Prod_Agua_bbl,
            Prod_Gas_mcf = EXCLUDED.Prod_Gas_mcf,
            Prod_Acumulada_Dia_bbl = EXCLUDED.Prod_Acumulada_Dia_bbl,
            SPM_Promedio = EXCLUDED.SPM_Promedio,
            Presion_Cabezal_psi = EXCLUDED.Presion_Cabezal_psi,
            Presion_Casing_psi = EXCLUDED.Presion_Casing_psi,
            PIP_psi = EXCLUDED.PIP_psi,
            Temperatura_Motor_F = EXCLUDED.Temperatura_Motor_F,
            Amperaje_Motor_A = EXCLUDED.Amperaje_Motor_A,
            Tiempo_Operacion_min = EXCLUDED.Tiempo_Operacion_min,
            Estado_Motor_Fin_Hora = EXCLUDED.Estado_Motor_Fin_Hora,
            Numero_Fallas_Hora = EXCLUDED.Numero_Fallas_Hora;
        
        RAISE NOTICE 'Capa horaria procesada exitosamente';
    END IF;
    
    -- =====================================================================
    -- 4. PROCESAMIENTO DIARIO
    -- =====================================================================
    
    IF p_procesar_diario THEN
        RAISE NOTICE 'Procesando capa diaria...';
        
        -- Ejecutar lógica de procesamiento diario
        -- (La lógica está en V2__stage_to_reporting_daily.sql)
        
        WITH datos_diarios AS (
            SELECT 
                p.well_id,
                DATE(p.timestamp_lectura) as fecha,
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
                COUNT(p.pip) as registros_validos
            FROM stage.tbl_pozo_produccion p
            WHERE DATE(p.timestamp_lectura) BETWEEN v_inicio AND v_fin
            GROUP BY p.well_id, DATE(p.timestamp_lectura)
        ),
        kpis_calculados AS (
            SELECT 
                dd.*,
                pd.diametro_embolo_bomba_in,
                pd.longitud_carrera_nominal_in,
                LEAST(dd.tiempo_op_raw, 24.0) as tiempo_op_clean,
                (0.000971 * POWER(pd.diametro_embolo_bomba_in, 2) * pd.longitud_carrera_nominal_in * dd.spm_promedio * 1440) as vol_teorico
            FROM datos_diarios dd
            LEFT JOIN reporting.dim_pozo pd ON dd.well_id = pd.pozo_id
        )
        INSERT INTO reporting.fact_operaciones_diarias (
            fecha_id, pozo_id, periodo_comparacion,
            produccion_fluido_bbl, produccion_petroleo_bbl, produccion_agua_bbl, produccion_gas_mcf,
            water_cut_pct, spm_promedio, spm_maximo, emboladas_totales,
            tiempo_operacion_hrs, tiempo_paro_noprog_hrs, consumo_energia_kwh, potencia_promedio_kw,
            presion_cabezal_psi, presion_casing_psi, pip_psi, carga_max_rod_lb, carga_min_rod_lb,
            llenado_bomba_pct, numero_fallas, flag_falla,
            volumen_teorico_bbl, kpi_efic_vol_pct, kpi_dop_pct, kpi_kwh_bbl, kpi_mtbf_hrs,
            kpi_uptime_pct, kpi_fill_efficiency_pct, completitud_datos_pct, calidad_datos_estado
        )
        SELECT
            TO_CHAR(k.fecha, 'YYYYMMDD')::INT, k.well_id, 'DIARIO',
            k.prod_fluido, k.prod_petroleo, k.prod_agua, k.prod_gas, k.water_cut,
            k.spm_promedio, k.spm_maximo, k.emboladas_totales, k.tiempo_op_clean, k.tiempo_paro_noprog,
            k.consumo_kwh, k.potencia_prom_kw, k.whp, k.chp, k.pip, k.rod_max, k.rod_min, k.pump_fill,
            k.fallas, k.flag_falla, k.vol_teorico,
            CASE WHEN k.vol_teorico > 0 THEN (k.prod_fluido / k.vol_teorico) * 100.0 ELSE 0 END,
            (k.tiempo_op_clean / 24.0) * 100.0,
            CASE WHEN k.prod_petroleo > 0 THEN k.consumo_kwh / k.prod_petroleo ELSE NULL END,
            CASE WHEN k.fallas > 0 THEN k.tiempo_op_clean / k.fallas ELSE NULL END,
            CASE WHEN (k.tiempo_op_clean + k.tiempo_paro_noprog) > 0 
                 THEN (k.tiempo_op_clean / (k.tiempo_op_clean + k.tiempo_paro_noprog)) * 100.0 
                 ELSE 0 END,
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
    END IF;
    
    -- =====================================================================
    -- 5. PROCESAMIENTO MENSUAL (Opcional)
    -- =====================================================================
    
    IF p_procesar_mensual THEN
        RAISE NOTICE 'Procesando capa mensual...';
        
        -- Agregación mensual desde fact_operaciones_diarias
        INSERT INTO reporting.FACT_OPERACIONES_MENSUALES (
            Anio_Mes, Pozo_ID, Total_Petroleo_bbl, Total_Agua_bbl, Total_Gas_mcf, Total_Fluido_bbl,
            Promedio_SPM, Promedio_WHP_psi, Promedio_CHP_psi, Promedio_Water_Cut_pct,
            Total_Fallas_Mes, Dias_Operando, Tiempo_Operacion_hrs, Tiempo_Paro_hrs,
            Eficiencia_Uptime_pct, Promedio_Efic_Vol_pct, Consumo_Energia_Total_kwh, KPI_KWH_BBL_Mes,
            Fecha_Ultima_Carga
        )
        SELECT
            dt.Anio_Mes, f.Pozo_ID,
            SUM(f.Produccion_Petroleo_bbl), SUM(f.Produccion_Agua_bbl), SUM(f.Produccion_Gas_mcf), SUM(f.Produccion_Fluido_bbl),
            AVG(f.SPM_Promedio), AVG(f.Presion_Cabezal_psi), AVG(f.Presion_Casing_psi), AVG(f.Water_Cut_pct),
            SUM(f.Numero_Fallas), COUNT(CASE WHEN f.Tiempo_Operacion_hrs > 0 THEN 1 END),
            SUM(f.Tiempo_Operacion_hrs), SUM(f.Tiempo_Paro_NoProg_hrs),
            CASE WHEN SUM(f.Tiempo_Operacion_hrs + f.Tiempo_Paro_NoProg_hrs) > 0 
                 THEN (SUM(f.Tiempo_Operacion_hrs) / SUM(f.Tiempo_Operacion_hrs + f.Tiempo_Paro_NoProg_hrs)) * 100.0 
                 ELSE 0 END,
            AVG(f.KPI_Efic_Vol_pct), SUM(f.Consumo_Energia_kwh),
            CASE WHEN SUM(f.Produccion_Petroleo_bbl) > 0 
                 THEN SUM(f.Consumo_Energia_kwh) / SUM(f.Produccion_Petroleo_bbl) 
                 ELSE 0 END,
            CURRENT_TIMESTAMP
        FROM reporting.FACT_OPERACIONES_DIARIAS f
        JOIN reporting.DIM_TIEMPO dt ON f.Fecha_ID = dt.Fecha_ID
        WHERE dt.Fecha BETWEEN v_inicio AND v_fin
        GROUP BY dt.Anio_Mes, f.Pozo_ID
        ON CONFLICT (Anio_Mes, Pozo_ID) DO UPDATE SET
            Total_Petroleo_bbl = EXCLUDED.Total_Petroleo_bbl,
            Total_Agua_bbl = EXCLUDED.Total_Agua_bbl,
            Eficiencia_Uptime_pct = EXCLUDED.Eficiencia_Uptime_pct,
            KPI_KWH_BBL_Mes = EXCLUDED.KPI_KWH_BBL_Mes,
            Fecha_Ultima_Carga = CURRENT_TIMESTAMP;
    END IF;
    
    RAISE NOTICE 'Pipeline de Carga a Reporting completado exitosamente.';
    
END;
$$;

COMMENT ON PROCEDURE reporting.sp_load_to_reporting IS 
'Procedimiento maestro para carga a reporting. 
Parámetros:
- p_fecha_inicio: Fecha inicio (NULL = CURRENT_DATE)
- p_fecha_fin: Fecha fin (NULL = CURRENT_DATE)
- p_procesar_horario: Procesar nivel horario (default: FALSE)
- p_procesar_diario: Procesar nivel diario (default: TRUE)
- p_procesar_mensual: Procesar nivel mensual (default: FALSE)';




