-- =========================================================================
-- ETL: STAGE → REPORTING - PROCESAMIENTO HORARIO
-- Transformación de datos horarios con cálculo de deltas desde acumuladores
-- 
-- Proyecto: HRP Hydrog - Sprint 2
-- Cliente: HYDROG, INC.
-- Equipo: ITMEET GIA Team
-- Fecha: 2025-12-04
-- Versión: 2.0.0
-- =========================================================================

-- Este script procesa datos horarios desde stage.tbl_pozo_produccion:
-- 1. Agrupa datos por (well_id, fecha, hora)
-- 2. Calcula deltas horarios usando Window Functions
-- 3. Maneja reinicios de medidores automáticamente
-- 4. Inserta en fact_operaciones_horarias

-- USO:
-- Ejecutar directamente con parámetros en DO block, o
-- Llamar desde procedimiento almacenado con parámetros

BEGIN;

-- =========================================================================
-- PASO 0: POBLAR DIMENSIONES NECESARIAS
-- =========================================================================

-- Poblar dim_hora si no está poblada
SELECT reporting.poblar_dim_hora();

-- Poblar dim_tiempo para el rango necesario (se ajustará según parámetros)
-- Por defecto, procesamos desde ayer hasta hoy
DO $$
DECLARE
    -- Parámetros configurables
    v_fecha_inicio DATE := COALESCE(
        (SELECT MIN(DATE(timestamp_lectura)) FROM stage.tbl_pozo_produccion 
         WHERE DATE(timestamp_lectura) >= CURRENT_DATE - INTERVAL '7 days'),
        CURRENT_DATE - INTERVAL '7 days'
    );
    v_fecha_fin DATE := COALESCE(
        (SELECT MAX(DATE(timestamp_lectura)) FROM stage.tbl_pozo_produccion),
        CURRENT_DATE
    );
BEGIN
    -- Poblar dimensión de tiempo para el rango necesario
    PERFORM reporting.poblar_dim_tiempo(v_fecha_inicio, v_fecha_fin);
    
    -- =========================================================================
    -- PASO 1: AGRUPACIÓN BASE POR HORA
    -- =========================================================================
    -- Obtenemos el estado "al final" de cada hora para cada pozo
    
    WITH base_horaria AS (
        SELECT 
            p.well_id,
            DATE(p.timestamp_lectura) as fecha_real,
            EXTRACT(HOUR FROM p.timestamp_lectura)::INT as hora_real,
            DATE_TRUNC('hour', p.timestamp_lectura) as fecha_hora,
            
            -- Métricas Promedio/Snapshot (valores representativos de la hora)
            AVG(p.spm_promedio) as spm,
            AVG(p.presion_cabezal) as whp,
            AVG(p.presion_casing) as chp,
            AVG(p.pip) as pip,
            -- Nota: temperatura_motor no está disponible en stage, usar temperatura_tanque_aceite como alternativa
            AVG(p.temperatura_tanque_aceite) as temp_motor,
            AVG(p.current_amperage) as amperaje,
            
            -- Acumuladores (Tomamos el MAX que representa el valor al final de la hora)
            MAX(p.produccion_petroleo_diaria) as acum_oil,
            MAX(p.produccion_agua_diaria) as acum_water,
            MAX(p.produccion_gas_diaria) as acum_gas,
            MAX(p.produccion_fluido_diaria) as acum_fluido,
            
            -- Tiempos (Delta dentro de la hora, en minutos)
            (MAX(p.horas_operacion_acumuladas) - COALESCE(MIN(p.horas_operacion_acumuladas), 0)) * 60.0 as run_min,
            MAX(p.conteo_poc_diario) as fallas_dia,
            
            -- Estado final en esa hora
            BOOL_OR(p.estado_motor) as algun_momento_encendido,
            
            -- Calidad de datos
            COUNT(*) as num_registros
            
        FROM stage.tbl_pozo_produccion p
        WHERE DATE(p.timestamp_lectura) BETWEEN v_fecha_inicio AND v_fecha_fin
        GROUP BY p.well_id, DATE(p.timestamp_lectura), EXTRACT(HOUR FROM p.timestamp_lectura), DATE_TRUNC('hour', p.timestamp_lectura)
    ),
    
    -- =========================================================================
    -- PASO 2: CÁLCULO DE DELTAS HORARIOS (Window Functions)
    -- =========================================================================
    -- La magia: Convertir acumulados diarios en deltas horarios
    -- Maneja automáticamente reinicios de medidores
    
    deltas_calculados AS (
        SELECT 
            b.*,
            
            -- Delta Petróleo: Acumulado Actual - Acumulado Hora Anterior
            -- Si es la hora 0 o el valor baja (reinicio), usamos el acumulado directo
            CASE 
                WHEN b.hora_real = 0 THEN b.acum_oil  -- Primera hora del día es el total acumulado
                WHEN (b.acum_oil - LAG(b.acum_oil) OVER (
                    PARTITION BY b.well_id, b.fecha_real 
                    ORDER BY b.hora_real
                )) < 0 THEN b.acum_oil  -- Reinicio detectado (medidor se reinició)
                ELSE COALESCE(
                    b.acum_oil - LAG(b.acum_oil) OVER (
                        PARTITION BY b.well_id, b.fecha_real 
                        ORDER BY b.hora_real
                    ), 
                    0
                )
            END as delta_oil,
            
            -- Delta Agua
            CASE 
                WHEN b.hora_real = 0 THEN b.acum_water
                ELSE GREATEST(0, COALESCE(
                    b.acum_water - LAG(b.acum_water) OVER (
                        PARTITION BY b.well_id, b.fecha_real 
                        ORDER BY b.hora_real
                    ), 
                    0
                ))
            END as delta_water,
            
            -- Delta Gas
            CASE 
                WHEN b.hora_real = 0 THEN b.acum_gas
                ELSE GREATEST(0, COALESCE(
                    b.acum_gas - LAG(b.acum_gas) OVER (
                        PARTITION BY b.well_id, b.fecha_real 
                        ORDER BY b.hora_real
                    ), 
                    0
                ))
            END as delta_gas,
            
            -- Delta Fluido (para Prod_Acumulada_Dia_bbl)
            CASE 
                WHEN b.hora_real = 0 THEN b.acum_fluido
                ELSE GREATEST(0, COALESCE(
                    b.acum_fluido - LAG(b.acum_fluido) OVER (
                        PARTITION BY b.well_id, b.fecha_real 
                        ORDER BY b.hora_real
                    ), 
                    0
                ))
            END as delta_fluido,
            
            -- Fallas en esta hora específica (delta de fallas)
            CASE 
                WHEN b.hora_real = 0 THEN b.fallas_dia
                ELSE GREATEST(0, 
                    b.fallas_dia - COALESCE(
                        LAG(b.fallas_dia) OVER (
                            PARTITION BY b.well_id, b.fecha_real 
                            ORDER BY b.hora_real
                        ),
                        0
                    )
                )
            END as delta_fallas
            
        FROM base_horaria b
    )
    
    -- =========================================================================
    -- PASO 3: INSERCIÓN EN FACT_OPERACIONES_HORARIAS
    -- =========================================================================
    
    INSERT INTO reporting.FACT_OPERACIONES_HORARIAS (
        Fecha_ID, 
        Hora_ID, 
        Pozo_ID, 
        Fecha_Hora,
        
        -- Deltas de Producción (lo producido en esta hora específica)
        Prod_Petroleo_bbl, 
        Prod_Agua_bbl, 
        Prod_Gas_mcf, 
        Prod_Acumulada_Dia_bbl,  -- Acumulado al final de la hora
        
        -- Dinámica Promedio (valores representativos de la hora)
        SPM_Promedio, 
        Presion_Cabezal_psi, 
        Presion_Casing_psi, 
        PIP_psi,
        Temperatura_Motor_F, 
        Amperaje_Motor_A,
        
        -- Tiempos y Estado
        Tiempo_Operacion_min, 
        Estado_Motor_Fin_Hora, 
        Numero_Fallas_Hora
    )
    SELECT
        TO_CHAR(d.fecha_real, 'YYYYMMDD')::INT as Fecha_ID,
        d.hora_real as Hora_ID,
        d.well_id as Pozo_ID,
        d.fecha_hora as Fecha_Hora,
        
        -- Métricas de producción (deltas horarios)
        ROUND(d.delta_oil::NUMERIC, 2) as Prod_Petroleo_bbl,
        ROUND(d.delta_water::NUMERIC, 2) as Prod_Agua_bbl,
        ROUND(d.delta_gas::NUMERIC, 2) as Prod_Gas_mcf,
        ROUND(d.acum_fluido::NUMERIC, 2) as Prod_Acumulada_Dia_bbl,  -- Acumulado total
        
        -- Dinámica promedio
        ROUND(d.spm::NUMERIC, 2) as SPM_Promedio,
        ROUND(d.whp::NUMERIC, 2) as Presion_Cabezal_psi,
        ROUND(d.chp::NUMERIC, 2) as Presion_Casing_psi,
        ROUND(d.pip::NUMERIC, 2) as PIP_psi,
        ROUND(d.temp_motor::NUMERIC, 2) as Temperatura_Motor_F,
        ROUND(d.amperaje::NUMERIC, 2) as Amperaje_Motor_A,
        
        -- Tiempos y estado
        LEAST(ROUND(d.run_min::NUMERIC, 2), 60.0) as Tiempo_Operacion_min,  -- Cap a 60 min por seguridad
        d.algun_momento_encendido as Estado_Motor_Fin_Hora,
        d.delta_fallas::INT as Numero_Fallas_Hora
        
    FROM deltas_calculados d
    WHERE d.num_registros > 0  -- Solo insertar si hay datos válidos
    
    -- UPSERT: Actualizar si ya existe (idempotencia)
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
    
    RAISE NOTICE 'Procesamiento horario completado para rango: % a %', v_fecha_inicio, v_fecha_fin;
    
END $$;

COMMIT;




