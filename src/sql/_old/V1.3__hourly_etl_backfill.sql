-- =========================================================================
-- ETL V1.3: INGESTA HORARIA + BACKFILL INTELIGENTE
-- Estrategia: Convertir acumulados diarios en deltas horarios.
-- =========================================================================

BEGIN;

DO $$
DECLARE
    -- Configura aquí el rango que quieres visualizar
    v_fecha_inicio DATE := '2025-11-18'; 
    v_fecha_fin    DATE := '2025-11-20'; -- Incluye hoy
BEGIN

    -- 1. CTE: Agregación Base (Snapshot por Hora)
    -- Obtenemos el estado "al final" de cada hora
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
            AVG(p.temperatura_motor) as temp_motor,
            AVG(p.current_amperage) as amperaje,
            
            -- Acumuladores (Tomamos el MAX que representa el valor al final de la hora)
            MAX(p.produccion_petroleo_diaria) as acum_oil,
            MAX(p.produccion_agua_diaria) as acum_water,
            MAX(p.produccion_gas_diaria) as acum_gas,
            
            -- Tiempos (Delta dentro de la hora)
            (MAX(p.horas_operacion_acumuladas) - MIN(p.horas_operacion_acumuladas)) * 60.0 as run_min,
            MAX(p.conteo_poc_diario) as fallas_dia,
            
            -- Estado final en esa hora
            BOOL_OR(p.estado_motor) as algun_momento_encendido
            
        FROM stage.tbl_pozo_produccion p
        WHERE DATE(p.timestamp_lectura) BETWEEN v_fecha_inicio AND v_fecha_fin
        GROUP BY 1, 2, 3, 4
    ),
    
    -- 2. CTE: Cálculo de Deltas (La Magia de Window Functions)
    deltas_calculados AS (
        SELECT 
            b.*,
            -- Delta Oil: Acumulado Actual - Acumulado Hora Anterior
            -- Si es la hora 0 o el valor baja (reinicio), usamos el acumulado directo.
            CASE 
                WHEN b.hora_real = 0 THEN b.acum_oil -- Primera hora del día es el total
                WHEN (b.acum_oil - LAG(b.acum_oil) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real)) < 0 THEN b.acum_oil -- Reinicio detectado
                ELSE COALESCE(b.acum_oil - LAG(b.acum_oil) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real), 0)
            END as delta_oil,
            
            CASE 
                WHEN b.hora_real = 0 THEN b.acum_water
                ELSE GREATEST(0, COALESCE(b.acum_water - LAG(b.acum_water) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real), 0))
            END as delta_water,
            
            CASE 
                WHEN b.hora_real = 0 THEN b.acum_gas
                ELSE GREATEST(0, COALESCE(b.acum_gas - LAG(b.acum_gas) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real), 0))
            END as delta_gas,

            -- Fallas en esta hora específica
            CASE 
                WHEN b.hora_real = 0 THEN b.fallas_dia
                ELSE GREATEST(0, b.fallas_dia - LAG(b.fallas_dia) OVER (PARTITION BY b.well_id, b.fecha_real ORDER BY b.hora_real))
            END as delta_fallas

        FROM base_horaria b
    )

    -- 3. Ingesta Final
    INSERT INTO reporting.FACT_OPERACIONES_HORARIAS (
        Fecha_ID, Hora_ID, Pozo_ID, Fecha_Hora,
        Prod_Petroleo_bbl, Prod_Agua_bbl, Prod_Gas_mcf, Prod_Acumulada_Dia_bbl,
        SPM_Promedio, Presion_Cabezal_psi, Presion_Casing_psi, PIP_psi,
        Temperatura_Motor_F, Amperaje_Motor_A,
        Tiempo_Operacion_min, Estado_Motor_Fin_Hora, Numero_Fallas_Hora
    )
    SELECT
        TO_CHAR(d.fecha_real, 'YYYYMMDD')::INT,
        d.hora_real,
        d.well_id,
        d.fecha_hora,
        
        -- Métricas limpias
        ROUND(d.delta_oil::NUMERIC, 2),
        ROUND(d.delta_water::NUMERIC, 2),
        ROUND(d.delta_gas::NUMERIC, 2),
        d.acum_oil, -- Guardamos también el acumulado para auditoría
        
        d.spm, d.whp, d.chp, d.pip,
        d.temp_motor, d.amperaje,
        LEAST(d.run_min, 60.0), -- Cap a 60 min por seguridad
        d.algun_momento_encendido,
        d.delta_fallas
        
    FROM deltas_calculados d
    ON CONFLICT (Fecha_ID, Hora_ID, Pozo_ID) 
    DO UPDATE SET
        Prod_Petroleo_bbl = EXCLUDED.Prod_Petroleo_bbl,
        SPM_Promedio = EXCLUDED.SPM_Promedio,
        Presion_Cabezal_psi = EXCLUDED.Presion_Cabezal_psi;
        
END $$;

COMMIT;