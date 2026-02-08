-- 3. Insertar los resultados de Calidad de Datos (DQ)
--    basados en las filas que acabamos de insertar.
INSERT INTO tbl_pozo_scada_dq (
    scada_id,
    timestamp_lectura,
    regla_rango_spm,
    regla_no_cero_consumo,
    regla_tolerancia_whp,
    resultado_dq
)
SELECT
    i.scada_id,
    i.timestamp_lectura,
    
    -- =================================================================
    -- Lógica de Reglas DQ (Ejemplos)
    -- Aquí se une con tu tabla de referencia de reglas
    -- =================================================================
    
    -- Regla 1: SPM debe estar en un rango (Ejemplo, no mapeado)
    CASE
        WHEN i.pump_avg_spm < 1 OR i.pump_avg_spm > 20 THEN 1 -- 1 = Falla
        ELSE 0
    END AS regla_rango_spm,
    
    -- Regla 2: Consumo de motor no puede ser cero
    CASE
        WHEN i.current_hp_motor <= 0 THEN 1 -- 1 = Falla
        ELSE 0 -- 0 = Pasa
    END AS regla_no_cero_consumo,
    
    -- Regla 3: WHP debe estar dentro de un rango
    -- (Usando la regla de 'datos_muestra.sql')
    CASE 
        WHEN i.whp_psi < dq_whp.min_val OR i.whp_psi > dq_whp.max_val THEN 1
        ELSE 0 
    END AS regla_tolerancia_whp,
    
    -- Resultado final de DQ
    CASE
        WHEN (CASE WHEN i.pump_avg_spm < 1 OR i.pump_avg_spm > 20 THEN 1 ELSE 0 END) = 1
          OR (CASE WHEN i.current_hp_motor <= 0 THEN 1 ELSE 0 END) = 1
          OR (CASE WHEN i.whp_psi < dq_whp.min_val OR i.whp_psi > dq_whp.max_val THEN 1 ELSE 0 END) = 1
        THEN 'FAIL'
        ELSE 'PASS'
    END AS resultado_dq

FROM
    InsertedSCADA i
-- Unir con la tabla de reglas de DQ
LEFT JOIN
    tbl_dq_rules dq_whp ON dq_whp.variable_name = 'whp_psi'
;

-- 4. Limpiar la tabla de aterrizaje para el próximo ciclo
TRUNCATE TABLE landing_scada_data;

-- Confirmar la transacción
COMMIT;