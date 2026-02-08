-- =============================================================================
-- PROCEDIMIENTO: EJECUTAR VALIDACIÓN DQ (V4 Normalizado - Robusto + CAST)
-- LÓGICA: Cruza tbl_pozo_produccion con referencial.tbl_dq_rules.
--         Usa rango de fechas explícito con CAST para orquestación.
-- =============================================================================

CREATE OR REPLACE PROCEDURE stage.sp_execute_dq_validation(
    p_fecha_inicio DATE,
    p_fecha_fin DATE,
    p_well_id INT DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rule RECORD;
    v_sql_check TEXT;
    v_column_exists BOOLEAN;
BEGIN
    RAISE NOTICE 'Iniciando Motor de Calidad de Datos (DQ) para el rango % al %...', p_fecha_inicio, p_fecha_fin;

    -- Iterar sobre las reglas activas en Referencial
    FOR v_rule IN 
        SELECT r.regla_id, r.variable_id, r.valor_min, r.valor_max, v.nombre_tecnico
        FROM referencial.tbl_dq_rules r
        JOIN referencial.tbl_maestra_variables v ON r.variable_id = v.variable_id
    LOOP
        -- Verificar si la columna existe en tbl_pozo_produccion
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_schema = 'stage' 
            AND table_name = 'tbl_pozo_produccion' 
            AND column_name = v_rule.nombre_tecnico
        ) INTO v_column_exists;

        IF NOT v_column_exists THEN
            RAISE WARNING 'La columna % no existe en tbl_pozo_produccion. Saltando regla %.', v_rule.nombre_tecnico, v_rule.regla_id;
            CONTINUE;
        END IF;

        -- Construir insert dinámico
        v_sql_check := format(
            'INSERT INTO stage.tbl_pozo_scada_dq (produccion_id, timestamp_lectura, variable_id, regla_id, valor_observado, valor_esperado_min, valor_esperado_max, resultado_dq)
             SELECT 
                produccion_id, 
                timestamp_lectura, 
                %L::INT, 
                %L::INT, 
                %I::DECIMAL, 
                %L::DECIMAL, 
                %L::DECIMAL, 
                CASE 
                    WHEN %I < %L::DECIMAL OR %I > %L::DECIMAL THEN %L 
                    ELSE %L 
                END
             FROM stage.tbl_pozo_produccion
             WHERE timestamp_lectura::DATE BETWEEN %L::DATE AND %L::DATE
             AND (%I IS NOT NULL)',
             v_rule.variable_id, v_rule.regla_id, v_rule.nombre_tecnico, v_rule.valor_min, v_rule.valor_max,
             v_rule.nombre_tecnico, v_rule.valor_min, v_rule.nombre_tecnico, v_rule.valor_max, 'FAIL', 'PASS',
             p_fecha_inicio::TEXT, p_fecha_fin::TEXT, v_rule.nombre_tecnico
        );

        IF p_well_id IS NOT NULL THEN
            v_sql_check := v_sql_check || format(' AND well_id = %L', p_well_id);
        END IF;

        v_sql_check := v_sql_check || ' ON CONFLICT DO NOTHING';

        EXECUTE v_sql_check;
        
        RAISE NOTICE 'Regla % ejecutada para variable %', v_rule.regla_id, v_rule.nombre_tecnico;
    END LOOP;

    RAISE NOTICE 'Validación DQ completada exitosamente.';
END;
$$;
