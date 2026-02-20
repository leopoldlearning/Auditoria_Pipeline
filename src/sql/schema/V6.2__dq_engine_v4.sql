/*
--------------------------------------------------------------------------------
-- MOTOR DE CALIDAD DE DATOS V6.2 (V4 COMPATIBLE) — Tier 2B
-- Migrado de V5 para independencia del stack legacy.
-- Valida reglas definidas en referencial.tbl_dq_rules contra stage.tbl_pozo_produccion
-- Fix: valor_max NULL-safe + sp_execute_consistency_validation
--------------------------------------------------------------------------------
*/

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
    v_col_name TEXT;          -- columna real en stage (traducida vía SCADA map)
BEGIN
    RAISE NOTICE 'Iniciando Motor de Calidad de Datos (DQ) V6.2 + SCADA Map...';

    FOR v_rule IN 
        SELECT 
            r.regla_id, r.variable_id, r.valor_min, r.valor_max,
            v.nombre_tecnico,
            -- Traducción: preferir columna_stage del SCADA map, fallback a nombre_tecnico
            COALESCE(vsm.columna_stage, v.nombre_tecnico) AS columna_stage_real
        FROM referencial.tbl_dq_rules r
        JOIN referencial.tbl_maestra_variables v ON r.variable_id = v.variable_id
        LEFT JOIN referencial.tbl_var_scada_map vsm ON vsm.id_formato1 = v.id_formato1
    LOOP
        v_col_name := v_rule.columna_stage_real;

        -- Validar existencia y tipo de columna en Stage (Solo numéricas)
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_schema = 'stage' 
              AND table_name = 'tbl_pozo_produccion' 
              AND column_name = v_col_name
              AND data_type IN ('numeric', 'integer', 'decimal', 'double precision', 'real', 'bigint', 'smallint')
        ) INTO v_column_exists;

        IF NOT v_column_exists THEN
            CONTINUE;
        END IF;

        -- NULL-safe: si valor_max es NULL, solo valida valor_min
        -- Si valor_min es NULL, solo valida valor_max
        v_sql_check := format(
            'INSERT INTO stage.tbl_pozo_scada_dq (
                produccion_id, timestamp_lectura, variable_id, regla_id,
                valor_observado, valor_esperado_min, valor_esperado_max, resultado_dq
            )
             SELECT 
                produccion_id, 
                timestamp_lectura, 
                %L::INT, 
                %L::INT, 
                %I::DECIMAL, 
                %L::DECIMAL, 
                %L::DECIMAL, 
                CASE 
                    WHEN %L::DECIMAL IS NOT NULL AND %I::DECIMAL < %L::DECIMAL THEN %L
                    WHEN %L::DECIMAL IS NOT NULL AND %I::DECIMAL > %L::DECIMAL THEN %L
                    ELSE %L 
                END
             FROM stage.tbl_pozo_produccion
             WHERE timestamp_lectura::DATE BETWEEN %L::DATE AND %L::DATE
               AND (%I::DECIMAL IS NOT NULL)',
             v_rule.variable_id, v_rule.regla_id, v_col_name,
             v_rule.valor_min, v_rule.valor_max,
             -- min check
             v_rule.valor_min, v_col_name, v_rule.valor_min, 'FAIL',
             -- max check
             v_rule.valor_max, v_col_name, v_rule.valor_max, 'FAIL',
             'PASS',
             p_fecha_inicio::TEXT, p_fecha_fin::TEXT,
             v_col_name
        );

        IF p_well_id IS NOT NULL THEN
            v_sql_check := v_sql_check || format(' AND well_id = %L', p_well_id);
        END IF;

        v_sql_check := v_sql_check || ' ON CONFLICT DO NOTHING';

        EXECUTE v_sql_check;
    END LOOP;

    RAISE NOTICE 'Validación DQ completada.';
END;
$$;


-- =============================================================================
-- MOTOR DE REGLAS DE CONSISTENCIA (RC-001..RC-006)
-- Evalúa tbl_reglas_consistencia contra dataset_current_values
-- =============================================================================
CREATE OR REPLACE PROCEDURE stage.sp_execute_consistency_validation()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rc RECORD;
    v_sql TEXT;
    v_count INT := 0;
    v_fail_count INT := 0;
BEGIN
    RAISE NOTICE '[DQ-RC] Iniciando validación de reglas de consistencia...';

    FOR v_rc IN 
        SELECT 
            rc.regla_id,
            rc.codigo_regla,
            rc.nombre_regla,
            rc.operador_comparacion,
            rc.severidad,
            v_med.nombre_tecnico AS col_medida,
            v_ref.nombre_tecnico AS col_referencia
        FROM referencial.tbl_reglas_consistencia rc
        LEFT JOIN referencial.tbl_maestra_variables v_med ON rc.variable_medida_id = v_med.variable_id
        LEFT JOIN referencial.tbl_maestra_variables v_ref ON rc.variable_referencia_id = v_ref.variable_id
        WHERE v_med.nombre_tecnico IS NOT NULL 
          AND v_ref.nombre_tecnico IS NOT NULL
    LOOP
        v_count := v_count + 1;

        -- Verificar que ambas columnas existen en dataset_current_values
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'reporting' AND table_name = 'dataset_current_values' 
              AND column_name = v_rc.col_medida
        ) OR NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'reporting' AND table_name = 'dataset_current_values' 
              AND column_name = v_rc.col_referencia
        ) THEN
            RAISE NOTICE '[DQ-RC] Saltando %: columna % o % no existe en CV', 
                v_rc.codigo_regla, v_rc.col_medida, v_rc.col_referencia;
            CONTINUE;
        END IF;

        -- Evaluar: se espera col_medida <operador> col_referencia
        -- Si la evaluación es FALSE → inconsistencia detectada
        v_sql := format(
            'SELECT COUNT(*) FROM reporting.dataset_current_values 
             WHERE %I IS NOT NULL AND %I IS NOT NULL 
               AND NOT (%I %s %I)',
            v_rc.col_medida, v_rc.col_referencia,
            v_rc.col_medida, v_rc.operador_comparacion, v_rc.col_referencia
        );

        DECLARE v_violations INT;
        BEGIN
            EXECUTE v_sql INTO v_violations;
            IF v_violations > 0 THEN
                v_fail_count := v_fail_count + 1;
                RAISE NOTICE '[DQ-RC] ❌ % (%): % pozos con inconsistencia [%]', 
                    v_rc.codigo_regla, v_rc.nombre_regla, v_violations, v_rc.severidad;
            ELSE
                RAISE NOTICE '[DQ-RC] ✅ % (%): OK', v_rc.codigo_regla, v_rc.nombre_regla;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE '[DQ-RC] ⚠️ Error evaluando %: %', v_rc.codigo_regla, SQLERRM;
        END;
    END LOOP;

    RAISE NOTICE '[DQ-RC] Validación completada: % reglas evaluadas, % con violaciones', 
        v_count, v_fail_count;
END;
$$;

COMMENT ON PROCEDURE stage.sp_execute_consistency_validation IS
'Evalúa las 6 reglas de consistencia (RC-001..RC-006) definidas en tbl_reglas_consistencia
contra dataset_current_values. Reporta violaciones vía RAISE NOTICE.
ORDEN EN PIPELINE: después de sp_populate_defaults (paso 8 del MASTER).';

