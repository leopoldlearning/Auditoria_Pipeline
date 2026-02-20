-- =============================================================================
-- V10__universal_to_reporting_bridge.sql
-- VERSION: 1.0.0
-- FECHA:   2026-02-16
-- =============================================================================
--
-- DESCRIPCIÓN:
--   Puente entre el esquema UNIVERSAL (resultados ML/análisis avanzado) y el
--   esquema REPORTING (dashboard/BI). Tres stored procedures que sincronizan:
--
--   1. sp_sync_cdi_to_reporting()  — CDI dynacards → dataset_current_values
--                                                    + dataset_latest_dynacard
--                                                    + fact_operaciones_horarias
--   2. sp_sync_ipr_to_reporting()  — IPR curvas     → dataset_current_values
--                                                    + fact_operaciones_horarias
--   3. sp_sync_arps_to_reporting() — ARPS decline   → dataset_kpi_business
--
-- FLUJO DE DATOS:
-- ┌─────────────────────────────────────────────────────────────────────┐
-- │  universal.patron ─┐                                               │
-- │  universal.stroke ──┼─→ sp_sync_cdi_to_reporting()                 │
-- │  universal.diagnostico                                             │
-- │        │                    ┌─→ reporting.dataset_current_values    │
-- │        └────────────────────┼─→ reporting.dataset_latest_dynacard  │
-- │                             └─→ reporting.fact_operaciones_horarias │
-- │                                                                    │
-- │  universal.ipr_resultados ──→ sp_sync_ipr_to_reporting()           │
-- │        │                    ┌─→ reporting.dataset_current_values    │
-- │        └────────────────────┘                                      │
-- │                                                                    │
-- │  universal.arps_resultados  ──→ sp_sync_arps_to_reporting()        │
-- │        │                    ┌─→ reporting.dataset_kpi_business      │
-- │        └────────────────────┘                                      │
-- └─────────────────────────────────────────────────────────────────────┘
--
-- DEPENDENCIAS (REQUIERE):
--   • universal schema V2  (patron, stroke, diagnostico, ipr, arps)
--   • reporting schema V4  (dataset_current_values, dataset_latest_dynacard,
--                           fact_operaciones_horarias, dataset_kpi_business)
--   • referencial.fnc_evaluar_variable()  (V7 — clasificación semáforo)
--
-- INVOCACIÓN:
--   Se deben llamar DESPUÉS de que los módulos ML/Python escriban en universal.
--   Ejemplo en MASTER_PIPELINE_RUNNER.py (futuro):
--     CALL reporting.sp_sync_cdi_to_reporting();
--     CALL reporting.sp_sync_ipr_to_reporting();
--     CALL reporting.sp_sync_arps_to_reporting();
--
-- COLUMNAS REPORTING DESTINO (ya existen en el DDL actual):
--   dataset_current_values:
--     ai_accuracy_act, ai_accuracy_status_*, ai_accuracy_severity_label,
--     ai_accuracy_target, ai_accuracy_variance_pct,
--     ipr_qmax_bpd, ipr_eficiencia_flujo_pct
--   dataset_latest_dynacard:
--     diagnostico_ia, superficie_json, fondo_json, carga_min/max_superficie
--   fact_operaciones_horarias:
--     ipr_qmax_teorico, kpi_ai_accuracy_pct, kpi_ai_accuracy_status_*
--   dataset_kpi_business:
--     eur_remanente_bbl, kpi_ai_accuracy_*
-- =============================================================================


-- =============================================================================
-- SP 1: CDI → REPORTING
-- =============================================================================
-- Sincroniza el diagnóstico más reciente de cada pozo desde el subsistema CDI
-- (patron → stroke → diagnostico) hacia las tablas de reporting.
--
-- Lógica:
--   1. Para cada pozo, toma el stroke más reciente con estado 'COMPLETADO'
--   2. Del ese stroke, toma el diagnóstico con mayor score (top-1 patrón)
--   3. Calcula ai_accuracy_pct comparando score ML vs validación experta
--      (si existe validación) o usa score directo como confidence
--   4. Actualiza dataset_current_values con el resultado + semáforo
--   5. Actualiza dataset_latest_dynacard con el patrón detectado
--   6. Actualiza fact_operaciones_horarias con el accuracy de la hora
-- =============================================================================

CREATE OR REPLACE PROCEDURE reporting.sp_sync_cdi_to_reporting()
LANGUAGE plpgsql AS $$
DECLARE
    v_eval RECORD;
BEGIN
    -- ─────────────────────────────────────────────────────────────
    -- PASO 1: Obtener diagnóstico top-1 más reciente por pozo
    -- ─────────────────────────────────────────────────────────────
    -- CTE que reúne: well_id, timestamp, patrón ganador, score, 
    -- y opcionalmente la validación experta para calcular accuracy
    WITH latest_stroke AS (
        SELECT DISTINCT ON (pp.well_id)
            pp.well_id,
            pp.timestamp_lectura,
            s.stroke_id,
            s.estado
        FROM universal.stroke s
        JOIN stage.tbl_pozo_produccion pp ON pp.produccion_id = s.produccion_id
        WHERE s.estado = 'COMPLETADO'
        ORDER BY pp.well_id, pp.timestamp_lectura DESC
    ),
    top_diagnostico AS (
        SELECT DISTINCT ON (ls.well_id)
            ls.well_id,
            ls.timestamp_lectura,
            ls.stroke_id,
            d.patron_id,
            p.nombre       AS pattern_name,
            p.criticidad   AS pattern_criticality,
            d.score        AS ml_score
        FROM latest_stroke ls
        JOIN universal.diagnostico d ON d.stroke_id = ls.stroke_id
        JOIN universal.patron p      ON p.patron_id = d.patron_id
        ORDER BY ls.well_id, d.score DESC
    ),
    with_validation AS (
        SELECT
            td.*,
            -- Si hay validación experta, accuracy = 1.0 si coincide patrón, 
            -- 0.0 si difiere. Si no hay validación, usar ml_score como proxy.
            COALESCE(
                CASE WHEN ve.patron_id = td.patron_id THEN 1.0 ELSE 0.0 END,
                td.ml_score::NUMERIC
            ) AS ai_accuracy_pct
        FROM top_diagnostico td
        LEFT JOIN LATERAL (
            SELECT patron_id
            FROM universal.validacion_experta
            WHERE stroke_id = td.stroke_id
            ORDER BY updated_at DESC
            LIMIT 1
        ) ve ON true
    )

    -- ─────────────────────────────────────────────────────────────
    -- PASO 2: Actualizar dataset_current_values
    -- ─────────────────────────────────────────────────────────────
    UPDATE reporting.dataset_current_values dcv
    SET
        ai_accuracy_act            = wv.ai_accuracy_pct * 100,
        ai_accuracy_variance_pct   = CASE 
            WHEN dcv.ai_accuracy_target > 0 
            THEN ((wv.ai_accuracy_pct * 100) - dcv.ai_accuracy_target) / dcv.ai_accuracy_target * 100
            ELSE 0 
        END,
        ai_accuracy_status_color   = ev.color_hex,
        ai_accuracy_status_level   = ev.status_nivel,
        ai_accuracy_status_label   = ev.status_label,
        ai_accuracy_severity_label = ev.severity_label
    FROM with_validation wv
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable(
        'AI_ACCURACY'::TEXT,
        wv.ai_accuracy_pct * 100,
        dcv.ai_accuracy_target,
        NULL::NUMERIC,            -- sin baseline
        'HIGHER_BETTER'::TEXT
    ) ev
    WHERE dcv.well_id = wv.well_id;

    -- ─────────────────────────────────────────────────────────────
    -- PASO 3: Actualizar dataset_latest_dynacard
    -- ─────────────────────────────────────────────────────────────
    INSERT INTO reporting.dataset_latest_dynacard (
        well_id, timestamp_carta, diagnostico_ia, updated_at
    )
    SELECT
        td.well_id,
        td.timestamp_lectura,
        td.pattern_name || ' (' || ROUND(td.ml_score * 100, 1) || '% - ' || td.pattern_criticality || ')',
        now()
    FROM top_diagnostico td
    ON CONFLICT (well_id) DO UPDATE SET
        timestamp_carta = EXCLUDED.timestamp_carta,
        diagnostico_ia  = EXCLUDED.diagnostico_ia,
        updated_at      = EXCLUDED.updated_at;

    -- ─────────────────────────────────────────────────────────────
    -- PASO 4: Actualizar fact_operaciones_horarias (fila correspondiente)
    -- ─────────────────────────────────────────────────────────────
    UPDATE reporting.fact_operaciones_horarias fh
    SET
        kpi_ai_accuracy_pct          = wv.ai_accuracy_pct * 100,
        kpi_ai_accuracy_variance_pct = CASE 
            WHEN fh.kpi_ai_accuracy_target > 0 
            THEN ((wv.ai_accuracy_pct * 100) - fh.kpi_ai_accuracy_target) / fh.kpi_ai_accuracy_target * 100
            ELSE 0 
        END,
        kpi_ai_accuracy_status_color   = ev.color_hex,
        kpi_ai_accuracy_status_level   = ev.status_nivel,
        kpi_ai_accuracy_severity_label = ev.severity_label
    FROM with_validation wv
    CROSS JOIN LATERAL referencial.fnc_evaluar_variable(
        'AI_ACCURACY'::TEXT,
        wv.ai_accuracy_pct * 100,
        fh.kpi_ai_accuracy_target,
        fh.kpi_ai_accuracy_baseline,
        'HIGHER_BETTER'::TEXT
    ) ev
    WHERE fh.well_id = wv.well_id
      AND fh.timestamp_lectura = wv.timestamp_lectura;

    RAISE NOTICE '[CDI→REPORTING] Sincronización completada.';
END;
$$;

COMMENT ON PROCEDURE reporting.sp_sync_cdi_to_reporting() IS
'Sincroniza diagnósticos CDI (dynacards) desde universal.stroke/diagnostico/patron hacia reporting.dataset_current_values, dataset_latest_dynacard y fact_operaciones_horarias.';


-- =============================================================================
-- SP 2: IPR → REPORTING
-- =============================================================================
-- Sincroniza el cálculo IPR más reciente de cada pozo hacia reporting.
--
-- Lógica:
--   1. Para cada pozo, toma el ipr_resultados más reciente
--   2. Actualiza dataset_current_values.ipr_qmax_bpd + ipr_eficiencia_flujo_pct
--   3. Actualiza fact_operaciones_horarias.ipr_qmax_teorico (fila más reciente)
-- =============================================================================

CREATE OR REPLACE PROCEDURE reporting.sp_sync_ipr_to_reporting()
LANGUAGE plpgsql AS $$
BEGIN
    -- ─────────────────────────────────────────────────────────────
    -- PASO 1: dataset_current_values — IPR más reciente por pozo
    -- ─────────────────────────────────────────────────────────────
    WITH latest_ipr AS (
        SELECT DISTINCT ON (well_id)
            well_id,
            qmax_bpd,
            punto_operacion_bpd,
            fecha_calculo
        FROM universal.ipr_resultados
        ORDER BY well_id, fecha_calculo DESC
    )
    UPDATE reporting.dataset_current_values dcv
    SET
        ipr_qmax_bpd           = li.qmax_bpd,
        ipr_eficiencia_flujo_pct = CASE 
            WHEN li.qmax_bpd > 0 AND dcv.bfpd_act IS NOT NULL
            THEN LEAST((dcv.bfpd_act / li.qmax_bpd) * 100, 100)
            ELSE dcv.ipr_eficiencia_flujo_pct
        END
    FROM latest_ipr li
    WHERE dcv.well_id = li.well_id;

    -- ─────────────────────────────────────────────────────────────
    -- PASO 2: fact_operaciones_horarias — ipr_qmax_teorico
    -- ─────────────────────────────────────────────────────────────
    -- Actualiza TODAS las filas horarias del pozo con el Qmax vigente
    -- al momento de cada lectura, usando el IPR calculado más cercano
    -- anterior o igual al timestamp de la fila horaria.
    UPDATE reporting.fact_operaciones_horarias fh
    SET ipr_qmax_teorico = sub.qmax_bpd
    FROM (
        SELECT DISTINCT ON (fh2.well_id, fh2.timestamp_lectura)
            fh2.well_id,
            fh2.timestamp_lectura,
            ipr.qmax_bpd
        FROM reporting.fact_operaciones_horarias fh2
        JOIN universal.ipr_resultados ipr 
            ON ipr.well_id = fh2.well_id
           AND ipr.fecha_calculo <= fh2.timestamp_lectura
        WHERE fh2.ipr_qmax_teorico IS NULL  -- solo filas sin valor (incremental)
        ORDER BY fh2.well_id, fh2.timestamp_lectura, ipr.fecha_calculo DESC
    ) sub
    WHERE fh.well_id = sub.well_id
      AND fh.timestamp_lectura = sub.timestamp_lectura;

    RAISE NOTICE '[IPR→REPORTING] Sincronización completada.';
END;
$$;

COMMENT ON PROCEDURE reporting.sp_sync_ipr_to_reporting() IS
'Sincroniza resultados IPR más recientes desde universal.ipr_resultados hacia reporting.dataset_current_values (Qmax, eficiencia flujo) y fact_operaciones_horarias (ipr_qmax_teorico).';


-- =============================================================================
-- SP 3: ARPS → REPORTING
-- =============================================================================
-- Sincroniza el pronóstico de declinación más reciente hacia dataset_kpi_business.
--
-- Lógica:
--   1. Para cada pozo, toma el arps_resultados_declinacion más reciente
--   2. Actualiza dataset_kpi_business.eur_remanente_bbl con EUR P50
-- =============================================================================

CREATE OR REPLACE PROCEDURE reporting.sp_sync_arps_to_reporting()
LANGUAGE plpgsql AS $$
BEGIN
    -- ─────────────────────────────────────────────────────────────
    -- dataset_kpi_business — EUR remanente desde ARPS (P50)
    -- ─────────────────────────────────────────────────────────────
    WITH latest_arps AS (
        SELECT DISTINCT ON (well_id)
            well_id,
            eur_bbl,
            eur_p50,
            pronostico_30d_bpd,
            pronostico_90d_bpd,
            fecha_analisis
        FROM universal.arps_resultados_declinacion
        ORDER BY well_id, fecha_analisis DESC
    )
    UPDATE reporting.dataset_kpi_business dkb
    SET
        eur_remanente_bbl = COALESCE(la.eur_p50, la.eur_bbl)
    FROM latest_arps la
    WHERE dkb.well_id = la.well_id;

    RAISE NOTICE '[ARPS→REPORTING] Sincronización completada.';
END;
$$;

COMMENT ON PROCEDURE reporting.sp_sync_arps_to_reporting() IS
'Sincroniza EUR remanente desde universal.arps_resultados_declinacion (P50) hacia reporting.dataset_kpi_business.';
