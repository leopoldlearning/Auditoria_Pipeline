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
--                                                    + fact_operaciones_horarias
--   2. sp_sync_ipr_to_reporting()  — IPR curvas     → dataset_current_values
--                                    IPR op.point  → ipr_eficiencia_flujo_pct
--                                                  + fact_operaciones_horarias
--   3. sp_sync_arps_to_reporting() — ARPS decline   → fact_operaciones_mensuales
--
-- FLUJO DE DATOS:
-- ┌─────────────────────────────────────────────────────────────────────┐
-- │  universal.patron ─┐                                               │
-- │  universal.stroke ──┼─→ sp_sync_cdi_to_reporting()                 │
-- │  universal.diagnostico                                             │
-- │        │                    ┌─→ reporting.dataset_current_values    │
-- │        └────────────────────┴─→ reporting.fact_operaciones_horarias │
-- │                                                                    │
-- │  universal.ipr_resultados ──→ sp_sync_ipr_to_reporting()           │
-- │  universal.ipr_puntos_operacion                                    │
-- │        │                    ┌─→ reporting.dataset_current_values    │
-- │        └────────────────────┘                                      │
-- │                                                                    │
-- │  universal.arps_resultados  ──→ sp_sync_arps_to_reporting()        │
-- │        │                    ┌─→ reporting.fact_operaciones_mensuales │
-- │        └────────────────────┘                                      │
-- └─────────────────────────────────────────────────────────────────────┘
--
-- DEPENDENCIAS (REQUIERE):
--   • universal schema V2  (patron, stroke, diagnostico, ipr_resultados,
--                           ipr_puntos_operacion, arps_resultados_declinacion)
--   • reporting schema V4  (dataset_current_values,
--                           fact_operaciones_horarias, fact_operaciones_mensuales)
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
--   fact_operaciones_horarias:
--     ipr_qmax_teorico, kpi_ai_accuracy_pct, kpi_ai_accuracy_status_*
--   fact_operaciones_mensuales:
--     remanent_reserves_bbl
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
    -- PASO 3: Actualizar fact_operaciones_horarias (fila correspondiente)
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
'Sincroniza diagnósticos CDI (dynacards) desde universal.stroke/diagnostico/patron hacia reporting.dataset_current_values y fact_operaciones_horarias.';


-- =============================================================================
-- SP 2: IPR → REPORTING
-- =============================================================================
-- Sincroniza curvas IPR y puntos de operación hacia reporting.
--
-- FUENTES:
--   • universal.ipr_resultados        — curvas DIA-24h / EVD (qmax, ip, curvas)
--   • universal.ipr_puntos_operacion  — puntos DIA-1h (eficiencia, q_actual, pwf)
--
-- CLASIFICACIÓN DE VARIABLES (READ / CALC / DERIVADO):
--   ipr_resultados:
--     qmax (CALC), ip (CALC), pwf_actual (CALC: Pe-q/IP), pip_actual (READ: SCADA PIP),
--     punto_operacion_bpd (CALC), curva_yacimiento (CALC), curva_bomba (CALC)
--   ipr_puntos_operacion:
--     q_actual (READ: SCADA bpd), pwf_calculado (CALC: Pe-q/IP),
--     ip_actual (DERIV: ip de curva), eficiencia (CALC: q/qmax*100)
--
-- CORRELACIÓN CON REPORTING:
--   • q_actual        = dataset_current_values.produccion_fluido_bpd_act (misma fuente SCADA)
--   • pip_actual      = dataset_current_values.pump_intake_pressure_psi_act (misma fuente SCADA)
--   • qmax            → dataset_current_values.ipr_qmax_bpd
--   • eficiencia      → dataset_current_values.ipr_eficiencia_flujo_pct
--                         PRIORIDAD: punto_operacion (DIA-1h, horario) > fallback curva (DIA-24h)
--
-- FLUJO EventBridge COMPATIBLE:
--   Rule 1 (5min): IPR Service DIA-24h → ipr_resultados
--   Rule 2 (1h):   IPR Service DIA-1h  → ipr_puntos_operacion
--   Rule 3 (5min): Reporting ETL        → CALL sp_sync_ipr_to_reporting()
--
-- Lógica:
--   PASO 1: Qmax desde ipr_resultados (curva más reciente por pozo)
--   PASO 2: Eficiencia — prefiere ipr_puntos_operacion (DIA-1h, más frecuente),
--           fallback a cálculo produccion_fluido_bpd_act / qmax
--   PASO 3: fact_operaciones_horarias — ipr_qmax_teorico incremental
-- =============================================================================

CREATE OR REPLACE PROCEDURE reporting.sp_sync_ipr_to_reporting()
LANGUAGE plpgsql AS $$
BEGIN
    -- ─────────────────────────────────────────────────────────────
    -- PASO 1: dataset_current_values — Qmax desde curva más reciente
    -- ─────────────────────────────────────────────────────────────
    WITH latest_ipr AS (
        SELECT DISTINCT ON (well_id)
            well_id,
            qmax,
            punto_operacion_bpd,
            fecha_calculo
        FROM universal.ipr_resultados
        ORDER BY well_id, fecha_calculo DESC
    )
    UPDATE reporting.dataset_current_values dcv
    SET
        ipr_qmax_bpd = li.qmax
    FROM latest_ipr li
    WHERE dcv.well_id = li.well_id;

    -- ─────────────────────────────────────────────────────────────
    -- PASO 1b: dataset_current_values — pwf_psi_act enriquecido
    --   V6 ya calcula pwf_psi_act = PIP + 0.433*(Hf-Hp) (READ)
    --   Si existe punto de operación DIA-1h (pwf_calculado, más
    --   preciso porque usa IP real del modelo), lo sobreescribe.
    --   Así pwf_psi_act NO depende de ipr_resultados directamente,
    --   pero se enriquece con el valor CALC si está disponible.
    -- ─────────────────────────────────────────────────────────────
    WITH latest_op_pwf AS (
        SELECT DISTINCT ON (well_id)
            well_id,
            pwf_calculado,
            fecha_calculo
        FROM universal.ipr_puntos_operacion
        ORDER BY well_id, fecha_calculo DESC
    )
    UPDATE reporting.dataset_current_values dcv
    SET
        pwf_psi_act = lop.pwf_calculado
    FROM latest_op_pwf lop
    WHERE dcv.well_id = lop.well_id
      AND lop.pwf_calculado IS NOT NULL;

    -- ─────────────────────────────────────────────────────────────
    -- PASO 2: dataset_current_values — Eficiencia flujo
    --   Prioridad: ipr_puntos_operacion.eficiencia (DIA-1h, horario)
    --   Fallback:  (produccion_fluido_bpd_act / qmax) * 100
    --
    --   q_actual (READ) es la misma lectura SCADA que ya vive en
    --   reporting como produccion_fluido_bpd_act — no se duplica,
    --   pero la eficiencia ya viene pre-calculada por el servicio.
    -- ─────────────────────────────────────────────────────────────
    WITH latest_op AS (
        SELECT DISTINCT ON (well_id)
            well_id,
            eficiencia,
            fecha_calculo
        FROM universal.ipr_puntos_operacion
        ORDER BY well_id, fecha_calculo DESC
    )
    UPDATE reporting.dataset_current_values dcv
    SET
        ipr_eficiencia_flujo_pct = COALESCE(
            -- Prioridad 1: eficiencia del punto de operación DIA-1h
            lop.eficiencia,
            -- Prioridad 2: cálculo desde producción actual / qmax
            CASE
                WHEN dcv.ipr_qmax_bpd > 0 AND dcv.produccion_fluido_bpd_act IS NOT NULL
                THEN LEAST((dcv.produccion_fluido_bpd_act / dcv.ipr_qmax_bpd) * 100, 100)
                ELSE dcv.ipr_eficiencia_flujo_pct
            END
        )
    FROM latest_op lop
    WHERE dcv.well_id = lop.well_id;

    -- ─────────────────────────────────────────────────────────────
    -- PASO 2b: Pozos SIN punto de operación — fallback solo curva
    -- ─────────────────────────────────────────────────────────────
    UPDATE reporting.dataset_current_values dcv
    SET
        ipr_eficiencia_flujo_pct = CASE
            WHEN dcv.ipr_qmax_bpd > 0 AND dcv.produccion_fluido_bpd_act IS NOT NULL
            THEN LEAST((dcv.produccion_fluido_bpd_act / dcv.ipr_qmax_bpd) * 100, 100)
            ELSE dcv.ipr_eficiencia_flujo_pct
        END
    WHERE dcv.ipr_eficiencia_flujo_pct IS NULL
      AND dcv.ipr_qmax_bpd IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM universal.ipr_puntos_operacion op
          WHERE op.well_id = dcv.well_id
      );

    -- ─────────────────────────────────────────────────────────────
    -- PASO 3: fact_operaciones_horarias — ipr_qmax_teorico
    -- ─────────────────────────────────────────────────────────────
    -- Actualiza TODAS las filas horarias del pozo con el Qmax vigente
    -- al momento de cada lectura, usando el IPR calculado más cercano
    -- anterior o igual al timestamp de la fila horaria.
    UPDATE reporting.fact_operaciones_horarias fh
    SET ipr_qmax_teorico = sub.qmax
    FROM (
        SELECT DISTINCT ON (fh2.pozo_id, fh2.fecha_hora)
            fh2.pozo_id,
            fh2.fecha_hora,
            ipr.qmax
        FROM reporting.fact_operaciones_horarias fh2
        JOIN universal.ipr_resultados ipr 
            ON ipr.well_id = fh2.pozo_id
           AND ipr.fecha_calculo <= fh2.fecha_hora
        WHERE fh2.ipr_qmax_teorico IS NULL  -- solo filas sin valor (incremental)
        ORDER BY fh2.pozo_id, fh2.fecha_hora, ipr.fecha_calculo DESC
    ) sub
    WHERE fh.pozo_id = sub.pozo_id
      AND fh.fecha_hora = sub.fecha_hora;

    RAISE NOTICE '[IPR→REPORTING] Sincronización completada (curvas + puntos operación).';
END;
$$;

COMMENT ON PROCEDURE reporting.sp_sync_ipr_to_reporting() IS
'Sincroniza IPR hacia reporting: Qmax desde ipr_resultados (DIA-24h/EVD), eficiencia desde ipr_puntos_operacion (DIA-1h, prioridad) o fallback produccion_bpd/qmax. También actualiza fact_operaciones_horarias.';


-- =============================================================================
-- SP 3: ARPS → REPORTING
-- =============================================================================
-- Sincroniza declinación hacia remanent_reserves_bbl en fact_operaciones_mensuales.
--
-- NOTA: eur_remanente_bbl y vida_util_estimada_dias fueron removidos de
--       dataset_kpi_business en la estandarización V7. El ARPS bridge ahora
--       solo actualiza fact_mensuales.
--
-- FLUJO EventBridge:
--   Rule (mensual): Declinación Service → universal.arps_resultados_declinacion
--     → CALL sp_sync_arps_to_reporting() (refresca RR en mensuales)
--
-- Prioridad: eur_p50 > eur_total
-- =============================================================================

CREATE OR REPLACE PROCEDURE reporting.sp_sync_arps_to_reporting()
LANGUAGE plpgsql AS $$
BEGIN
    -- ─────────────────────────────────────────────────────────────
    -- fact_operaciones_mensuales — remanent_reserves_bbl
    -- ─────────────────────────────────────────────────────────────
    WITH latest_arps AS (
        SELECT DISTINCT ON (well_id)
            well_id,
            eur_total,
            eur_p50,
            fecha_analisis
        FROM universal.arps_resultados_declinacion
        ORDER BY well_id, fecha_analisis DESC
    )
    UPDATE reporting.fact_operaciones_mensuales fom
    SET
        remanent_reserves_bbl = COALESCE(la.eur_p50, la.eur_total)
                                - COALESCE(fom.produccion_petroleo_acumulada_bbl, 0)
    FROM latest_arps la
    WHERE fom.pozo_id = la.well_id;

    RAISE NOTICE '[ARPS→REPORTING] Sincronización completada (fact_mensuales).';
END;
$$;

COMMENT ON PROCEDURE reporting.sp_sync_arps_to_reporting() IS
'Sincroniza ARPS hacia reporting: remanent_reserves_bbl = ARPS(eur_p50|eur_total) - Np en fact_mensuales. Invocado mensualmente por EventBridge.';
