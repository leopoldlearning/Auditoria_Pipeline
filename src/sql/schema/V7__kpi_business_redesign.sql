/*
================================================================================
V7 - REDISEÑO dataset_kpi_business (WIDE) + PREFIJO kpi_
================================================================================
Fecha: 2026-02-12
Propósito: Tabla WIDE consolidada para BI con 3 períodos por KPI en columnas,
           prefijo kpi_ en métricas, semáforos vs current_value.
           Diseño Zero-Calc para dashboard (frontend consume sin JOINs).
================================================================================

LAYOUT DE NOMENCLATURA ESTANDARIZADO:
─────────────────────────────────────
{base}_{period}              → Valor del período
  Períodos: _current (snapshot), _dia (diario), _mes (mensual)
{base}_target                → Objetivo/meta
{base}_baseline              → Referencia histórica
{base}_variance_pct          → ((val - target) / target) * 100
{base}_status_color          → HEX (#00CC66, #FFBB33, #FF4444, #B0B0B0)
{base}_status_level          → 0=OK, 1=WARNING, 2=CRITICAL, 3=NO_DATA
{base}_severity_label        → 'EXCELENTE','NORMAL','ALERTA','CRÍTICO','SIN DATOS'

BASES ESTANDARIZADAS:
─────────────────────────────────────
1. kpi_mtbf_hrs   (Mean Time Between Failures – Horas)
   + kpi_mtbf_days (= hrs/24, conversión para períodos largos)
2. kpi_uptime_pct (Disponibilidad – Porcentaje)
3. kpi_kwh_bbl    (Eficiencia Energética – kWh/barril)
4. kpi_vol_eff_pct(Eficiencia Volumétrica – Porcentaje)
5. ai_accuracy    (Precisión IA – Porcentaje)

DISEÑO WIDE (1 fila = 1 fecha × 1 pozo):
─────────────────────────────────────
  PK: (fecha, well_id)
  Cada fila contiene _current, _dia, _mes → NO columna periodo.
  Semáforos en fila de hoy = copiados de dataset_current_values.
  Semáforos en filas históricas = computados sobre _dia vs target.
  Horario: NO semáforos, solo raw KPIs en fact_operaciones_horarias.

COLUMNAS DESCARTADAS (simplificación):
─────────────────────────────────────
  fail_count, tiempo_operacion_hrs, tiempo_paro_hrs,
  costo_energia_usd, lifting_cost_usd_bbl,
  eur_remanente_bbl, produccion_acumulada_bbl, vida_util_estimada_dias
================================================================================
*/

-- =============================================================================
-- 1. COLUMNAS ADICIONALES EN dataset_current_values
-- =============================================================================
ALTER TABLE reporting.dataset_current_values
    ADD COLUMN IF NOT EXISTS mtbf_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS mtbf_severity_label VARCHAR(20),
    ADD COLUMN IF NOT EXISTS mtbf_target DECIMAL(10,2),
    ADD COLUMN IF NOT EXISTS mtbf_baseline DECIMAL(10,2);

ALTER TABLE reporting.dataset_current_values
    ADD COLUMN IF NOT EXISTS kpi_uptime_pct_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS kpi_uptime_pct_baseline DECIMAL(5,2);

ALTER TABLE reporting.dataset_current_values
    ADD COLUMN IF NOT EXISTS kpi_kwh_bbl_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS kpi_kwh_bbl_baseline DECIMAL(10,2);

ALTER TABLE reporting.dataset_current_values
    ADD COLUMN IF NOT EXISTS vol_eff_status_level INTEGER,
    ADD COLUMN IF NOT EXISTS vol_eff_target DECIMAL(5,2),
    ADD COLUMN IF NOT EXISTS vol_eff_baseline DECIMAL(5,2);

ALTER TABLE reporting.dataset_current_values
    ADD COLUMN IF NOT EXISTS ai_accuracy_status_level INTEGER;

-- =============================================================================
-- 2. RECREAR dataset_kpi_business (WIDE: _current, _dia, _mes POR KPI)
-- =============================================================================
DROP TABLE IF EXISTS reporting.dataset_kpi_business CASCADE;

CREATE TABLE reporting.dataset_kpi_business (
    -- === IDENTIFICADORES ===
    kpi_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fecha DATE NOT NULL,
    well_id INTEGER NOT NULL,
    nombre_pozo VARCHAR(100),
    campo VARCHAR(100),
    region VARCHAR(50),

    -- ═══════════════════════════════════════════════════════════════════════
    -- KPI 1: MTBF (Mean Time Between Failures) – kpi_mtbf_hrs / kpi_mtbf_days
    -- ═══════════════════════════════════════════════════════════════════════
    kpi_mtbf_hrs_current DECIMAL(10,2),      -- Snapshot (dataset_current_values)
    kpi_mtbf_hrs_dia DECIMAL(10,2),          -- Agregado diario
    kpi_mtbf_hrs_mes DECIMAL(10,2),          -- Agregado mensual
    kpi_mtbf_hrs_target DECIMAL(10,2),       -- Objetivo
    kpi_mtbf_hrs_baseline DECIMAL(10,2),     -- Referencia histórica
    kpi_mtbf_hrs_variance_pct DECIMAL(8,2),  -- ((val-target)/target)*100
    kpi_mtbf_hrs_status_color VARCHAR(7),    -- Semáforo color
    kpi_mtbf_hrs_status_level INTEGER,       -- 0=OK,1=WARN,2=CRIT,3=NO_DATA
    kpi_mtbf_hrs_severity_label VARCHAR(20), -- EXCELENTE/NORMAL/ALERTA/CRÍTICO
    kpi_mtbf_days DECIMAL(10,2),             -- = kpi_mtbf_hrs / 24

    -- ═══════════════════════════════════════════════════════════════════════
    -- KPI 2: UPTIME (Disponibilidad) – kpi_uptime_pct
    -- ═══════════════════════════════════════════════════════════════════════
    kpi_uptime_pct_current DECIMAL(5,2),
    kpi_uptime_pct_dia DECIMAL(5,2),
    kpi_uptime_pct_mes DECIMAL(5,2),
    kpi_uptime_pct_target DECIMAL(5,2),
    kpi_uptime_pct_baseline DECIMAL(5,2),
    kpi_uptime_pct_variance_pct DECIMAL(8,2),
    kpi_uptime_pct_status_color VARCHAR(7),
    kpi_uptime_pct_status_level INTEGER,
    kpi_uptime_pct_severity_label VARCHAR(20),

    -- ═══════════════════════════════════════════════════════════════════════
    -- KPI 3: KWH_BBL (Eficiencia Energética) – kpi_kwh_bbl
    -- ═══════════════════════════════════════════════════════════════════════
    kpi_kwh_bbl_current DECIMAL(10,4),
    kpi_kwh_bbl_dia DECIMAL(10,4),
    kpi_kwh_bbl_mes DECIMAL(10,4),
    kpi_kwh_bbl_target DECIMAL(10,4),
    kpi_kwh_bbl_baseline DECIMAL(10,4),
    kpi_kwh_bbl_variance_pct DECIMAL(8,2),
    kpi_kwh_bbl_status_color VARCHAR(7),
    kpi_kwh_bbl_status_level INTEGER,
    kpi_kwh_bbl_severity_label VARCHAR(20),
    consumo_kwh DECIMAL(10,2),

    -- ═══════════════════════════════════════════════════════════════════════
    -- KPI 4: VOL_EFF (Eficiencia Volumétrica) – kpi_vol_eff_pct
    -- ═══════════════════════════════════════════════════════════════════════
    kpi_vol_eff_pct_current DECIMAL(5,2),
    kpi_vol_eff_pct_dia DECIMAL(5,2),
    kpi_vol_eff_pct_mes DECIMAL(5,2),
    kpi_vol_eff_pct_target DECIMAL(5,2),
    kpi_vol_eff_pct_baseline DECIMAL(5,2),
    kpi_vol_eff_pct_variance_pct DECIMAL(8,2),
    kpi_vol_eff_pct_status_color VARCHAR(7),
    kpi_vol_eff_pct_status_level INTEGER,
    kpi_vol_eff_pct_severity_label VARCHAR(20),
    produccion_real_bbl DECIMAL(10,2),
    produccion_teorica_bbl DECIMAL(10,2),

    -- ═══════════════════════════════════════════════════════════════════════
    -- KPI 5: AI_ACCURACY (Precisión IA) – ai_accuracy
    -- ═══════════════════════════════════════════════════════════════════════
    ai_accuracy_current DECIMAL(5,2),
    ai_accuracy_dia DECIMAL(5,2),
    ai_accuracy_mes DECIMAL(5,2),
    ai_accuracy_target DECIMAL(5,2),
    ai_accuracy_baseline DECIMAL(5,2),
    ai_accuracy_variance_pct DECIMAL(8,2),
    ai_accuracy_status_color VARCHAR(7),
    ai_accuracy_status_level INTEGER,
    ai_accuracy_severity_label VARCHAR(20),

    -- ═══════════════════════════════════════════════════════════════════════
    -- CONTEXTO
    -- ═══════════════════════════════════════════════════════════════════════
    calidad_datos_pct DECIMAL(5,2),
    fecha_calculo TIMESTAMP DEFAULT NOW(),

    -- === CONSTRAINT ===
    UNIQUE(fecha, well_id)
);

-- Índices para consultas BI
CREATE INDEX IF NOT EXISTS idx_kpi_biz_fecha
    ON reporting.dataset_kpi_business(fecha);
CREATE INDEX IF NOT EXISTS idx_kpi_biz_pozo
    ON reporting.dataset_kpi_business(well_id);
CREATE INDEX IF NOT EXISTS idx_kpi_biz_pozo_fecha
    ON reporting.dataset_kpi_business(well_id, fecha DESC);
CREATE INDEX IF NOT EXISTS idx_kpi_biz_region
    ON reporting.dataset_kpi_business(region, fecha DESC);

-- =============================================================================
-- 3. CONSTANTES DE EVALUACIÓN (Tarifas y Umbrales Default)
-- =============================================================================
CREATE TABLE IF NOT EXISTS referencial.tbl_config_kpi (
    config_id SERIAL PRIMARY KEY,
    kpi_nombre VARCHAR(50) NOT NULL,
    parametro VARCHAR(50) NOT NULL,
    valor DECIMAL(15,4),
    unidad VARCHAR(20),
    descripcion TEXT,
    valor_texto VARCHAR(100),
    activo BOOLEAN DEFAULT TRUE,
    UNIQUE(kpi_nombre, parametro)
);

-- Idempotent: agregar columna si tabla ya existe
ALTER TABLE referencial.tbl_config_kpi ADD COLUMN IF NOT EXISTS valor_texto VARCHAR(100);

INSERT INTO referencial.tbl_config_kpi (kpi_nombre, parametro, valor, unidad, descripcion)
VALUES
    -- Tarifa energética
    ('ENERGIA', 'tarifa_kwh_usd', 0.12, 'USD/kWh', 'Tarifa eléctrica promedio'),
    -- MTBF
    ('KPI_MTBF', 'target_default_hrs', 2000, 'horas', 'Objetivo MTBF default'),
    ('KPI_MTBF', 'baseline_default_hrs', 1500, 'horas', 'Baseline MTBF default'),
    ('KPI_MTBF', 'warning_threshold_pct', 80, '%', 'Umbral warning: <80% del target'),
    ('KPI_MTBF', 'critical_threshold_pct', 50, '%', 'Umbral crítico: <50% del target'),
    -- Uptime
    ('KPI_UPTIME', 'target_default_pct', 95, '%', 'Objetivo uptime default'),
    ('KPI_UPTIME', 'baseline_default_pct', 90, '%', 'Baseline uptime default'),
    ('KPI_UPTIME', 'warning_threshold_pct', 90, '%', 'Umbral warning'),
    ('KPI_UPTIME', 'critical_threshold_pct', 80, '%', 'Umbral crítico'),
    -- kWh/bbl (menor es mejor)
    ('KPI_KWH_BBL', 'target_default', 10, 'kWh/bbl', 'Objetivo eficiencia energética'),
    ('KPI_KWH_BBL', 'baseline_default', 12, 'kWh/bbl', 'Baseline eficiencia energética'),
    ('KPI_KWH_BBL', 'warning_threshold', 15, 'kWh/bbl', 'Umbral warning'),
    ('KPI_KWH_BBL', 'critical_threshold', 20, 'kWh/bbl', 'Umbral crítico'),
    -- Vol Eff
    ('KPI_VOL_EFF', 'target_default_pct', 85, '%', 'Objetivo eficiencia volumétrica'),
    ('KPI_VOL_EFF', 'baseline_default_pct', 80, '%', 'Baseline eficiencia volumétrica'),
    ('KPI_VOL_EFF', 'warning_threshold_pct', 70, '%', 'Umbral warning'),
    ('KPI_VOL_EFF', 'critical_threshold_pct', 50, '%', 'Umbral crítico'),
    -- AI Accuracy
    ('AI_ACCURACY', 'target_default_pct', 85, '%', 'Objetivo precisión IA'),
    ('AI_ACCURACY', 'baseline_default_pct', 85, '%', 'Baseline precisión IA'),
    ('AI_ACCURACY', 'warning_threshold_pct', 75, '%', 'Umbral warning'),
    ('AI_ACCURACY', 'critical_threshold_pct', 60, '%', 'Umbral crítico'),
    -- Targets operativos centralizados
    ('PUMP_FILL_MONITOR', 'target_default_pct', 70, '%', 'Target llenado de bomba default'),
    ('GAS_FILL_MONITOR', 'target_default_pct', 30, '%', 'Target llenado de gas default'),
    ('ROAD_LOAD', 'target_default_pct', 100, '%', 'Target road load default'),
    ('TANK_FLUID_TEMPERATURE', 'target_default_f', 120, '°F', 'Target temperatura tanque default'),
    ('DAILY_DOWNTIME', 'target_default_min', 90, 'min', 'Target downtime diario máximo'),
    ('PUMP_SPM', 'target_default', 3, 'spm', 'Target SPM default'),
    ('WELL_HEAD_PRESSURE', 'baseline_default_psi', 1200, 'PSI', 'Baseline WHP default'),
    ('LIFTING_COST', 'default_usd_bbl', 2.50, 'USD/bbl', 'Costo de levantamiento default'),
    ('FREQ_VSD', 'default_hz', 60, 'Hz', 'Frecuencia VSD default (sin sensor)'),
    -- Stroke Length Variance thresholds
    ('PUMP_STROKE', 'variance_warning_pct', 5, '%', 'Umbral warning varianza stroke length'),
    ('PUMP_STROKE', 'variance_critical_pct', 15, '%', 'Umbral crítico varianza stroke length'),
    -- Lift Efficiency
    ('LIFT_EFFICIENCY', 'target_default_pct', 85, '%', 'Target lift efficiency default'),
    ('LIFT_EFFICIENCY', 'baseline_default_pct', 80, '%', 'Baseline lift efficiency default'),
    -- Pump Volume displacement constant
    ('PUMP_VOLUME', 'displacement_constant', 0.000971, 'bbl/stroke', 'Constante de desplazamiento de bomba'),
    -- Vol Eff cap
    ('KPI_VOL_EFF', 'cap_pct', 150.0, '%', 'Techo máximo eficiencia volumétrica'),
    -- Data Quality OK threshold
    ('DATA_QUALITY', 'ok_threshold_pct', 0.9, 'ratio', 'Umbral calidad datos para estado OK'),
    -- DQ Fail threshold level (V8 scale)
    ('DQ', 'fail_threshold_level', 4, 'level', 'Nivel mínimo status_level para DQ FAIL')
ON CONFLICT (kpi_nombre, parametro) DO NOTHING;

-- Region default (valor texto)
INSERT INTO referencial.tbl_config_kpi (kpi_nombre, parametro, valor_texto, descripcion)
VALUES ('DEFAULT', 'region', 'PECOS VALLEY', 'Región default para pozos sin región asignada')
ON CONFLICT (kpi_nombre, parametro) DO NOTHING;

-- =============================================================================
-- 4. STORED PROCEDURE: POBLAR dataset_kpi_business (WIDE – 3 pasos)
-- =============================================================================
CREATE OR REPLACE PROCEDURE reporting.poblar_kpi_business(
    p_fecha_inicio DATE DEFAULT CURRENT_DATE - 30,
    p_fecha_fin DATE DEFAULT CURRENT_DATE
)
LANGUAGE plpgsql AS $$
DECLARE
    v_tarifa_kwh DECIMAL(10,4);
    v_mtbf_target DECIMAL(10,2);
    v_mtbf_baseline DECIMAL(10,2);
    v_uptime_target DECIMAL(5,2);
    v_uptime_baseline DECIMAL(5,2);
    v_kwh_target DECIMAL(10,4);
    v_kwh_baseline DECIMAL(10,4);
    v_vol_eff_target DECIMAL(5,2);
    v_vol_eff_baseline DECIMAL(5,2);
    v_ai_accuracy_target DECIMAL(5,2);
    v_ai_accuracy_baseline DECIMAL(5,2);
BEGIN
    -- ─── Obtener configuración ────────────────────────────────────────────
    SELECT COALESCE(valor, 0.12) INTO v_tarifa_kwh
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='ENERGIA' AND parametro='tarifa_kwh_usd';

    SELECT COALESCE(valor, 2000) INTO v_mtbf_target
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_MTBF' AND parametro='target_default_hrs';
    SELECT COALESCE(valor, 1500) INTO v_mtbf_baseline
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_MTBF' AND parametro='baseline_default_hrs';

    SELECT COALESCE(valor, 95) INTO v_uptime_target
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_UPTIME' AND parametro='target_default_pct';
    SELECT COALESCE(valor, 90) INTO v_uptime_baseline
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_UPTIME' AND parametro='baseline_default_pct';

    SELECT COALESCE(valor, 10) INTO v_kwh_target
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_KWH_BBL' AND parametro='target_default';
    SELECT COALESCE(valor, 12) INTO v_kwh_baseline
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_KWH_BBL' AND parametro='baseline_default';

    SELECT COALESCE(valor, 85) INTO v_vol_eff_target
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_VOL_EFF' AND parametro='target_default_pct';
    SELECT COALESCE(valor, 80) INTO v_vol_eff_baseline
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_VOL_EFF' AND parametro='baseline_default_pct';

    SELECT COALESCE(valor, 85) INTO v_ai_accuracy_target
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='AI_ACCURACY' AND parametro='target_default_pct';
    SELECT COALESCE(valor, 85) INTO v_ai_accuracy_baseline
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='AI_ACCURACY' AND parametro='baseline_default_pct';

    -- =====================================================================
    -- PASO A: UPSERT DIARIO (fact_operaciones_diarias → kpi_*_dia)
    --         Semáforos calculados sobre valor _dia vs target.
    -- =====================================================================
    WITH diario_raw AS (
        SELECT
            dt.fecha,
            d.pozo_id,
            p.nombre_pozo,
            p.campo,
            p.region,
            -- MTBF raw
            CASE
                WHEN d.numero_fallas > 0 THEN d.tiempo_operacion_hrs / d.numero_fallas
                WHEN COALESCE(d.kpi_mtbf_hrs, 0) > 0 THEN d.kpi_mtbf_hrs
                WHEN prod.horas_operacion_acumuladas > 0 THEN prod.horas_operacion_acumuladas
                ELSE NULL
            END AS mtbf_raw,
            d.numero_fallas,
            -- Targets & baselines
            COALESCE(p.mtbf_target, v_mtbf_target) AS mtbf_tgt,
            COALESCE(p.mtbf_baseline, v_mtbf_baseline) AS mtbf_bsl,
            COALESCE(p.kpi_uptime_pct_target, v_uptime_target) AS uptime_tgt,
            v_uptime_baseline AS uptime_bsl,
            COALESCE(p.kpi_kwh_bbl_target, v_kwh_target) AS kwh_tgt,
            COALESCE(p.kpi_kwh_bbl_baseline, v_kwh_baseline) AS kwh_bsl,
            COALESCE(p.vol_eff_target, v_vol_eff_target) AS vol_eff_tgt,
            v_vol_eff_baseline AS vol_eff_bsl,
            -- Raw values
            d.kpi_uptime_pct AS uptime_raw,
            d.kpi_kwh_bbl AS kwh_bbl_raw,
            d.kpi_efic_vol_pct AS vol_eff_raw,
            -- Supporting
            d.tiempo_operacion_hrs,
            d.tiempo_paro_noprog_hrs,
            d.consumo_energia_kwh,
            d.produccion_fluido_bbl,
            d.volumen_teorico_bbl,
            d.completitud_datos_pct
        FROM reporting.fact_operaciones_diarias d
        JOIN reporting.dim_tiempo dt ON d.fecha_id = dt.fecha_id
        JOIN reporting.dim_pozo p ON d.pozo_id = p.pozo_id
        LEFT JOIN LATERAL (
            SELECT prod_inner.horas_operacion_acumuladas
            FROM stage.tbl_pozo_produccion prod_inner
            WHERE prod_inner.well_id = p.pozo_id
            ORDER BY prod_inner.timestamp_lectura DESC
            LIMIT 1
        ) prod ON true
        WHERE dt.fecha BETWEEN p_fecha_inicio AND p_fecha_fin
    )
    -- NOTA ARQUITECTÓNICA: Los semáforos (variance_pct, status_color, status_level,
    -- severity_label) SOLO se pueblan desde dataset_current_values en Paso C.
    -- Las filas históricas (DIARIO) solo llevan valores raw + targets/baselines.
    -- Los semáforos de diario/mensual residen en fact_operaciones_diarias/mensuales.
    -- En el futuro podrán anexarse a esta tabla si el dashboard lo requiere.
    INSERT INTO reporting.dataset_kpi_business (
        fecha, well_id, nombre_pozo, campo, region,
        -- MTBF (raw + targets)
        kpi_mtbf_hrs_dia, kpi_mtbf_hrs_target, kpi_mtbf_hrs_baseline,
        kpi_mtbf_days,
        -- UPTIME (raw + targets)
        kpi_uptime_pct_dia, kpi_uptime_pct_target, kpi_uptime_pct_baseline,
        -- KWH_BBL (raw + targets)
        kpi_kwh_bbl_dia, kpi_kwh_bbl_target, kpi_kwh_bbl_baseline,
        consumo_kwh,
        -- VOL_EFF (raw + targets)
        kpi_vol_eff_pct_dia, kpi_vol_eff_pct_target, kpi_vol_eff_pct_baseline,
        produccion_real_bbl, produccion_teorica_bbl,
        -- AI_ACCURACY defaults
        ai_accuracy_target, ai_accuracy_baseline,
        -- Context
        calidad_datos_pct
    )
    SELECT
        r.fecha,
        r.pozo_id,
        r.nombre_pozo,
        r.campo,
        r.region,

        -- ── MTBF (raw + targets, SIN semáforos) ──
        r.mtbf_raw,
        r.mtbf_tgt,
        r.mtbf_bsl,
        CASE WHEN r.mtbf_raw IS NOT NULL THEN r.mtbf_raw / 24.0 ELSE NULL END,

        -- ── UPTIME (raw + targets, SIN semáforos) ──
        r.uptime_raw,
        r.uptime_tgt,
        r.uptime_bsl,

        -- ── KWH/BBL (raw + targets, SIN semáforos) ──
        r.kwh_bbl_raw,
        r.kwh_tgt,
        r.kwh_bsl,
        r.consumo_energia_kwh,

        -- ── VOL EFF (raw + targets, SIN semáforos) ──
        r.vol_eff_raw,
        r.vol_eff_tgt,
        r.vol_eff_bsl,
        r.produccion_fluido_bbl,
        r.volumen_teorico_bbl,

        -- AI Accuracy (targets/baselines, sin valor _dia por ahora)
        v_ai_accuracy_target,
        v_ai_accuracy_baseline,

        -- Context
        r.completitud_datos_pct

    FROM diario_raw r
    ON CONFLICT (fecha, well_id)
    DO UPDATE SET
        nombre_pozo                  = EXCLUDED.nombre_pozo,
        campo                        = EXCLUDED.campo,
        region                       = EXCLUDED.region,
        kpi_mtbf_hrs_dia             = EXCLUDED.kpi_mtbf_hrs_dia,
        kpi_mtbf_hrs_target          = EXCLUDED.kpi_mtbf_hrs_target,
        kpi_mtbf_hrs_baseline        = EXCLUDED.kpi_mtbf_hrs_baseline,
        kpi_mtbf_days                = EXCLUDED.kpi_mtbf_days,
        kpi_uptime_pct_dia           = EXCLUDED.kpi_uptime_pct_dia,
        kpi_uptime_pct_target        = EXCLUDED.kpi_uptime_pct_target,
        kpi_uptime_pct_baseline      = EXCLUDED.kpi_uptime_pct_baseline,
        kpi_kwh_bbl_dia              = EXCLUDED.kpi_kwh_bbl_dia,
        kpi_kwh_bbl_target           = EXCLUDED.kpi_kwh_bbl_target,
        kpi_kwh_bbl_baseline         = EXCLUDED.kpi_kwh_bbl_baseline,
        consumo_kwh                  = EXCLUDED.consumo_kwh,
        kpi_vol_eff_pct_dia          = EXCLUDED.kpi_vol_eff_pct_dia,
        kpi_vol_eff_pct_target       = EXCLUDED.kpi_vol_eff_pct_target,
        kpi_vol_eff_pct_baseline     = EXCLUDED.kpi_vol_eff_pct_baseline,
        produccion_real_bbl          = EXCLUDED.produccion_real_bbl,
        produccion_teorica_bbl       = EXCLUDED.produccion_teorica_bbl,
        ai_accuracy_target           = EXCLUDED.ai_accuracy_target,
        ai_accuracy_baseline         = EXCLUDED.ai_accuracy_baseline,
        calidad_datos_pct            = EXCLUDED.calidad_datos_pct,
        fecha_calculo                = NOW();

    -- =====================================================================
    -- PASO B: UPDATE MENSUAL (fact_operaciones_mensuales → kpi_*_mes)
    --         Cada día del mes recibe los mismos agregados mensuales.
    -- =====================================================================
    UPDATE reporting.dataset_kpi_business kb
    SET
        kpi_mtbf_hrs_mes = CASE
            WHEN m.total_fallas_mes > 0 THEN m.tiempo_operacion_hrs / m.total_fallas_mes
            WHEN m.tiempo_operacion_hrs > 0 THEN m.tiempo_operacion_hrs
            ELSE NULL
        END,
        kpi_uptime_pct_mes = m.eficiencia_uptime_pct,
        kpi_kwh_bbl_mes = m.kpi_kwh_bbl_mes,
        kpi_vol_eff_pct_mes = m.promedio_efic_vol_pct,
        fecha_calculo = NOW()
    FROM reporting.fact_operaciones_mensuales m
    WHERE kb.well_id = m.pozo_id
      AND TO_CHAR(kb.fecha, 'YYYY-MM') = m.anio_mes
      AND kb.fecha BETWEEN p_fecha_inicio AND p_fecha_fin;

    -- =====================================================================
    -- PASO C: ASEGURAR FILA HOY + UPDATE CURRENT + SEMÁFOROS
    --         (dataset_current_values → kpi_*_current + semáforos copiados)
    --         Fila de hoy: INSERT safeguard.
    --         Semáforos: UPDATE en TODAS las filas del rango.
    -- =====================================================================

    -- C.1  Garantizar que existe fila para hoy por cada pozo activo
    INSERT INTO reporting.dataset_kpi_business (
        fecha, well_id, nombre_pozo, campo, region,
        kpi_mtbf_hrs_target, kpi_mtbf_hrs_baseline,
        kpi_uptime_pct_target, kpi_uptime_pct_baseline,
        kpi_kwh_bbl_target, kpi_kwh_bbl_baseline,
        kpi_vol_eff_pct_target, kpi_vol_eff_pct_baseline,
        ai_accuracy_target, ai_accuracy_baseline
    )
    SELECT
        CURRENT_DATE,
        c.well_id,
        c.nombre_pozo,
        c.campo,
        c.region,
        COALESCE(p.mtbf_target, v_mtbf_target),
        COALESCE(p.mtbf_baseline, v_mtbf_baseline),
        COALESCE(p.kpi_uptime_pct_target, v_uptime_target),
        v_uptime_baseline,
        COALESCE(p.kpi_kwh_bbl_target, v_kwh_target),
        COALESCE(p.kpi_kwh_bbl_baseline, v_kwh_baseline),
        COALESCE(p.vol_eff_target, v_vol_eff_target),
        v_vol_eff_baseline,
        v_ai_accuracy_target,
        v_ai_accuracy_baseline
    FROM reporting.dataset_current_values c
    LEFT JOIN reporting.dim_pozo p ON c.well_id = p.pozo_id
    WHERE CURRENT_DATE BETWEEN p_fecha_inicio AND p_fecha_fin
    ON CONFLICT (fecha, well_id) DO NOTHING;

    -- C.2  Poblar _current + sobrescribir semáforos para TODAS las filas del rango
    UPDATE reporting.dataset_kpi_business kb
    SET
        -- Valores current
        kpi_mtbf_hrs_current     = c.kpi_mtbf_hrs_act,
        kpi_uptime_pct_current   = c.kpi_uptime_pct_act,
        kpi_kwh_bbl_current      = c.kpi_kwh_bbl_act,
        kpi_vol_eff_pct_current  = c.kpi_vol_eff_pct_act,
        ai_accuracy_current      = c.ai_accuracy_act,

        -- MTBF days (= hrs / 24)
        kpi_mtbf_days = CASE WHEN c.kpi_mtbf_hrs_act IS NOT NULL
                             THEN c.kpi_mtbf_hrs_act / 24.0 ELSE NULL END,

        -- Semáforos MTBF (copiados)
        kpi_mtbf_hrs_variance_pct   = c.mtbf_variance_pct,
        kpi_mtbf_hrs_status_color   = c.mtbf_status_color,
        kpi_mtbf_hrs_status_level   = c.mtbf_status_level,
        kpi_mtbf_hrs_severity_label = c.mtbf_severity_label,

        -- Semáforos UPTIME (copiados + variance calculada)
        kpi_uptime_pct_variance_pct = CASE
            WHEN COALESCE(p.kpi_uptime_pct_target, v_uptime_target) > 0 THEN
                ROUND((c.kpi_uptime_pct_act - COALESCE(p.kpi_uptime_pct_target, v_uptime_target))
                      / COALESCE(p.kpi_uptime_pct_target, v_uptime_target) * 100, 2)
            ELSE NULL END,
        kpi_uptime_pct_status_color   = c.kpi_uptime_pct_status_color,
        kpi_uptime_pct_status_level   = c.kpi_uptime_pct_status_level,
        kpi_uptime_pct_severity_label = c.kpi_uptime_pct_severity_label,

        -- Semáforos KWH_BBL (copiados + variance calculada — MENOR es mejor → invertida)
        kpi_kwh_bbl_variance_pct = CASE
            WHEN COALESCE(p.kpi_kwh_bbl_target, v_kwh_target) > 0 THEN
                ROUND((COALESCE(p.kpi_kwh_bbl_target, v_kwh_target) - c.kpi_kwh_bbl_act)
                      / COALESCE(p.kpi_kwh_bbl_target, v_kwh_target) * 100, 2)
            ELSE NULL END,
        kpi_kwh_bbl_status_color   = c.kpi_kwh_bbl_status_color,
        kpi_kwh_bbl_status_level   = c.kpi_kwh_bbl_status_level,
        kpi_kwh_bbl_severity_label = c.kpi_kwh_bbl_severity_label,

        -- Semáforos VOL_EFF (copiados + variance calculada)
        kpi_vol_eff_pct_variance_pct = CASE
            WHEN COALESCE(p.vol_eff_target, v_vol_eff_target) > 0 THEN
                ROUND((c.kpi_vol_eff_pct_act - COALESCE(p.vol_eff_target, v_vol_eff_target))
                      / COALESCE(p.vol_eff_target, v_vol_eff_target) * 100, 2)
            ELSE NULL END,
        kpi_vol_eff_pct_status_color   = c.vol_eff_status_color,
        kpi_vol_eff_pct_status_level   = c.vol_eff_status_level,
        kpi_vol_eff_pct_severity_label = c.vol_eff_severity_label,

        -- Semáforos AI_ACCURACY (copiados + variance calculada)
        ai_accuracy_variance_pct = CASE
            WHEN v_ai_accuracy_target > 0 AND c.ai_accuracy_act IS NOT NULL THEN
                ROUND((c.ai_accuracy_act - v_ai_accuracy_target)
                      / v_ai_accuracy_target * 100, 2)
            ELSE NULL END,
        ai_accuracy_status_color   = c.ai_accuracy_status_color,
        ai_accuracy_status_level   = c.ai_accuracy_status_level,
        ai_accuracy_severity_label = c.ai_accuracy_severity_label,

        fecha_calculo = NOW()
    FROM reporting.dataset_current_values c
    JOIN reporting.dim_pozo p ON c.well_id = p.pozo_id
    WHERE kb.well_id = c.well_id
      AND kb.fecha BETWEEN p_fecha_inicio AND p_fecha_fin;

    RAISE NOTICE 'KPIs Business WIDE actualizados para % a % (DIARIO + MENSUAL + CURRENT)',
                 p_fecha_inicio, p_fecha_fin;
END;
$$;

-- =============================================================================
-- 5. SP: ACTUALIZAR MTBF STATUS EN dataset_current_values
-- =============================================================================
CREATE OR REPLACE PROCEDURE reporting.actualizar_mtbf_status_current()
LANGUAGE plpgsql AS $$
DECLARE
    v_mtbf_target DECIMAL(10,2);
BEGIN
    SELECT COALESCE(valor, 2000) INTO v_mtbf_target
    FROM referencial.tbl_config_kpi WHERE kpi_nombre='KPI_MTBF' AND parametro='target_default_hrs';

    UPDATE reporting.dataset_current_values dcv
    SET
        mtbf_target = COALESCE(dp.mtbf_target, v_mtbf_target),
        mtbf_baseline = dp.mtbf_baseline,
        mtbf_status_level = CASE
            WHEN dcv.kpi_mtbf_hrs_act >= COALESCE(dp.mtbf_target, v_mtbf_target) THEN 0
            WHEN dcv.kpi_mtbf_hrs_act >= COALESCE(dp.mtbf_target, v_mtbf_target) * 0.8 THEN 1
            WHEN dcv.kpi_mtbf_hrs_act IS NOT NULL THEN 2
            ELSE 3
        END,
        mtbf_severity_label = CASE
            WHEN dcv.kpi_mtbf_hrs_act >= COALESCE(dp.mtbf_target, v_mtbf_target) * 10 THEN 'EXCELENTE'
            WHEN dcv.kpi_mtbf_hrs_act >= COALESCE(dp.mtbf_target, v_mtbf_target) THEN 'NORMAL'
            WHEN dcv.kpi_mtbf_hrs_act >= COALESCE(dp.mtbf_target, v_mtbf_target) * 0.8 THEN 'ALERTA'
            WHEN dcv.kpi_mtbf_hrs_act IS NOT NULL THEN 'CRÍTICO'
            ELSE 'SIN DATOS'
        END
    FROM reporting.dim_pozo dp
    WHERE dcv.well_id = dp.pozo_id;

    RAISE NOTICE 'MTBF status actualizado en dataset_current_values';
END;
$$;

-- =============================================================================
-- 6. DOCUMENTACIÓN
-- =============================================================================
COMMENT ON TABLE reporting.dataset_kpi_business IS
'Tabla WIDE consolidada de KPIs para dashboards de BI.
Diseño: 1 fila = 1 fecha × 1 pozo (PK: fecha, well_id).
Períodos en columnas: {base}_current, {base}_dia, {base}_mes.

NOMENCLATURA ESTANDARIZADA:
  kpi_mtbf_hrs   — Mean Time Between Failures (horas)
  kpi_mtbf_days  — Conversión MTBF a días (= hrs/24)
  kpi_uptime_pct — Disponibilidad (%)
  kpi_kwh_bbl    — Eficiencia Energética (kWh/barril)
  kpi_vol_eff_pct — Eficiencia Volumétrica (%)
  ai_accuracy    — Precisión IA (%)

Derivados por KPI: _target, _baseline, _variance_pct, _status_color, _status_level, _severity_label.

SP: reporting.poblar_kpi_business(fecha_inicio, fecha_fin)
   PASO A: fact_diarias → _dia + targets/baselines
   PASO B: fact_mensuales → _mes
   PASO C: current_values → _current + semáforos

Horario: NO incluido, consultar fact_operaciones_horarias directamente.';
