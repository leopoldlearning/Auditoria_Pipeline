-- =============================================================================
-- ESQUEMA: universal
-- VERSION: 2.0.0
-- FECHA:   2026-02-16
-- AUTOR:   Pipeline BP010 / Auditoría IA
-- =============================================================================
--
-- DESCRIPCIÓN:
--   Capa de análisis avanzado, modelos ML y diagnóstico de cartas dinagráficas.
--   Almacena resultados de:
--     • Clasificación CDI de dynacards (patron → stroke → diagnostico → validación)
--     • IPR — Inflow Performance Relationship (curvas de afluencia)
--     • ARPS — Decline Curve Analysis (pronóstico de declinación)
--
-- CHANGE LOG:
--   V1.0.0 (2025)       — Creación inicial: ipr_resultados, arps_resultados_declinacion,
--                          cartas_dinagraficas_diagnosticos. UUID PK, VARCHAR id_pozo, sin FK.
--   V2.0.0 (2026-02-16) — Rediseño completo:
--     • CDI normalizado: patron → stroke → diagnostico → validacion_experta (4 tablas)
--     • IPR actualizado: BIGSERIAL PK, INTEGER well_id, FK a stage.tbl_pozo_maestra,
--       NUMERIC en vez de FLOAT, punto_operacion_bpd añadido
--     • ARPS actualizado: BIGSERIAL PK, INTEGER well_id, FK a stage.tbl_pozo_maestra,
--       pronostico_30d/90d y EUR P10/P50/P90 añadidos
--     • ELIMINADO: cartas_dinagraficas_diagnosticos (reemplazado por subsistema CDI)
--     • DROP SCHEMA CASCADE removido (idempotente con IF NOT EXISTS)
--     • Todas las tablas con COMMENT, created_at, índices en FK
--
-- DEPENDENCIAS (REQUIERE — creados antes en init_schemas.py):
--   • stage.tbl_pozo_maestra       (well_id PK)          ← FK en IPR, ARPS
--   • stage.tbl_pozo_produccion    (produccion_id PK)     ← FK en stroke
--
-- CONSUMIDORES (usan este esquema):
--   • simulate_universal_data.py   — datos de prueba (requiere adaptación a V2)
--   • (futuro) módulo Autopilot CDI — clasificación de dynacards
--   • (futuro) módulo IPR/ARPS      — pronóstico de reservas
--
-- TABLAS (6):
--   A. universal.patron                      — Catálogo de patrones de falla CDI
--   B. universal.stroke                      — Carreras de bombeo por registro de producción
--   C. universal.diagnostico                 — Resultado ML: score por patrón por stroke
--   D. universal.validacion_experta          — Validación humana (ground truth)
--   E. universal.ipr_resultados              — Curvas IPR y métricas de productividad
--   F. universal.arps_resultados_declinacion — Parámetros y pronóstico de declinación
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS universal;


-- =============================================================================
-- SECCIÓN A: CATÁLOGO DE PATRONES DE FALLA (CDI)
-- =============================================================================
-- Catálogo maestro de patrones reconocibles en cartas dinagráficas.
-- Ejemplos: "Golpe de Fluido", "Gas Lock", "Fuga de Válvula Viajera",
--           "Operación Normal", "Anclaje Deficiente", etc.
-- Cada patrón tiene una criticidad asignada que determina urgencia de acción.
-- Registros típicos: 10-30 patrones estándar de la industria.
-- =============================================================================

CREATE TABLE universal.patron (
    patron_id    SMALLSERIAL  PRIMARY KEY,                             -- PK auto-incremental (SMALLINT)
    nombre       VARCHAR(100) NOT NULL,                                -- Nombre del patrón (ej: "Golpe de Fluido")
    criticidad   VARCHAR(20)  NOT NULL,                                -- Nivel: BAJA | MEDIA | ALTA | CRITICA
    descripcion  TEXT,                                                 -- Descripción técnica del patrón
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()                   -- Fecha de registro
);

COMMENT ON TABLE universal.patron IS
'Catálogo maestro de patrones de falla identificables en cartas dinagráficas (CDI). Cada patrón representa una condición operativa del sistema de bombeo mecánico.';


-- =============================================================================
-- SECCIÓN B: REGISTRO DE CARRERAS (STROKE)
-- =============================================================================
-- Cada fila representa una carrera de bombeo (stroke) extraída de un registro
-- de producción SCADA. La relación 1:1 con produccion_id asegura que cada
-- registro de producción genera exactamente un stroke para análisis CDI.
--
-- Flujo de estados:
--   PENDIENTE → EN_PROCESO → COMPLETADO
--                           → ERROR (si falla el modelo)
--
-- NOTA: produccion_id es INTEGER (no BIGINT) para coincidir con
--       stage.tbl_pozo_produccion.produccion_id (int4).
-- =============================================================================

CREATE TABLE universal.stroke (
    stroke_id     BIGSERIAL   PRIMARY KEY,                             -- PK auto-incremental
    produccion_id INTEGER     NOT NULL,                                -- FK → stage.tbl_pozo_produccion (1:1)
    estado        VARCHAR(20) NOT NULL DEFAULT 'PENDIENTE',            -- PENDIENTE | EN_PROCESO | COMPLETADO | ERROR
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),                   -- Fecha de creación

    CONSTRAINT uq_stroke_produccion
        UNIQUE (produccion_id),

    CONSTRAINT fk_stroke_produccion
        FOREIGN KEY (produccion_id)
        REFERENCES stage.tbl_pozo_produccion (produccion_id)
);

COMMENT ON TABLE universal.stroke IS
'Registro de carreras de bombeo vinculadas 1:1 a stage.tbl_pozo_produccion. Controla el estado del pipeline de clasificación CDI para cada lectura de producción.';


-- =============================================================================
-- SECCIÓN C: DIAGNÓSTICO ML DE CARTAS DINAGRÁFICAS
-- =============================================================================
-- Resultado del modelo de clasificación: cada stroke puede tener N diagnósticos
-- (uno por patrón evaluado). El campo score (0.0000 a 1.0000) indica la
-- probabilidad de que el patrón aplique.
--
-- Relación: stroke 1:N diagnostico (top-K patrones por stroke)
-- Ejemplo: stroke_id=42 → [{patron="Gas Lock", score=0.92},
--                            {patron="Op. Normal", score=0.05}, ...]
-- =============================================================================

CREATE TABLE universal.diagnostico (
    diagnostico_id BIGSERIAL    PRIMARY KEY,                           -- PK auto-incremental
    stroke_id      BIGINT       NOT NULL,                              -- FK → universal.stroke
    patron_id      SMALLINT     NOT NULL,                              -- FK → universal.patron
    score          NUMERIC(5,4) NOT NULL CHECK (score BETWEEN 0 AND 1),-- Probabilidad 0.0000–1.0000
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),                -- Fecha de diagnóstico

    CONSTRAINT fk_diagnostico_stroke
        FOREIGN KEY (stroke_id)
        REFERENCES universal.stroke (stroke_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_diagnostico_patron
        FOREIGN KEY (patron_id)
        REFERENCES universal.patron (patron_id)
);

CREATE INDEX idx_diagnostico_stroke ON universal.diagnostico (stroke_id);
CREATE INDEX idx_diagnostico_patron ON universal.diagnostico (patron_id);

COMMENT ON TABLE universal.diagnostico IS
'Resultados del modelo ML de clasificación CDI. Cada fila es un score de probabilidad para un patrón específico en un stroke dado. Soporta top-K ranking de diagnósticos por carrera.';


-- =============================================================================
-- SECCIÓN D: VALIDACIÓN EXPERTA (GROUND TRUTH)
-- =============================================================================
-- Registro de validación humana para entrenamiento supervisado y auditoría.
-- Un experto confirma o corrige el diagnóstico ML asignando el patrón real.
-- Soporta re-validación: updated_at se actualiza con cada corrección.
-- Clave para el ciclo de mejora continua del modelo CDI.
-- =============================================================================

CREATE TABLE universal.validacion_experta (
    validacion_id BIGSERIAL    PRIMARY KEY,                            -- PK auto-incremental
    stroke_id     BIGINT       NOT NULL,                               -- FK → universal.stroke
    patron_id     SMALLINT     NOT NULL,                               -- FK → universal.patron (patrón validado)
    experto       VARCHAR(100) NOT NULL,                               -- Identificador del experto validador
    comentario    TEXT,                                                 -- Observaciones técnicas del experto
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),                  -- Fecha de validación
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),                  -- Fecha de última modificación

    CONSTRAINT fk_ve_stroke
        FOREIGN KEY (stroke_id)
        REFERENCES universal.stroke (stroke_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_ve_patron
        FOREIGN KEY (patron_id)
        REFERENCES universal.patron (patron_id)
);

CREATE INDEX idx_validacion_stroke ON universal.validacion_experta (stroke_id);

COMMENT ON TABLE universal.validacion_experta IS
'Validación humana de diagnósticos CDI. Permite a expertos confirmar/corregir la clasificación ML, generando ground truth para re-entrenamiento del modelo.';


-- =============================================================================
-- SECCIÓN E: IPR — INFLOW PERFORMANCE RELATIONSHIP
-- =============================================================================
-- Resultados del modelo IPR: curvas de afluencia del yacimiento al pozo.
-- Cada análisis genera Qmax (tasa máxima), IP (índice de productividad),
-- punto de operación, y opcionalmente la curva completa en formato JSONB.
--
-- Fuente: módulo src/ipr/ (Python) — métodos Vogel, Fetkovitch, Jones, Darcy.
-- Granularidad: 1 fila por pozo × fecha × método.
--
-- V1→V2 CAMBIOS:
--   • UUID PK → BIGSERIAL (consistencia con pipeline)
--   • VARCHAR id_pozo → INTEGER well_id + FK a stage.tbl_pozo_maestra
--   • FLOAT → NUMERIC (precisión decimal)
--   • Añadido: punto_operacion_bpd, UNIQUE constraint, created_at
-- =============================================================================

CREATE TABLE universal.ipr_resultados (
    ipr_id              BIGSERIAL    PRIMARY KEY,                      -- PK auto-incremental (antes UUID)
    well_id             INTEGER      NOT NULL,                         -- FK → stage.tbl_pozo_maestra
    fecha_calculo       TIMESTAMPTZ  NOT NULL,                         -- Timestamp del cálculo
    metodo              VARCHAR(100),                                  -- Método: Vogel | Fetkovitch | Jones | Darcy
    qmax_bpd            NUMERIC(10,2),                                 -- Tasa máxima teórica (bpd)
    ip_factor           NUMERIC(10,4),                                 -- Índice de Productividad (bpd/psi)
    punto_operacion_bpd NUMERIC(10,2),                                 -- Q esperado en punto de operación (bpd)
    curva_yacimiento    JSONB,                                         -- Curva completa: {"q": [...], "pwf": [...]}
    alertas             JSONB,                                         -- Alertas generadas: ["presión baja", ...]
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),            -- Auditoría

    CONSTRAINT uq_ipr_pozo_fecha UNIQUE (well_id, fecha_calculo),

    CONSTRAINT fk_ipr_pozo
        FOREIGN KEY (well_id)
        REFERENCES stage.tbl_pozo_maestra (well_id)
);

CREATE INDEX idx_ipr_pozo_fecha ON universal.ipr_resultados (well_id, fecha_calculo);

COMMENT ON TABLE universal.ipr_resultados IS
'Resultados del análisis IPR (Inflow Performance Relationship). Almacena curvas de afluencia, Qmax, IP y punto de operación calculados por pozo y fecha.';


-- =============================================================================
-- SECCIÓN F: ARPS — DECLINE CURVE ANALYSIS
-- =============================================================================
-- Resultados del modelo de declinación: parámetros del ajuste (qi, di, b),
-- bondad de ajuste (R²), reservas estimadas (EUR) con incertidumbre
-- probabilística (P10/P50/P90), y pronósticos a 30/90 días.
--
-- Fuente: módulo src/declinacion_reservas/ (Python)
-- Granularidad: 1 fila por pozo × fecha × tipo_curva.
--
-- V1→V2 CAMBIOS:
--   • UUID PK → BIGSERIAL (consistencia con pipeline)
--   • VARCHAR id_pozo → INTEGER well_id + FK a stage.tbl_pozo_maestra
--   • FLOAT → NUMERIC (precisión decimal)
--   • Añadido: pronostico_30d_bpd, pronostico_90d_bpd, created_at
--   • UNIQUE ampliado: incluye tipo_curva (soporta múltiples ajustes por fecha)
-- =============================================================================

CREATE TABLE universal.arps_resultados_declinacion (
    arps_id            BIGSERIAL    PRIMARY KEY,                       -- PK auto-incremental (antes UUID)
    well_id            INTEGER      NOT NULL,                          -- FK → stage.tbl_pozo_maestra
    fecha_analisis     TIMESTAMPTZ  NOT NULL,                          -- Timestamp del análisis
    tipo_curva         VARCHAR(50),                                    -- Exponencial | Hiperbólica | Armónica
    qi_bpd             NUMERIC(10,2),                                  -- Tasa inicial de producción (bpd)
    di_nominal         NUMERIC(8,6),                                   -- Tasa de declinación nominal (1/día)
    b_factor           NUMERIC(5,3),                                   -- Factor b: 0=exp, 0<b<1=hip, b=1=arm
    r_squared          NUMERIC(6,4),                                   -- Coeficiente R² del ajuste (0–1)
    eur_bbl            NUMERIC(12,2),                                  -- Estimated Ultimate Recovery (bbl)
    eur_p10            NUMERIC(12,2),                                  -- EUR percentil 10 (optimista)
    eur_p50            NUMERIC(12,2),                                  -- EUR percentil 50 (caso más probable)
    eur_p90            NUMERIC(12,2),                                  -- EUR percentil 90 (conservador)
    pronostico_30d_bpd NUMERIC(10,2),                                  -- Pronóstico a 30 días (bpd)
    pronostico_90d_bpd NUMERIC(10,2),                                  -- Pronóstico a 90 días (bpd)
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),            -- Auditoría

    CONSTRAINT uq_arps_pozo_fecha_tipo UNIQUE (well_id, fecha_analisis, tipo_curva),

    CONSTRAINT fk_arps_pozo
        FOREIGN KEY (well_id)
        REFERENCES stage.tbl_pozo_maestra (well_id)
);

CREATE INDEX idx_arps_pozo_fecha ON universal.arps_resultados_declinacion (well_id, fecha_analisis);

COMMENT ON TABLE universal.arps_resultados_declinacion IS
'Resultados del análisis de curvas de declinación (Arps). Parámetros del modelo, EUR con incertidumbre P10/P50/P90, y pronósticos a corto plazo por pozo.';
