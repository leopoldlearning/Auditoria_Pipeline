-- =============================================================================
-- Schema for UNIVERSAL Layer
-- =============================================================================
-- Purpose: Store the results of analyses, aggregations, KPIs, and ML model
-- outputs. This layer is the source for visualization, reporting, and alerts.
-- =============================================================================

-- Drop schema if it exists to ensure a clean setup
DROP SCHEMA IF EXISTS universal CASCADE;

-- Create the schema for universal data (analytics results)
CREATE SCHEMA universal;

-- -----------------------------------------------------------------------------
-- Table: universal.ipr_resultados
-- Business Process: Inflow Performance Relationship (IPR) Results
-- Reference: src/ipr/data_structures.py -> class ResultadoIPR
-- -----------------------------------------------------------------------------
CREATE TABLE universal.ipr_resultados (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_pozo VARCHAR(50) NOT NULL,
    fecha_calculo TIMESTAMPTZ NOT NULL,
    metodo VARCHAR(100),
    qmax FLOAT,
    ip FLOAT,
    curva_yacimiento JSONB, -- Stores { "q": [...], "pwf": [...] }
    alertas JSONB -- Stores a JSON array of strings, e.g., ["High pressure drop"]
);

CREATE INDEX idx_ipr_resultados_pozo_fecha ON universal.ipr_resultados(id_pozo, fecha_calculo);
COMMENT ON TABLE universal.ipr_resultados IS 'Stores the calculated IPR curves and associated metrics.';

-- -----------------------------------------------------------------------------
-- Table: universal.arps_resultados_declinacion
-- Business Process: Decline Curve Analysis (ARPS) Results
-- Reference: src/declinacion_reservas/data_structures.py -> class ResultadoDeclinacion
-- -----------------------------------------------------------------------------
CREATE TABLE universal.arps_resultados_declinacion (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_pozo VARCHAR(50) NOT NULL,
    fecha_analisis TIMESTAMPTZ NOT NULL,
    tipo_curva VARCHAR(50), -- e.g., 'Exponencial', 'Hiperbolica', 'Armonica'
    qi FLOAT,
    di FLOAT,
    b FLOAT,
    r_squared FLOAT,
    eur_total FLOAT,
    eur_p50 FLOAT,
    eur_p90 FLOAT,
    eur_p10 FLOAT,
    CONSTRAINT unique_arps_analysis UNIQUE (id_pozo, fecha_analisis, tipo_curva)
);

CREATE INDEX idx_arps_resultados_pozo_fecha ON universal.arps_resultados_declinacion(id_pozo, fecha_analisis);
COMMENT ON TABLE universal.arps_resultados_declinacion IS 'Stores the parameters and results of ARPS decline curve analysis.';

-- -----------------------------------------------------------------------------
-- Table: universal.cartas_dinagraficas_diagnosticos
-- Business Process: Mechanical Pumping Diagnostics Results
-- Reference: src/bombeo_mecanico/data_structures.py -> class DynamicCard (output fields)
-- -----------------------------------------------------------------------------
CREATE TABLE universal.cartas_dinagraficas_diagnosticos (
    id_pozo VARCHAR(50) NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    pattern_id VARCHAR(50),
    pattern_name VARCHAR(255),
    pattern_score FLOAT,
    criticality VARCHAR(50), -- e.g., 'Low', 'Medium', 'High', 'Critical'
    PRIMARY KEY (id_pozo, "timestamp")
);

COMMENT ON TABLE universal.cartas_dinagraficas_diagnosticos IS 'Stores the diagnostic results and pattern classification for each dynamometer card.';
