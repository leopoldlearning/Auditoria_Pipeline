-- =============================================================================
-- TABLA: DATASET CURRENT VALUES (Capa Reporting)
-- DESCRIPCIÓN: Snapshot de último valor conocido para tableros de tiempo real.
-- FRECUENCIA DE ESCRITURA: Alta (Upsert continuo).
-- =============================================================================

-- Asegurar idempotencia (si se corre de nuevo, limpia lo anterior)

DROP TABLE IF EXISTS reporting.tbl_current_values CASCADE;

CREATE TABLE reporting.tbl_current_values (
    -- ==========================================
    -- 1. IDENTIFICACIÓN Y JERARQUÍA
    -- ==========================================
    well_id INT PRIMARY KEY,                    -- ID: 1
    cliente VARCHAR(100),                       -- ID: 12
    region VARCHAR(100),                        -- ID: 14
    campo VARCHAR(100),                         -- ID: 15
    nombre_pozo VARCHAR(100),                   -- ID: 49
    turno_operativo VARCHAR(20),                -- (Calculado)

    -- ==========================================
    -- 2. VITALIDAD Y ESTADO
    -- ==========================================
    ultima_actualizacion TIMESTAMP,             -- ID: 50
    minutos_sin_reportar INT,                   -- (Calculado: Latencia)
    estado_comunicacion VARCHAR(20),            -- (Calculado: Semáforo)
    motor_running_flag BOOLEAN,                 -- ID: 120

    -- ==========================================
    -- 3. PRODUCCIÓN (Caudales)
    -- ==========================================
    total_fluid_today_bbl DECIMAL(10,2),        -- ID: 107 (Qact / Qf)
    oil_today_bbl DECIMAL(10,2),                -- ID: 108 (Qo)
    water_today_bbl DECIMAL(10,2),              -- ID: 109 (Qw)
    gas_today_mcf DECIMAL(10,2),                -- ID: 110 (Qg)
    water_cut_pct DECIMAL(5,2),                 -- ID: 57
    qf_fluid_flow_bpd DECIMAL(10,2),            -- ID: 65 (Monitor Flujo)

    -- ==========================================
    -- 4. SENSORES DE PRESIÓN Y NIVEL
    -- ==========================================
    whp_psi DECIMAL(10,2),                      -- ID: 54
    chp_psi DECIMAL(10,2),                      -- ID: 55
    pip_psi DECIMAL(10,2),                      -- ID: 61
    pdp_psi DECIMAL(10,2),                      -- ID: 62
    nivel_fluido_tvd DECIMAL(10,2),             -- ID: 59

    -- ==========================================
    -- 5. DINÁMICA DE SARTA (Card 6 & Bomba)
    -- ==========================================
    spm_actual DECIMAL(5,2),                    -- ID: 51
    llenado_bomba_pct DECIMAL(5,2),             -- ID: 64
    gas_fill_monitor DECIMAL(5,2),              -- ID: 96
    rod_weight_air_lb DECIMAL(10,2),            -- ID: 72
    rod_weight_buoyant_lb DECIMAL(10,2),        -- ID: 73
    api_max_load_lb DECIMAL(10,2),              -- ID: 75
    carga_unidad_pct DECIMAL(5,2),              -- ID: 80
    falla_vibracion_grados DECIMAL(5,2),        -- ID: 91

    -- ==========================================
    -- 6. PROFUNDIDADES (Diseño)
    -- ==========================================
    formation_depth_ft DECIMAL(10,2),           -- ID: 38
    pump_depth_ft DECIMAL(10,2),                -- ID: 39

    -- ==========================================
    -- 7. ENERGÍA Y RUNTIME
    -- ==========================================
    potencia_hp DECIMAL(10,2),                  -- ID: 66
    amperaje_a DECIMAL(10,2),                   -- ID: 67
    kpi_kwh_bbl DECIMAL(10,3),                  -- ID: 71
    eficiencia_sistema_pct DECIMAL(5,2),        -- ID: 118
    runtime_diario_pct DECIMAL(5,2),            -- ID: 106
    eventos_poc_hoy INT,                        -- ID: 115

    -- ==========================================
    -- 8. METADATOS Y CALIDAD
    -- ==========================================
    dq_status VARCHAR(10),                      -- (Calc: Calidad Dato)
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para Filtros Rápidos (Slicers en PowerBI)
CREATE INDEX idx_cur_cliente ON reporting.tbl_current_values(cliente);
CREATE INDEX idx_cur_region ON reporting.tbl_current_values(region);
CREATE INDEX idx_cur_campo ON reporting.tbl_current_values(campo);
CREATE INDEX idx_cur_status ON reporting.tbl_current_values(estado_comunicacion);