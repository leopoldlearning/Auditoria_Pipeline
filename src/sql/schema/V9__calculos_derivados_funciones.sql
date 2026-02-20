/*
================================================================================
V9 - FUNCIONES DE CÁLCULO DERIVADO (SCHEMA: stage)
================================================================================
Fecha: 2026-02-09
Propósito: Funciones IMMUTABLE de cálculo físico/ingenieril
Ubicación: stage.fnc_calc_* (Zero-Calc Architecture)
Filosofía: REPORTING = solo datos pre-calculados. La lógica va en STAGE.

DEPENDENCIAS:
  - V4__stage_schema_redesign.sql (esquema stage debe existir)

NOTA: Los SPs que USAN estas funciones están en:
  → src/sql/process/V9__calculos_derivados_process.sql
================================================================================
*/

-- =============================================================================
-- 1.1 Calcular Nivel de Fluido TVD desde PIP
-- =============================================================================
CREATE OR REPLACE FUNCTION stage.fnc_calc_fluid_level_tvd(
    p_pip_psi DECIMAL,
    p_gravedad_api DECIMAL
) RETURNS DECIMAL AS $$
DECLARE
    v_sg DECIMAL;
BEGIN
    IF p_pip_psi IS NULL OR p_gravedad_api IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Convertir gravedad API a gravedad específica
    v_sg := 141.5 / (131.5 + p_gravedad_api);
    
    IF v_sg <= 0 THEN
        RETURN NULL;
    END IF;
    
    -- TVD = PIP / (0.433 * SG)
    RETURN ROUND(p_pip_psi / (0.433 * v_sg), 2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION stage.fnc_calc_fluid_level_tvd IS
'Calcula nivel de fluido TVD: TVD = PIP / (0.433 * SG). SG = 141.5/(131.5+API)';

-- =============================================================================
-- 1.2 Calcular Presión de Fondo Fluyente (Pwf)
-- =============================================================================
CREATE OR REPLACE FUNCTION stage.fnc_calc_pwf(
    p_pip_psi DECIMAL,
    p_fluid_level_tvd DECIMAL,
    p_gravedad_api DECIMAL,
    p_pump_depth DECIMAL
) RETURNS DECIMAL AS $$
DECLARE
    v_sg DECIMAL;
    v_hydrostatic DECIMAL;
BEGIN
    IF p_pip_psi IS NULL OR p_fluid_level_tvd IS NULL 
       OR p_gravedad_api IS NULL OR p_pump_depth IS NULL THEN
        RETURN NULL;
    END IF;
    
    v_sg := 141.5 / (131.5 + p_gravedad_api);
    
    IF p_pump_depth <= p_fluid_level_tvd THEN
        RETURN p_pip_psi;
    END IF;
    
    -- Pwf = PIP + (pump_depth - fluid_level) * 0.433 * SG
    v_hydrostatic := (p_pump_depth - p_fluid_level_tvd) * 0.433 * v_sg;
    RETURN ROUND(p_pip_psi + v_hydrostatic, 2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION stage.fnc_calc_pwf IS
'Calcula presión de fondo fluyente: Pwf = PIP + (depth - level) * 0.433 * SG';

-- =============================================================================
-- 1.3 Calcular Carga Hidráulica de Unidad (%)
-- =============================================================================
CREATE OR REPLACE FUNCTION stage.fnc_calc_hydralift_load_pct(
    p_max_rod_load DECIMAL,
    p_rated_klb DECIMAL
) RETURNS DECIMAL AS $$
BEGIN
    IF p_max_rod_load IS NULL OR p_rated_klb IS NULL OR p_rated_klb <= 0 THEN
        RETURN NULL;
    END IF;
    
    -- Load % = (max_rod_load / (rated_klb * 1000)) * 100
    RETURN ROUND((p_max_rod_load / (p_rated_klb * 1000)) * 100, 2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION stage.fnc_calc_hydralift_load_pct IS
'Calcula carga hidráulica %: (max_rod_load / (rated_klb * 1000)) * 100';

-- =============================================================================
-- 1.4 Calcular Road Load (%)
-- =============================================================================
CREATE OR REPLACE FUNCTION stage.fnc_calc_road_load_pct(
    p_max_rod_load DECIMAL,
    p_api_max_load DECIMAL
) RETURNS DECIMAL AS $$
BEGIN
    IF p_max_rod_load IS NULL OR p_api_max_load IS NULL OR p_api_max_load <= 0 THEN
        RETURN NULL;
    END IF;
    
    RETURN ROUND((p_max_rod_load / p_api_max_load) * 100, 2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION stage.fnc_calc_road_load_pct IS
'Calcula road load %: (max_rod_load / api_max_load) * 100';

-- =============================================================================
-- 1.5 Calcular Varianza Porcentual Genérica
-- =============================================================================
CREATE OR REPLACE FUNCTION stage.fnc_calc_variance_pct(
    p_actual DECIMAL,
    p_target DECIMAL
) RETURNS DECIMAL AS $$
BEGIN
    IF p_actual IS NULL OR p_target IS NULL OR p_target = 0 THEN
        RETURN NULL;
    END IF;
    
    RETURN ROUND(((p_actual - p_target) / p_target) * 100, 2);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION stage.fnc_calc_variance_pct IS
'Calcula varianza porcentual genérica: ((actual - target) / target) * 100';
