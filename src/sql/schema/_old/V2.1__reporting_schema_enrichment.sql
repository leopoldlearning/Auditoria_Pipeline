/*
------------------------------------------------------------------------------------------------------------------------
-- SCRIPT INFORMATION BLOCK
------------------------------------------------------------------------------------------------------------------------
-- FILE NAME:         V2.1__reporting_schema_enrichment.sql
-- DESCRIPTION:       Enriquecimiento Reporting para Dashboard (Dimensiones y Hechos)
-- AUTHOR:            ITMEET / Data Architecture
-- DATE:              2025-12-12
------------------------------------------------------------------------------------------------------------------------
*/

BEGIN;

-- =============================================================================
-- 1. ACTUALIZACIÓN DE DIMENSIÓN: DIM_POZO
-- Variables estáticas o de diseño (IDs 72, 75, 39, 38, 46, 128)
-- =============================================================================
ALTER TABLE reporting.DIM_POZO
    ADD COLUMN IF NOT EXISTS Rod_Weight_In_Air_lb DECIMAL(10, 2),      -- ID 72
    ADD COLUMN IF NOT EXISTS API_Max_Fluid_Load_lb DECIMAL(10, 2),     -- ID 75
    ADD COLUMN IF NOT EXISTS Pump_Depth_ft DECIMAL(10, 2),             -- ID 39
    ADD COLUMN IF NOT EXISTS Formation_Depth_ft DECIMAL(10, 2),        -- ID 38
    ADD COLUMN IF NOT EXISTS Hydraulic_Load_Rated_klb DECIMAL(10, 2),  -- ID 46
    ADD COLUMN IF NOT EXISTS Total_Reserves_bbl DECIMAL(14, 2);        -- ID 128

-- =============================================================================
-- 2. ACTUALIZACIÓN DE HECHOS: FACT_OPERACIONES_HORARIAS
-- Variables dinámicas granulares + ID 65 (Fluid Flow)
-- =============================================================================
ALTER TABLE reporting.FACT_OPERACIONES_HORARIAS
    ADD COLUMN IF NOT EXISTS Lift_Efficiency_pct DECIMAL(5, 2),        -- ID 118
    ADD COLUMN IF NOT EXISTS Bouyant_Rod_Weight_lb DECIMAL(10, 2),     -- ID 73
    ADD COLUMN IF NOT EXISTS Fluid_Level_TVD_ft DECIMAL(10, 2),        -- ID 59
    ADD COLUMN IF NOT EXISTS PDP_psi DECIMAL(10, 2),                   -- ID 62
    ADD COLUMN IF NOT EXISTS Tank_Fluid_Temp_F DECIMAL(10, 2),         -- ID 94
    ADD COLUMN IF NOT EXISTS Motor_Power_Hp DECIMAL(10, 2),            -- ID 66
    ADD COLUMN IF NOT EXISTS Fluid_Flow_Monitor_bpd DECIMAL(10, 2),    -- ID 65
    ADD COLUMN IF NOT EXISTS Current_Stroke_Length_in DECIMAL(10, 2);  -- ID 68 (Solo Horaria)

-- =============================================================================
-- 3. ACTUALIZACIÓN DE HECHOS: FACT_OPERACIONES_DIARIAS
-- Variables dinámicas agregadas (Promedios) - Se excluye ID 68
-- =============================================================================
ALTER TABLE reporting.FACT_OPERACIONES_DIARIAS
    ADD COLUMN IF NOT EXISTS Promedio_Lift_Efficiency_pct DECIMAL(5, 2),
    ADD COLUMN IF NOT EXISTS Promedio_Bouyant_Rod_Weight_lb DECIMAL(10, 2),
    ADD COLUMN IF NOT EXISTS Promedio_Fluid_Level_TVD_ft DECIMAL(10, 2),
    ADD COLUMN IF NOT EXISTS Promedio_PDP_psi DECIMAL(10, 2),
    ADD COLUMN IF NOT EXISTS Promedio_Tank_Fluid_Temp_F DECIMAL(10, 2),
    ADD COLUMN IF NOT EXISTS Promedio_Motor_Power_Hp DECIMAL(10, 2),
    ADD COLUMN IF NOT EXISTS Promedio_Fluid_Flow_Monitor_bpd DECIMAL(10, 2);

-- =============================================================================
-- 4. ACTUALIZACIÓN DE HECHOS: FACT_OPERACIONES_MENSUALES
-- Variables dinámicas agregadas (Promedios)
-- =============================================================================
ALTER TABLE reporting.FACT_OPERACIONES_MENSUALES
    ADD COLUMN IF NOT EXISTS Promedio_Lift_Efficiency_pct DECIMAL(5, 2),
    ADD COLUMN IF NOT EXISTS Promedio_Bouyant_Rod_Weight_lb DECIMAL(10, 2),
    ADD COLUMN IF NOT EXISTS Promedio_Fluid_Level_TVD_ft DECIMAL(10, 2),
    ADD COLUMN IF NOT EXISTS Promedio_PDP_psi DECIMAL(10, 2),
    ADD COLUMN IF NOT EXISTS Promedio_Tank_Fluid_Temp_F DECIMAL(10, 2),
    ADD COLUMN IF NOT EXISTS Promedio_Motor_Power_Hp DECIMAL(10, 2),
    ADD COLUMN IF NOT EXISTS Promedio_Fluid_Flow_Monitor_bpd DECIMAL(10, 2);

COMMIT;