/*
------------------------------------------------------------------------------------------------------------------------
-- SCRIPT INFORMATION BLOCK
------------------------------------------------------------------------------------------------------------------------
-- FILE NAME:         V2.2__stage_schema_modifications.sql
-- DESCRIPTION:       Actualización Stage (IDs 59)
-- AUTHOR:            ITMEET / Data Architecture
-- DATE:              2025-12-15
------------------------------------------------------------------------------------------------------------------------
*/

BEGIN;

-- =============================================================================
-- 1. ACTUALIZACIÓN TABLA PRODUCCIÓN (Variable ID 59)
-- =============================================================================
ALTER TABLE stage.tbl_pozo_produccion
    ADD COLUMN IF NOT EXISTS nivel_fluido_dinamico DECIMAL;

COMMENT ON COLUMN stage.tbl_pozo_produccion.nivel_fluido_dinamico IS 'ID 59: Fluid Level TVD, ft - Nivel de fluido dinámico (Variable de diseño)';

COMMIT;