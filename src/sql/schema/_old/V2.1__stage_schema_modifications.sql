/*
------------------------------------------------------------------------------------------------------------------------
-- SCRIPT INFORMATION BLOCK
------------------------------------------------------------------------------------------------------------------------
-- FILE NAME:         V2.1__stage_schema_modifications.sql
-- DESCRIPTION:       Actualización Stage (IDs 128, 65 y Limpieza DQ)
-- AUTHOR:            ITMEET / Data Architecture
-- DATE:              2025-12-12
------------------------------------------------------------------------------------------------------------------------
*/

BEGIN;

-- =============================================================================
-- 1. ACTUALIZACIÓN TABLA RESERVAS (Variable ID 128)
-- =============================================================================
-- Se agrega la reserva inicial teórica para cálculos de agotamiento.
ALTER TABLE stage.tbl_pozo_reservas
    ADD COLUMN IF NOT EXISTS reserva_inicial_teorica DECIMAL(14, 2);

COMMENT ON COLUMN stage.tbl_pozo_reservas.reserva_inicial_teorica IS 'ID 128: Total Reserves (bbl) - Reserva inicial teórica (Input Cliente)';

-- =============================================================================
-- 2. ACTUALIZACIÓN TABLA PRODUCCIÓN (Variable ID 65)
-- =============================================================================
-- Se agrega el monitor de flujo del VSD para comparativas de IPR.
ALTER TABLE stage.tbl_pozo_produccion
    ADD COLUMN IF NOT EXISTS fluid_flow_monitor_bpd DECIMAL(10, 2);

COMMENT ON COLUMN stage.tbl_pozo_produccion.fluid_flow_monitor_bpd IS 'ID 65: Fluid Flow Monitor (bpd) - Dato VSD crítico para validación de curva IPR';

-- =============================================================================
-- 3. LIMPIEZA DE ARQUITECTURA (Eliminación DQ Rules Local)
-- =============================================================================
-- Se elimina la tabla local. Las reglas se moverán al esquema REFERENCIAL.
-- Se usa CASCADE para eliminar cualquier vista o restricción dependiente.
DROP TABLE IF EXISTS stage.tbl_dq_rules CASCADE;

COMMIT;