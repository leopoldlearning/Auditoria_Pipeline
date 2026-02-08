-- 02_validacion_kpis_dashboard.sql
-- Script de validación de calidad de datos y consistencia

-- 1. Resumen de Registros por Tabla
SELECT 'stage.tbl_pozo_maestra' as tabla, count(*) as total FROM stage.tbl_pozo_maestra
UNION ALL
SELECT 'stage.tbl_pozo_reservas', count(*) FROM stage.tbl_pozo_reservas
UNION ALL
SELECT 'reporting.dim_pozo', count(*) FROM reporting.dim_pozo
UNION ALL
SELECT 'reporting.dim_tiempo', count(*) FROM reporting.dim_tiempo
UNION ALL
SELECT 'reporting.dataset_current_values', count(*) FROM reporting.dataset_current_values;

-- 2. Validación de Integridad Referencial (Stage)
-- Verificar si hay reservas sin pozo maestro asociado
SELECT r.well_id, r.fecha_registro
FROM stage.tbl_pozo_reservas r
LEFT JOIN stage.tbl_pozo_maestra m ON r.well_id = m.well_id
WHERE m.well_id IS NULL;

-- 3. Validación de Reglas de Negocio (Reporting)
-- Verificar consistencia de estado vs colores (si aplica lógica Zero-Calc)
-- Nota: Como no tenemos datos de producción real (SCADA), verificamos metadatos.

-- 4. Verificación de Ingesta Reporting
-- Validar que los pozos de stage llegaron a reporting
SELECT s.well_id, s.nombre_pozo,
       CASE WHEN r.well_id IS NOT NULL THEN 'OK' ELSE 'MISSING' END as status_reporting
FROM stage.tbl_pozo_maestra s
LEFT JOIN reporting.dim_pozo r ON s.well_id = r.well_id;

-- 5. Validación de Nulos en Campos Críticos
SELECT well_id, nombre_pozo
FROM stage.tbl_pozo_maestra
WHERE well_id IS NULL OR nombre_pozo IS NULL;
