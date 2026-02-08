-- ╔════════════════════════════════════════════════════════════╗
-- ║  ⚡ DIAGNÓSTICO RÁPIDO DE NULLs - CONSULTAS ESENCIALES     ║
-- ╚════════════════════════════════════════════════════════════╝

-- Ejecuta estas 5 consultas para diagnóstico inmediato:

-- 1. ¿Llegaron datos al landing?
SELECT 'landing_scada_data' AS tabla, COUNT(*) AS registros, 
       COUNT(DISTINCT unit_id) AS pozos FROM stage.landing_scada_data;

-- 2. ¿Cuántos pozos maestros sin datos?
SELECT COUNT(*) AS pozos_sin_produccion FROM stage.tbl_pozo_maestra m
WHERE NOT EXISTS (SELECT 1 FROM stage.tbl_pozo_produccion p WHERE p.well_id = m.well_id);

-- 3. ¿Qué tan ancha es la tabla de producción? (% de NULLs)
SELECT 'tbl_pozo_produccion' AS tabla,
  ROUND(100.0 * COUNT(spm_promedio) / COUNT(*)) AS spm_cobertura_pct,
  ROUND(100.0 * COUNT(produccion_petroleo_diaria) / COUNT(*)) AS prod_cobertura_pct,
  ROUND(100.0 * COUNT(presion_cabezal) / COUNT(*)) AS presion_cobertura_pct
FROM stage.tbl_pozo_produccion;

-- 4. ¿Tiene cada pozo sus reservas?
SELECT COUNT(*) AS pozos_sin_reservas FROM stage.tbl_pozo_maestra m
WHERE NOT EXISTS (SELECT 1 FROM stage.tbl_pozo_reservas r WHERE r.well_id = m.well_id);

-- 5. ¿Llegaron datos a REPORTING?
SELECT 'FACT_OPERACIONES_DIARIAS' AS tabla, COUNT(*) AS registros,
  ROUND(100.0 * COUNT(Produccion_Petroleo_bbl) / COUNT(*)) AS produccion_cobertura_pct
FROM reporting.FACT_OPERACIONES_DIARIAS;
