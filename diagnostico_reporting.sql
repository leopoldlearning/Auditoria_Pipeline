-- ╔════════════════════════════════════════════════════════════════╗
-- ║  🔥 DIAGNÓSTICO DE CAPA REPORTING - ¿Por qué solo 1 registro?  ║
-- ╚════════════════════════════════════════════════════════════════╝

-- PASO 1: ¿Cuántos datos hay en STAGE listos para ir a REPORTING?
SELECT 'stage.tbl_pozo_produccion' AS tabla, 
       COUNT(*) AS total_registros,
       COUNT(DISTINCT well_id) AS pozos_unicos,
       MIN(timestamp_lectura) AS fecha_inicio,
       MAX(timestamp_lectura) AS fecha_fin
FROM stage.tbl_pozo_produccion;

-- PASO 2: ¿Cuántos llegaron a REPORTING?
SELECT 'reporting.FACT_OPERACIONES_DIARIAS' AS tabla,
       COUNT(*) AS total_registros,
       COUNT(DISTINCT well_id) AS pozos_unicos,
       MIN(fecha_operacion) AS fecha_inicio,
       MAX(fecha_operacion) AS fecha_fin
FROM reporting.FACT_OPERACIONES_DIARIAS;

-- PASO 3: Diferencia de pozos - ¿Cuáles pozos NO llegaron a REPORTING?
SELECT m.well_id, m.nombre_pozo,
       COUNT(pp.produccion_id) AS registros_stage,
       COALESCE(COUNT(fod.fecha_operacion), 0) AS registros_reporting
FROM stage.tbl_pozo_maestra m
LEFT JOIN stage.tbl_pozo_produccion pp ON m.well_id = pp.well_id
LEFT JOIN reporting.FACT_OPERACIONES_DIARIAS fod ON m.well_id = fod.well_id
GROUP BY m.well_id, m.nombre_pozo
ORDER BY registros_reporting DESC, registros_stage DESC;

-- PASO 4: ¿Hay registros en las tablas dimensionales?
SELECT 'dim_tiempo' AS tabla, COUNT(*) FROM reporting.dim_tiempo
UNION ALL
SELECT 'dim_hora' AS tabla, COUNT(*) FROM reporting.dim_hora
UNION ALL  
SELECT 'dim_pozos_operaciones' AS tabla, COUNT(*) FROM reporting.dim_pozos_operaciones;

-- PASO 5: ¿El último registro en REPORTING es reciente o antiguo?
SELECT fecha_operacion, well_id, 
       Produccion_Petroleo_bbl,
       Presion_Cabezal_psi,
       COUNT(*) as registros
FROM reporting.FACT_OPERACIONES_DIARIAS
GROUP BY fecha_operacion, well_id, Produccion_Petroleo_bbl, Presion_Cabezal_psi
ORDER BY fecha_operacion DESC
LIMIT 10;

-- PASO 6: ¿Hay errores de integridad referencial? (FKs rotas)
SELECT 'FACT → dim_tiempo' AS verificacion,
       COUNT(*) as registros_sin_FK
FROM reporting.FACT_OPERACIONES_DIARIAS f
WHERE NOT EXISTS (SELECT 1 FROM reporting.dim_tiempo d WHERE d.fecha = f.fecha_operacion)
UNION ALL
SELECT 'FACT → dim_pozos_operaciones' AS verificacion,
       COUNT(*) as registros_sin_FK
FROM reporting.FACT_OPERACIONES_DIARIAS f
WHERE NOT EXISTS (SELECT 1 FROM reporting.dim_pozos_operaciones d WHERE d.well_id = f.well_id);

-- PASO 7: ¿Cuál es el estado de la tabla FACT? (¿está truncada?)
SELECT 
    table_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||table_name)) AS tamanio,
    CASE 
        WHEN (SELECT COUNT(*) FROM reporting.FACT_OPERACIONES_DIARIAS) = 0 THEN 'VACÍA'
        WHEN (SELECT COUNT(*) FROM reporting.FACT_OPERACIONES_DIARIAS) = 1 THEN 'CASI VACÍA (1 fila)'
        WHEN (SELECT COUNT(*) FROM reporting.FACT_OPERACIONES_DIARIAS) < 100 THEN 'MUY PEQUEÑA (<100 filas)'
        ELSE 'NORMAL'
    END AS estado
FROM information_schema.tables
WHERE table_name = 'FACT_OPERACIONES_DIARIAS' AND table_schema = 'reporting';

-- PASO 8: Última ejecución de sp_load_to_reporting - ¿Fue exitosa?
-- (Busca en logs si existen, o verifica si la tabla fue truncada recientemente)
SELECT 
    schemaname, tablename,
    last_vacuum, last_autovacuum,
    last_analyze, last_autoanalyze
FROM pg_stat_user_tables
WHERE tablename = 'FACT_OPERACIONES_DIARIAS' AND schemaname = 'reporting';

-- PASO 9: ¿El problema es un WHERE muy restrictivo?
-- Verifica qué condiciones filtra sp_load_to_reporting
SELECT COUNT(*) as total_produccion,
       COUNT(CASE WHEN Produccion_Petroleo_bbl > 0 THEN 1 END) as con_produccion,
       COUNT(CASE WHEN Produccion_Petroleo_bbl IS NULL THEN 1 END) as nulls,
       COUNT(CASE WHEN timestamp_lectura > NOW() - INTERVAL '30 days' THEN 1 END) as ultimos_30_dias
FROM stage.tbl_pozo_produccion;

-- PASO 10: Compara estructura de STAGE vs REPORTING (¿Columnas diferentes?)
SELECT column_name, data_type FROM information_schema.columns 
WHERE table_schema = 'stage' AND table_name = 'tbl_pozo_produccion'
LIMIT 5;

SELECT column_name, data_type FROM information_schema.columns 
WHERE table_schema = 'reporting' AND table_name = 'FACT_OPERACIONES_DIARIAS'
LIMIT 5;
