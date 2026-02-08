-- 01_auditoria_esquemas.sql
-- Validación de esquemas creados vs definiciones SQL

-- ============================================================================
-- SECCIÓN 1: VERIFICAR ESQUEMAS EXISTENTES
-- ============================================================================

SELECT 
    'ESQUEMAS CREADOS' AS categoria,
    schema_name,
    NULL::integer AS num_tablas
FROM information_schema.schemata 
WHERE schema_name IN ('stage', 'universal', 'referencial', 'reporting')

UNION ALL

SELECT 
    'CONTEO DE TABLAS',
    schemaname,
    COUNT(*)::integer
FROM pg_tables
WHERE schemaname IN ('stage', 'universal', 'referencial', 'reporting')
GROUP BY schemaname
ORDER BY categoria, schema_name;

-- ============================================================================
-- SECCIÓN 2: DETALLE DE TABLAS POR ESQUEMA
-- ============================================================================

-- STAGE
SELECT 'STAGE' AS esquema, tablename AS tabla
FROM pg_tables
WHERE schemaname = 'stage'
ORDER BY tablename;

-- UNIVERSAL
SELECT 'UNIVERSAL' AS esquema, tablename AS tabla
FROM pg_tables
WHERE schemaname = 'universal'
ORDER BY tablename;

-- REFERENCIAL
SELECT 'REFERENCIAL' AS esquema, tablename AS tabla
FROM pg_tables
WHERE schemaname = 'referencial'
ORDER BY tablename;

-- REPORTING
SELECT 'REPORTING' AS esquema, tablename AS tabla
FROM pg_tables
WHERE schemaname = 'reporting'
ORDER BY tablename;

-- ============================================================================
-- SECCIÓN 3: VALIDACIÓN DE DATOS EN REFERENCIAL
-- ============================================================================

SELECT 
    'referencial.tbl_maestra_variables' AS tabla,
    COUNT(*)::integer AS registros
FROM referencial.tbl_maestra_variables

UNION ALL

SELECT 
    'referencial.tbl_dq_rules',
    COUNT(*)::integer
FROM referencial.tbl_dq_rules

UNION ALL

SELECT 
    'referencial.tbl_reglas_consistencia',
    COUNT(*)::integer
FROM referencial.tbl_reglas_consistencia;

-- ============================================================================
-- SECCIÓN 4: ESTRUCTURA DE TABLAS PRINCIPALES
-- ============================================================================

-- Columnas de stage.tbl_pozo_maestra
SELECT 
    'stage.tbl_pozo_maestra' AS tabla,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'stage' 
  AND table_name = 'tbl_pozo_maestra'
ORDER BY ordinal_position;

-- Columnas de referencial.tbl_maestra_variables
SELECT 
    'referencial.tbl_maestra_variables' AS tabla,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'referencial' 
  AND table_name = 'tbl_maestra_variables'
ORDER BY ordinal_position;

-- ============================================================================
-- NOTAS PARA EL AUDITOR
-- ============================================================================
-- 1. Verificar que todos los esquemas esperados existan
-- 2. Comparar el número de tablas con las definiciones SQL
-- 3. Validar que referencial tenga datos de seed cargados
-- 4. Revisar la estructura de columnas de tablas críticas
