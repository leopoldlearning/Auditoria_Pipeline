# Comandos psql para Auditoría - Reemplazo de DBeaver

## EJECUTAR SCRIPT DE AUDITORÍA

Desde PowerShell (en una nueva terminal):

```powershell
cd D:\ITMeet\Operaciones\BP010-data-pipelines-auditoria
docker exec -i bp010-audit-db psql -U audit -d etl_data < 01_auditoria_esquemas.sql > auditoria_resultados.txt
```

Esto generará un archivo `auditoria_resultados.txt` con todos los resultados.

## CONSULTAS CLAVE PARA LA AUDITORÍA

Si estás en psql interactivo (`docker exec -it bp010-audit-db psql -U audit -d etl_data`):

### 1. Ver todos los esquemas creados
```sql
\dn
```

### 2. Ver todas las tablas por esquema
```sql
-- Stage
\dt stage.*

-- Referencial
\dt referencial.*

-- Reporting
\dt reporting.*

-- Universal
\dt universal.*
```

### 3. Contar registros en tablas clave
```sql
-- Referencial (debe tener datos de seed)
SELECT 'tbl_maestra_variables' AS tabla, COUNT(*) AS registros FROM referencial.tbl_maestra_variables
UNION ALL
SELECT 'tbl_dq_rules', COUNT(*) FROM referencial.tbl_dq_rules
UNION ALL
SELECT 'tbl_reglas_consistencia', COUNT(*) FROM referencial.tbl_reglas_consistencia;
```

### 4. Ver estructura de tablas principales
```sql
-- Stage
\d stage.tbl_pozo_maestra
\d stage.tbl_pozo_reservas

-- Referencial
\d referencial.tbl_maestra_variables

-- Reporting
\d reporting.dim_pozo
\d reporting.fact_operaciones_diarias
```

### 5. Exportar resultados a archivo
Desde psql:
```sql
\o auditoria_manual.txt
-- Ejecutar tus consultas
SELECT * FROM referencial.tbl_maestra_variables;
-- Más consultas...
\o
```

## GENERAR REPORTE COMPLETO

Ejecutar desde PowerShell:

```powershell
docker exec bp010-audit-db psql -U audit -d etl_data -c "
SELECT 
    schemaname AS esquema,
    tablename AS tabla,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS tamaño
FROM pg_tables
WHERE schemaname IN ('stage', 'universal', 'referencial', 'reporting')
ORDER BY schemaname, tablename;
" > esquemas_auditoria.txt
```

## CONTINUAR CON FASE 3: INGESTA DE DATOS

Una vez validados los esquemas, podemos proceder con:
1. Ejecutar notebooks de ingesta
2. Validar datos en stage
3. Ejecutar transformaciones
4. Validar reporting

**¿Quieres que continuemos con la Fase 3 usando psql para validación, dejando DBeaver de lado por ahora?**
