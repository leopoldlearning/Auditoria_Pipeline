# Hallazgo de Auditoría: Uso de DROP IF EXISTS CASCADE en Scripts de Migración

## Descripción del Problema

Los scripts de inicialización de esquemas (`V3__referencial_schema_redesign.sql`, `V3__reporting_schema_redesign.sql`, etc.) contienen la siguiente instrucción al inicio:

```sql
DROP SCHEMA IF EXISTS referencial CASCADE;
CREATE SCHEMA referencial;
```

## Riesgo Identificado

**Severidad: ALTA** en entornos de producción

Esta práctica es **destructiva** y puede causar:
- **Pérdida total de datos** si se ejecuta accidentalmente en producción
- **Inconsistencia de estado** si el script falla después del DROP pero antes de recrear las tablas
- **Imposibilidad de rollback** una vez ejecutado el DROP CASCADE

## Recomendaciones

### Para Entornos de Desarrollo/Pruebas ✅
El uso de `DROP IF EXISTS CASCADE` es **aceptable** porque:
- Permite reiniciar desde cero fácilmente
- Facilita pruebas repetitivas
- No hay datos críticos en riesgo

### Para Entornos de Producción ❌
**NUNCA** usar `DROP CASCADE` en scripts de migración. En su lugar:

1. **Usar herramientas de migración versionadas** (Flyway, Liquibase, Alembic):
   ```sql
   -- V3__add_new_column.sql
   ALTER TABLE referencial.tbl_maestra_variables 
   ADD COLUMN IF NOT EXISTS nueva_columna VARCHAR(100);
   ```

2. **Implementar migraciones incrementales**:
   - No borrar esquemas completos
   - Usar `ALTER TABLE` en lugar de `DROP/CREATE`
   - Mantener compatibilidad hacia atrás

3. **Separar scripts por entorno**:
   - `init_dev.sql` → Con DROP CASCADE para desarrollo
   - `migrate_prod_v3.sql` → Solo ALTER/CREATE sin DROP para producción

4. **Protecciones de seguridad**:
   ```sql
   -- Agregar validación de entorno
   DO $$
   BEGIN
       IF current_database() = 'etl_data_prod' THEN
           RAISE EXCEPTION 'DROP CASCADE prohibido en producción';
       END IF;
   END $$;
   ```

## Impacto en Esta Auditoría

Durante el proceso de auditoría, la ejecución repetida de estos scripts causó que el esquema `referencial` quedara temporalmente sin tablas, lo que demuestra la fragilidad del enfoque actual.

## Acción Recomendada

Refactorizar el sistema de migraciones del proyecto para:
1. Separar scripts de inicialización (desarrollo) de scripts de migración (producción)
2. Implementar una herramienta de control de versiones de BD (Flyway recomendado)
3. Documentar procedimientos de despliegue seguros
