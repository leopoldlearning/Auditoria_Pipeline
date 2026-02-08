# Plan de Acción: Resultados de Auditoría BP010

## Hallazgos Críticos - Acción Inmediata Requerida

### 🔴 CRÍTICO-01: DROP CASCADE en Scripts SQL
**Severidad**: CRÍTICA  
**Tiempo Estimado**: 1 día  
**Responsable**: Database Admin / DevOps

**Acción**:
1. Crear carpeta `src/sql/migrations/` para scripts de producción
2. Refactorizar scripts:
   - `init_dev.sql` → mantener DROP CASCADE (solo desarrollo)
   - `V3_to_V4_migrate_prod.sql` → usar ALTER TABLE (producción)
3. Agregar validación en todos los scripts V*.sql:
   ```sql
   DO $$
   BEGIN
       IF current_database() ~ 'prod|production' THEN
           RAISE EXCEPTION 'Scripts destructivos bloqueados en producción';
       END IF;
   END $$;
   ```

**Validación**: Ejecutar en staging sin DROP CASCADE

---

### 🟡 ALTO-01: Credenciales Hardcodeadas
**Severidad**: ALTA  
**Tiempo Estimado**: 2 días  
**Responsable**: Data Engineer

**Acción**:
1. Crear `.env.template` con todas las variables requeridas
2. Refactorizar notebooks:
   - Eliminar líneas 495-501 de `0_1_udf_to_stage_AWS_v0.ipynb`
   - Usar exclusivamente `os.getenv()` para DB_USER, DB_PASSWORD, etc.
3. Actualizar `.gitignore` para excluir `.env`
4. Documentar en README:
   ```markdown
   ## Variables de Entorno Requeridas
   - DB_HOST
   - DB_PORT
   - DB_USER
   - DB_NAME
   - DEV_DB_PASSWORD
   ```

**Validación**: Ejecutar notebooks sin credenciales visibles en código

---

### 🟡 MEDIO-01: SchemaManager Desactualizado
**Severidad**: MEDIA  
**Tiempo Estimado**: 1 día  
**Responsable**: ML Engineer

**Acción**:
1. Abrir `src/schema_manager.py`
2. Actualizar líneas:
   - L158: `V2__stage_schema.sql` → `V4__stage_schema_redesign.sql`
   - L198: `V1__reporting_schema.sql` → `V3__reporting_schema_redesign.sql`
3. Agregar referencial al SchemaManager:
   ```python
   def init_referencial_tables(self, engine: Engine):
       referencial_sql_path = self.schema_base_path / "V3__referencial_schema_redesign.sql"
       # ...
   ```
4. Actualizar tests de integración

**Validación**: Ejecutar procesos ML y verificar que usen schemas V3/V4

---

## Mejoras de Mediano Plazo

### 📋 Implementar Flyway
**Prioridad**: Media  
**Tiempo Estimado**: 3 días

**Pasos**:
1. `pip install flyway` (o usar Docker image)
2. Crear `flyway.conf`:
   ```ini
   flyway.url=jdbc:postgresql://localhost:5432/etl_data
   flyway.user=audit
   flyway.password=${DB_PASSWORD}
   flyway.locations=filesystem:src/sql/migrations
   ```
3. Renombrar scripts existentes al formato Flyway:
   - `V1__universal_schema.sql` → ya compatible ✓
   - `V3__referencial_schema_redesign.sql` → renombrar a `V3.1__referencial_redesign.sql`
4. Ejecutar: `flyway migrate`

---

### 📋 Parametrizar Notebooks
**Prioridad**: Media  
**Tiempo Estimado**: 2 días

**Cambios**:
1. Reemplazar:
   ```python
   while os.path.basename(os.getcwd()) != 'BP010-data-pipelines':
       os.chdir("../")
   ```
   Por:
   ```python
   PROJECT_ROOT = os.getenv('PROJECT_ROOT', Path(__file__).parent.parent)
   os.chdir(PROJECT_ROOT)
   ```

2. Agregar a `.env`:
   ```
   PROJECT_ROOT=D:\ITMeet\Operaciones\BP010-data-pipelines
   DATA_PATH=D:\ITMeet\Operaciones\BP010-data-pipelines\data
   ```

---

## Cronograma Propuesto

| Semana | Tarea | Responsable |
|--------|-------|-------------|
| 1 | CRÍTICO-01: Proteger scripts SQL | DB Admin |
| 1 | ALTO-01: Eliminar credenciales hardcodeadas | Data Engineer |
| 2 | MEDIO-01: Actualizar SchemaManager | ML Engineer |
| 2 | Crear tests de integración | QA/DevOps |
| 3 | Implementar Flyway | DevOps |
| 3 | Parametrizar notebooks | Data Engineer |
| 4 | Documentación y capacitación | Tech Lead |

---

## Checklist de Validación Pre-Producción

Antes de desplegar a producción, verificar:

- [ ] ✅ Scripts SQL **NO** contienen `DROP CASCADE`
- [ ] ✅ **Cero credenciales** hardcodeadas en código
- [ ] ✅ Todas las variables de entorno documentadas en README
- [ ] ✅ `.env` en `.gitignore`
- [ ] ✅ SchemaManager usa versiones correctas (V3/V4)
- [ ] ✅ Flyway configurado para migraciones
- [ ] ✅ Tests de integración pasando
- [ ] ✅ Backup de base de datos antes de migración
- [ ] ✅ Plan de rollback documentado
- [ ] ✅ Monitoreo configurado (alertas de errores)

---

## Contacto

**Para preguntas sobre este plan**:
- Arquitectura de datos: [Data Architect]
- Seguridad: [Security Lead]
- Implementación: [Tech Lead]

**Fecha de Creación**: 02 de Febrero de 2026  
**Fecha Límite Críticos**: 09 de Febrero de 2026  
**Próxima Revisión**: 16 de Febrero de 2026
