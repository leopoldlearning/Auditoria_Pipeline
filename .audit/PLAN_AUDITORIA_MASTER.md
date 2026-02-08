# PLAN MAESTRO DE AUDITORÍA - BP010 DATA PIPELINES
**Fecha creación**: 2026-02-07 16:16:00  
**Estado inicial**: BACKUP creado (84.24 MB)  
**Objetivo**: Pipeline completo funcional con schema reporting completo

---

## 🎯 OBJETIVOS PRINCIPALES

### 1. **Variables Faltantes en Reporting**
- [ ] Verificar `current_stroke_length_act` en `reporting.dataset_current_values`
- [ ] Verificar `pump_avg_spm_act` en `reporting.dataset_current_values`
- [ ] Crear variables derivadas (_target, _var_pct, _status_color, _severity_label)

### 2. **Coherencia Variables: 01_maestra_variables.csv vs V3__reporting_schema_redesign.sql**
- [ ] Extraer todas las variables definidas en `01_maestra_variables.csv` (columna: "Nombre en el esquema REPORTING")
- [ ] Comparar con columnas existentes en `V3__reporting_schema_redesign.sql`
- [ ] Identificar GAPS y crear listado de variables faltantes

### 3. **Esquema Referencial - Integración Completa**
- [ ] Cargar `Rangos_validacion_variables_petroleras_limpio.py` en `referencial.tbl_limites_pozo`
- [ ] Integrar reglas de calidad desde `02_reglas_calidad.csv` → `referencial.dq_rules`
- [ ] Integrar reglas de consistencia desde `03_reglas_consistencia.csv` → `referencial.tbl_reglas_consistencia`
- [ ] Crear/actualizar `referencial.tbl_var_scada_map` con mapeo completo SCADA→Stage→Reporting

### 4. **Paneles BI - Catálogo Completo**
- [ ] Extraer paneles únicos de `hoja_validacion.csv` (columna "Panel(es) de Uso")
- [ ] Crear/popular `referencial.tbl_ref_paneles_bi`
- [ ] Asignar IDs únicos a cada elemento del dashboard

### 5. **Nomenclatura y Mapeo**
- [ ] Auditar nombres de variables en Stage vs Reporting
- [ ] Verificar IDs de Formato1 en toda la cadena (SCADA → Landing → Raw → Stage → Reporting)
- [ ] Crear vista consolidada de trazabilidad completa

### 6. **Stored Procedures y Lógica de Cálculo**
- [ ] Revisar `V5__stored_procedures.SQL` para cálculo de variables derivadas
- [ ] Asegurar que todas las variables _target, _color, _severity se calculan correctamente
- [ ] Validar que use valores desde `referencial` (Zero-Hardcode)

### 7. **Ejecución y Validación**
- [ ] Ejecutar `MASTER_PIPELINE_RUNNER.py` en entorno `auditor`
- [ ] Verificar que todas las tablas se pueblan correctamente
- [ ] Auditar valores nulos en Stage, Referencial y Reporting
- [ ] Generar reporte de calidad de datos

---

## 📋 FASES DE EJECUCIÓN

### **FASE 1: ANÁLISIS Y DIAGNÓSTICO** (2-3 horas)
1. Inventario completo de variables (maestra, stage, reporting)
2. Análisis de gaps y discrepancias
3. Generación de matriz de trazabilidad

### **FASE 2: CORRECCIÓN DE ESQUEMAS** (3-4 horas)
4. Modificar `V3__reporting_schema_redesign.sql`
5. Actualizar esquema `referencial`
6. Crear/actualizar vistas de mapeo

### **FASE 3: INTEGRACIÓN DE DATOS REFERENCIALES** (2-3 horas)
7. Cargar límites del cliente
8. Cargar reglas de calidad
9. Cargar reglas de consistencia
10. Popular catálogo de paneles BI

### **FASE 4: VALIDACIÓN Y EJECUCIÓN** (2-3 horas)
11. Ejecutar pipeline completo
12. Validar poblamiento de tablas
13. Auditar nulos y calidad de datos
14. Generar reporte final

---

## 🔍 ARCHIVOS CLAVE A REVISAR

### Inputs Referenciales
- `inputs_referencial/01_maestra_variables.csv` ✓ (170 líneas)
- `inputs_referencial/02_reglas_calidad.csv` ✓ (37 líneas)
- `inputs_referencial/03_reglas_consistencia.csv` ✓ (7 reglas)
- `inputs_referencial/Rangos_validacion_variables_petroleras_limpio.py`
- `data/hoja_validacion.csv` ✓ (182 líneas)

### Esquemas SQL
- `src/sql/schema/V3__reporting_schema_redesign.sql`
- `src/sql/schema/V2__referencial_schema.sql`
- `src/sql/schema/V1__stage_schema.sql`
- `src/sql/schema/V5__stored_procedures.SQL`

### ETL/Pipeline
- `MASTER_PIPELINE_RUNNER.py`
- `src/sql/transformations/*.sql`

---

## 📊 CRITERIOS DE ÉXITO

✅ **Pipeline ejecuta de principio a fin sin errores**  
✅ **Todas las variables de `01_maestra_variables.csv` existen en reporting**  
✅ **Esquema `referencial` contiene todos los límites, reglas y mapeos**  
✅ **No hay valores nulos inesperados en `reporting.dataset_current_values`**  
✅ **Trazabilidad completa SCADA → Landing → Raw → Stage → Reporting**  
✅ **Variables derivadas (_color, _status, _target) se calculan correctamente**  
✅ **Dashboard puede consumir directamente desde reporting sin cálculos adicionales**

---

## ⚠️ REGLAS DE MODIFICACIÓN

- ❌ **NO MODIFICAR NADA EN**: `D:\ITMeet\Operaciones\BP010-data-pipelines`
- ✅ **SÍ MODIFICAR EN**: `D:\ITMeet\Operaciones\BP010-data-pipelines-auditoria`
- 🔍 **Consultar para referencia**: BP010-data-pipelines (solo lectura)
- 💾 **Backup disponible**: `BP010-data-pipelines-auditoria-BACKUPS\BACKUP_INICIAL_*.zip`
- 🔄 **Rollback**: Ejecutar `ROLLBACK_AUDITORIA.ps1`

---

**Inicio de auditoría**: 2026-02-07 16:16:00  
**Responsable**: Agente Antigravity  
**Estado**: EN CURSO
