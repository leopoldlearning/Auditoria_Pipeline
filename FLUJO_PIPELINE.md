# Flujo del Pipeline BP010 (guía para espejo en AWS Lambda)

**Última actualización:** 2026-02-16  
**Objetivo:** Referenciar archivos, responsabilidades y orden de ejecución del pipeline actual (local Docker + Python) para replicarlo en AWS (Lambdas + RDS/PostgreSQL).

---

## Arquitectura General

| Componente | Tecnología | Detalle |
|---|---|---|
| Base de datos | PostgreSQL 15.15 | Docker `bp010-audit-db`, puerto 5433, DB `etl_data` |
| Orquestador | Python 3.11.8 | [MASTER_PIPELINE_RUNNER.py](MASTER_PIPELINE_RUNNER.py) |
| Esquemas | 4 | `stage`, `referencial`, `reporting`, `universal` |
| DDL scripts | 15 archivos SQL | Agrupados en 7 familias (ver init_schemas.py) |
| Datos fuente | CSV/SQL dumps/Excel | Carpetas `inputs_referencial/`, `data/`, `API Hydrog manual/` |

---

## Esquemas y Tablas (resumen)

| Esquema | Tablas | Propósito |
|---|---|---|
| **stage** | 5 | Landing SCADA (EAV), maestra pozo, producción pivotada, reservas, resultados DQ |
| **referencial** | 13 | Unidades (38 std), estados operativos, paneles BI, maestra variables (130), mapa SCADA (65), límites pozo, reglas DQ (35), reglas consistencia (6), mapa DQ↔RC (13), funciones evaluación |
| **reporting** | 21 tablas + 8 vistas | Dims, facts (horaria/diaria/mensual particionada), snapshots, KPIs business, dataset_latest_dynacard |
| **universal** | 6 | CDI (patron, stroke, diagnostico, validacion_experta), IPR, ARPS — para ML |

---

## Visión General: 7 Fases

```
FASE 1: INIT (DDL)          → Crea 4 schemas + 15 scripts SQL (tablas, SPs, funciones, vistas)
FASE 2: LOAD (datos)         → 2.1 Referencial + 2.2 Ingesta telemetría + 2.3 Seeds
FASE 3: DQ (calidad)         → Valida 35 reglas (9 activas con SCADA map) contra stage
FASE 4: TRANSFORM (hechos)   → Stage → Reporting (horario/diario/mensual + snapshot)
FASE 5: ENRICH (negocio)     → Targets + semáforos V8 + derivados V9 + KPIs V7
FASE 6: DEFAULTS (baselines) → Rellena valores faltantes y configs base
FASE 7: CONSISTENCY (RC)     → Valida 6 reglas de consistencia física (RC-001..RC-006)
```

---

## Detalle por Fase

### FASE 1: INIT (DDL + SPs + Funciones)

**Script:** [init_schemas.py](init_schemas.py) ejecuta 15 archivos SQL en orden.

| Familia | Archivo | Contenido |
|---|---|---|
| 1 — DDL | [V4__stage_schema_redesign.sql](src/sql/schema/V4__stage_schema_redesign.sql) | `stage.*` (landing EAV, maestra, produccion, reservas, scada_dq) |
| 1 — DDL | [V4__referencial_schema_redesign.sql](src/sql/schema/V4__referencial_schema_redesign.sql) | `referencial.*` (unidades, estados, paneles, variables, mapa SCADA, límites, DQ rules, RC rules, consistencia_map, funciones eval, vistas) |
| 1 — DDL | [V2__universal_schema.sql](src/sql/schema/V2__universal_schema.sql) | `universal.*` (patron, stroke, diagnostico, validacion_experta, ipr, arps) |
| 1 — DDL | [V4__reporting_schema_redesign.sql](src/sql/schema/V4__reporting_schema_redesign.sql) | `reporting.*` (dims, facts particionadas, datasets, dynacard) |
| 2 — Funciones | [V9__calculos_derivados_funciones.sql](src/sql/schema/V9__calculos_derivados_funciones.sql) | `stage.fnc_calc_*` (fluid_level, pwf, hydralift, road_load, variance) |
| 2 — Funciones | [V7__sistema_clasificacion_universal.sql](src/sql/schema/V7__sistema_clasificacion_universal.sql) | `referencial.fnc_evaluar_variable()` + config_evaluacion + catalogo_status |
| 3 — SPs ETL | [V6.1__historical_reporting_engine_v4.sql](src/sql/schema/V6.1__historical_reporting_engine_v4.sql) | `reporting.sp_load_to_reporting()` (horario/diario/mensual) |
| 3 — SPs ETL | [V6.2__dq_engine_v4.sql](src/sql/schema/V6.2__dq_engine_v4.sql) | `stage.sp_execute_dq_validation()` (usa SCADA map para traducción columnas) + `stage.sp_execute_consistency_validation()` |
| 3 — SPs ETL | [V6.3__sync_dim_pozo_targets_v4.sql](src/sql/schema/V6.3__sync_dim_pozo_targets_v4.sql) | `reporting.sp_sync_dim_pozo_targets()` |
| 3 — SPs ETL | [V6__stored_procedures_v4_compatible.sql](src/sql/schema/V6__stored_procedures_v4_compatible.sql) | `reporting.actualizar_current_values_v4()` (snapshot zero-calc) |
| 4 — KPI | [V7__kpi_business_redesign.sql](src/sql/schema/V7__kpi_business_redesign.sql) | `dataset_kpi_business` + `reporting.poblar_kpi_business()` |
| 5 — Evaluación | [V8__evaluacion_semaforos_reporting.sql](src/sql/schema/V8__evaluacion_semaforos_reporting.sql) | `reporting.aplicar_evaluacion_universal()` (SET-BASED, escala 0-9) + `sp_populate_defaults()` |
| 5 — Derivados | [V9__calculos_derivados_process.sql](src/sql/process/V9__calculos_derivados_process.sql) | `reporting.sp_calcular_derivados_completos()` |
| 6 — Bridge | [V10__universal_to_reporting_bridge.sql](src/sql/schema/V10__universal_to_reporting_bridge.sql) | `sp_sync_cdi/ipr/arps_to_reporting()` — puente universal→reporting (3 SPs, se invocan cuando ML escriba datos) |
| 7 — Vistas | [V12__vistas_helper_frontend.sql](src/sql/schema/V12__vistas_helper_frontend.sql) | `vw_dashboard_main`, `vw_kpi_daily/monthly`, `vw_well_selector`, `vw_alerts` |

**Resultado:** 4 schemas creados, ~45 tablas, ~20 SPs/funciones, 8+ vistas.

---

### FASE 2: LOAD (Referencial + Ingesta + Seeds)

#### 2.1 Referencial Master — [load_referencial.py](load_referencial.py)

Carga el catálogo completo en `referencial.*` desde CSVs:

| Paso | Fuente | Destino | Detalle |
|---|---|---|---|
| Unidades estándar | [06_unidades_standar.csv](inputs_referencial/06_unidades_standar.csv) | `tbl_ref_unidades` (38 filas) | Catálogo canónico: símbolo + nombre completo |
| Mapa de unidades | [05_unidades.csv](inputs_referencial/05_unidades.csv) | Normalización interna | Traduce unidades crudas (`PSI`, `Pies`) → estándar (`psi`, `ft`) via `UNIT_NORMALIZE` |
| Estados operativos | Hardcoded | `tbl_ref_estados_operativos` (5 filas) | NORMAL/WARNING/CRITICAL/OFFLINE/UNKNOWN |
| Paneles BI | [hoja_validacion.csv](data/hoja_validacion.csv) | `tbl_ref_paneles_bi` | Paneles del dashboard |
| Maestra variables | [Variables_ID_stage.csv](data/Variables_ID_stage.csv) | `tbl_maestra_variables` (130 filas) | id_formato1-nombre_tecnico, vinculado a unidad y panel |
| Límites pozo | [Rangos_validacion_variables_petroleras_limpio.py](inputs_referencial/Rangos_validacion_variables_petroleras_limpio.py) | `tbl_limites_pozo` (17 filas) | min/max warning/critical + targets por variable |
| Reglas DQ | [02_reglas_calidad.csv](inputs_referencial/02_reglas_calidad.csv) | `tbl_dq_rules` (35 filas) | Parsing inteligente: Representatividad (`>0`→min=0.0001, `0-100%`→min=0/max=100), Latencia (`<2s`→2seg) |
| Reglas consistencia | Hardcoded (6 RC) | `tbl_reglas_consistencia` (6 filas) | RC-001..RC-006 con variable_medida/referencia |
| Mapa DQ↔RC | [02_reglas_calidad.csv](inputs_referencial/02_reglas_calidad.csv) col. Consistencia | `tbl_dq_consistencia_map` (13 filas) | Junction table: qué variables participan en qué RC |
| Enriquecimiento DQ | `tbl_limites_pozo` | `tbl_dq_rules.valor_max` | Solo donde CSV no define max (WHP→2000, CHP→2000) |
| Mapa SCADA | [V1__stage_to_stage.sql](src/sql/process/_archive/V1__stage_to_stage.sql) (regex) | `tbl_var_scada_map` (65 filas) | Traduce id_formato1 ↔ columna_stage real |

#### 2.2 Ingesta Telemetría — [ingest_real_telemetry.py](ingest_real_telemetry.py)

| Fuente | Destino | Tipo |
|---|---|---|
| `tbl_maestra_*.sql` | `stage.tbl_pozo_maestra` | SQL dump |
| `tbl_pozo_produccion_*.sql` | `stage.tbl_pozo_produccion` | SQL dump (1 registro pre-pivotado) |
| `landing_scada_data_*.sql` | `stage.landing_scada_data` | SQL dump (EAV: var_id + measure) |
| Excel reservas | `stage.tbl_reservas_pozo` | Excel |

#### 2.3 Seeds — `CALL referencial.sp_seed_defaults()`

Completa datos faltantes: `baseline = target`, `critical = warning * factor`, volatilidad por clasificación, corrección target kWh/bbl.

---

### FASE 3: DQ (Validación de Calidad de Datos)

**Procedimiento:** `CALL stage.sp_execute_dq_validation(fecha_inicio, fecha_fin, well_id)`  
**Definido en:** [V6.2__dq_engine_v4.sql](src/sql/schema/V6.2__dq_engine_v4.sql)

**Flujo interno:**
1. Para cada regla en `tbl_dq_rules`, obtiene `columna_stage` via `LEFT JOIN tbl_var_scada_map` (traducción nombre_tecnico → columna real)
2. Verifica existencia de columna numérica en `stage.tbl_pozo_produccion` via `information_schema`
3. Ejecuta validación: `valor < min → FAIL` | `valor > max → FAIL` | else `PASS`
4. Escribe resultados en `stage.tbl_pozo_scada_dq`

**Cobertura actual:** 9 de 35 reglas ejecutables (variables con columna numérica en stage). Las 26 restantes son de diseño/yacimiento (no viven en producción SCADA) o TEXT (dinagráficas).

---

### FASE 4: TRANSFORM (Stage → Reporting Facts + Snapshot)

#### 4.1 Facts — `CALL reporting.sp_load_to_reporting(fecha_inicio, fecha_fin, TRUE, TRUE, TRUE)`
- **Horario:** CTE `base_horaria` + `deltas_calculados` → `fact_operaciones_horarias`
- **Diario:** CTE `datos_diarios` + `kpis_calculados` → `fact_operaciones_diarias`
- **Mensual:** Agregación desde diarias → `fact_operaciones_mensuales`

#### 4.2 Snapshot — `CALL reporting.actualizar_current_values_v4()`
- Zero-Calc: copia datos más recientes de hechos a `dataset_current_values`

---

### FASE 5: ENRICH (Targets + Semáforos + Derivados + KPIs)

| Paso | Procedimiento | Archivo | Acción |
|---|---|---|---|
| 5.1 | `reporting.sp_sync_dim_pozo_targets()` | [V6.3](src/sql/schema/V6.3__sync_dim_pozo_targets_v4.sql) | Sincroniza targets de referencial → reporting.dim_pozo |
| 5.2 | `reporting.aplicar_evaluacion_universal()` | [V8](src/sql/schema/V8__evaluacion_semaforos_reporting.sql) | SET-BASED: evalúa semáforos (escala 0-9) sobre dataset_current_values |
| 5.3 | `reporting.sp_calcular_derivados_completos()` | [V9](src/sql/process/V9__calculos_derivados_process.sql) | Derivados en current_values, horarios, KPIs horarios, promedios diarios, reagregación mensual |
| 5.4 | `reporting.poblar_kpi_business()` | [V7](src/sql/schema/V7__kpi_business_redesign.sql) | KPIs de negocio wide-table: producción, declinación, eficiencia |

---

### FASE 6: DEFAULTS (Baselines)

**Procedimiento:** `CALL reporting.sp_populate_defaults()`  
**Definido en:** [V8__evaluacion_semaforos_reporting.sql](src/sql/schema/V8__evaluacion_semaforos_reporting.sql)  
**Acción:** Rellena baselines, configs faltantes y parámetros desde `tbl_config_kpi`.

---

### FASE 7: CONSISTENCY (Reglas de Consistencia Física)

**Procedimiento:** `CALL stage.sp_execute_consistency_validation()`  
**Definido en:** [V6.2__dq_engine_v4.sql](src/sql/schema/V6.2__dq_engine_v4.sql)

| Regla | Validación | Severidad |
|---|---|---|
| RC-001 | `max_rod_load > min_rod_load` | CRITICAL |
| RC-002 | `max_rod_load > rod_weight_buoyant` | HIGH |
| RC-003 | `well_head_pressure < FBHP` | CRITICAL |
| RC-004 | `FBHP < presion_estatica_yacimiento` | HIGH |
| RC-005 | `profundidad_bomba < profundidad_yacimiento` | MEDIUM |
| RC-006 | `radio_pozo < radio_drenaje` | MEDIUM |

Evalúa contra `reporting.dataset_current_values`. Reporta violaciones vía `RAISE NOTICE`.

---

## Archivos de Configuración / Datos

| Archivo | Tipo | Rol |
|---|---|---|
| `inputs_referencial/02_reglas_calidad.csv` | CSV | 35 reglas DQ: representatividad, latencia, consistencia |
| `inputs_referencial/05_unidades.csv` | CSV | Mapeo id_formato1 → unidad cruda |
| `inputs_referencial/06_unidades_standar.csv` | CSV | 38 unidades estándar (nombre + abreviatura canónica) |
| `inputs_referencial/Rangos_validacion_*.py` | Python dict | Límites operativos por variable (min/max warning/critical) |
| `data/Variables_ID_stage.csv` | CSV | Verdad de IDs: id_formato1 → nombre_tecnico |
| `data/hoja_validacion.csv` | CSV | Mapeo variable → panel BI + ident dashboard |
| `src/sql/process/_archive/V1__stage_to_stage.sql` | SQL | Fuente para regex → mapa SCADA (var_id_scada ↔ columna_stage) |
| `API Hydrog manual/*.sql` | SQL dump | Datos mock: maestra, producción, landing SCADA |

---

## Puente Universal → Reporting (Familia 6)

**Archivo:** [V10__universal_to_reporting_bridge.sql](src/sql/schema/V10__universal_to_reporting_bridge.sql)

3 SPs creados pero **no invocados automáticamente** en el pipeline (universal.* está vacío hasta que los módulos ML escriban datos):

| SP | Origen | Destino |
|---|---|---|
| `sp_sync_cdi_to_reporting()` | universal.stroke + diagnostico + patron | dataset_current_values (ai_accuracy_*), dataset_latest_dynacard, fact_horarias |
| `sp_sync_ipr_to_reporting()` | universal.ipr_resultados | dataset_current_values (ipr_qmax, eficiencia), fact_horarias |
| `sp_sync_arps_to_reporting()` | universal.arps_resultados | dataset_kpi_business (eur_remanente_bbl) |

---

## Migración a AWS: Diagnóstico y Corrección

### ⚠️ Problemas Detectados (Lambdas Existentes en BP010-data-pipelines/docker/)

El código actual en `BP010-data-pipelines/docker/` tiene 7 brechas críticas:

| # | Problema | Archivo afectado | Impacto |
|---|---|---|---|
| 1 | **No hay INIT DDL** — Ninguna Lambda despliega los 15 SQL files | Ambas Lambdas | RDS no tiene schemas ni SPs |
| 2 | **No hay carga referencial** — No existe equivalente a `load_referencial.py` | Ambas Lambdas | Variables/reglas/unidades/SCADA map = 0 filas |
| 3 | **No hay `sp_seed_defaults()`** | Ambas Lambdas | Baselines, umbrales, volatilidades = NULL |
| 4 | **Reporting Lambda obsoleta** — Usa `V1__stage_to_reporting_daily.sql` (SQL crudo pre-V4) | `rds-reporting-etl-project/` | No ejecuta ningún SP V4 |
| 5 | **Fases 3,5,6,7 inexistentes** — DQ, evaluación, derivados, KPIs, consistency no se llaman | `rds-reporting-etl-project/` | Dashboard sin semáforos ni KPIs |
| 6 | **Stage Lambda borra landing** — `DELETE FROM stage.landing_scada_data` antes de insertar | `rds-stage-etl-project/` | Pierde historial |
| 7 | **FLUJO_PIPELINE.md describía Step Functions inexistentes** | Documentación | Guía engañosa |

### ✅ Corrección: 3 Lambdas + Arquitectura Target

```
                            ┌──────────────────────────────────┐
                            │   UNA VEZ (Deploy / Schema Change)│
                            │                                  │
                            │  Lambda 1: INIT RDS              │
                            │  aws/lambda_init_rds.py          │
                            │  → DROP+CREATE 4 schemas         │
                            │  → Ejecuta 15 SQL files          │
                            │                                  │
                            │  Lambda 2: LOAD REFERENCIAL      │
                            │  aws/lambda_load_referencial.py  │
                            │  → CSVs desde S3 → referencial.* │
                            │  → sp_seed_defaults()            │
                            └──────────────┬───────────────────┘
                                           │
          ┌────────────────────────────────┼──────────────────────────────────┐
          │                                │    RECURRENTE (cada ciclo)       │
          │                                ▼                                 │
          │  ┌─────────────────────────────────────────┐                     │
          │  │  Lambda Stage (existente)               │                     │
          │  │  rds-stage-etl-project/                 │                     │
          │  │  1. API SCADA → landing_scada_data      │                     │
          │  │  2. V1__stage_to_stage.sql (pivot)      │                     │
          │  └────────────────┬────────────────────────┘                     │
          │                   │                                              │
          │                   ▼                                              │
          │  ┌─────────────────────────────────────────┐                     │
          │  │  Lambda 3: PIPELINE V4 (NUEVO)          │                     │
          │  │  aws/lambda_pipeline_v4.py              │                     │
          │  │                                         │                     │
          │  │  F3: CALL stage.sp_execute_dq_validation│                     │
          │  │  F4: CALL reporting.sp_load_to_reporting│                     │
          │  │      CALL reporting.actualizar_current_v│                     │
          │  │  F5: CALL reporting.sp_sync_dim_pozo_tar│                     │
          │  │      CALL reporting.aplicar_evaluacion_u│                     │
          │  │      CALL reporting.sp_calcular_derivado│                     │
          │  │      CALL reporting.poblar_kpi_business │                     │
          │  │  F6: CALL reporting.sp_populate_defaults│                     │
          │  │  F7: CALL stage.sp_execute_consistency_v│                     │
          │  └─────────────────────────────────────────┘                     │
          └──────────────────────────────────────────────────────────────────┘
```

### Lambdas Creadas (carpeta `aws/`)

| Lambda | Archivo | Trigger | Qué hace |
|---|---|---|---|
| **INIT RDS** | [aws/lambda_init_rds.py](aws/lambda_init_rds.py) | Manual / CodePipeline | DROP + CREATE 4 schemas, 15 SQL files (DDL, SPs, funciones, vistas) |
| **LOAD REFERENCIAL** | [aws/lambda_load_referencial.py](aws/lambda_load_referencial.py) | Después de INIT / config change | CSVs desde S3 → tbl_ref_unidades (38), tbl_maestra_variables (130), tbl_dq_rules (35), tbl_var_scada_map (65), tbl_limites_pozo (17), RC rules (6), junction map (13) + sp_seed_defaults() |
| **PIPELINE V4** | [aws/lambda_pipeline_v4.py](aws/lambda_pipeline_v4.py) | EventBridge cron / Step Functions después de Stage Lambda | Fases 3→7: DQ → Facts → Snapshot → Targets → Semáforos → Derivados → KPIs → Defaults → Consistency |

### Datos en S3 (para Lambda LOAD REFERENCIAL)

```
s3://{S3_BUCKET_CONFIG}/config/
  ├── Variables_ID_stage.csv          → Maestra variables (source of truth)
  ├── 02_reglas_calidad.csv           → 35 reglas DQ + consistencia
  ├── 05_unidades.csv                 → Mapeo unidades crudas
  ├── 06_unidades_standar.csv         → 38 unidades estándar
  ├── hoja_validacion.csv             → Paneles BI
  ├── Rangos_validacion_*.py          → Límites operativos
  └── V1__stage_to_stage.sql          → Para regex → SCADA map
```

### Env vars requeridas por Lambda

| Variable | Lambda | Valor |
|---|---|---|
| `TARGET_SECRET_NAME` | Todas | Nombre del secreto en Secrets Manager |
| `AWS_REGION` | Todas | e.g. `us-east-1` |
| `S3_BUCKET_SQL` | INIT RDS | Bucket con los 15 SQL files en `sql/schema/` + `sql/process/` |
| `S3_BUCKET_CONFIG` | LOAD REFERENCIAL | Bucket con CSVs en `config/` |
| `LOOKBACK_DAYS` | PIPELINE V4 | Default `3650` (~10 años) |
| `API_SECRET_NAME` | Stage Lambda | Secreto de credenciales SCADA API |

### Fix adicional recomendado para Stage Lambda existente

En `rds-stage-etl-project/lambda_handler.py`, reemplazar:
```python
# ❌ ANTES (destructivo — pierde historial):
execute_sql(conn, "DELETE FROM stage.landing_scada_data;")

# ✅ DESPUÉS (preserva historial, inserta nuevos):
# No truncar. El INSERT con ON CONFLICT ya maneja duplicados.
# O si se necesita ventana:
execute_sql(conn, """
    DELETE FROM stage.landing_scada_data
    WHERE moddate < CURRENT_DATE - INTERVAL '30 days';
""")
```

### Orden de Deploy AWS (Paso a Paso)

```
1. Subir SQL files a S3:      s3://{bucket}/sql/schema/*.sql + sql/process/*.sql
2. Subir CSVs config a S3:    s3://{bucket}/config/*.csv + *.py + *.sql
3. Deploy Lambda INIT RDS:    → Ejecutar UNA VEZ → Crea schemas + SPs
4. Deploy Lambda REFERENCIAL:  → Ejecutar UNA VEZ → Carga catálogos
5. Verificar en RDS:
   - SELECT count(*) FROM referencial.tbl_maestra_variables;    → 130
   - SELECT count(*) FROM referencial.tbl_var_scada_map;        → 65
   - SELECT count(*) FROM referencial.tbl_dq_rules;             → 35
   - SELECT count(*) FROM referencial.tbl_ref_unidades;         → 38
6. Deploy Stage Lambda (ya existe): API → landing → pivot
7. Deploy Lambda PIPELINE V4:  → Cronificar con EventBridge/Step Functions
8. Test completo: Ejecutar Stage → Pipeline V4 → Verificar dashboard
```

### Notas
- INIT + REFERENCIAL se ejecutan **una sola vez** (o ante cambios de schema/config)
- Stage Lambda + Pipeline V4 se ejecutan **cada ciclo** (horario/diario según necesidad)
- Mantener UNIQUE `(well_id, timestamp_lectura)` en stage para dedupe
- El motor DQ usa `tbl_var_scada_map` para traducir `nombre_tecnico` → `columna_stage` real
- Ajustar timeouts Lambda: PIPELINE V4 puede tomar 30-60s → usar 120s timeout mínimo
- Para volumen alto, usar Step Functions con retry/catch por fase
- Variables sin sensor seguirán NULL hasta mapear IDN; V9 tiene fallback para current_values
