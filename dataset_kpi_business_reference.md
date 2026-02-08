# 📊 DOCUMENTO DE REFERENCIA: `reporting.dataset_kpi_business`

## 🎯 RESUMEN EJECUTIVO

| Aspecto | Detalle |
|--------|---------|
| **Tabla** | `reporting.dataset_kpi_business` |
| **Schema** | V3__reporting_schema_redesign.sql (L403-430) |
| **Población** | V2_reporting_engine.sql (L657-761) |
| **Procedimiento** | `reporting.sp_load_kpi_business(fecha_inicio, fecha_fin)` |
| **Ingesta en Pipeline** | MASTER_PIPELINE_RUNNER.py L150-157 |
| **Paso del Pipeline** | 5.5 - Loading Business KPIs |
| **Frecuencia** | Una vez por ciclo completo |
| **Tipo de Merge** | `ON CONFLICT (fecha, well_id) DO UPDATE` |

---

## 🔍 DEFINICIÓN DE TABLA

**Localización:** [V3__reporting_schema_redesign.sql](V3__reporting_schema_redesign.sql#L403-L430)

```sql
CREATE TABLE IF NOT EXISTS reporting.dataset_kpi_business (
    kpi_id BIGINT GENERATED ALWAYS AS IDENTITY,
    fecha DATE,
    well_id INT,
    nombre_pozo VARCHAR(100),
    campo VARCHAR(100),
    uptime_pct DECIMAL(5,2),
    tiempo_operacion_hrs DECIMAL(4,2),
    mtbf_dias DECIMAL(10,2),
    fail_count INT,
    costo_energia_usd DECIMAL(12,2),
    kwh_por_barril DECIMAL(10,4),
    lifting_cost_usd_bbl DECIMAL(10,2),
    eur_remanente_bbl DECIMAL(14,2),
    vida_util_estimada_dias INT,
    PRIMARY KEY (fecha, well_id)
);
```

### Columnas

| # | Columna | Tipo | Relleno | Fuente |
|---|---------|------|---------|--------|
| 1 | `kpi_id` | BIGINT | Auto | - |
| 2 | `fecha` | DATE | ✅ | dim_tiempo.fecha |
| 3 | `well_id` | INT | ✅ | fact_operaciones_diarias.pozo_id |
| 4 | `nombre_pozo` | VARCHAR(100) | ✅ | dim_pozo.nombre_pozo |
| 5 | `campo` | VARCHAR(100) | ✅ | dim_pozo.campo |
| 6 | `uptime_pct` | DECIMAL(5,2) | ✅ | fact_operaciones_diarias.kpi_uptime_pct |
| 7 | `tiempo_operacion_hrs` | DECIMAL(4,2) | ✅ | fact_operaciones_diarias.tiempo_operacion_hrs |
| 8 | `mtbf_dias` | DECIMAL(10,2) | ✅ | Calculado: kpi_mtbf_hrs / 24.0 |
| 9 | `fail_count` | INT | ✅ | fact_operaciones_diarias.numero_fallas |
| 10 | `costo_energia_usd` | DECIMAL(12,2) | ⏸️ | NULL (sin fuente) |
| 11 | `kwh_por_barril` | DECIMAL(10,4) | ✅ | fact_operaciones_diarias.kpi_kwh_bbl |
| 12 | `lifting_cost_usd_bbl` | DECIMAL(10,2) | ⏸️ | NULL (sin fuente) |
| 13 | `eur_remanente_bbl` | DECIMAL(14,2) | ⏸️ | NULL (sin fuente económica) |
| 14 | `vida_util_estimada_dias` | INT | ⏸️ | NULL (sin modelo ARPS) |

**Constraint PK:** `(fecha, well_id)` - Previene duplicados día/pozo

---

## 📥 LLENADO - PROCEDIMIENTO `sp_load_kpi_business`

**Ubicación:** [V2_reporting_engine.sql](V2_reporting_engine.sql#L657-L761)

### Firma
```sql
CREATE OR REPLACE PROCEDURE reporting.sp_load_kpi_business(
    p_fecha_inicio DATE,
    p_fecha_fin DATE
)
LANGUAGE plpgsql
AS $$
```

### Parámetros
- `p_fecha_inicio` - Fecha inicial del rango de KPIs
- `p_fecha_fin` - Fecha final del rango de KPIs

### Flujo de Ejecución (2 inserciones)

#### ✅ **INSERCIÓN 1: KPIs DIARIOS** (L667-L713)
```
Fuentes:
├─ fact_operaciones_diarias (daily facts)
├─ dim_tiempo (fecha mapping)
└─ dim_pozo (metadata pozo)

Lógica:
├─ Toma kpi_uptime_pct directamente
├─ Toma tiempo_operacion_hrs del día
├─ Calcula MTBF: kpi_mtbf_hrs / 24.0
├─ Cuenta fallas: numero_fallas
└─ Calcula eficiencia: kpi_kwh_bbl

Merge:
└─ ON CONFLICT: actualiza 5 campos
```

**Query Resumida:**
```sql
SELECT
    dt.fecha,
    d.pozo_id,
    p.nombre_pozo, p.campo,
    d.kpi_uptime_pct,
    d.tiempo_operacion_hrs,
    d.kpi_mtbf_hrs / 24.0 AS mtbf_dias,
    d.numero_fallas,
    NULL, NULL, NULL, NULL, NULL
FROM reporting.fact_operaciones_diarias d
JOIN reporting.dim_tiempo dt ON ...
JOIN reporting.dim_pozo p ON ...
WHERE dt.fecha BETWEEN p_fecha_inicio AND p_fecha_fin
  AND d.periodo_comparacion = 'DIARIO'
```

#### ✅ **INSERCIÓN 2: KPIs MENSUALES** (L715-L761)
```
Fuentes:
├─ fact_operaciones_mensuales (monthly aggregates)
└─ dim_pozo (metadata)

Lógica:
├─ Convierte anio_mes a DATE (primer día)
├─ Usa eficiencia_uptime_pct mensual
├─ Suma tiempo_operacion_hrs del mes
├─ Calcula MTBF mensual: (hrs / fallas) / 24.0
└─ Cuenta fallas acumuladas

Merge:
└─ ON CONFLICT: actualiza mismo set 5 campos
```

**Query Resumida:**
```sql
SELECT
    TO_DATE(m.anio_mes || '-01', 'YYYY-MM-DD'),
    m.pozo_id,
    p.nombre_pozo, p.campo,
    m.eficiencia_uptime_pct,
    m.tiempo_operacion_hrs,
    (m.tiempo_operacion_hrs / NULLIF(m.total_fallas_mes, 0)) / 24.0,
    m.total_fallas_mes,
    NULL, NULL, NULL, NULL, NULL
FROM reporting.fact_operaciones_mensuales m
JOIN reporting.dim_pozo p ON ...
WHERE TO_DATE(...) BETWEEN p_fecha_inicio AND p_fecha_fin
```

---

## 🔄 INGESTA EN PIPELINE

**Archivo:** [MASTER_PIPELINE_RUNNER.py](MASTER_PIPELINE_RUNNER.py#L150-L157)

```python
# PASO 5.5: KPIs DE NEGOCIO
print("\n>>> 5.5 Loading Business KPIs...")
execute_sql_query("""
    CALL reporting.sp_load_kpi_business(
        '2020-01-01'::DATE,
        '2030-12-31'::DATE
    );
""")
print("[OK] Business KPIs loaded.")
```

### Contexto en Pipeline

```
PASO 1:    init_schemas.py (Reset DDL)
PASO 2:    ingest_real_telemetry.py (Ingesta cruda)
PASO 3:    V5__stored_procedures.sql (Procedures)
PASO 3.1:  sp_execute_dq_validation() (DQ Engine)
PASO 3.5:  V2_reporting_engine.sql (Carga engine)
PASO 5:    sp_load_to_reporting() ← FACTS (diarios/mensuales)
┌─ PASO 5.5: sp_load_kpi_business() ← AQUÍ ESTAMOS (KPI Business)
│ (Toma FACTS ya llenos y agrupa en KPIs)
└─ Rango: 2020-01-01 a 2030-12-31
PASO 6:    actualizar_current_values_v3() (Snapshot)
PASO 6.5:  sp_apply_color_logic() (Semáforos)
```

---

## 📊 FLUJO DE DATOS

```
RAW DATA (tbl_pozo_produccion)
    ↓
[Transformaciones V5]
    ↓
fact_operaciones_horarias (hourly facts)
    ↓
fact_operaciones_diarias (daily facts aggregated)
    ↓
fact_operaciones_mensuales (monthly totals)
    ↓
────────────────────────────────────────────
sp_load_kpi_business() ← [AQUÍ ENCAJA]
├─ Lee daily facts
├─ Lee monthly aggregates
├─ Calcula KPIs derivados (MTBF, ratios)
├─ Joins con dim_tiempo, dim_pozo
└─ INSERT/UPDATE en dataset_kpi_business
────────────────────────────────────────────
    ↓
dataset_kpi_business (KPI Business Dataset)
    ↓
[Snapshot & Color Logic]
    ↓
dataset_current_values (Última fila por pozo)
    ↓
Dashboards / BI Tools
```

---

## 🎯 CAMPOS SIN DATOS

Estos 4 campos necesitan implementación:

| Campo | Razón | Solución Sugerida |
|-------|-------|-------------------|
| `costo_energia_usd` | Falta tabla de costos energéticos | Crear tbl_costos_energia con tarifa(fecha, pozo) |
| `lifting_cost_usd_bbl` | Falta modelo de costos operativos | Crear modelo: (opex_mensual / bbl_producidos) |
| `eur_remanente_bbl` | Falta modelo ARPS/EUR calculado | Implementar sp_calculate_eur() con parametrizacion |
| `vida_util_estimada_dias` | Falta curva de depleción | Usar EUR + producción decline rate |

---

## 📋 CHECKLIST DE VALIDACIÓN

- [ ] `fact_operaciones_diarias` tiene datos (verificar COUNT)
- [ ] `fact_operaciones_mensuales` tiene datos
- [ ] `dim_tiempo` está poblada (rango 2020-2030)
- [ ] `dim_pozo` tiene pozos
- [ ] Rango de fechas en parámetros cubre datos
- [ ] `ON CONFLICT` merge funciona sin errors
- [ ] Campos con datos (5) están llenos
- [ ] Campos NULL son esperados (4)

---

## 🔧 QUERIES ÚTILES

### Ver contenido actual
```sql
SELECT fecha, well_id, nombre_pozo, uptime_pct, mtbf_dias, fail_count, kwh_por_barril
FROM reporting.dataset_kpi_business
ORDER BY fecha DESC
LIMIT 10;
```

### Ver NULL density
```sql
SELECT
  COUNT(*) total,
  COUNT(uptime_pct) uptime_filled,
  COUNT(costo_energia_usd) costo_filled,
  COUNT(lifting_cost_usd_bbl) lifting_filled,
  COUNT(eur_remanente_bbl) eur_filled
FROM reporting.dataset_kpi_business;
```

### Re-ejecutar procedimiento
```sql
CALL reporting.sp_load_kpi_business('2025-01-01'::DATE, '2025-12-31'::DATE);
```

### Ver últimos KPIs por pozo
```sql
SELECT DISTINCT ON (well_id)
  well_id, nombre_pozo, fecha, uptime_pct, mtbf_dias
FROM reporting.dataset_kpi_business
ORDER BY well_id, fecha DESC;
```

---

## 📞 REFERENCIAS RÁPIDAS

| Necesito... | Ver archivo | Línea |
|-------------|-------------|-------|
| Definición tabla | V3__reporting_schema_redesign.sql | 403-430 |
| Procedimiento carga | V2_reporting_engine.sql | 657-761 |
| Ejecución pipeline | MASTER_PIPELINE_RUNNER.py | 150-157 |
| Valores por defecto | MASTER_PIPELINE_RUNNER.py | 154-155 |
| Dimensiones | V3__reporting_schema_redesign.sql | 1-100 |
| Facts | V3__reporting_schema_redesign.sql | 100-350 |

---

**Última actualización:** Feb 5, 2026
**Documento:** dataset_kpi_business_reference.md
