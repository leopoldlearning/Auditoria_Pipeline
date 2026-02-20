# Auditoría de Orquestación del Pipeline BP010

**Fecha:** 2026-02-19  
**Archivo auditado:** `MASTER_PIPELINE_RUNNER.py`  
**Objetivo de la auditoría:** validar si la orquestación actual cubre el flujo productivo completo de los esquemas `stage`, `universal`, `referencial` y `reporting`, y garantizar que la **última carta** disponible en `stage` llegue de forma directa a `reporting`.

---

## 1) Contexto de referencia (target producción)

Según `D:\ITMeet\Operaciones\hrp-hydrog-processes\README.md`, el comportamiento esperado en producción es:

- `Stage ETL` cada 60s (`RAW -> STAGE`)
- Servicios `IPR`, `Declinación`, `Cartas` hacia `UNIVERSAL` en intervalos (5-10 min)
- `Reporting ETL` cada 5 min (`STAGE + UNIVERSAL -> REPORTING`)

Conclusión: el patrón objetivo es **continuo, desacoplado y por intervalos**, no un proceso batch monolítico de “full reset”.

---

## 2) Hallazgos del estado actual

## 2.1 Orquestación actual en `MASTER_PIPELINE_RUNNER.py`

El runner actual ejecuta secuencialmente:

1. INIT (`init_schemas.py`)
2. LOAD (`load_referencial.py`, `ingest_real_telemetry.py`, `sp_seed_defaults`)
3. DQ (`stage.sp_execute_dq_validation`)
4. TRANSFORM (`reporting.sp_load_to_reporting`, `actualizar_current_values_v4`)
5. ENRICH (`sp_sync_dim_pozo_targets`, `aplicar_evaluacion_universal`, `sp_calcular_derivados_completos`, `poblar_kpi_business`)
6. DEFAULTS (`sp_populate_defaults`)
7. CONSISTENCY (`stage.sp_execute_consistency_validation`)

### Hallazgo A — Modo batch/no productivo
- El runner combina INIT + carga histórica + transformaciones en una sola corrida.
- Esto es útil para auditoría/reproceso, pero no coincide con operación productiva near-real-time.

### Hallazgo B — Universal bridge no orquestado
- Existen SPs de puente en `V10__universal_to_reporting_bridge.sql`:
  - `reporting.sp_sync_cdi_to_reporting()`
  - `reporting.sp_sync_ipr_to_reporting()`
  - `reporting.sp_sync_arps_to_reporting()`
- El runner actual **no los invoca**.
- Resultado: aunque `universal` tenga datos, `reporting` no necesariamente los consume.

### Hallazgo C — Requisito “última carta stage -> reporting” no cubierto explícitamente
- `stage.tbl_pozo_produccion` sí contiene la última carta cruda:
  - `surface_rod_position`, `surface_rod_load`, `downhole_pump_position`, `downhole_pump_load`
- `reporting.dataset_latest_dynacard` existe, pero hoy el flujo operativo previsto en V10 se alimenta desde `universal` (CDI), no directo desde `stage`.
- Si CDI/ML se retrasa, `dataset_latest_dynacard` puede quedar desactualizado frente a `stage`.

### Hallazgo D — Desalineaciones técnicas en bridge actual
- En `V10`, algunos campos de join/actualización no están alineados con V4 (`timestamp_lectura` vs `fecha_hora`, y nombre de columnas de producción actuales).
- Impacto: riesgo de que sync Universal->Reporting no aplique correctamente.

---

## 3) Riesgos de negocio/técnicos

- **R1. Staleness de dashboard dynacard:** el dashboard puede no reflejar la carta más reciente recibida por SCADA.
- **R2. Acoplamiento excesivo a corrida batch:** costos altos y latencia para operación en producción.
- **R3. Brecha STAGE/UNIVERSAL/REPORTING:** datos en Universal sin reflejo oportuno en Reporting.
- **R4. Reprocesos innecesarios:** ejecutar INIT en ciclos productivos incrementa riesgo operativo.

---

## 4) Propuesta de orquestación objetivo (producción)

## 4.1 Principio operativo

Separar en dos modos:

- **Modo Inicialización (one-time / cambios de schema):** INIT + cargas base referencial.
- **Modo Operación Continua:** ciclos incrementales por intervalo.

## 4.2 Flujo continuo recomendado

### Ciclo 1 (cada 60s) — Stage
1. Ingesta API -> `stage.landing_scada_data`
2. Pivot -> `stage.tbl_pozo_produccion`
3. **Sync carta más reciente Stage->Reporting** (ver 4.3)

### Ciclo 2 (cada 5-10 min) — Universal
4. IPR/Declinación/Cartas escriben en `universal.*`
5. Ejecutar bridge:
   - `CALL reporting.sp_sync_cdi_to_reporting()`
   - `CALL reporting.sp_sync_ipr_to_reporting()`
   - `CALL reporting.sp_sync_arps_to_reporting()`

### Ciclo 3 (cada 5 min) — Reporting
6. `CALL reporting.sp_load_to_reporting(...)`
7. `CALL reporting.actualizar_current_values_v4()`
8. `CALL reporting.sp_sync_dim_pozo_targets()`
9. `CALL reporting.aplicar_evaluacion_universal()`
10. `CALL reporting.sp_calcular_derivados_completos(...)`
11. `CALL reporting.poblar_kpi_business(...)`
12. `CALL reporting.sp_populate_defaults()`
13. `CALL stage.sp_execute_dq_validation(...)`
14. `CALL stage.sp_execute_consistency_validation()`

---

## 4.3 Requisito crítico: “última carta de stage a reporting”

Para cumplir el requisito incluso sin esperar el ciclo ML:

### Recomendación funcional
Agregar una sincronización directa (SP o job SQL incremental) de:

- **Origen:** `stage.tbl_pozo_produccion` (último `timestamp_lectura` por `well_id`)
- **Destino:** `reporting.dataset_latest_dynacard`

Mapeo mínimo:

- `well_id` -> `well_id`
- `timestamp_lectura` -> `timestamp_carta`
- `surface_rod_position/surface_rod_load` -> `superficie_json`
- `downhole_pump_position/downhole_pump_load` -> `fondo_json`
- `diagnostico_ia` -> valor provisional (`'PENDIENTE_ML'`) si aún no llega diagnóstico universal

### Regla de precedencia recomendada
- **Primero** publicar carta cruda de `stage` (latencia mínima).
- **Después**, cuando llegue CDI en `universal`, actualizar solo `diagnostico_ia` y/o enriquecimientos sin perder la carta más reciente.

---

## 5) Ajustes puntuales recomendados al runner

1. Mantener `MASTER_PIPELINE_RUNNER.py` como runner de auditoría/reproceso.
2. Crear un runner productivo incremental (sin INIT por defecto).
3. Incluir llamadas de `V10` en el flujo productivo.
4. Añadir paso explícito de “sync latest dynacard stage->reporting”.
5. Parametrizar modo de ejecución (`FULL_RESET`, `INCREMENTAL`, `UNIVERSAL_ONLY`, `REPORTING_ONLY`).

---

## 6) Plan de implementación sugerido

## Fase 1 (rápida, 1-2 días)
- Activar bridge Universal->Reporting en orquestación.
- Corregir desalineaciones de columnas/joins en `V10` con `V4`.

## Fase 2 (2-4 días)
- Implementar sync directo `stage -> dataset_latest_dynacard`.
- Definir precedencia Stage vs Universal para `diagnostico_ia`.

## Fase 3 (3-5 días)
- Separar runner batch y runner incremental.
- Configurar ejecución por intervalos (Step Functions / scheduler).

---

## 7) Criterios de aceptación (QA)

- CA-01: cada nueva lectura con carta en `stage.tbl_pozo_produccion` aparece en `reporting.dataset_latest_dynacard` en <= 1 ciclo de stage.
- CA-02: llegada de CDI en `universal` enriquece `diagnostico_ia` sin borrar la carta más reciente.
- CA-03: `reporting.dataset_current_values` refleja `ipr_qmax_bpd` y `ipr_eficiencia_flujo_pct` cuando haya IPR en universal.
- CA-04: `eur_remanente_bbl` en `dataset_kpi_business` se sincroniza desde ARPS.
- CA-05: pipeline incremental corre sin ejecutar INIT.

---

## 8) Conclusión ejecutiva

El pipeline actual está bien para auditoría y reconstrucción histórica, pero para producción requiere desacoplarse en ciclos incrementales y habilitar el puente `universal -> reporting`.  
Para tu objetivo específico, la medida clave es incorporar una ruta explícita **`stage (última carta) -> reporting.dataset_latest_dynacard`** con precedencia temporal, de modo que el dashboard siempre muestre la última carta disponible, aun si el ML llega después.
