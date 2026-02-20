# Auditoría del Pipeline Master (Versión V4.5)

**Fecha**: 2026-02-19
**Estado**: ✅ EJECUCIÓN EXITOSA (EXIT_CODE: 0)
**Versión Auditada**: `MASTER_PIPELINE_RUNNER.py` (Local Audit Mode)

## 1. Resumen Ejecutivo
El pipeline orquestador ha sido auditado y validado exitosamente en un entorno local aislado. Se verificó la integración completa desde la ingesta de datos crudos (Stage) hasta la generación de vistas para el frontend (Reporting), incluyendo la simulación de componentes avanzados de Inteligencia Artificial (Universal).

> **Nota Crítica de Infraestructura**: Se detectaron y corrigieron discrepancias entre el esquema de base de datos (`stage`) y los dumps de ingesta heredados (`tbl_pozo_maestra.sql`). Estas correcciones (`_in` suffix) ya están aplicadas en los scripts DDL V4.

## 2. Diagrama de Flujo del Proceso

El orquestador maestro ejecuta las siguientes fases de manera secuencial y atómica:

1.  **Inicialización (Fase 1)**: 
    - Reinicio completo de esquemas (`DROP CASCADE` / `CREATE`).
    - Carga de semillas referenciales (variables, rangos, mapas SCADA).
    - *Script*: `init_schemas_v4.py`

2.  **Ingesta Híbrida (Fase 2)**:
    - Carga masiva de Dumps SQL para Maestra y Producción (`stage`).
    - Carga de Reservas desde Excel (`tbl_pozo_reservas`).
    - Landing de datos SCADA crudos.
    - *Script*: `ingest_real_telemetry.py`

3.  **Data Quality (Fase 3)**:
    - Ejecución de reglas de negocio sobre datos crudos.
    - Generación de reportes de calidad en `stage.tbl_pozo_scada_dq`.
    - *Script*: `apply_data_quality.py`

4.  **Simulación AI / Universal (Fase 3.5 - Opcional)**:
    - Generación sintética de resultados de IA (IPR Vogel, Arps Declinación, Patrones CDI).
    - **Nota**: La generación de `curva_bomba` está deshabilitada por configuración.
    - Activado por variable de entorno: `SIMULATE_UNIVERSAL=true`.
    - *Script*: `simulate_universal_data.py`

5.  **Transformación Histórica (Fase 4)**:
    - ETL de Stage a Reporting (`fact_operaciones_horarias`, `fact_operaciones_diarias`).
    - Cálculo de métricas derivadas (deltas, promedios).
    - *Script*: `execute_historical_etl.py` (Ejecuta SPs `sp_load_to_reporting`).

6.  **Bridge Universal (Fase 4.5 - V10)**:
    - Sincronización de resultados Universal hacia Reporting (`dataset_current_values`).
    - **Feature V4.5**: Transferencia directa de cartas dinagráficas (texto plano) sin conversión JSON.
    - *Script*: `execute_bridge.py`

7.  **Enriquecimiento de Negocio (Fase 5)**:
    - Cálculo de KPIs financieros y operativos (Uptime, MTBF, Costo/BBL).
    - *Script*: `enrich_business_kpis.py`

8.  **Vistas de Frontend (Fase 6)**:
    - Generación/Actualización de vistas materializadas y lógicas para el Dashboard.
    - *Script SQL*: `V12__vistas_helper_frontend.sql`

## 3. Requisitos de Infraestructura

Para el despliegue en producción, asegurar:

*   **Variables de Entorno (.env)**:
    *   `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
    *   `SIMULATE_UNIVERSAL`: `false` (en Prod, los datos Universal deben venir de modelos reales, no simulación).
    
*   **Permisos de Base de Datos**:
    *   El usuario debe tener privilegios de `DROP/CREATE SCHEMA` para ejecución Full Reset.
    *   Para ejecución incremental (futuro), se requerirán permisos de `TRUNCATE/INSERT`.

## 4. Evidencia de Validación

**Logs de Ejecución**:
- `final_audit_run_retry2.log`: Muestra `EXIT_CODE: 0`.
- Tiempos de ejecución: ~20s total en entorno local.

**Integridad de Datos (Muestreo)**:
| Tabla | Estado | Observación |
| :--- | :--- | :--- |
| `stage.tbl_pozo_maestra` | ✅ Poblada | Esquema corregido (`_in`). |
| `stage.tbl_pozo_produccion` | ✅ Poblada | Datos SCADA ingestados. |
| `reporting.dataset_current_values` | ✅ Poblada | Incluye Cartas Dinagráficas (Text) y Datos Universal (IPR sin curva bomba). |
| `reporting.fact_operaciones_diarias` | ✅ Poblada | KPIs diarios calculados. |

---
*Este documento certifica que el pipeline está operativo y alineado con la arquitectura de referencia V4.5.*
