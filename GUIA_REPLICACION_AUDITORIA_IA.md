# Guía de Replicación de Auditoría: BP010 Data Pipelines
> **Target Audience**: Agentes de IA / Ingenieros de Datos
> **Objetivo**: Replicar la auditoría técnica del repositorio `BP010-data-pipelines` desde cero, asegurando un entorno aislado y resultados consistentes.

## 1. Principios de Auditoría (Aislamiento)
Para garantizar una auditoría segura, **NUNCA** ejecutes código directamente sobre el repositorio original ni uses la base de datos de producción/desarrollo externa.
1.  Crea un directorio de auditoría separado (ej: `BP010-data-pipelines-auditoria`).
2.  Crea tu propio entorno Docker con PostgreSQL 15 local.
3.  Copia selectivamente los artefactos (`src`, `data`) del repo original al de auditoría.

## 2. Fase de Infraestructura (Setup)

### 2.1 Docker Compose
Crea un `docker-compose.yml` que levante:
-   **PostgreSQL 15**: Puerto mapeado (ej: `5433:5432` host:container).
-   **Adminer**: Para inspección visual rápida.
-   **Network**: Aislada.

### 2.2 Variables de Entorno (.env)
Configura un `.env` local. **CRÍTICO**: No uses las credenciales del repo original.
```ini
DB_USER=audit
DEV_DB_PASSWORD=audit
DB_HOST=localhost
DB_PORT=5433
DB_NAME=etl_data
```

## 3. Fase de Análisis y Corrección de Código (Patching)

Antes de ejecutar nada, debes corregir errores estructurales detectados en el código original:

### 3.1 Notebooks
Los notebooks `.ipynb` originales tienen dos problemas graves que debes parchear (automáticamente vía script Python o `sed`):
1.  **Path Looping**: Tienen celdas que buscan recursivamente el directorio raíz. Reemplaza esa lógica por un simple `os.chdir(PROJECT_ROOT)`.
2.  **Credenciales Hardcodeadas**: Buscan usuarios/passwords fijos en el código (ej: "hydrog_ml_user"). Reemplaza todas las asignaciones `DB_USER = "..."` por `os.getenv('DB_USER')`.

### 3.2 SQL Schema Versions
El código Python (`SchemaManager`) puede apuntar a versiones antiguas (`V1`, `V2`).
-   **Acción**: Inspecciona `src/sql/schema`. Usa las versiones más altas disponibles (`V3`, `V4`) para la creación de tablas.
-   **Riesgo**: `DROP SCHEMA CASCADE` está presente en scripts de migración. Bloquéalo o ejecútalo solo en tu entorno Docker aislado.

## 4. Fase de Ingesta de Datos (DATA SOURCE)

Originalmente, este pipeline dependía de una API externa (AWS) sin credenciales disponibles. Tradicionalmente se usaba simulación, pero ahora contamos con datos extraídos manualmente.

### 4.1 Fuente de Verdad (Real vs Simulada)
- **Datos Reales**: Si están disponibles, use los archivos ubicados en `D:\ITMeet\Operaciones\API Hydrog manual`. Estos archivos contienen la telemetría real extraída vía script manual.
- **Simulación (Fallback)**: Si no hay acceso a los archivos reales, analice `src/sql/process/V1__stage_to_stage.sql` para extraer los `var_id` y generar datos < 1000 para evitar *Numeric Overflow*.

### 4.2 Importancia de la Capa de Calidad (tbl_pozo_scada_dq)
> [!IMPORTANT]
> **¿Por qué es necesaria la tabla DQ en Stage?**
> A diferencia de otros sistemas donde la validación ocurre al final, aquí la tabla `stage.tbl_pozo_scada_dq` actúa como un **Gatekeeper (Guardián)**:
> 1. **Zero-Noise**: Asegura que el Dashbord (`reporting`) no consuma basura.
> 2. **Trazabilidad**: Permite auditar qué regla falló (`regla_id`) para un dato específico sin tener que re-analizar el crudo.
> 3. **Consistencia Zero-Calc**: El script de lógica de colores lee el estado de DQ para decidir si muestra un valor o un aviso de "Dato No Confiable".

## 5. Fase de Ejecución (Pipeline Sequence)

Ejecuta los notebooks en este orden estricto (usando `nbconvert --inplace`):

1.  `0_1_udf_to_stage.ipynb`: Carga datos estáticos (Excel UDF).
2.  **[Ejecutar Script Simulación]**: Inyecta datos SCADA simulados.
3.  `0_3_stage_to_stage.ipynb`: Transforma landing -> stage tables.
4.  `1_1_stage_to_reporting.ipynb`: Mueve data a Reporting (Dims/Facts).
5.  `1_2_actualizar_current_values.ipynb`: Genera snapshot de KPIs.

## 6. Verificación de Resultados

Al finalizar, consulta la tabla `reporting.dataset_current_values`. Deberías ver:
-   1 registro para el pozo 5.
-   Datos poblados en columnas de producción, presión y estado.
-   **Hallazgo a Reportar**: Inconsistencia semántica (`rpm_motor` mapped to `freq_vsd_hz`) y limitación de tipos de datos (`DECIMAL(5,2)`).

---
*Esta guía garantiza que cualquier agente inteligente pueda reproducir los hallazgos de seguridad y estabilidad sin acceso al entorno productivo real.*

## 7. Arquitectura Revelada y Hallazgos Finales (Post-Auditoría Profunda)

Tras una "cacería" exhaustiva del código fuente, se ha descifrado la arquitectura real de producción. Usa esta referencia para entender lo que estás auditando:

### 7.1 Mapa de Microservicios (Dual-Lambda)
El sistema no es un script monolítico, sino dos servicios serverless desacoplados que orquestan el flujo real:

1.  **Servicio de Ingesta (Stage Service)** 🟡
    *   **Ubicación**: `docker/rds-stage-etl-project/etl_app/lambda_handler.py`.
    *   **Responsabilidad**: Ingesta API + Transformación Pivot.
    *   **Flujo**: Trigger -> `V1__raw_to_stage` (Vertical) -> `V1__stage_to_stage` (Horizontal).
    *   **Nota**: Este componente suele ser invisible en ejecuciones locales de notebooks.

2.  **Servicio de Reportes (Reporting Service)** 🟢
    *   **Ubicación**: `docker/rds-reporting-etl-project/etl_app/lambda_handler.py`.
    *   **Responsabilidad**: Cálculo de KPIs y Snapshot.
    *   **Flujo**: Trigger -> `V1__stage_to_reporting` -> `V3__actualizar_current_values`.

### 7.2 Genealogía del Dato (Lineage)
Es vital distinguir el origen de los datos para no confundir metadatos con telemetría:

*   **Rama Estática (Excel)**:
    *   Archivo `Formato1_Excel_Reservas.xlsx` -> `udf_to_stage` -> **`stage.tbl_pozo_maestra`**.
    *   *Propósito*: Define la identidad del pozo (Nombre, Ubicación, Equipo Instalado).
*   **Rama Dinámica (API/SCADA)**:
    *   API Externa -> `raw_to_stage` -> `landing` -> `stage_to_stage` -> **`stage.tbl_pozo_produccion`**.
    *   *Propósito*: Define el estado operativo (Presión, Flujo, Temperatura).

### 7.4 Hallazgo: Gaps en Orquestación (Horaria y DQ) [NUEVO]
Tras la auditoría profunda, se detectaron los siguientes "puntos ciegos" en la orquestación local:

1.  **Ingesta Horaria Desactivada**: Los notebooks locales invocan `sp_load_to_reporting` con `p_procesar_horario = FALSE` por defecto. Se requiere habilitar este flag explícitamente para ver datos en `reporting.fact_operaciones_horarias`.
2.  **DQ Engine Desconectado**: Existe la infraestructura para Calidad de Datos (`referencial.tbl_dq_rules` y `stage.tbl_pozo_scada_dq`), pero no hay un script Python o Stored Procedure que ejecute las validaciones en el formato V4 (Normalizado). Esto causa que el Dashboard muestre siempre `PASS` de forma errónea.

### 7.5 La Vulnerabilidad Crítica (El "Eslabón Débil")
... (contenido anterior) ...


## 8. Documentación de Población de Tablas (Lineage)

Para que un auditor o agente de IA entienda el origen de los datos, siga esta matriz de población:

### 8.1 Capa Referencial (Cerebro del Sistema)
| Tabla | Origen Primario | Proceso de Carga |
| :--- | :--- | :--- |
| `tbl_maestra_variables` | `01_maestra_variables.csv` | `V3__referencial_seed_data.sql` |
| `tbl_dq_rules` | `02_reglas_calidad.csv` | `V3__referencial_seed_data.sql` |
| `tbl_limites_pozo` | `04_esquema_reporting_zero_calc.csv` | **Manual Patch (V4)** - Requerido para KPIs. |

### 8.2 Capa Stage (Datos Crudos y Pivoteados)
| Tabla | Origen Primario | Proceso de Carga |
| :--- | :--- | :--- |
| `tbl_pozo_maestra` | `Formato1_Excel_Reservas.xlsx` | `0_1_udf_to_stage_AWS_v0.ipynb` |
| `landing_scada_data` | API AWS (Prod) / Simulation (Audit) | `0_2_raw_to_stage_AWS_v0.ipynb` |
| `tbl_pozo_produccion`| `landing_scada_data` | `V1__stage_to_stage.sql` (Pivote EAV -> Wide) |

### 8.3 Capa Reporting (Consumo BI)
| Tabla | Origen Primario | Proceso de Carga |
| :--- | :--- | :--- |
| `dim_tiempo` / `dim_hora` | Scripts SQL DDL | Pobladas durante el SQL Init o SP. |
| `fact_operaciones_horarias`| `stage.tbl_pozo_produccion` | `sp_load_to_reporting(..., TRUE, ...)` |
| `fact_operaciones_diarias`| `stage.tbl_pozo_produccion` | `V1__stage_to_reporting.sql` |
| `fact_operaciones_mensuales`| `fact_operaciones_diarias` | `CALL reporting.sp_load_to_reporting(..., TRUE)` |
| `dataset_current_values` | `stage` + `referencial` | `V3__actualizar_current_values.sql` |

### 8.4 Capa Universal (IA/ML)
| Tabla | Estado | Observación |
| :--- | :--- | :--- |
| `ipr_resultados` | **Vacía** | Requiere ejecución de modelos externos no presentes en este repo core. |
| `arps_resultados_declinacion`| **Vacía** | Pendiente integración de flujos de predicción. |

> [!TIP]
> **Modificación Sugerida**: Automatizar la carga de `tbl_limites_pozo` en el script `generate_referencial_seed.py` para evitar tablas vacías en nuevas instalaciones.

## 9. El "Golden Flow": Orquestación Maestra Final
Para asegurar que el equipo de Desarrollo y Producción pueda correr todo el pipeline con datos reales y validaciones automáticas, se ha creado el orquestador unificado.

### 9.1 La Secuencia Maestra
El archivo `MASTER_PIPELINE_RUNNER.py` coordina las 7 fases del sistema en el orden correcto:

1.  **DDL Setup**: Crea los 4 esquemas (V3/V4).
2.  **Master Data**: Carga pozos y variables desde Excel/Config.
3.  **Real Ingestion**: Toma los archivos de `API Hydrog manual`.
4.  **Pivot transform**: Convierte telemetría cruda a formato pozo-columna.
5.  **DQ Engine (EL GUARDIÁN)**: Ejecuta `sp_execute_dq_validation`. **Este paso marca cada dato como PASS/FAIL antes de llegar a Reporting**.
6.  **Reporting Layers**: Genera hechos Horarios, Diarios y Mensuales.
7.  **Snapshot Final**: Actualiza `dataset_current_values` con los últimos KPIs y el estado de DQ.

### 9.2 Instrucciones de Uso
1.  Coloque los extractos de la API en `D:\ITMeet\Operaciones\API Hydrog manual`.
2.  Active el entorno virtual: `auditor\Scripts\activate`.
3.  Ejecute: `python MASTER_PIPELINE_RUNNER.py`.
4.  Visualice los resultados en Adminer:
    -   `stage.tbl_pozo_scada_dq`: Resultados de calidad.
    -   `reporting.dataset_current_values`: KPIs finales validados.

### 9.3 Solución de Problemas y Consideraciones Técnicas [NUEVO]

Para una replicación exitosa en entornos Windows/IA, tenga en cuenta:

> [!WARNING]
> **Codificación SQL (Encoding)**: Los scripts SQL originales pueden contener acentos o caracteres especiales. El `init_schemas.py` ha sido configurado para leer en `latin-1` y transmitir en `utf-8`. Si crea nuevos scripts, asegúrese de usar una codificación consistente para evitar errores de `invalid byte sequence`.

> [!IMPORTANT]
> **Compatibilidad de Consola (Unicode)**: Se han eliminado los Emojis de los logs del orquestador (`MASTER_PIPELINE_RUNNER.py`) para evitar el error `UnicodeEncodeError` al redirigir la salida a archivos `.log` en sistemas Windows. Se recomienda usar prefijos como `[OK]` o `[ERROR]`.

> [!NOTE]
> **Type Casting en SQL**: Al invocar procedimientos almacenados (como el motor DQ) desde Python, use casting explícito (ej: `'2026-02-01'::DATE`) para evitar ambigüedades de tipo `unknown` en PostgreSQL.

---
> [!TIP]
> **Consideración Final**: Este flujo garantiza que ningún dato llegue al Dashboard sin haber pasado por el filtro de reglas del esquema `Referencial`.
