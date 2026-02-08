# 🔍 Auditoría de NULLs - MASTER_PIPELINE_RUNNER
**Fecha**: 2026-02-05 08:27:20
**Objetivo**: Identificar fuentes de valores NULL en tablas después del pipeline
**Agentes**: Postgres Expert, DevOps Engineer, Data Scientist

---

## 🗄️ Análisis PostgreSQL - Fuentes de NULLs

### 1. ESQUEMA stage - landing_scada_data
**Archivo**: src/sql/schema/V4__stage_schema_redesign.sql

**Columnas Sin Restricción NOT NULL:**
- unit_id (mapeo a well_id - puede ser NULL)
- location_id
- var_id
- measure
- moddate
- modtime

**Riesgo**: Si landing_scada_data llega con NULLs en estos campos, se propagan a tbl_pozo_produccion

---

### 2. ESQUEMA stage - tbl_pozo_maestra

**Campos NULLABLE:**
- cliente (ID: 12)
- pais (ID: 13)
- region (ID: 14)
- campo (ID: 15)
- api_number (ID: 5)
- coordenadas_pozo (ID: 4)
- tipo_pozo (ID: 3)
- profundidad_completacion (ID: 2)
- intervalo_disparos (ID: 26)
- radio_pozo (ID: 19)
- espesor_formacion (ID: 23)
- nombre_yacimiento (ID: 16)
- permeabilidad_abosluta (ID: 17)
- permeabilidad_horizontal (ID: 28)
- radio_drenaje (ID: 20)
- presion_inicial_yacimiento (ID: 21)
- temperatura_yacimiento (ID: 22)
- tipo_levantamiento (ID: 37)
- profundidad_vertical_yacimiento (ID: 38)
- profundidad_vertical_bomba (ID: 39)
- diametro_embolo_bomba (ID: 33)
- longitud_carrera_nominal (ID: 42)
- potencia_nominal_motor (ID: 43)
- corriente_nominal_motor (ID: 44)
- voltaje_nominal (ID: 45)
- frecuencia_nominal_motor (ID: 85)
- carga_nominal_unidad (ID: 46)
- carga_minima_nominal_sarta (ID: 47)
- num_software (ID: 87)
- peso_sarta_aire (ID: 72)
- carga_maxima_fluido_api (ID: 75)

**Crítico**: Casi todos los campos son NULLABLE excepto:
- well_id (PRIMARY KEY)
- nombre_pozo (UNIQUE NOT NULL)
- fecha_registro (NOT NULL)
- fecha_creacion (NOT NULL DEFAULT CURRENT_TIMESTAMP)

**Causa Potencial 1**: ReservasDataIngestor mapea IDs → columnas, pero si un ID no existe en los datos origen, la columna queda NULL.

---

### 3. ESQUEMA stage - tbl_pozo_reservas

**Campos NULLABLE:**
- gravedad_api (ID: 10)
- viscosidad_crudo (ID: 18)
- viscosidad_superficie (ID: 29)
- presion_burbujeo (ID: 24)
- presion_estatica_yacimiento (ID: 25)
- factor_volumetrico (ID: 30)
- gravedad_especifica_agua (ID: 63)
- permeabilidad_vertical (ID: 162)
- radio_equivalente (ID: 159)
- longitud_horizontal (ID: 160)
- factor_dano (ID: 161)
- presion_fondo_fluyente_critico (ID: 27)
- wc_critico (ID: 32)
- llenado_bomba_minimo (ID: 48)
- q_esperado (ID: 152)
- reserva_inicial_teorica (ID: 128)
- contenido_finos (ID: 58)
- otros_pvt (ID: 31)

**Causa Potencial 2**: Si ingest_reservas_manual() inserta datos sintéticos, pero faltan campos reales → NULLs

---

### 4. ESQUEMA stage - tbl_pozo_produccion (Tabla Ancha)

**MÁS DE 80 COLUMNAS NULLABLE**

Campos críticos que frecuentemente son NULL:
- presion_cabezal (ID: 54)
- presion_casing (ID: 55)
- temperatura_cabezal (ID: 56)
- produccion_fluido_diaria (ID: 107)
- produccion_petroleo_diaria (ID: 108)
- produccion_agua_diaria (ID: 109)
- spm_promedio (ID: 51)
- pump_fill_monitor (ID: 64)
- potencia_actual_motor (ID: 66)
- nivel_fluido_flop (ID: 60)

**Causa Potencial 3**: landing_scada_data tiene var_ids dispersos. Si var_id=51 no llega en una lectura → spm_promedio=NULL

---

### 5. FOREIGN KEY CONSTRAINTS - LEFT JOINs Que Generan NULLs

`sql
-- En tbl_pozo_reservas
CONSTRAINT fk_pozo_reserva FOREIGN KEY (well_id) REFERENCES stage.tbl_pozo_maestra (well_id) 
  ON DELETE CASCADE

-- En tbl_pozo_produccion
CONSTRAINT fk_pozo_scada FOREIGN KEY (well_id) REFERENCES stage.tbl_pozo_maestra (well_id) 
  ON DELETE CASCADE
`

**Riesgo**: Si 0_1_udf_to_stage_AWS_v0.ipynb NO popula well_id en tbl_pozo_maestra correctamente, los INSERTs posteriores fallan → Registros son rechazados.

---

## ⚙️ Análisis DevOps - Transformaciones que Pueden Generar NULLs

### 1. Script: ingest_real_telemetry.py

**Problema A**: Búsqueda de archivos por coincidencia de nombre
`python
maestra_files = [f for f in files if "landing" in f.lower() and f.endswith(".sql")]
prod_files = [f for f in files if "produccion" in f.lower() and f.endswith(".sql")]
`

**Riesgo**: Si archivos NO coinciden exactamente, NO se ingestan → Tablas vacías → NULLs en JOINs posteriores

**Problema B**: ingest_reservas_manual() inserta datos DUMMY
`python
INSERT INTO stage.tbl_pozo_reservas (
    well_id, fecha_registro, 
    gravedad_api, presion_burbujeo, presion_estatica_yacimiento, 
    permeabilidad_vertical, factor_dano
) VALUES (
    5, CURRENT_DATE, 
    35.5, 2200.0, 4500.0, 
    15.0, 0.5
)
`

**Riesgo**: Solo inserta 1 pozo (well_id=5). Si hay más pozos en tbl_pozo_maestra → tbl_pozo_reservas incompleta → Todos los otros pozos quedan NULL en JOINs

---

### 2. Script: 0_1_udf_to_stage_AWS_v0.ipynb

**Problema C**: Clase ReservasDataIngestor mapea IDs → Columnas
`python
self.mapeo_maestros = {
    1: 'well_id',
    2: 'profundidad_completacion',
    ...
    87: 'num_software'
}
`

**Riesgo**: Si archivo origen NO contiene ID: 2, profundidad_completacion = NULL. **Si hay 162 IDs pero solo llegan 50**, 112 columnas quedan NULL.

**Problema D**: Transformar_datos() busca por ID exacto
`python
df_filtrado = df[df['ID'].isin(mapeo.keys())].copy()
for _, row in df_filtrado.iterrows():
    id_campo = int(row['ID'])
    nombre_columna = mapeo[id_campo]
    valor = row['Valor']
`

**Riesgo**: Si ID no está en mapeo → fila ignorada. Datos perdidos = NULLs.

---

### 3. Script: 0_3_stage_to_stage_AWS_v0.ipynb

**Problema E**: Ejecuta V1__stage_to_stage.sql
- ¿Usa LEFT JOIN? → Puede dejar NULLs
- ¿Filtra por condiciones? → Puede descartar registros
- ¿Convierte tipos? → CAST fallido = NULL

**Problema F**: Validaciones DQ sin marcar NULL
`sql
CALL stage.sp_execute_dq_validation('2026-02-01'::DATE, '2026-02-28'::DATE)
`

Si sp_execute_dq_validation falla pero no hace ROLLBACK → Datos parciales + NULLs

---

### 4. Script: 1_2_actualizar_current_values_v3.ipynb

**Problema G**: V3__actualizar_current_values.sql
`sql
SELECT ... FROM stage.tbl_pozo_produccion
ORDER BY timestamp_lectura DESC
`

Si tbl_pozo_produccion está vacía o NULLs en timestamp_lectura → reporting.dataset_current_values queda NULL

**Problema H**: V3__logic_color_calculation.sql
- Calcula semáforos comparando ACTUAL vs TARGET
- Si ACTUAL = NULL → Semáforo = NULL

---

## 📊 Análisis Data Science - Flujos de Datos con Pérdida

### 1. INGESTA: landing_scada_data

**Datos esperados** desde D:\ITMeet\Operaciones\API Hydrog manual\tbl_*.sql:
- tbl_landing_202602021632.sql
- tbl_pozo_produccion_202602021632.sql

**Verificación**: ¿Llegan datos? ¿Completos?

**Potencial Fuga #1**: Si archivos no se encuentran → landing_scada_data vacía

---

### 2. TRANSFORMACIÓN: landing → tbl_pozo_maestra + tbl_pozo_reservas

**Clase ReservasDataIngestor**:
- Mapea 37 IDs para maestros
- Mapea 20 IDs para reservas
- Filtra por ID en mapeo

**Potencial Fuga #2**: Si datos origen tienen IDs NO mapeados → Datos descartados

**Ejemplo**: ID 200 (no mapeado) → No se crea columna → NULL forzado

---

### 3. TRANSFORMACIÓN: landing_scada_data → tbl_pozo_produccion

**Problema**: landing_scada_data tiene estructura larga (var_id, measure)
Necesita PIVOT a estructura ancha (80+ columnas)

**Potencial Fuga #3**: Si var_id no existe en mapeo → medida descartada → Columna NULL

**Ejemplo en pipeline**:
- Lectura #1: var_id=51 (SPM) → spm_promedio = 120
- Lectura #2: var_id=54 (WHP) → whp_psi = 2500
- Lectura #3: var_id=999 (NUEVO) → DESCARTADO → NULL

---

### 4. VALIDACIÓN: sp_execute_dq_validation()

**Reglas de validación** en eferencial.tbl_dq_rules:
- Si valor FUERA de rango → Marca DQ_FAIL
- Pero ¿qué pasa después? ¿Se mantiene el NULL o se descarta?

**Potencial Fuga #4**: Validación marca NULL pero no corrige

---

### 5. AGREGACIÓN: STAGE → REPORTING

**sp_load_to_reporting()** agrega datos por fecha/hora

**Potencial Fuga #5**: Si fuente tiene NULLs, agregación calcula SUM(NULL) = NULL
`sql
SUM(produccion_petroleo_diaria) -- Si NULL en algunos registros, suma = NULL
AVG(spm_promedio) -- Si NULL, promedio = NULL
`

---

### 6. SNAPSHOT: dataset_current_values

**Consulta**: "Última lectura por pozo"
`sql
SELECT * FROM stage.tbl_pozo_produccion
WHERE (well_id, timestamp_lectura) IN (
    SELECT well_id, MAX(timestamp_lectura) 
    FROM stage.tbl_pozo_produccion
    GROUP BY well_id
)
`

**Potencial Fuga #6**: Si última lectura tiene campos NULL → dataset_current_values heredaría NULLs

---

## 🎯 DIAGNÓSTICO: 10 Fuentes Más Probables de NULLs

| # | Fuente | Causa | Solución |
|---|--------|-------|----------|
| 1 | landing_scada_data | Archivos no encontrados | Verificar D:\ITMeet\Operaciones\API Hydrog manual\ |
| 2 | tbl_pozo_maestra | ReservasDataIngestor solo mapea 37 IDs | Incluir más IDs en mapeo |
| 3 | tbl_pozo_reservas | ingest_reservas_manual() solo inserta well_id=5 | Generar para todos los pozos |
| 4 | tbl_pozo_produccion | var_ids incompletos en landing | PIVOT debe tener todas las columnas |
| 5 | tbl_pozo_produccion | landing_scada_data sin var_ids críticos | Verificar qué var_ids llegan |
| 6 | tbl_pozo_maestra | FK constraint rechaza si well_id falta | Validar 0_1_udf_to_stage |
| 7 | FACT_OPERACIONES_* | LEFT JOIN con STAGE NULL | Cambiar a INNER JOIN |
| 8 | dataset_current_values | Última lectura con campos NULL | Filtrar timestamp_lectura NOT NULL |
| 9 | Semáforos | V3__logic_color_calculation con ACTUAL NULL | Usar COALESCE en cálculos |
| 10 | Agregaciones | SUM/AVG de NULL devuelve NULL | Usar COALESCE(campo, 0) |

---

## ✅ PRÓXIMOS PASOS DE VERIFICACIÓN

### Paso 1: Verificar Ingesta (Postgres Expert)
\\\sql
SELECT COUNT(*), COUNT(unit_id), COUNT(var_id), COUNT(measure) 
FROM stage.landing_scada_data;
\\\

### Paso 2: Verificar Maestros (DevOps Engineer)
\\\sql
SELECT COUNT(*), COUNT(well_id), COUNT(nombre_pozo) 
FROM stage.tbl_pozo_maestra;
\\\

### Paso 3: Verificar Completitud (Data Scientist)
\\\sql
SELECT well_id, COUNT(DISTINCT var_id) as variables_unicas,
       COUNT(*) as total_registros
FROM stage.landing_scada_data
GROUP BY well_id
LIMIT 10;
\\\

### Paso 4: Detectar NULLs por Columna (Postgres Expert)
\\\sql
SELECT 
    COUNT(*) as total_filas,
    COUNT(spm_promedio) as spm_no_null,
    COUNT(produccion_petroleo_diaria) as prod_no_null,
    COUNT(whp_psi) as whp_no_null
FROM stage.tbl_pozo_produccion;
\\\

---

## 📋 ANÁLISIS COMPLETADO

✅ Identificadas 10 fuentes potenciales de NULLs
✅ Mapeadas a etapas específicas del pipeline
✅ Propuestas verificaciones SQL
✅ Recomendadas soluciones por agente

