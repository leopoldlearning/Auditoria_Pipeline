# 🔥 PLAN DE ACCIÓN - AUDITORÍA DE NULLs EN PIPELINE

## ⚡ INICIO RÁPIDO (5 MINUTOS)

### Paso 1: Ejecuta el Diagnóstico Rápido
```bash
# Opción A: Con psql conectado a etl_data
\i diagnostico_rapido.sql

# Opción B: Con DBeaver
# - Abre DBeaver
# - Conecta a localhost:5433 etl_data
# - Abre "diagnostico_rapido.sql"
# - Ejecuta: Ctrl+Alt+X

# Opción C: Desde PowerShell
psql -U audit -d etl_data -h localhost -p 5433 -f diagnostico_rapido.sql
```

**Qué esperar (5 segundos de ejecución):**
```
landing_scada_data     | 150000  | 42     (Si = 0 → PROBLEMA CRÍTICO #1)
pozos_sin_produccion   | 8       (Si > 5 → PROBLEMA CRÍTICO #2)
spm_cobertura_pct      | 65      (Si < 50% → PROBLEMA CRÍTICO #3)
pozos_sin_reservas     | 12      (Si > 10 → PROBLEMA CRÍTICO #4)
FACT_OPERACIONES_DIARIAS| 45000  | 85    (Si < 70% → PROBLEMA CRÍTICO #5)
```

---

## 📋 MATRIZ DE DIAGNÓSTICO

| Consulta | Si el resultado es... | Significa | Archivo a revisar |
|----------|----------------------|-----------|------------------|
| **landing_scada_data** | **= 0** | No llegó NINGÚN dato | `ingest_real_telemetry.py` FALLA |
| | < 1000 | Datos incompletos | Revisar archivos .sql en `API Hydrog manual/` |
| **pozos_sin_produccion** | **> 0** | Hay pozos huérfanos | `0_3_stage_to_stage` usa LEFT JOIN |
| **spm_cobertura_pct** | **< 40%** | Transformación PIVOT falló | `0_1_udf_to_stage` mapeos incorrectos |
| **pozos_sin_reservas** | **> 0** | Reservas NO se ingirieron | `ingest_reservas_manual()` falla |
| **FACT_OPERACIONES_DIARIAS** | **< 60% cobertura** | Agregaciones propagan NULLs | SQL `SUM(NULL) = NULL` → Falta COALESCE |

---

## 🎯 ACCIONES INMEDIATAS (POR ORDEN DE CRITICIDAD)

### 🔴 CRÍTICO - Ejecutar AHORA

#### 1. Verifica que `landing_scada_data` tenga registros
```sql
SELECT COUNT(*) FROM stage.landing_scada_data;
```

**Si = 0:**
```bash
# El ingreso de datos no funcionó
# Causas posibles:
# A) Archivos .sql NO encontrados en D:\ITMeet\Operaciones\API Hydrog manual\
# B) Patrón de búsqueda en ingest_real_telemetry.py es incorrecto
# C) Permiso denegado para leer archivos

# Solución inmediata:
python -c "
import os
archivos = os.listdir('D:\\ITMeet\\Operaciones\\API Hydrog manual\\')
print('Archivos disponibles:')
for f in archivos:
    print(f'  - {f}')
"
```

#### 2. Expande los mapeos de IDs en `0_1_udf_to_stage_AWS_v0.ipynb`
**Problema:** Solo mapea 57 IDs → 123 IDs quedan huérfanos
**Solución:** Busca esta línea en el notebook:
```python
mapeo_maestros = {1: 'well_id', 2: 'profundidad_completacion', ...}
```

Reemplaza con:
```python
# Generar mapeos automáticos para IDs sin nombre
mapeo_maestros = {}
for id_val in range(1, 163):  # Cubrir todos los 162 IDs
    mapeo_maestros[id_val] = f'campo_dinamico_{id_val}'

# Luego añade los nombres conocidos:
mapeo_maestros.update({
    1: 'well_id', 2: 'profundidad_completacion', 
    # ... resto de nombres conocidos
})
```

#### 3. Fix en `ingest_reservas_manual()` - Solo toma 1 pozo
Busca en `ingest_real_telemetry.py`:
```python
def ingest_reservas_manual(engine):
    # ❌ MAL - Solo inserta pozo 5
    INSERT INTO stage.tbl_pozo_reservas VALUES (5, ...)
```

Reemplaza con:
```python
def ingest_reservas_manual(engine):
    # ✅ BIEN - Inserta para TODOS los pozos
    pozos = pd.read_sql("SELECT DISTINCT well_id FROM stage.tbl_pozo_maestra", engine)
    for pozo_id in pozos['well_id']:
        INSERT INTO stage.tbl_pozo_reservas VALUES (pozo_id, ...)
```

### 🟡 IMPORTANTE - Ejecutar después de críticos

#### 4. Agrega COALESCE a agregaciones en `V1__stage_to_stage.sql`
Busca:
```sql
-- ❌ MAL
SELECT well_id, SUM(produccion) AS total
FROM landing_scada_data
GROUP BY well_id;
```

Reemplaza con:
```sql
-- ✅ BIEN
SELECT well_id, COALESCE(SUM(produccion), 0) AS total
FROM landing_scada_data
WHERE produccion IS NOT NULL
GROUP BY well_id;
```

#### 5. Corrige LEFT JOINs en transformaciones
Busca en cualquier SQL:
```sql
-- ❌ MAL - Produce NULLs en pozos sin datos
FROM tbl_pozo_maestra mm
LEFT JOIN tbl_pozo_produccion pp ON mm.well_id = pp.well_id
```

Reemplaza con:
```sql
-- ✅ BIEN - Solo positivos con datos
FROM tbl_pozo_maestra mm
INNER JOIN tbl_pozo_produccion pp ON mm.well_id = pp.well_id
-- Si necesitas NULLs, usa COALESCE para nombrar pozos:
SELECT COALESCE(pp.well_id, mm.well_id) AS well_id
```

---

## 📊 PASO 2: Análisis Detallado

**Después de ejecutar diagnostico_rapido.sql, si aún hay NULLs:**

```bash
# Ejecutar análisis completo de 10 fuentes:
psql -U audit -d etl_data -h localhost -p 5433 -f diagnostico_nulls.sql
```

Esto te mostrará exactamente DÓNDE están los NULLs en:
- Cada columna de `tbl_pozo_produccion`
- Cada procedimiento almacenado
- Cada notebook de transformación

---

## 🚀 PASO 3: Re-ejecutar Pipeline

```bash
cd D:\ITMeet\Operaciones\BP010-data-pipelines-auditoria
python MASTER_PIPELINE_RUNNER.py
```

Monitorea la salida. Debería completar en 5-10 minutos.

---

## ✅ PASO 4: Validar Correcciones

```bash
# Ejecuta diagnóstico rápido nuevamente
psql -U audit -d etl_data -h localhost -p 5433 -f diagnostico_rapido.sql

# Compara con resultados anteriores
```

**Éxito = Cobertura pasó de 65% → 95%+**

---

## 📚 DOCUMENTOS DE REFERENCIA

| Archivo | Qué tiene | Cuándo leerlo |
|---------|-----------|---------------|
| `RESUMEN_NULLS.txt` | 10 fuentes de NULLs + soluciones | Después de diagnostico_rapido.sql |
| `auditoria_nulls_pipeline.md` | Análisis de 3 agentes | Si necesitas entender root cause profundamente |
| `diagnostico_nulls.sql` | 10 consultas detalladas | Después de RESUMEN_NULLS |

---

## 🆘 TROUBLESHOOTING

### Problema: "psql: command not found"
```bash
# PowerShell - Agregar PostgreSQL al PATH
$env:Path += ";C:\Program Files\PostgreSQL\17\bin"
psql -U audit -d etl_data -h localhost -p 5433 -f diagnostico_rapido.sql
```

### Problema: "ROLE audit does not exist"
```bash
# Usar usuario postgres en su lugar:
psql -U postgres -d etl_data -h localhost -p 5433 -f diagnostico_rapido.sql
```

### Problema: "Database etl_data does not exist"
```bash
# Ver bases disponibles:
psql -U postgres -h localhost -p 5433 -l

# O conectar a diferente base:
psql -U postgres -d postgres -h localhost -p 5433 -f diagnostico_rapido.sql
```

---

## 📞 PREGUNTAS FRECUENTES

**P: ¿Cuánto tiempo toma el diagnóstico?**
R: diagnostico_rapido.sql = 5 segundos. diagnostico_nulls.sql = 30 segundos.

**P: ¿Debo parar el pipeline mientras corro diagnósticos?**
R: No. Puedes ejecutar diagnósticos sin afectar pipeline. Pero si vas a aplicar fixes, sí detén MASTER_PIPELINE_RUNNER.

**P: ¿Qué pasa si tengo todos los NULLs?**
R: Significa que landing_scada_data = 0. El problema es 100% en `ingest_real_telemetry.py` o archivos fuente.

**P: Los datos están bien pero aún hay 20% NULLs ¿qué hago?**
R: Son NULLs legítimos de campos opcionales. Usa COALESCE con valores por defecto apropiados.

---

**🎯 META FINAL:** Reducir cobertura de NULLs de X% → >95% en todas las columnas críticas
