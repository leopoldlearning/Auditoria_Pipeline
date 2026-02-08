# DIAGNÓSTICO DE NOMENCLATURA - V4 REPORTING SCHEMA

**Fecha**: 2026-02-08 09:01:53

## 1. Estado Actual (V3)

- `fact_operaciones_horarias`: 27 columnas
- `fact_operaciones_diarias`: 43 columnas
- `fact_operaciones_mensuales`: 27 columnas
- `dataset_current_values`: 92 columnas

## 2. Inconsistencias Detectadas

✅ No se detectaron inconsistencias mayores.

## 3. Correcciones del Usuario

### horarias

- **AGREGAR**: produccion_fluido_bbl
  - ID: 107
  - Comentario: Incluir en reporting operaciones horarias. NO existe

- **RENOMBRAR**: prod_petroleo_bbl → prod_petroleo_diaria_bpd
  - ID: 108
  - Comentario: Cambiar prod_petroleo_bbl por prod_petroleo_diaria_bpd

### mensuales

- **RENOMBRAR**: total_petroleo_bbl → produccion_petroleo_acumulada_bbl
  - ID: 98
  - Comentario: Cambiar total_petroleo_bbl por produccion_petroleo_acumulada_bbl

- **AGREGAR**: remanent_reserves_bbl
  - ID: 130
  - Comentario: Calculado. Incluir en reporting.fact_operaciones_mensuales

- **AGREGAR**: prom_produccion_fluido_bbl
  - ID: 107
  - Comentario: Incluir en operaciones mensuales. NO existe

### current_values

- **AGREGAR**: estado_motor_on
  - ID: 120
  - Comentario: Variable binaria para saber si motor está encendido. Barra verde/rojo en dashboard

