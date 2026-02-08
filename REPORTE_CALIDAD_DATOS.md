# Reporte de Calidad de Datos

## Resumen de Ingesta
- **Stage (Maestra)**: 1 pozo (Correcto vs Input)
- **Stage (Reservas)**: 1 registro
- **Reporting (Pozo)**: 1 pozo
- **Reporting (KPIs)**: 1 registro en `dataset_current_values`

## Consistencia
- ✅ **Completitud**: 100% de pozos de stage pasaron a reporting.
- ✅ **Reglas de Negocio**: Se aplicaron transformaciones.
- ⚠️ **Estructura**: `reporting.dim_pozo` usa nombres de columna diferentes a `stage` (verificando `well_id`).

## Aclaración sobre API Externa
El notebook `0_2` hace referencia a una API de AWS (`execute-api.us-east-1.amazonaws.com`), **no a la NASA**.
- **Estado**: Requiere credenciales (`x-pass`) no provistas.
- **Impacto**: No afecta la prueba de concepto actual con datos de Excel (UDF).
