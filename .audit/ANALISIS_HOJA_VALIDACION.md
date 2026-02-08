# ANÁLISIS DE VARIABLES - HOJA VALIDACIÓN vs SQL

**Fecha**: 2026-02-07 19:12:16

## Resumen

- Variables esperadas (hoja_validacion.csv): **110**
- Columnas encontradas en SQL: **226**
- Variables FALTANTES: **40**

## Variables Faltantes

| # | Variable | Tabla Destino |
|---|----------|---------------|
| 1 | `ai_accuracy_act` | reporting.dataset_current_values |
| 2 | `ai_accuracy_baseline` | reporting.DIM_POZO |
| 3 | `ai_accuracy_day` | reporting.FACT_OPERACIONES_DIARIAS |
| 4 | `ai_accuracy_target` | reporting.DIM_POZO |
| 5 | `alerta_inclinacion_grados` | reporting.DIM_POZO |
| 6 | `calculado` | N/A  |
| 7 | `carga_minima_nominal_sarta` | reporting.DIM_POZO |
| 8 | `casing_head_pressure_psi_act` | reporting.dataset_current_values |
| 9 | `current_stroke_length_act_in` | reporting.dataset_current_values |
| 10 | `curva_ ipr` | reporting.FACT_OPERACIONES_HORARIAS |
| 11 | `daily_downtime_target` | reporting.DIM_POZO |
| 12 | `estado_motor_on` | reporting.fcat_operaciones_diarias |
| 13 | `falla_inclinacion_grados` | reporting.DIM_POZO |
| 14 | `gas_fill_monitor_pct_act` | reporting.dataset_current_values |
| 15 | `hydralift_unit_load_status_eff_high` | reporting.DIM_POZO |
| 16 | `hydralift_unit_load_status_eff_low` | reporting.DIM_POZO |
| 17 | `hydraulic_load_rated_lb` | reporting.DIM_POZO |
| 18 | `hydraulic_load_rated_pct` | reporting.DIM_POZO |
| 19 | `kpi_vol_eff_baseline` | reporting.DIM_POZO |
| 20 | `longitud_carrera_nominal_in` | reporting.DIM_POZO |
| 21 | `motor_current_a_act` | reporting.dataset_current_values |
| 22 | `motor_current_a_avg_day` | reporting.FACT_OPERACIONES_DIARIAS |
| 23 | `motor_current_a_rate` | reporting.DIM_POZO |
| 24 | `motor_current_avg_7d` | reporting.FACT_OPERACIONES_DIARIAS |
| 25 | `motor_current_target` | reporting.DIM_POZO |
| 26 | `motor_power_hp_act` | reporting.dataset_current_values |
| 27 | `mtbf_variance_pct` | reporting.dataset_current_values |
| 28 | `numero_fallas_avg_7d` | reporting.FACT_OPERACIONES_DIARIAS |
| 29 | `produccion_petroleo_acumulada_bbl` | reporting.FACT_OPERACIONES_MENSUALES |
| 30 | `pump_avg_spm_act` | reporting.dataset_current_values |
| 31 | `pump_discharge_pressure_psi_act` | reporting.dataset_current_values |
| 32 | `pump_intake_pressure_psi_act` | reporting.dataset_current_values |
| 33 | `pump_spm_status_color` | reporting.dataset_current_values |
| 34 | `pwf_psi_act` | reporting.dataset_current_values |
| 35 | `remanent_reserves_bbl` | calculado |
| 36 | `rod_weight_buoyant_lb_act` | reporting.dataset_current_values |
| 37 | `sat_event_marker` | reporting.fact_operaciones_diarias |
| 38 | `shift_type` | reporting.DIM_HORA |
| 39 | `time_granularity` | N/A |
| 40 | `well_head_pressure_psi_act` | reporting.dataset_current_values |

## Desglose por Tabla SQL

### reporting.dataset_current_values
Total columnas: 92

### reporting.dataset_kpi_business
Total columnas: 14

### reporting.dataset_latest_dynacard
Total columnas: 8

### reporting.dim_hora
Total columnas: 3

### reporting.dim_pozo
Total columnas: 38

### reporting.dim_tiempo
Total columnas: 10

### reporting.fact_operaciones_diarias
Total columnas: 43

### reporting.fact_operaciones_horarias
Total columnas: 27

### reporting.fact_operaciones_mensuales
Total columnas: 27

