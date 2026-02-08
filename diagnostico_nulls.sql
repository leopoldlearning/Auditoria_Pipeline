-- =====================================================================
-- DIAGNÓSTICO DE NULLs EN PIPELINE - MASTER_PIPELINE_RUNNER
-- =====================================================================

-- PASO 1: Verificar que landing_scada_data tiene datos
SELECT 
    'landing_scada_data' as tabla,
    COUNT(*) as total_filas,
    COUNT(DISTINCT unit_id) as pozos_unicos,
    COUNT(DISTINCT var_id) as variables_unicas,
    COUNT(unit_id) as unit_id_no_null,
    COUNT(var_id) as var_id_no_null,
    COUNT(measure) as measure_no_null
FROM stage.landing_scada_data;

-- PASO 2: Verificar completitud de tbl_pozo_maestra
SELECT 
    'tbl_pozo_maestra' as tabla,
    COUNT(*) as total_filas,
    COUNT(DISTINCT well_id) as pozos_unicos,
    COUNT(well_id) as well_id_no_null,
    COUNT(nombre_pozo) as nombre_no_null,
    COUNT(cliente) as cliente_no_null,
    COUNT(campo) as campo_no_null,
    COUNT(tipo_levantamiento) as tipo_lev_no_null
FROM stage.tbl_pozo_maestra;

-- PASO 3: Verificar tbl_pozo_reservas
SELECT 
    'tbl_pozo_reservas' as tabla,
    COUNT(*) as total_filas,
    COUNT(DISTINCT well_id) as pozos_unicos,
    COUNT(well_id) as well_id_no_null,
    COUNT(gravedad_api) as gravedad_api_no_null,
    COUNT(presion_estatica_yacimiento) as presion_no_null,
    COUNT(factor_dano) as factor_dano_no_null
FROM stage.tbl_pozo_reservas;

-- PASO 4: CRÍTICO - Contador de NULLs en tbl_pozo_produccion (TABLA ANCHA)
SELECT 
    'tbl_pozo_produccion' as tabla,
    COUNT(*) as total_filas,
    COUNT(DISTINCT well_id) as pozos_unicos,
    COUNT(DISTINCT timestamp_lectura) as lecturas_unicas,
    -- Campos críticos de presión
    COUNT(presion_cabezal) as presion_cabezal_nn,
    COUNT(presion_casing) as presion_casing_nn,
    COUNT(whp_psi) as whp_psi_nn,
    -- Campos críticos de producción
    COUNT(produccion_petroleo_diaria) as prod_pet_nn,
    COUNT(produccion_agua_diaria) as prod_agua_nn,
    COUNT(porcentaje_agua) as porcentaje_agua_nn,
    -- Campos críticos de operación
    COUNT(spm_promedio) as spm_promedio_nn,
    COUNT(pump_fill_monitor) as pump_fill_nn,
    COUNT(potencia_actual_motor) as potencia_nn,
    COUNT(rpm_motor) as rpm_nn,
    COUNT(estado_motor) as estado_motor_nn
FROM stage.tbl_pozo_produccion;

-- PASO 5: Detectar pozos sin datos en tbl_pozo_produccion
SELECT 
    mp.well_id,
    mp.nombre_pozo,
    COALESCE(COUNT(pp.produccion_id), 0) as registros_produccion,
    COALESCE(COUNT(DISTINCT pp.timestamp_lectura), 0) as fechas_unicas
FROM stage.tbl_pozo_maestra mp
LEFT JOIN stage.tbl_pozo_produccion pp ON mp.well_id = pp.well_id
GROUP BY mp.well_id, mp.nombre_pozo
HAVING COUNT(pp.produccion_id) = 0
ORDER BY mp.well_id;

-- PASO 6: Verificar cobertura de var_ids en landing
SELECT 
    var_id,
    COUNT(*) as registros,
    COUNT(DISTINCT unit_id) as pozos,
    MIN(moddate) as primer_registro,
    MAX(moddate) as ultimo_registro
FROM stage.landing_scada_data
WHERE var_id IS NOT NULL
GROUP BY var_id
ORDER BY registros DESC
LIMIT 20;

-- PASO 7: Detectar pozos incompletos en reservas
SELECT 
    mm.well_id,
    mm.nombre_pozo,
    COALESCE(mr.reserva_id, 'SIN RESERVAS') as estado_reservas,
    COALESCE(mr.gravedad_api, 'NULL') as gravedad_api
FROM stage.tbl_pozo_maestra mm
LEFT JOIN stage.tbl_pozo_reservas mr ON mm.well_id = mr.well_id
ORDER BY mm.well_id;

-- PASO 8: Verificar validación de datos (DQ)
SELECT 
    COUNT(*) as total_registros_dq,
    COUNT(resultado_dq) as registros_dq_evaluados,
    COUNT(CASE WHEN resultado_dq = 'PASS' THEN 1 END) as pass_count,
    COUNT(CASE WHEN resultado_dq = 'FAIL' THEN 1 END) as fail_count
FROM stage.tbl_pozo_scada_dq;

-- PASO 9: Verificar REPORTING - FACT_OPERACIONES_DIARIAS
SELECT 
    COUNT(*) as total_fact_diarios,
    COUNT(Fecha_ID) as fecha_id_no_null,
    COUNT(Pozo_ID) as pozo_id_no_null,
    COUNT(Produccion_Petroleo_bbl) as prod_pet_no_null,
    COUNT(KPI_Efic_Vol_pct) as kpi_efic_no_null,
    COUNT(KPI_DOP_pct) as kpi_dop_no_null
FROM reporting.FACT_OPERACIONES_DIARIAS;

-- PASO 10: Verificar REPORTING - dataset_current_values  
SELECT 
    COUNT(*) as total_current,
    COUNT(well_id) as well_id_no_null,
    COUNT(nombre_pozo) as nombre_no_null,
    COUNT(oil_today_bbl) as oil_no_null,
    COUNT(water_cut_pct) as wc_no_null,
    COUNT(spm_actual) as spm_no_null,
    COUNT(motor_running_flag) as motor_no_null,
    COUNT(estado_comunicacion) as estado_no_null,
    COUNT(dq_status) as dq_no_null
FROM reporting.dataset_current_values;
