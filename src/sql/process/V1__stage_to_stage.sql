-- ============================================================================
-- TRANSFORM_STAGE_PRODUCCION_COMPLETO.SQL
-- Transformación completa RAW -> STAGE para tbl_pozo_produccion
-- Proyecto: HRP Hydrog - Sprint 2
-- 
-- Descripción:
--   Transforma datos de landing_scada_data (formato EAV) a tbl_pozo_produccion
--   (formato ancho) usando PIVOT con CASE WHEN.
--
--
-- Autor: ML Engineering Team / ITMEET GIA
-- Fecha: 2025-11-18
-- ============================================================================

-- Iniciar transacción
BEGIN;

-- ============================================================================
-- CTE: Pivotar datos de formato largo (EAV) a formato ancho
-- ============================================================================

WITH PivotedData AS (
    SELECT
        m.well_id,
        NOW() AS timestamp_lectura,

        -- ====================================================================
        -- OPERACIÓN Y MONITOREO DEL EQUIPO
        -- ====================================================================
        
        -- ID: 51 | IDN: 727 | Pump Average SPM
        MAX(CASE WHEN l.var_id = 727 THEN CAST(l.measure AS FLOAT) END) AS spm_promedio,
        
        -- ID: 52 | IDN: 12058 | Request SPM Up
        MAX(CASE WHEN l.var_id = 12058 THEN CAST(l.measure AS FLOAT) END) AS spm_solicitado_arriba,
        
        -- ID: 53 | IDN: 12059 | Request SPM Down
        MAX(CASE WHEN l.var_id = 12059 THEN CAST(l.measure AS FLOAT) END) AS spm_solicitado_abajo,
        
        -- ID: 60 | IDN: 11 | FLOP, ft
        MAX(CASE WHEN l.var_id = 11 THEN CAST(l.measure AS FLOAT) END) AS nivel_fluido_flop,
        
        -- ID: 64 | IDN: 692 | pump fill monitor
        MAX(CASE WHEN l.var_id = 692 THEN CAST(l.measure AS FLOAT) END) AS pump_fill_monitor,
        
        -- ID: 66 | IDN: 717 | Current HP_Motor
        MAX(CASE WHEN l.var_id = 717 THEN CAST(l.measure AS FLOAT) END) AS potencia_actual_motor,
        
        -- ID: 67 | IDN: 12295 | Current AMP_Motor (Motor Thermal RMS)
        MAX(CASE WHEN l.var_id = 12295 THEN CAST(l.measure AS FLOAT) END) AS current_amperage,
        
        -- ID: 68 | IDN: NULL | Current surface unit stroke, inches
        --0 AS carrera_actual_unidad,  -- Sin mapeo IDN -- Calculado (Sacar máximo)
        
        -- ID: 69 | IDN: NULL | Current pump stroke, Inches
        --0 AS carrera_actual_bomba,  -- Sin mapeo IDN -- Calculado (Sacar máximo)
        
        -- ID: 70 | IDN: NULL | %AMP = Current AMP/Rated current -- Calculado
        --0 AS porcentaje_amperaje,  -- Sin mapeo IDN
         
        -- ID: 86 | IDN: 12277 | Motor RPM
        MAX(CASE WHEN l.var_id = 12277 THEN CAST(l.measure AS FLOAT) END) AS rpm_motor,
        
        -- ID: 95 | IDN: 321 | drive current time
        MAX(CASE WHEN l.var_id = 321 THEN CAST(l.measure AS FLOAT) END) AS tiempo_actual_drive,
        
        -- ID: 120 | IDN: 3 | Motor ON Status
        BOOL_OR(CASE WHEN l.var_id = 3 THEN CAST(l.measure AS BOOLEAN) END) AS estado_motor,
        
        -- ID: 122 | IDN: 728 | S rod stroke -- Longitud de carrera nominal actual S (Sensor de posición lineal)
        MAX(CASE WHEN l.var_id = 728 THEN CAST(l.measure AS FLOAT) END) AS s_road_stroke,

        -- ID: 42 | IDN: 714 | unit rtd stroke -- Longitud de carrera nominal de la unidad de bomneo(Diseño)
        MAX(CASE WHEN l.var_id = 714 THEN CAST(l.measure AS FLOAT) END) AS longitud_carrera_nominal_unidad,

        -- ====================================================================
        -- PRESIONES Y TEMPERATURAS
        -- ====================================================================
        
        -- ID: 54 | IDN: 137 | Well head pressure (WHP)
        MAX(CASE WHEN l.var_id = 137 THEN CAST(l.measure AS FLOAT) END) AS presion_cabezal,
        
        -- ID: 55 | IDN: 131 | Casing head pressure (CHP)
        MAX(CASE WHEN l.var_id = 131 THEN CAST(l.measure AS FLOAT) END) AS presion_casing,
        
        -- ID: 56 | IDN: 140 | THP, F
        MAX(CASE WHEN l.var_id = 140 THEN CAST(l.measure AS FLOAT) END) AS temperatura_cabezal,
        
        -- ID: 61 | IDN: 740 | PIP (Pump Intake Pressure)
        MAX(CASE WHEN l.var_id = 740 THEN CAST(l.measure AS FLOAT) END) AS pip,
        
        -- ID: 62 | IDN: 741 | Pump Discharge Pressure
        MAX(CASE WHEN l.var_id = 741 THEN CAST(l.measure AS FLOAT) END) AS presion_descarga_bomba,
        
        -- ID: 83 | IDN: NULL | Temp. Motor
        -- 0 AS temperatura_motor,  -- Sin mapeo IDN 
        
        -- ID: 84 | IDN: NULL | Temp. Aceite Bomba (Tanque)
        -- 0 AS temperatura_aceite_tanque,  -- Sin mapeo IDN -- Dar de Baja
        
        -- ID: 93 | IDN: 12282 | hyd cyl prs value
        MAX(CASE WHEN l.var_id = 12282 THEN CAST(l.measure AS FLOAT) END) AS presion_cilindro_hidraulico,
        
        -- ID: 94 | IDN: 12285 | oil tank temperature
        MAX(CASE WHEN l.var_id = 12285 THEN CAST(l.measure AS FLOAT) END) AS temperatura_tanque_aceite,

        -- ====================================================================
        -- PRODUCCIÓN Y FLUIDOS (DIARIOS)
        -- ====================================================================
        
        -- ID: 57 | IDN: 772 | Water cut
        MAX(CASE WHEN l.var_id = 772 THEN CAST(l.measure AS FLOAT) END) AS porcentaje_agua,
        
        -- ID: 96 | IDN: 12184 | gas fill monitor
        MAX(CASE WHEN l.var_id = 12184 THEN CAST(l.measure AS FLOAT) END) AS monitor_llenado_gas,
        
        -- ID: 107 | IDN: 284 | Daily Fluid production
        MAX(CASE WHEN l.var_id = 284 THEN CAST(l.measure AS FLOAT) END) AS produccion_fluido_diaria,
        
        -- ID: 108 | IDN: 1216 | Oil production daily
        MAX(CASE WHEN l.var_id = 1216 THEN CAST(l.measure AS FLOAT) END) AS produccion_petroleo_diaria,
        
        -- ID: 109 | IDN: 1217 | Daily Water production
        MAX(CASE WHEN l.var_id = 1217 THEN CAST(l.measure AS FLOAT) END) AS produccion_agua_diaria,
        
        -- ID: 110 | IDN: 286 | Daily Gas production
        MAX(CASE WHEN l.var_id = 286 THEN CAST(l.measure AS FLOAT) END) AS produccion_gas_diaria,
        
        -- ID: 113 | IDN: 883 | Daily Leakage
        MAX(CASE WHEN l.var_id = 883 THEN CAST(l.measure AS FLOAT) END) AS fuga_diaria,
        
        -- ID: 65 | IDN: ??? | Fluid Flow Monitor BPD
        MAX(CASE WHEN l.var_id = 65 THEN CAST(l.measure AS FLOAT) END) AS fluid_flow_monitor_bpd,
        
        -- ID: 119 | IDN: 866 | Liquid Fill Monitor
        MAX(CASE WHEN l.var_id = 866 THEN CAST(l.measure AS FLOAT) END) AS monitor_llenado_liquido,
        

        -- ====================================================================
        -- PRODUCCIÓN Y FLUIDOS (ACUMULADOS / INSTANTÁNEOS)
        -- ====================================================================
        
        -- ID: 97 | IDN: 13 | Fluid production meter
        MAX(CASE WHEN l.var_id = 13 THEN CAST(l.measure AS FLOAT) END) AS medidor_produccion_fluido,
        
        -- ID: 98 | IDN: 1218 | Accum_Oil production (np)
        MAX(CASE WHEN l.var_id = 1218 THEN CAST(l.measure AS FLOAT) END) AS produccion_petroleo_acumulada,
        
        -- ID: 99 | IDN: 1219 | Water production meter
        MAX(CASE WHEN l.var_id = 1219 THEN CAST(l.measure AS FLOAT) END) AS medidor_produccion_agua,
        
        -- ID: 100 | IDN: 298 | Gas production meter
        MAX(CASE WHEN l.var_id = 298 THEN CAST(l.measure AS FLOAT) END) AS medidor_produccion_gas,

        -- ====================================================================
        -- CARGAS Y CARRERAS
        -- ====================================================================
        
        -- ID: 72 | IDN: 776 | Rod Weight In Air, Lb (MOVED TO tbl_pozo_maestra)
        -- MAX(CASE WHEN l.var_id = 776 THEN CAST(l.measure AS FLOAT) END) AS peso_sarta_aire,
        
        -- ID: 73 | IDN: 733 | rod weight buoyant
        MAX(CASE WHEN l.var_id = 733 THEN CAST(l.measure AS FLOAT) END) AS rod_weight_buoyant,
        
        -- ID: 74 | IDN: 793 | Pump Load Monitor, Lb
        MAX(CASE WHEN l.var_id = 793 THEN CAST(l.measure AS FLOAT) END) AS monitor_carga_bomba,
        
        -- ID: 75 | IDN: 917 | API Maximum Fluid Load, Lb (MOVED TO tbl_pozo_maestra)
        -- MAX(CASE WHEN l.var_id = 917 THEN CAST(l.measure AS FLOAT) END) AS carga_maxima_fluido_api,
        
        -- ID: 76 | IDN: 715 | maximum rod load
        MAX(CASE WHEN l.var_id = 715 THEN CAST(l.measure AS FLOAT) END) AS maximum_rod_load,
        
        -- ID: 77 | IDN: 716 | Minimum Rod Load
        MAX(CASE WHEN l.var_id = 716 THEN CAST(l.measure AS FLOAT) END) AS minimum_rod_load,
        
        -- ID: 79 | IDN: 1135 | Gearbox Load, %
        MAX(CASE WHEN l.var_id = 1135 THEN CAST(l.measure AS FLOAT) END) AS carga_caja_engranajes,
        
        -- ID: 121 | IDN: 12296 | API pump stroke
        MAX(CASE WHEN l.var_id = 12296 THEN CAST(l.measure AS FLOAT) END) AS carrera_bomba_api,

        -- ====================================================================
        -- INDICADORES DE EFICIENCIA Y POC
        -- ====================================================================
        
        -- ID: 71 | IDN: 282 | Kwh/Bbl
        MAX(CASE WHEN l.var_id = 282 THEN CAST(l.measure AS FLOAT) END) AS kwh_por_barril,
        
        -- ID: 106 | IDN: 293 | Daily Run percent
        MAX(CASE WHEN l.var_id = 293 THEN CAST(l.measure AS FLOAT) END) AS porcentaje_operacion_diario,
        
        -- ID: 114 | IDN: 292 | Daily POC Down time
        MAX(CASE WHEN l.var_id = 292 THEN CAST(l.measure AS FLOAT) END) AS tiempo_parada_poc_diario,
        
        -- ID: 115 | IDN: 291 | Daily POC Count
        MAX(CASE WHEN l.var_id = 291 THEN CAST(l.measure AS INT) END) AS conteo_poc_diario,
        
        -- ID: 117 | IDN: 282 | Gauge Energy Usage
        -- MAX(CASE WHEN l.var_id = 282 THEN CAST(l.measure AS FLOAT) END) AS uso_energia_medidor,
        --0 AS uso_energia_medidor,  -- Conflicto IDN -- Duplicada.
        
        -- ID: 118 | IDN: 8 | Lift Efficiency
        MAX(CASE WHEN l.var_id = 8 THEN CAST(l.measure AS FLOAT) END) AS eficiencia_levantamiento,

        -- ====================================================================
        -- SENSORES Y OTROS
        -- ====================================================================
        
        -- ID: 78 | IDN: 766 | Anchor Vertical Depth
        MAX(CASE WHEN l.var_id = 766 THEN CAST(l.measure AS FLOAT) END) AS anchor_vertical_depth,
        
        -- ID: 82 | IDN: 12283 | stem tilt -- Pendiente pozo
        MAX(CASE WHEN l.var_id = 12283 THEN CAST(l.measure AS FLOAT) END) AS inclinacion_vastago,
        
        -- ID: 88 | IDN: 12279 | cylinder tilt x
        MAX(CASE WHEN l.var_id = 12279 THEN CAST(l.measure AS FLOAT) END) AS inclinacion_cilindro_x,
        
        -- ID: 89 | IDN: 12280 | cylinder tilt y
        MAX(CASE WHEN l.var_id = 12280 THEN CAST(l.measure AS FLOAT) END) AS inclinacion_cilindro_y,
        
        -- ID: 90 | IDN: 12268 | cyl tilt warn deg
        MAX(CASE WHEN l.var_id = 12268 THEN CAST(l.measure AS FLOAT) END) AS alerta_inclinacion_grados,
        
        -- ID: 91 | IDN: 12269 | cyl tilt fault deg
        MAX(CASE WHEN l.var_id = 12269 THEN CAST(l.measure AS FLOAT) END) AS falla_inclinacion_grados,
        
        -- ID: 92 | IDN: 12281 | linear pos
        MAX(CASE WHEN l.var_id = 12281 THEN CAST(l.measure AS FLOAT) END) AS posicion_lineal,

        -- ====================================================================
        -- ACUMULADORES Y CONTADORES
        -- ====================================================================
        
        -- ID: 101 | IDN: 694 | Pump stroke counter
        MAX(CASE WHEN l.var_id = 694 THEN CAST(l.measure AS INT) END) AS contador_emboladas,
        
        -- ID: 102 | IDN: 1206 | Cumulative run hours
        MAX(CASE WHEN l.var_id = 1206 THEN CAST(l.measure AS FLOAT) END) AS horas_operacion_acumuladas,
        
        -- ID: 103 | IDN: 294 | gauge run time accum
        MAX(CASE WHEN l.var_id = 294 THEN CAST(l.measure AS FLOAT) END) AS tiempo_operacion_medidor_acum,
        
        -- ID: 104 | IDN: 934 | gauge power meter accum
        MAX(CASE WHEN l.var_id = 934 THEN CAST(l.measure AS FLOAT) END) AS energia_medidor_acumulada,
        
        -- ID: 105 | IDN: 299 | gauge strokes accum
        MAX(CASE WHEN l.var_id = 299 THEN CAST(l.measure AS INT) END) AS emboladas_medidor_acumuladas,
        
        -- ID: 111 | IDN: 289 | Daily Strokes
        MAX(CASE WHEN l.var_id = 289 THEN CAST(l.measure AS INT) END) AS emboladas_diarias,
        
        -- ID: 112 | IDN: 290 | Daily Avg fill
        MAX(CASE WHEN l.var_id = 290 THEN CAST(l.measure AS FLOAT) END) AS llenado_promedio_diario,
        
        -- ID: 116 | IDN: 283 | gauge power meter daily
        MAX(CASE WHEN l.var_id = 283 THEN CAST(l.measure AS FLOAT) END) AS potencia_medidor_diaria,
        
        -- ID: 123 | IDN: 898 | POC powerup strokes
        MAX(CASE WHEN l.var_id = 898 THEN CAST(l.measure AS INT) END) AS emboladas_arranque_poc,
        
        -- ID: 124 | IDN: 899 | POC standby strokes
        MAX(CASE WHEN l.var_id = 899 THEN CAST(l.measure AS INT) END) AS emboladas_espera_poc,
        
        -- ID: 125 | IDN: 896 | gauge POC count accum
        MAX(CASE WHEN l.var_id = 896 THEN CAST(l.measure AS INT) END) AS conteo_poc_medidor_acum,
        
        -- ID: 126 | IDN: 1188 | gauge POC down time accum
        MAX(CASE WHEN l.var_id = 1188 THEN CAST(l.measure AS FLOAT) END) AS tiempo_parada_poc_medidor_acum,
        
        -- ID: 127 | IDN: 288 | Daily gauge avg spm
        MAX(CASE WHEN l.var_id = 288 THEN CAST(l.measure AS FLOAT) END) AS spm_promedio_diario_medidor,

        -- ====================================================================
        -- TARJETAS DE DINAMÓMETRO
        -- ====================================================================
        
        -- ID: 155 | IDN: 10000 | Current Inch Surface Card
        MAX(CASE WHEN l.var_id = 10000 THEN CAST(l.measure AS TEXT) END) AS surface_rod_position,
        
        -- ID: 156 | IDN: 10001 | Current Lb Surface Card
        MAX(CASE WHEN l.var_id = 10001 THEN CAST(l.measure AS TEXT) END) AS surface_rod_load,
        
        -- ID: 157 | IDN: 10002 | Current Inch Downhole Pump Card
        MAX(CASE WHEN l.var_id = 10002 THEN CAST(l.measure AS TEXT) END) AS downhole_pump_position,
        
        -- ID: 158 | IDN: 10003 | Current Lb Downhole Pump Card
        MAX(CASE WHEN l.var_id = 10003 THEN CAST(l.measure AS TEXT) END) AS downhole_pump_load,

        -- ====================================================================
        -- MISCELANEOS
        -- ====================================================================
         -- ID: 59 | IDN: 59 | Fluid Level TVD, ft
        MAX(CASE WHEN l.var_id = 10 THEN CAST(l.measure AS FLOAT) END) AS nivel_fluido_dinamico


    FROM
        stage.landing_scada_data l
    INNER JOIN
        stage.tbl_pozo_maestra m ON l.unit_id = m.well_id
    GROUP BY
        m.well_id
)

-- ============================================================================
-- INSERCIÓN EN TABLA STAGE
-- ============================================================================

INSERT INTO stage.tbl_pozo_produccion (
    well_id,
    timestamp_lectura,
    
    -- Operación y Monitoreo
    spm_promedio,
    spm_solicitado_arriba,
    spm_solicitado_abajo,
    nivel_fluido_flop,
    pump_fill_monitor,
    potencia_actual_motor,
    current_amperage,
    --carrera_actual_unidad,
    --carrera_actual_bomba,
    --porcentaje_amperaje,
    rpm_motor,
    tiempo_actual_drive,
    estado_motor,
    longitud_carrera_nominal_unidad,
    
    -- Presiones y Temperaturas
    presion_cabezal,
    presion_casing,
    temperatura_cabezal,
    pip,
    presion_descarga_bomba,
    --temperatura_motor,
    --temperatura_aceite_tanque,
    presion_cilindro_hidraulico,
    temperatura_tanque_aceite,
    
    -- Producción Diaria
    porcentaje_agua,
    monitor_llenado_gas,
    produccion_fluido_diaria,
    produccion_petroleo_diaria,
    produccion_agua_diaria,
    produccion_gas_diaria,
    fuga_diaria,
    fluid_flow_monitor_bpd,
    monitor_llenado_liquido,
    
    -- Producción Acumulada
    medidor_produccion_fluido,
    produccion_petroleo_acumulada,
    medidor_produccion_agua,
    medidor_produccion_gas,
    
    -- Cargas y Carreras
    -- peso_sarta_aire, -- MOVED TO tbl_pozo_maestra
    rod_weight_buoyant,
    monitor_carga_bomba,
    -- carga_maxima_fluido_api, -- MOVED TO tbl_pozo_maestra
    maximum_rod_load,
    minimum_rod_load,
    carga_caja_engranajes,
    carrera_bomba_api,
    
    -- Eficiencia y POC
    kwh_por_barril,
    porcentaje_operacion_diario,
    tiempo_parada_poc_diario,
    conteo_poc_diario,
    --uso_energia_medidor,
    eficiencia_levantamiento,
    
    -- Sensores
    anchor_vertical_depth,
    inclinacion_vastago,
    inclinacion_cilindro_x,
    inclinacion_cilindro_y,
    alerta_inclinacion_grados,
    falla_inclinacion_grados,
    posicion_lineal,
    
    -- Acumuladores
    contador_emboladas,
    horas_operacion_acumuladas,
    tiempo_operacion_medidor_acum,
    energia_medidor_acumulada,
    emboladas_medidor_acumuladas,
    emboladas_diarias,
    llenado_promedio_diario,
    potencia_medidor_diaria,
    emboladas_arranque_poc,
    emboladas_espera_poc,
    conteo_poc_medidor_acum,
    tiempo_parada_poc_medidor_acum,
    spm_promedio_diario_medidor,
    
    -- Dinamómetro
    surface_rod_position,
    surface_rod_load,
    downhole_pump_position,
    downhole_pump_load,
    nivel_fluido_dinamico
)
SELECT
    well_id,
    timestamp_lectura,
    
    -- Operación y Monitoreo
    spm_promedio,
    spm_solicitado_arriba,
    spm_solicitado_abajo,
    nivel_fluido_flop,
    pump_fill_monitor,
    potencia_actual_motor,
    current_amperage,
    --carrera_actual_unidad,
    --carrera_actual_bomba,
    --porcentaje_amperaje,
    rpm_motor,
    tiempo_actual_drive,
    estado_motor,
    longitud_carrera_nominal_unidad, ---CAmbio de nombre
    
    -- Presiones y Temperaturas
    presion_cabezal,
    presion_casing,
    temperatura_cabezal,
    pip,
    presion_descarga_bomba,
    --temperatura_motor,
    --temperatura_aceite_tanque,
    presion_cilindro_hidraulico,
    temperatura_tanque_aceite,
    
    -- Producción Diaria
    porcentaje_agua,
    monitor_llenado_gas,
    produccion_fluido_diaria,
    produccion_petroleo_diaria,
    produccion_agua_diaria,
    produccion_gas_diaria,
    fuga_diaria,
    fluid_flow_monitor_bpd,
    monitor_llenado_liquido,
    
    -- Producción Acumulada
    medidor_produccion_fluido,
    produccion_petroleo_acumulada,
    medidor_produccion_agua,
    medidor_produccion_gas,
    
    -- Cargas y Carreras
    -- peso_sarta_aire, -- MOVED TO tbl_pozo_maestra
    rod_weight_buoyant,
    monitor_carga_bomba,
    -- carga_maxima_fluido_api, -- MOVED TO tbl_pozo_maestra
    maximum_rod_load,
    minimum_rod_load,
    carga_caja_engranajes,
    carrera_bomba_api,
    
    -- Eficiencia y POC
    kwh_por_barril,
    porcentaje_operacion_diario,
    tiempo_parada_poc_diario,
    conteo_poc_diario,
    --uso_energia_medidor,
    eficiencia_levantamiento,
    
    -- Sensores
    anchor_vertical_depth,
    inclinacion_vastago,
    inclinacion_cilindro_x,
    inclinacion_cilindro_y,
    alerta_inclinacion_grados,
    falla_inclinacion_grados,
    posicion_lineal,
    
    -- Acumuladores
    contador_emboladas,
    horas_operacion_acumuladas,
    tiempo_operacion_medidor_acum,
    energia_medidor_acumulada,
    emboladas_medidor_acumuladas,
    emboladas_diarias,
    llenado_promedio_diario,
    potencia_medidor_diaria,
    emboladas_arranque_poc,
    emboladas_espera_poc,
    conteo_poc_medidor_acum,
    tiempo_parada_poc_medidor_acum,
    spm_promedio_diario_medidor,
    
    -- Dinamómetro
    surface_rod_position,
    surface_rod_load,
    downhole_pump_position,
    downhole_pump_load,
    nivel_fluido_dinamico
    
FROM
    PivotedData
ON CONFLICT (well_id, timestamp_lectura)
DO UPDATE SET
    -- Actualizar todos los campos si ya existe el registro
    spm_promedio = EXCLUDED.spm_promedio,
    spm_solicitado_arriba = EXCLUDED.spm_solicitado_arriba,
    spm_solicitado_abajo = EXCLUDED.spm_solicitado_abajo,
    nivel_fluido_flop = EXCLUDED.nivel_fluido_flop,
    pump_fill_monitor = EXCLUDED.pump_fill_monitor,
    potencia_actual_motor = EXCLUDED.potencia_actual_motor,
    current_amperage = EXCLUDED.current_amperage,
    rpm_motor = EXCLUDED.rpm_motor,
    estado_motor = EXCLUDED.estado_motor,
    presion_casing = EXCLUDED.presion_casing,
    temperatura_cabezal = EXCLUDED.temperatura_cabezal,
    pip = EXCLUDED.pip,
    presion_descarga_bomba = EXCLUDED.presion_descarga_bomba,
    porcentaje_agua = EXCLUDED.porcentaje_agua,
    produccion_fluido_diaria = EXCLUDED.produccion_fluido_diaria,
    produccion_petroleo_diaria = EXCLUDED.produccion_petroleo_diaria,
    produccion_agua_diaria = EXCLUDED.produccion_agua_diaria,
    monitor_carga_bomba = EXCLUDED.monitor_carga_bomba,
    eficiencia_levantamiento = EXCLUDED.eficiencia_levantamiento;

COMMIT;

-- ============================================================================
-- NOTAS IMPORTANTES
-- ============================================================================
-- 
-- 1. CAMPOS SIN MAPEO IDN (8 variables):
--    - presion_cabezal (ID 54)
--    - carrera_actual_unidad (ID 68)
--    - carrera_actual_bomba (ID 69)
--    - porcentaje_amperaje (ID 70) - Se puede calcular: current_amperage / corriente_nominal_motor
--    - temperatura_motor (ID 83)
--    - temperatura_aceite_tanque (ID 84)
--    - produccion_gas_diaria (ID 110)
--    - well_id (ID 164) - Ya viene de la tabla maestra
--    - reserva_inicial_teorica (ID 128) - dato entregado por el cliente. ID nuevo
--
-- 2. CONFLICTOS DE IDN:
--    IDN 282 está mapeado a 3 variables diferentes:
--    - ID 57: porcentaje_agua (water_cut)
--    - ID 71: kwh_por_barril
--    - ID 117: uso_energia_medidor
--    Se mantiene solo water_cut, los otros quedan NULL
--
-- 3. TIMESTAMP:
--    - timestamp_lectura viene de landing_scada_data.datatime
--    - Representa el momento de la lectura SCADA en tiempo real
--
-- 4. TARJETAS DINAMÓMETRO:
--    - Las 4 tarjetas (IDs 155-158) se almacenan como TEXT
--    - Contienen arrays de mediciones serializadas
--
-- 5. EJECUCIÓN:
--    Este script se debe ejecutar periódicamente (cada minuto/cada SPM)
--    según la frecuencia de actualización de landing_scada_data
--
-- 6. PERFORMANCE:
--    - El PIVOT con MAX(CASE...) es eficiente para ~70 columnas
--    - Considera índices en landing_scada_data(unit_id, datatime, var_id)
--    - Usa particionamiento en tbl_pozo_produccion si hay millones de registros
--
-- ============================================================================