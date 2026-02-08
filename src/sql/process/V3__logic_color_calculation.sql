/*
--------------------------------------------------------------------------------
-- PROCESO: CÁLCULO DE SEMÁFOROS Y TARGETS (V3 - ZERO-CALC & STANDARD)
-- DESCRIPCIÓN: Actualiza dataset_current_values y dim_pozo con configuración maestra.
-- ARQUITECTURA: Referencial (Master) -> Reporting (Slave/View)
--               El código SQL no contiene "números mágicos" ni lógica dura.
--------------------------------------------------------------------------------
*/

-- -------------------------------------------------------------------
-- 1. SINCRONIZACIÓN DE CONFIGURACIÓN (Referencial -> Reporting)
-- -------------------------------------------------------------------
-- Se copian Targets, Baselines y LÍMITES de la maestra a la dimensión.
-- -------------------------------------------------------------------

UPDATE reporting.dim_pozo dp
SET 
    -- 1.1 MTBF
    mtbf_target = (SELECT target_value FROM referencial.tbl_limites_pozo lim JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id WHERE lim.pozo_id = dp.pozo_id AND var.nombre_tecnico = 'kpi_mtbf'),
    mtbf_baseline = (SELECT baseline_value FROM referencial.tbl_limites_pozo lim JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id WHERE lim.pozo_id = dp.pozo_id AND var.nombre_tecnico = 'kpi_mtbf'),
    
    -- 1.2 SPM
    pump_spm_target = (SELECT target_value FROM referencial.tbl_limites_pozo lim JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id WHERE lim.pozo_id = dp.pozo_id AND var.nombre_tecnico = 'spm_promedio'),
    
    -- 1.3 Fluid Fill
    pump_fill_monitor_target = (SELECT target_value FROM referencial.tbl_limites_pozo lim JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id WHERE lim.pozo_id = dp.pozo_id AND var.nombre_tecnico = 'llenado_bomba_pct'),

    -- 1.4 Road Load (Carga Varilla) - Límites Dinámicos
    -- Se mapea: Min_Warning -> Eff_Low (Piso), Max_Warning -> Eff_High (Techo)
    road_load_status_eff_low = (SELECT min_warning FROM referencial.tbl_limites_pozo lim JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id WHERE lim.pozo_id = dp.pozo_id AND var.nombre_tecnico = 'carga_varilla_pct'),
    road_load_status_eff_high = (SELECT max_warning FROM referencial.tbl_limites_pozo lim JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id WHERE lim.pozo_id = dp.pozo_id AND var.nombre_tecnico = 'carga_varilla_pct'),

    -- 1.5 Hydraulic Load (Carga Unidad) - Límites Dinámicos
    hydraulic_load_status_eff_low = (SELECT min_warning FROM referencial.tbl_limites_pozo lim JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id WHERE lim.pozo_id = dp.pozo_id AND var.nombre_tecnico = 'carga_unidad_pct'),
    hydraulic_load_status_eff_high = (SELECT max_warning FROM referencial.tbl_limites_pozo lim JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id WHERE lim.pozo_id = dp.pozo_id AND var.nombre_tecnico = 'carga_unidad_pct');


-- -------------------------------------------------------------------
-- 2. CÁLCULO DE SEMÁFOROS BASADOS EN LÍMITES MAESTROS (Variables Numéricas)
-- -------------------------------------------------------------------

UPDATE reporting.dataset_current_values curr
SET 
    whp_status_color = CASE 
        WHEN curr.whp_psi >= lim.max_critical THEN (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'CRITICAL')
        WHEN curr.whp_psi >= lim.max_warning  THEN (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'WARNING')
        ELSE (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'NORMAL')
    END,

    spm_status_color = CASE
        WHEN (curr.spm_actual > lim.max_critical OR curr.spm_actual < lim.min_critical) THEN 
            (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'CRITICAL')
        WHEN (curr.spm_actual > lim.max_warning OR curr.spm_actual < lim.min_warning) THEN 
            (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'WARNING')
        ELSE 
            (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'NORMAL')
    END,

    spm_target = lim.target_value

FROM referencial.tbl_limites_pozo lim
JOIN referencial.tbl_maestra_variables var ON var.variable_id = lim.variable_id
WHERE curr.well_id = lim.pozo_id
  AND var.nombre_tecnico IN ('presion_cabezal', 'spm_promedio');

-- -------------------------------------------------------------------
-- 3. VARIABLES DE ESTADO Y FLAGS ZERO-CALC (Lógica Dinámica)
-- -------------------------------------------------------------------
-- Ahora usamos los valores de DIM_POZO (sincronizados en Paso 1)
-- en lugar de números hardcodeados (50, 95).
-- -------------------------------------------------------------------

UPDATE reporting.dataset_current_values curr
SET 
    -- ... (Lógica Comunicación y AI igual que antes) ...
    color_estado_comunicacion = CASE 
        WHEN estado_comunicacion = 'OFFLINE' THEN (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'CRITICAL')
        WHEN estado_comunicacion = 'DELAYED' THEN (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'WARNING')
        ELSE (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'NORMAL')
    END,

    -- Road Load Status - Usando Límites Dinámicos de Dim Pozo
    road_load_status_level = CASE
        WHEN (road_load_pct_act < dp.road_load_status_eff_low OR road_load_pct_act > dp.road_load_status_eff_high) THEN 3
        WHEN (road_load_pct_act >= (dp.road_load_status_eff_high - 5) AND road_load_pct_act <= dp.road_load_status_eff_high) THEN 2
        ELSE 1
    END,

    road_load_status_color = CASE
        WHEN (road_load_pct_act < dp.road_load_status_eff_low OR road_load_pct_act > dp.road_load_status_eff_high) THEN (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'CRITICAL')
        WHEN (road_load_pct_act >= (dp.road_load_status_eff_high - 5) AND road_load_pct_act <= dp.road_load_status_eff_high) THEN (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'WARNING')
        ELSE (SELECT color_hex FROM referencial.tbl_ref_estados_operativos WHERE codigo_estado = 'NORMAL')
    END,

    -- Texto dinámico
    road_load_status_legend_text = CONCAT(dp.road_load_status_eff_low, '% <= Target <= ', dp.road_load_status_eff_high, '%'),
    road_load_status_threshold_red = dp.road_load_status_eff_high

FROM reporting.dim_pozo dp
WHERE curr.well_id = dp.pozo_id;


-- -------------------------------------------------------------------
-- 4. CÁLCULO DE VARIACIONES 
-- -------------------------------------------------------------------

UPDATE reporting.dataset_current_values curr
SET 
    pump_spm_var_pct = CASE 
        WHEN dp.pump_spm_target > 0 THEN ((curr.spm_actual / dp.pump_spm_target) - 1) * 100 
        ELSE 0 
    END,

    pump_fill_monitor_var = CASE
        WHEN dp.pump_fill_monitor_target > 0 THEN ((curr.pump_fill_monitor_pct / dp.pump_fill_monitor_target) - 1) * 100
        ELSE 0
    END,

    kpi_mtbf_variance_pct = CASE
        WHEN dp.mtbf_target > 0 THEN ((curr.kpi_mtbf_hrs_act / dp.mtbf_target) - 1) * 100
        ELSE 0
    END

FROM reporting.dim_pozo dp
WHERE curr.well_id = dp.pozo_id;
