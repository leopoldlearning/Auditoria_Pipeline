/*
--------------------------------------------------------------------------------
-- SYNC DIM POZO TARGETS V6.3 (V4 COMPATIBLE)
-- Recuperado de V5: Sincroniza targets y límites desde referencial hacia dim_pozo
-- Prerrequisito: V4 schema
--------------------------------------------------------------------------------
*/

CREATE OR REPLACE PROCEDURE reporting.sp_sync_dim_pozo_targets()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Iniciando sincronización de Targets Referencial -> Dim Pozo...';

    UPDATE reporting.dim_pozo dp
    SET 
        mtbf_target = COALESCE(
            (SELECT target_value FROM referencial.tbl_limites_pozo lim 
             JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
             WHERE lim.pozo_id = dp.pozo_id AND var.nombre_tecnico = 'kpi_mtbf'),
            (SELECT target_value FROM referencial.tbl_limites_pozo lim 
             JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
             WHERE lim.pozo_id = 1 AND var.nombre_tecnico = 'kpi_mtbf')
        ),
        pump_spm_target = COALESCE(
            (SELECT target_value FROM referencial.tbl_limites_pozo lim 
             JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
             WHERE lim.pozo_id = dp.pozo_id AND var.nombre_tecnico = 'pump_avg_spm_act'),
            (SELECT target_value FROM referencial.tbl_limites_pozo lim 
             JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
             WHERE lim.pozo_id = 1 AND var.nombre_tecnico = 'pump_avg_spm_act')
        ),
        pump_fill_monitor_target = COALESCE(
            (SELECT target_value FROM referencial.tbl_limites_pozo lim 
             JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
             WHERE lim.pozo_id = dp.pozo_id AND var.nombre_tecnico = 'pump_fill_monitor_pct'),
            (SELECT target_value FROM referencial.tbl_limites_pozo lim 
             JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
             WHERE lim.pozo_id = 1 AND var.nombre_tecnico = 'pump_fill_monitor_pct')
        ),
        road_load_status_eff_low = COALESCE(
            (SELECT min_warning FROM referencial.tbl_limites_pozo lim 
             JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
             WHERE lim.pozo_id = dp.pozo_id AND var.nombre_tecnico = 'carga_varilla_pct'),
            (SELECT min_warning FROM referencial.tbl_limites_pozo lim 
             JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
             WHERE lim.pozo_id = 1 AND var.nombre_tecnico = 'carga_varilla_pct')
        ),
        road_load_status_eff_high = COALESCE(
            (SELECT max_warning FROM referencial.tbl_limites_pozo lim 
             JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
             WHERE lim.pozo_id = dp.pozo_id AND var.nombre_tecnico = 'carga_varilla_pct'),
            (SELECT max_warning FROM referencial.tbl_limites_pozo lim 
             JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
             WHERE lim.pozo_id = 1 AND var.nombre_tecnico = 'carga_varilla_pct')
        );

    RAISE NOTICE 'Sincronización dimensión pozo completada.';
END;
$$;
