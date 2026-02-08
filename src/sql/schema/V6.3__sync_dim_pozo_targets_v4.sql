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
        mtbf_target = (
            SELECT target_value
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'kpi_mtbf'
        ),
        mtbf_baseline = (
            SELECT baseline_value
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'kpi_mtbf'
        ),
        pump_spm_target = (
            SELECT target_value
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'pump_avg_spm_act' -- V4 Name (antes spm_promedio)
        ),
        pump_fill_monitor_target = (
            SELECT target_value
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'pump_fill_monitor_pct' -- V4 Name (antes llenado_bomba_pct/pump_fill_monitor)
        ),
        road_load_status_eff_low = (
            SELECT min_warning
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'carga_varilla_pct' 
        ),
        road_load_status_eff_high = (
            SELECT max_warning
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'carga_varilla_pct'
        ),
        hydraulic_load_status_eff_low = (
            SELECT min_warning
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'carga_unidad_pct'
        ),
        hydraulic_load_status_eff_high = (
            SELECT max_warning
            FROM referencial.tbl_limites_pozo lim
            JOIN referencial.tbl_maestra_variables var ON lim.variable_id = var.variable_id
            WHERE lim.pozo_id = dp.pozo_id
              AND var.nombre_tecnico = 'carga_unidad_pct'
        );

    RAISE NOTICE 'Sincronización dimensión pozo completada.';
END;
$$;
