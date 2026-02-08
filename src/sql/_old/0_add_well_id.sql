BEGIN;

-- Insertar o actualizar los pozos en la tabla maestra,
-- incluyendo el nombre_pozo (var_id = 306)
INSERT INTO stage.tbl_pozo_maestra (
    well_id,
    nombre_pozo
)
SELECT
    unit_id AS well_id,
    MAX(CASE WHEN var_id = 306 THEN CAST(measure AS TEXT) ELSE NULL END) AS nombre_pozo
FROM
    stage.landing_scada_data
WHERE
    unit_id IS NOT NULL
GROUP BY
    unit_id

ON CONFLICT (well_id)
DO UPDATE SET
    nombre_pozo = EXCLUDED.nombre_pozo
    
WHERE 
    stage.tbl_pozo_maestra.nombre_pozo IS DISTINCT FROM EXCLUDED.nombre_pozo;

COMMIT;