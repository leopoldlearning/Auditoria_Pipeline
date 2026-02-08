-- REFERENCIAL MASTER EXTENSION (V3.5 compatible)
-- No rompe estructura existente, solo agrega objetos auxiliares.

SET search_path TO referencial, public;

-- 1) Mapa SCADA → Formato1 → Stage
CREATE TABLE IF NOT EXISTS referencial.tbl_var_scada_map (
    var_id_scada INT PRIMARY KEY,
    id_formato1 INT NOT NULL,
    columna_stage TEXT NOT NULL,
    comentario TEXT
);

-- 2) Asegurar que tbl_maestra_variables tenga columnas clave (por si viene vacía)
-- (NO se alteran columnas existentes, solo se asume definición previa)

-- 3) Vista unificada de variables (Maestra + SCADA + Unidades)
CREATE OR REPLACE VIEW referencial.vw_variables_scada_stage AS
SELECT
    mv.variable_id,
    mv.id_formato1,
    mv.nombre_tecnico,
    mv.tabla_origen,
    mv.clasificacion_logica,
    mv.volatilidad,
    u.simbolo AS unidad,
    vsm.var_id_scada,
    vsm.columna_stage,
    vsm.comentario
FROM referencial.tbl_maestra_variables mv
LEFT JOIN referencial.tbl_var_scada_map vsm
       ON vsm.id_formato1 = mv.id_formato1
LEFT JOIN referencial.tbl_ref_unidades u
       ON u.unidad_id = mv.unidad_id;
