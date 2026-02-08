-- 1. Tabla de aterrizaje para los datos crudos
CREATE TABLE stage.landing_scada_data (
    idn INT,
    unit_id INT, -- Este se mapeará a well_id
    location_id INT,
    var_id INT,
    measure TEXT,
    datatime TIMESTAMP,
    createuser VARCHAR(100),
    craetedate DATE,
    moduser VARCHAR(100),
    moddate DATE
);
-- Índice para acelerar el pivoteo
CREATE INDEX idx_landing_scada_data ON stage.landing_scada_data (unit_id, datatime);

-- 2. Tabla de ejemplo para las reglas de DQ
CREATE TABLE stage.tbl_dq_rules (
    variable_name VARCHAR(50) PRIMARY KEY,
    min_val DECIMAL,
    max_val DECIMAL
);