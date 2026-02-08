/*
 * CONSULTA DE EXTRACCIÓN (PARA EJECUTAR EN EL MOTOR RAW)
 * Obtiene el último valor registrado hoy (CURRENT_DATE) para cada
 * combinación de unit_id, location_id y var_id.
 */

WITH DatosHoy AS (
    -- Paso 1: Combinar la fecha y hora, y filtrar solo por los registros de HOY
    SELECT
        idn,
        unit_id,
        location_id,
        var_id,
        measure,
        (createdate + datatime)::TIMESTAMP AS datatime, 
        createuser,
        createdate AS createdate, -- Renombramos para STAGE
        moduser,
        moddate AS moddate         -- Renombramos para STAGE
    FROM
        data
    WHERE
        createdate = CURRENT_DATE -- Filtra solo registros de hoy
),
DatosRankeados AS (
    -- Paso 2: Asignar un ranking a cada registro. 
    -- El 'rn = 1' será el más reciente para ese grupo.
    SELECT
        *,
        ROW_NUMBER() OVER(
            -- Particionamos por la clave única del sensor/variable
            PARTITION BY unit_id, location_id, var_id 
            -- Ordenamos por la fecha/hora descendente (el más nuevo primero)
            ORDER BY datatime DESC, idn DESC -- idn como desempate
        ) AS rn
    FROM
        DatosHoy
)
-- Paso 3: Seleccionar solo los registros más recientes (rn = 1)
-- Las columnas ya coinciden con la tabla STAGE 'landing_scada_data'
SELECT
    idn,
    unit_id,
    location_id,
    var_id,
    measure,
    datatime,
    createuser,
    createdate,
    moduser,
    moddate
FROM
    DatosRankeados
WHERE
    rn = 1;