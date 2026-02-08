-- =========================================================================
-- ARQUITECTURA V1.3: GRANULARIDAD HORARIA
-- Propósito: Habilitar visualización de alta frecuencia y drill-down.
-- =========================================================================

-- 1. DIMENSIÓN HORA (Simple, para facilitar filtros en BI)
CREATE TABLE reporting.DIM_HORA (
    Hora_ID INT PRIMARY KEY,      -- 0 a 23
    Hora_Etiqueta VARCHAR(10),    -- "00:00", "01:00"...
    Turno_Operativo VARCHAR(20)   -- Ejemplo: "Dia", "Noche"
);

-- Poblar DIM_HORA (Solo se ejecuta una vez)
INSERT INTO reporting.DIM_HORA (Hora_ID, Hora_Etiqueta, Turno_Operativo)
SELECT 
    h, 
    TO_CHAR(h, 'FM00') || ':00',
    CASE WHEN h BETWEEN 6 AND 18 THEN 'Turno Dia' ELSE 'Turno Noche' END
FROM generate_series(0, 23) as h
ON CONFLICT DO NOTHING;

-- 2. TABLA DE HECHOS HORARIA
-- Nota: Optimizada para volumen (x24 registros vs diaria)
CREATE TABLE reporting.FACT_OPERACIONES_HORARIAS (
    Fact_Hora_ID BIGINT GENERATED ALWAYS AS IDENTITY,
    Fecha_ID INT NOT NULL,
    Hora_ID INT NOT NULL,
    Pozo_ID INT NOT NULL,
    
    -- TIMESTAMP REAL (Para ordenamiento preciso en gráficas)
    Fecha_Hora TIMESTAMP NOT NULL,
    
    -- MÉTRICAS DE PRODUCCIÓN (Deltas Horarios)
    Prod_Petroleo_bbl DECIMAL(10, 2), -- Lo producido SOLO en esa hora
    Prod_Agua_bbl DECIMAL(10, 2),
    Prod_Gas_mcf DECIMAL(10, 2),
    Prod_Acumulada_Dia_bbl DECIMAL(10, 2), -- Snapshot del acumulador diario al fin de la hora
    
    -- DINÁMICA Y OPERACIÓN (Promedios Horarios)
    SPM_Promedio DECIMAL(5, 2),
    Presion_Cabezal_psi DECIMAL(10, 2),
    Presion_Casing_psi DECIMAL(10, 2),
    PIP_psi DECIMAL(10, 2),
    Temperatura_Motor_F DECIMAL(10, 2),
    Amperaje_Motor_A DECIMAL(10, 2),
    
    -- ESTADOS Y TIEMPOS
    Tiempo_Operacion_min DECIMAL(5, 2), -- Minutos operando en la hora (max 60)
    Estado_Motor_Fin_Hora BOOLEAN,      -- ¿Cómo terminó la hora?
    Numero_Fallas_Hora INT,
    
    -- CLAVES Y ÍNDICES
    PRIMARY KEY (Fecha_ID, Hora_ID, Pozo_ID), -- Partition Key lógica
    CONSTRAINT fk_hora_tiempo FOREIGN KEY (Fecha_ID) REFERENCES reporting.DIM_TIEMPO(Fecha_ID),
    CONSTRAINT fk_hora_dim FOREIGN KEY (Hora_ID) REFERENCES reporting.DIM_HORA(Hora_ID),
    CONSTRAINT fk_hora_pozo FOREIGN KEY (Pozo_ID) REFERENCES reporting.DIM_POZO(Pozo_ID)
);

-- Índices para velocidad en Dashboards
CREATE INDEX idx_fact_hora_fechahora ON reporting.FACT_OPERACIONES_HORARIAS (Fecha_Hora);
CREATE INDEX idx_fact_hora_pozo ON reporting.FACT_OPERACIONES_HORARIAS (Pozo_ID, Fecha_Hora);

COMMENT ON TABLE reporting.FACT_OPERACIONES_HORARIAS IS 'Granularidad Horaria: Permite ver tendencias intra-día.';