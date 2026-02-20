-- Script Generado Automáticamente por generate_referencial_seed.py
-- Fecha: 2026-01-21

-- 1. Carga de Maestra de Variables
TRUNCATE TABLE referencial.tbl_maestra_variables RESTART IDENTITY CASCADE;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (6, 'fecha_registro', 'stage.tbl_pozo_maestra', 'INPUT_MANUAL') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (12, 'Cliente', 'stage.tbl_pozo_maestra', 'INPUT_MANUAL') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (14, 'region', 'stage.tbl_pozo_maestra', 'INPUT_MANUAL') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (15, 'campo', 'stage.tbl_pozo_maestra', 'INPUT_MANUAL') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (49, 'nombre_pozo', 'stage.tbl_pozo_maestra', 'INPUT_MANUAL') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (51, 'spm_promedio', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (64, 'pump_fill_monitor', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (96, 'monitor_llenado_gas', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (118, 'eficiencia_levantamiento', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (76, 'maximum_rod_load', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (77, 'minimum_rod_load', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (74, 'monitor_carga_bomba', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (72, 'peso_sarta_aire', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (73, 'rod_weight_buoyant', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (75, 'carga_maxima_fluido_api', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (59, 'nivel_fluido_dinamico', 'stage.tbl_pozo_reservas', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (39, 'profundidad_vertical_bomba', 'stage.tbl_pozo_maestra', 'INPUT_MANUAL') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (38, 'profundidad_vertical_yacimiento', 'stage.tbl_pozo_maestra', 'INPUT_MANUAL') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (107, 'produccion_fluido_diaria', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (108, 'produccion_petroleo_diaria', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (109, 'produccion_agua_diaria', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (151, 'PWF_psi', 'Capa Universal', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (54, 'presion_cabezal', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (55, 'presion_casing', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (61, 'PIP', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (62, 'presion_descarga_bomba', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (98, 'produccion_petroleo_acumulada', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (130, 'Reserva remanente', 'calculado', 'CALCULADO') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (128, 'reserva_inicial_teorica', 'stage.tbl_pozo_reservas', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (42, 'longitud_carrera_nominal', 'stage.tbl_pozo_maestra', 'INPUT_MANUAL') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (68, 'carrera_actual_unidad', 'calculado', 'CALCULADO') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (46, 'carga_nominal_unidad', 'stage.tbl_pozo_maestra', 'INPUT_MANUAL') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (94, 'temperatura_tanque_aceite', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (43, 'potencia_nominal_motor', 'stage.tbl_pozo_maestra', 'INPUT_MANUAL') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (66, 'potencia_actual_motor', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (67, 'current_amperage', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (44, 'corriente_nominal_motor', 'stage.tbl_pozo_maestra', 'INPUT_MANUAL') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (120, 'estado_motor', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (106, 'porcentaje_operacion_diario', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (114, 'tiempo_parada_poc_diario', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (88, 'inclinacion_cilindro_x', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (89, 'inclinacion_cilindro_y', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (90, 'alerta_inclinacion_grados', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (91, 'falla_inclinacion_grados', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (47, 'carga_minima_nominal_sarta', 'stage.tbl_pozo_maestra', 'INPUT_MANUAL') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (115, 'Numero_Fallas', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;
INSERT INTO referencial.tbl_maestra_variables (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica) VALUES (71, 'kwh_por_barril', 'stage.tbl_pozo_produccion', 'SENSOR') ON CONFLICT (nombre_tecnico) DO NOTHING;

-- 2. Carga de Reglas de Calidad
TRUNCATE TABLE referencial.tbl_dq_rules RESTART IDENTITY CASCADE;
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, latencia_max_segundos) SELECT variable_id, 0.0001, 2 FROM referencial.tbl_maestra_variables WHERE id_formato1 = 51; -- spm_promedio
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, latencia_max_segundos) SELECT variable_id, 0.0001, 2 FROM referencial.tbl_maestra_variables WHERE id_formato1 = 54; -- presion_cabezal
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, latencia_max_segundos) SELECT variable_id, 0.0001, 2 FROM referencial.tbl_maestra_variables WHERE id_formato1 = 64; -- pump_fill_monitor
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, latencia_max_segundos) SELECT variable_id, 0.0001, 2 FROM referencial.tbl_maestra_variables WHERE id_formato1 = 107; -- produccion_fluido_diaria
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, latencia_max_segundos) SELECT variable_id, 0.0001, 2 FROM referencial.tbl_maestra_variables WHERE id_formato1 = 108; -- produccion_petroleo_diaria
INSERT INTO referencial.tbl_dq_rules (variable_id, valor_min, latencia_max_segundos) SELECT variable_id, 0.0001, 2 FROM referencial.tbl_maestra_variables WHERE id_formato1 = 109; -- produccion_agua_diaria

-- 3. Carga de Reglas de Consistencia
TRUNCATE TABLE referencial.tbl_reglas_consistencia RESTART IDENTITY CASCADE;
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto) VALUES ('RC-001', 'Cargas de la Barra 1', 'Maximum rod load > Minimum rod load');
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto) VALUES ('RC-002', 'Cargas de la Barra 2', 'Maximum rod load > Rod weight buoyant');
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto) VALUES ('RC-003', 'Gradiente de PresiÃ³n del Fondo a la Superficie', 'PresiÃ³n de fondo fluyente (FBHP) > Well head pressure (WHP)');
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto) VALUES ('RC-004', 'Relaciones de PresiÃ³n', 'PresiÃ³n de fondo fluyente (FBHP) < PresiÃ³n estÃ¡tica del yacimiento (Ps)');
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto) VALUES ('RC-005', 'Relaciones de Profundidad', 'Profundidad vertical de la bomba < Profundidad vertical del yacimiento');
INSERT INTO referencial.tbl_reglas_consistencia (codigo_rc, descripcion, criterio_texto) VALUES ('RC-006', 'Relaciones de GeometrÃ­a', 'Radio del pozo < Radio equivalente < Radio de drenaje');
