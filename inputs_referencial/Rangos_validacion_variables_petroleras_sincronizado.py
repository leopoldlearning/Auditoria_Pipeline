# ==============================================================================
# DICCIONARIO DE RANGOS DE VALIDACIÓN - SINCRONIZADO CON tbl_maestra_variables
# ==============================================================================
# 
# Generado automáticamente por sincronización con la base de datos
# Contiene mapeos entre variables petroleras simplificadas y nombres técnicos SQL
# 
# Estructura:
#   - key: nombre_tecnico_sql (de tbl_maestra_variables)
#   - value: dict con:
#     - descripcion: descripción clara
#     - unidad: unidad de medición
#     - Rango_Min: valor mínimo
#     - Rango_Max: valor máximo
#     - tipo: tipo de dato

VARIABLES_PETROLERAS = {

    # Original: CHP
    # ID:55
    # Coincidencia: mapeo_automático (score: 0.311)
    'presion_casing': {
        'descripcion': 'Presión en el casing (revestidor)',
        'tipo': float,
        'unidad': 'psi',
        'ejemplo': 1100.0,
        'Rango_Min': 0.0,
        'Rango_Max': 2000.0,
    },

    # Original: Dp
    # ID:33
    # Coincidencia: mapeo_automático (score: 0.312)
    'diametro_embolo_bomba': {
        'descripcion': 'Diámetro del pistón de la bomba',
        'tipo': float,
        'unidad': 'in',
        'ejemplo': 2.25,
        'Rango_Min': 1.0,
        'Rango_Max': 4.75,
    },

    # Original: Efic_Vol
    # ID: CALCULADO. NO APLICA
    # Coincidencia: mapeo_automático (score: 0.395)
    'kpi_vol_eff_pct': {
        'descripcion': 'Eficiencia Volumétrica Calculada',
        'tipo': float,
        'unidad': '%',
        'ejemplo': 30.0,
        'Rango_Min': 0.0,
        'Rango_Max': 100.0,
    },

    # Original: Hp
    # ID: 43
    # Coincidencia: mapeo_automático (score: 0.35)
    'potencia_nominal_motor': {
        'descripcion': 'Potencia nominal consumida por el motor de la unidad',
        'tipo': float,
        'unidad': 'hp',
        'ejemplo': 50.0,
        'Rango_Min': 5.0,
        'Rango_Max': 300.0,
    },

    # Original: LC
    # ID: 42
    # Coincidencia: mapeo_automático (score: 0.478)
    'longitud_carrera_nominal': {
        'descripcion': 'Longitud de carrera nominal de la unidad de bombeo(diseño)',
        'tipo': float,
        'unidad': 'in',
        'ejemplo': 120.0,
        'Rango_Min': 26.0,
        'Rango_Max': 400.0,
    },

    # Original: LC_nom
    # ID: 122
    # Coincidencia: mapeo_automático (score: 0.635)
    'longitud_carrera_nominal_actual_S': {
        'descripcion': 'Longitud de carrera nominal actual S (Sensor de posición lineal) ',
        'tipo': float,
        'unidad': 'in',
        'ejemplo': 120.0,
        'Rango_Min': 26.0,
        'Rango_Max': 366.0,
    },

    # Original: LC_real_unidad
    # ID: 68---calculado en reporting. Sin IDN de SCADA
    # Coincidencia: mapeo_automático (score: 0.397)
    'carrera_actual_unidad': {
        'descripcion': 'Longitud de Carrera real medida por sensores',
        'tipo': float,
        'unidad': 'in',
        'ejemplo': 110.0,
        'Rango_Min': 26.0,
        'Rango_Max': 366.0,
    },

    # Original: MTBF
    # ID: CALCULADO. NO APLICA
    # Coincidencia: mapeo_automático (score: 0.316)
    'kpi_mtbf_hrs': {
        'descripcion': 'Tiempo medio entre fallas',
        'tipo': float,
        'unidad': 'd/falla',
        'ejemplo': 0.0,
        'Rango_Min': 0.0,
        'Rango_Max': 2000.0,
    },

    # Original: PIP
    # ID:61
    # Coincidencia: mapeo_automático (score: 0.263)
    'pip': {
        'descripcion': 'Presión en la entrada de la bomba de subsuelo',
        'tipo': float,
        'unidad': 'psi',
        'ejemplo': 1000.0,
        'Rango_Min': 0.0,
        'Rango_Max': 5000.0,
    },

    # Original: Rreserva_inicial_teorica
    # ID:128
    # Coincidencia: mapeo_automático (score: 0.328)
    'reserva_inicial_teorica': {
        'descripcion': 'Reserva remanente inicial al momento de instalación',
        'tipo': float,
        'unidad': 'bbl',
        'ejemplo': 200000.0,
        'Rango_Min': 0.0,
        'Rango_Max': 5000000.0,
    },

    # Original: SNE
    # ID: CALCULADO. NO APLICA
    # Coincidencia: mapeo_automático (score: 0.331)
    'porcentaje_carrera_no_efectiva': {
        'descripcion': 'Porcentaje de carrera no efectiva',
        'tipo': float,
        'unidad': '%',
        'ejemplo': 15.0,
        'Rango_Min': 0.0,
        'Rango_Max': 100.0,
    },

    # Original: spm_promedio_diario_medidor
    # ID: 127
    # Coincidencia: mapeo_automático (score: 0.333)
    'spm_promedio_diario_medidor': {
        'descripcion': 'Promedio diario de golpes por minuto (Stroke Per Minute)',
        'tipo': float,
        'unidad': 'spm/d',
        'ejemplo': 1.8,
        'Rango_Min': 0.0,
        'Rango_Max': 10.0,
    },

    # Original: WHP
    # ID: 54
    # Coincidencia: mapeo_automático (score: 0.318)
    'presion_cabezal': {
        'descripcion': 'Presión en el cabezal del pozo',
        'tipo': float,
        'unidad': 'psi',
        'ejemplo': 1200.0,
        'Rango_Min': 0.0,
        'Rango_Max': 2000.0,
    },

    # Original: kwh_bl
    # ID: 71
    # Coincidencia: mapeo_automático (score: 0.435)
    'kwh_por_barril': {
        'descripcion': 'Consumo energético por barril producido',
        'tipo': float,
        'unidad': 'kWh/bbl',
        'ejemplo': 2.0,
        'Rango_Min': 0.0,
        'Rango_Max': 20.0,
    },

    # Original: llenado_bomba_minimo 
    # ID: 48 
    # Coincidencia: mapeo_automático (score: 0.368)
    'llenado_bomba_minimo': {
        'descripcion': 'Porcentaje de llenado real/efectivo de la bomba',
        'tipo': float,
        'unidad': '%',
        'ejemplo': 80.0,
        'Rango_Min': 0.0,
        'Rango_Max': 100.0,
    },

    # Original: produccion_fluido_diaria
    # ID: 107
    # Coincidencia: mapeo_automático (score: 0.281)
    'produccion_fluido_diaria': {
        'descripcion': 'Tasa de fluido producido por dia (BFPD)',
        'tipo': float,
        'unidad': 'bbl/día',
        'ejemplo': 450.0,
        'Rango_Min': 0.0,
        'Rango_Max': 5000.0,
    },

    # Original: t
    # ID:103
    # Coincidencia: mapeo_automático (score: 0.286)
    'tiempo_operacion_medidor_acum': {
        'descripcion': 'Tiempo acumulado de operación sin fallas',
        'tipo': float,
        'unidad': 'd',
        'ejemplo': 180.0,
        'Rango_Min': 0.0,
        'Rango_Max': 2000.0,
    },

    # Original: tiempo_parada_poc_diario
    # ID: 114
    # Coincidencia: mapeo_automático (score: 0.391)
    'tiempo_parada_poc_diario': {
        'descripcion': 'Tiempo no operativo ',
        'tipo': float,
        'unidad': 'h',
        'ejemplo': 36.0,
        'Rango_Min': 0.0,
        'Rango_Max': 3200.0,
    },

    # Original: tiempo_parada_poc_medidor_acum
    # ID: 126
    # Coincidencia: mapeo_automático (score: 0.277)
    'tiempo_parada_poc_medidor_acum': {
        'descripcion': 'Tiempo de parada POC acumulado (medidor)',
        'tipo': float,
        'unidad': 'h',
        'ejemplo': 22.0,
        'Rango_Min': 0.0,
        'Rango_Max': 3200.0,
    },

    # Original: tiempo_actual_drive
    # ID: 95
    # Coincidencia: mapeo_automático (score: 0.292)
    'tiempo_actual_drive': {
        'descripcion': 'Tiempo actual del drive/variador',
        'tipo': float,
        'unidad': 'h',
        'ejemplo': 42.0,
        'Rango_Min': 0,
        'Rango_Max': 3200,
    },
}

# ==============================================================================
# MAPEO ORIGINAL → TÉCNICO (para referencia)
# ==============================================================================
