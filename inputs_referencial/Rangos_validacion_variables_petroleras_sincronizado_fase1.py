# Archivo preliminar generado por FASE 1

VARIABLES_PETROLERAS = {
    # Original: ql (score: 0.281)
    'carga_maxima_fluido_api': {
        'descripcion': 'Tasa de fluido producido (BFPD)',
        'tipo': float,
        'unidad': 'bbl/día',
        'ejemplo': 450.0,
        'Rango_Min': 0.0,
        'Rango_Max': 5000.0,
    },

    # Original: Dp (score: 0.312)
    'diametro_embolo_bomba': {
        'descripcion': 'Diámetro del pistón de la bomba',
        'tipo': float,
        'unidad': 'in',
        'ejemplo': 2.25,
        'Rango_Min': 1.0,
        'Rango_Max': 4.75,
    },

    # Original: LC (score: 0.478)
    'longitud_carrera_nominal': {
        'descripcion': 'Longitud de carrera de la bomba',
        'tipo': float,
        'unidad': 'in',
        'ejemplo': 120.0,
        'Rango_Min': 26.0,
        'Rango_Max': 400.0,
    },

    # Original: porc_llenado (score: 0.368)
    'llenado_bomba_minimo': {
        'descripcion': 'Porcentaje de llenado real/efectivo de la bomba',
        'tipo': float,
        'unidad': '%',
        'ejemplo': 80.0,
        'Rango_Min': 0.0,
        'Rango_Max': 100.0,
    },

    # Original: WHP (score: 0.318)
    'presion_cabezal': {
        'descripcion': 'Presión en el cabezal del pozo',
        'tipo': float,
        'unidad': 'psi',
        'ejemplo': 1200.0,
        'Rango_Min': 0.0,
        'Rango_Max': 2000.0,
    },

    # Original: CHP (score: 0.311)
    'presion_casing': {
        'descripcion': 'Presión en el casing (revestidor)',
        'tipo': float,
        'unidad': 'psi',
        'ejemplo': 1100.0,
        'Rango_Min': 0.0,
        'Rango_Max': 2000.0,
    },

    # Original: PIP (score: 0.263)
    'presion_descarga_bomba': {
        'descripcion': 'Presión en la entrada de la bomba de subsuelo',
        'tipo': float,
        'unidad': 'psi',
        'ejemplo': 1000.0,
        'Rango_Min': 0.0,
        'Rango_Max': 5000.0,
    },

    # Original: Hp (score: 0.35)
    'potencia_nominal_motor': {
        'descripcion': 'Potencia consumida por el motor de la unidad',
        'tipo': float,
        'unidad': 'hp',
        'ejemplo': 50.0,
        'Rango_Min': 5.0,
        'Rango_Max': 300.0,
    },

    # Original: volumen_bomba_teorico (score: 0.262)
    'presion_descarga_bomba': {
        'descripcion': 'Volumen teórico máximo que puede desplazar la bomba',
        'tipo': float,
        'unidad': 'bbl/día',
        'ejemplo': 180.0,
        'Rango_Min': 0.0,
        'Rango_Max': 5000.0,
    },

    # Original: Efic_Vol (score: 0.395)
    'eficiencia_levantamiento': {
        'descripcion': 'Eficiencia Volumétrica Calculada',
        'tipo': float,
        'unidad': '%',
        'ejemplo': 30.0,
        'Rango_Min': 0.0,
        'Rango_Max': 100.0,
    },

    # Original: t (score: 0.286)
    'tiempo_operacion_medidor_acum': {
        'descripcion': 'Tiempo acumulado de operación sin fallas',
        'tipo': float,
        'unidad': 'd',
        'ejemplo': 180.0,
        'Rango_Min': 0.0,
        'Rango_Max': 2000.0,
    },

    # Original: tnp (score: 0.277)
    'tiempo_actual_drive': {
        'descripcion': 'Tiempo acumulado de paros no programados',
        'tipo': float,
        'unidad': 'h',
        'ejemplo': 22.0,
        'Rango_Min': 0.0,
        'Rango_Max': 3200.0,
    },

    # Original: tpf (score: 0.292)
    'tiempo_actual_drive': {
        'descripcion': 'Tiempo acumulado de paros por falla',
        'tipo': float,
        'unidad': 'h',
        'ejemplo': 42.0,
        'Rango_Min': 0,
        'Rango_Max': 3200,
    },

    # Original: tno (score: 0.391)
    'tiempo_actual_drive': {
        'descripcion': 'Tiempo no operativo ',
        'tipo': float,
        'unidad': 'h',
        'ejemplo': 36.0,
        'Rango_Min': 0.0,
        'Rango_Max': 3200.0,
    },

    # Original: MTBF (score: 0.316)
    'tiempo_operacion_medidor_acum': {
        'descripcion': 'Tiempo medio entre fallas',
        'tipo': float,
        'unidad': 'd/falla',
        'ejemplo': 0.0,
        'Rango_Min': 0.0,
        'Rango_Max': 2000.0,
    },

    # Original: kwh_bl (score: 0.435)
    'kwh_por_barril': {
        'descripcion': 'Consumo energético por barril producido',
        'tipo': float,
        'unidad': 'kWh/bbl',
        'ejemplo': 2.0,
        'Rango_Min': 0.0,
        'Rango_Max': 20.0,
    },

    # Original: RR (score: 0.328)
    'reserva_inicial_teorica': {
        'descripcion': 'Reserva remanente inicial al momento de instalación',
        'tipo': float,
        'unidad': 'bbl',
        'ejemplo': 200000.0,
        'Rango_Min': 0.0,
        'Rango_Max': 5000000.0,
    },

    # Original: LC_real (score: 0.397)
    'longitud_carrera_nominal': {
        'descripcion': 'Longitud de Carrera real medida por sensores',
        'tipo': float,
        'unidad': 'in',
        'ejemplo': 110.0,
        'Rango_Min': 26.0,
        'Rango_Max': 366.0,
    },

    # Original: LC_nom (score: 0.635)
    'longitud_carrera_nominal': {
        'descripcion': 'Longitud de Carrera nominal (diseño) ',
        'tipo': float,
        'unidad': 'in',
        'ejemplo': 120.0,
        'Rango_Min': 26.0,
        'Rango_Max': 366.0,
    },

    # Original: SNE (score: 0.331)
    'longitud_carrera_nominal': {
        'descripcion': 'Porcentaje de carrera no efectiva',
        'tipo': float,
        'unidad': '%',
        'ejemplo': 15.0,
        'Rango_Min': 0.0,
        'Rango_Max': 100.0,
    },

    # Original: SPM_dia (score: 0.333)
    'spm_promedio_diario_medidor': {
        'descripcion': 'Promedio diario de golpes por minuto (Stroke Per Minute)',
        'tipo': float,
        'unidad': 'spm/d',
        'ejemplo': 1.8,
        'Rango_Min': 0.0,
        'Rango_Max': 10.0,
    },

    # Original: SPM_nominal (score: 0.291)
    'carga_minima_nominal_sarta': {
        'descripcion': 'Rango nominal de diseño para golpes por minuto',
        'tipo': tuple,
        'unidad': 'spm',
        'ejemplo': (0.5, 2.2),
        'Rango_Min': 0.5,
        'Rango_Max': 5.0,
    },

}
