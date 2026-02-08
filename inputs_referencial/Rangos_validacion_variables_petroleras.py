# ==============================================================================
# DICCIONARIO DE DATOS Y RANGOS FÍSICOS/LÓGICOS
# ==============================================================================

# Nomenclatura: [Nombre Simplificado]
# Tipo: Tipo de dato de Python
# Unidad: Unidad de medición de campo (US Oilfield/Métrica)
# Rango_Min / Rango_Max: Rango físico o lógico típico para validación (QA)

VARIABLES_PETROLERAS = {
    # --- VARIABLES DE ENTRADA GENERALES ---
    'ql': {
        'descripcion': 'Tasa de fluido producido (BFPD)',
        'tipo': float,
        'unidad': 'bbl/día',
        'ejemplo': 450.0,
        'Rango_Min': 0.0,
        'Rango_Max': 5000.0
    },
    'Dp': {
        'descripcion': 'Diámetro del pistón de la bomba',
        'tipo': float,
        'unidad': 'in',
        'ejemplo': 2.25,
        'Rango_Min': 1.00,
        'Rango_Max': 4.75 #---Bombas de casing-pozos someros muy alto volumen
    },
    'SPM': {
        'descripcion': 'Golpes de la bomba por minuto (Stroke Per Minute)',
        'tipo': float,
        'unidad': 'spm',
        'ejemplo': 8.5,
        'Rango_Min': 0.0,
        'Rango_Max': 10.0
    },
    'LC': {
        'descripcion': 'Longitud de carrera de la bomba',
        'tipo': float,
        'unidad': 'in',
        'ejemplo': 120.0,
        'Rango_Min': 26.0, #---C-40D
        'Rango_Max': 400.0 #---ClienteRev
    },
    'porc_llenado': {
        'descripcion': 'Porcentaje de llenado real/efectivo de la bomba',
        'tipo': float,
        'unidad': '%',
        'ejemplo': 80.0,
        'Rango_Min': 0.0,
        'Rango_Max': 100.0
    },
    # --- VARIABLES DE PRESIÓN ---
    'WHP': {
    'descripcion': 'Presión en el cabezal del pozo',
    'tipo': float,
    'unidad': 'psi',
    'ejemplo': 1200.0,
    'Rango_Min': 0.0,
    'Rango_Max': 2000.0
    },
    'CHP': {
        'descripcion': 'Presión en el casing (revestidor)',
        'tipo': float,
        'unidad': 'psi',
        'ejemplo': 1100.0,
        'Rango_Min': 0.0,
        'Rango_Max': 2000.0
    },
    'PIP': {
        'descripcion': 'Presión en la entrada de la bomba de subsuelo',
        'tipo': float,
        'unidad': 'psi',
        'ejemplo': 1000.0,
        'Rango_Min': 0.0,
        'Rango_Max': 5000.0
    },
    'P_ref': {
        'descripcion': 'Presión de referencia esperada para el sistema',
        'tipo': float,
        'unidad': 'psi',
        'ejemplo': 1300.0,
        'Rango_Min': 0.0,
        'Rango_Max': 5000.0 #---ClienteRev
    },
    # --- VARIABLES PARA KPI DE ENERGÍA ---
    'Hp': {
        'descripcion': 'Potencia consumida por el motor de la unidad',
        'tipo': float,
        'unidad': 'hp', #--- Caballos de fuerza
        'ejemplo': 50.0,
        'Rango_Min': 5.0,
        'Rango_Max': 300.0
    },
    
    'Qmax': {
        'descripcion': 'Caudal máximo teórico estimado por curva IPR o diseño',
        'tipo': float,
        'unidad': 'bbl/día',
        'ejemplo': 1000.0,
        'Rango_Min': 0.0,
        'Rango_Max': 5000.0
    },
    
        # --- VARIABLES DE SALIDA (A CALCULAR) ---
    'volumen_bomba_teorico': {
        'descripcion': 'Volumen teórico máximo que puede desplazar la bomba',
        'tipo': float,
        'unidad': 'bbl/día',
        'ejemplo': 180.0,
        'Rango_Min': 0.0,
        'Rango_Max': 5000.0 #---ClienteRev
    },
    'Efic_Vol': {
        'descripcion': 'Eficiencia Volumétrica Calculada',
        'tipo': float,
        'unidad': '%',
        'ejemplo': 30.0,
        'Rango_Min': 0.0,
        'Rango_Max': 100.0
    },
    
    
    # --- VARIABLES PARA KPI DE CONFIABILIDAD (MTBF) ---
    't': {
        'descripcion': 'Tiempo acumulado de operación sin fallas',
        'tipo': float,
        'unidad': 'd',
        'ejemplo': 180.0,
        'Rango_Min': 0.0,
        'Rango_Max': 2000.0
    },
    'n_fallas': {
        'descripcion': 'Número de fallas o paros no programados en el periodo',
        'tipo': int,
        'unidad': 'unidades',
        'ejemplo': 2,
        'Rango_Min': 0,
        'Rango_Max': 400
    },
    
    'tnp': {
        'descripcion': 'Tiempo acumulado de paros no programados',
        'tipo': float,
        'unidad': 'h',
        'ejemplo': 22.0,
        'Rango_Min': 0.0,
        'Rango_Max': 3200.0
        
    },
        
    'tpf': {
        'descripcion': 'Tiempo acumulado de paros por falla',
        'tipo': float,
        'unidad': 'h',
        'ejemplo': 42.0,
        'Rango_Min': 0,
        'Rango_Max': 3200
        
    },
        
    'tno': {
        'descripcion': 'Tiempo no operativo ',#--- (excluye mantenimiento programado)
        'tipo': float,
        'unidad': 'h',
        'ejemplo': 36.0,
        'Rango_Min': 0.0,
        'Rango_Max': 3200.0
    },
    'upt': {
        'descripcion': 'Disponibilidad operacional del sistema',
        'tipo': float,
        'unidad': '%',
        'ejemplo': 95.0,
        'Rango_Min': 0.0,
        'Rango_Max': 100.0
    },
                
    'DOP': {
        'descripcion': 'Disponibilidad Operativa calculada',
        'tipo': float,
        'unidad': '%',
        'ejemplo': 2,
        'Rango_Min': 0.0,
        'Rango_Max': 100.0
        
            },
    
    'MTBF': {
        'descripcion': 'Tiempo medio entre fallas',
        'tipo': float,
        'unidad': 'd/falla',
        'ejemplo': 0.0,
        'Rango_Min': 0.0,
        'Rango_Max': 2000.0
         
    },
    # --- Variables para KPI de Eficiencia Energética ---
    'kwh_bl': {
        'descripcion': 'Consumo energético por barril producido',
        'tipo': float,
        'unidad': 'kWh/bbl',
        'ejemplo': 2.0,
        'Rango_Min': 0.0,
        'Rango_Max': 20.0
    },
# --- VARIABLES PARA KPI DE GESTIÓN DE RESERVAS (RB) ---
    'RR': {
        'descripcion': 'Reserva remanente inicial al momento de instalación',
        'tipo': float,
        'unidad': 'bbl',
        'ejemplo': 200000.0,
        'Rango_Min': 0.0,
        'Rango_Max': 5000000.0 #---ClienteRev
        
    },
    'Np': {
        'descripcion': 'Volumen acumulado de petróleo producido desde el arranque',
        'tipo': float,
        'unidad': 'bbl',
        'ejemplo': 200000.0,
        'Rango_Min': 0.0, #---ClienteRev
        'Rango_Max': 5000000.0 #---ClienteRev
        
    },
    'RB': {
        'descripcion': 'Balance de reserva actual (RR - Np)',
        'tipo': float,
        'unidad': 'bbl',
        'ejemplo': 200000.0,
        'Rango_Min': 0.0,
        'Rango_Max': 1000000.0
        
        
    },        
    # --- VARIABLES PARA KPI DE DIAGNÓSTICO (SNE) ---
    'LC_real': {
        'descripcion': 'Longitud de Carrera real medida por sensores',
        'tipo': float,
        'unidad': 'in',
        'ejemplo': 110.0,
        'Rango_Min': 26.0,
        'Rango_Max': 366.0
    },
    'LC_nom': {
        'descripcion': 'Longitud de Carrera nominal (diseño) ',
        'tipo': float,
        'unidad': 'in',
        'ejemplo': 120.0,
        'Rango_Min': 26.0,
        'Rango_Max': 366.0
    },
    
        'SNE': {
        'descripcion': 'Porcentaje de carrera no efectiva',
        'tipo': float,
        'unidad': '%',
        'ejemplo': 15.0,
        'Rango_Min': 0.0,
        'Rango_Max': 100.0
    },
    #--- VARIABLES PARA KPI DE DIAGNÓSTICO (SPM PROMEDIO) ---
        'SPM_dia': {
        'descripcion': 'Promedio diario de golpes por minuto (Stroke Per Minute)', #--- SPM promedio del dia
        'tipo': float,
        'unidad': 'spm/d',
        'ejemplo': 1.8,
        'Rango_Min': 0.0,
        'Rango_Max': 10.0
    },
    'SPM_nominal': {
        'descripcion': 'Rango nominal de diseño para golpes por minuto',
        'tipo': tuple,
        'unidad': 'spm',
        'ejemplo': (0.5, 2.2),
        'Rango_Min': 0.5,
        'Rango_Max': 5.0 #---ClienteRev
    },
}
