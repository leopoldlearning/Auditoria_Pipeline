#!/usr/bin/env python3
"""
Script para generar un archivo de rangos de validación sincronizado con tbl_maestra_variables.
Compara descripciones y crea mapeos automáticos donde es posible.
"""

import os
import re
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import difflib

load_dotenv()

# Conexión a BD
DB_URL = f"postgresql://{os.getenv('DB_USER', 'audit')}:{os.getenv('DEV_DB_PASSWORD', 'audit')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5433')}/{os.getenv('DB_NAME', 'etl_data')}"
engine = create_engine(DB_URL)

# Cargar VARIABLES_PETROLERAS del archivo actual
import importlib.util
spec = importlib.util.spec_from_file_location(
    "variables_ref",
    "inputs_referencial/Rangos_de_validacion_variables_petroleras.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
variables_petroleras = module.VARIABLES_PETROLERAS

print("=" * 80)
print("GENERADOR DE RANGOS DE VALIDACIÓN SINCRONIZADO")
print("=" * 80)

# Obtener variables técnicas de la BD
with engine.begin() as conn:
    result = conn.execute(text("""
        SELECT variable_id, nombre_tecnico, id_formato1
        FROM referencial.tbl_maestra_variables
        ORDER BY nombre_tecnico
    """))
    variables_bd = {row[1]: {'id': row[0], 'id_fmt': row[2]} for row in result}

print(f"\nVariables en BD: {len(variables_bd)}")
print(f"Variables en VARIABLES_PETROLERAS: {len(variables_petroleras)}")

# Función para encontrar similitud entre strings
def similitud(s1, s2):
    """Retorna un score de 0 a 1 indicando similitud entre strings"""
    s1_lower = s1.lower().replace('_', ' ')
    s2_lower = s2.lower().replace('_', ' ')
    return difflib.SequenceMatcher(None, s1_lower, s2_lower).ratio()

# Mapeo automático mejorado con palabras clave
# Primero: mapeos por palabra clave exacta en descripción
palabras_clave_bd = {}
for nombre_tecnico in variables_bd.keys():
    palabras = set(nombre_tecnico.lower().replace('_', ' ').split())
    palabras_clave_bd[nombre_tecnico] = palabras

mapeos = {}
for var_simple, data in variables_petroleras.items():
    descripcion = data.get('descripcion', '').lower()
    palabras_desc = set(descripcion.split())
    
    # Búsqueda por palabra clave exacta en descripción
    mejores_matches = []
    
    for nombre_tecnico in variables_bd.keys():
        # Score 1: coincidencia de palabras clave
        palabras_tecnico = palabras_clave_bd[nombre_tecnico]
        coincidencias = len(palabras_desc & palabras_tecnico)
        score_palabra_clave = coincidencias / max(len(palabras_desc), len(palabras_tecnico)) if palabras_desc else 0
        
        # Score 2: similitud de strings
        score_nombre = similitud(var_simple, nombre_tecnico)
        score_desc = similitud(descripcion, nombre_tecnico.replace('_', ' '))
        
        # Scoring combinado: priorizar palabra clave > descripción > nombre
        score_final = (score_palabra_clave * 0.5) + (score_desc * 0.35) + (score_nombre * 0.15)
        
        if score_final > 0.25:  # Umbral más conservador
            mejores_matches.append((nombre_tecnico, score_final))
    
    if mejores_matches:
        mejores_matches.sort(key=lambda x: x[1], reverse=True)
        mejor = mejores_matches[0]
        mapeos[var_simple] = {
            'nombre_tecnico': mejor[0],
            'coincidencia': 'mapeo_automático',
            'score': round(mejor[1], 3),
            'alternativas': [m[0] for m in mejores_matches[1:3]]
        }
    else:
        mapeos[var_simple] = {
            'nombre_tecnico': None,
            'coincidencia': 'sin_mapeo',
            'score': 0.0
        }

# Resumen
mapeadas = sum(1 for m in mapeos.values() if m['nombre_tecnico'])
sin_mapeo = len(mapeos) - mapeadas

print(f"\nResultados del mapeo:")
print(f"  Mapeadas: {mapeadas}/{len(mapeos)}")
print(f"  Sin mapeo: {sin_mapeo}/{len(mapeos)}")

# Generar nuevo archivo Python
nuevo_archivo_contenido = '''# ==============================================================================
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
'''

# Agregar variables al nuevo archivo
count_mapeadas = 0
for var_simple in sorted(variables_petroleras.keys()):
    data = variables_petroleras[var_simple]
    mapeo = mapeos.get(var_simple, {})
    nombre_tecnico = mapeo.get('nombre_tecnico')
    
    if nombre_tecnico:  # Solo incluir variables mapeadas
        count_mapeadas += 1
        descripcion = data.get('descripcion', 'N/A').replace("'", "\\'")
        tipo_nombre = data.get('tipo', float).__name__
        coincidencia = mapeo.get('coincidencia', 'desconocida')
        score = mapeo.get('score', 0)
        nuevo_archivo_contenido += f"\n    # Original: {var_simple}\n"
        nuevo_archivo_contenido += f"    # Coincidencia: {coincidencia} (score: {score})\n"
        nuevo_archivo_contenido += f"    '{nombre_tecnico}': {{\n"
        nuevo_archivo_contenido += f"        'descripcion': '{descripcion}',\n"
        nuevo_archivo_contenido += f"        'tipo': {tipo_nombre},\n"
        nuevo_archivo_contenido += f"        'unidad': '{data.get('unidad', '')}',\n"
        nuevo_archivo_contenido += f"        'ejemplo': {data.get('ejemplo')},\n"
        nuevo_archivo_contenido += f"        'Rango_Min': {data.get('Rango_Min')},\n"
        nuevo_archivo_contenido += f"        'Rango_Max': {data.get('Rango_Max')},\n"
        nuevo_archivo_contenido += f"    }},\n"

nuevo_archivo_contenido += '''
}

# ==============================================================================
# MAPEO ORIGINAL → TÉCNICO (para referencia)
# ==============================================================================

MAPEO_ORIGINAL = {
'''

for var_simple in sorted(mapeos.keys()):
    mapeo = mapeos[var_simple]
    if mapeo['nombre_tecnico']:
        nuevo_archivo_contenido += f"    '{var_simple}': '{mapeo['nombre_tecnico']}',  # score: {mapeo['score']}\n"

nuevo_archivo_contenido += '''
}
'''

# Guardar nuevo archivo
nuevo_path = "inputs_referencial/Rangos_validacion_variables_petroleras_sincronizado.py"
with open(nuevo_path, "w", encoding="utf-8") as f:
    f.write(nuevo_archivo_contenido)

print(f"\n✓ Archivo generado: {nuevo_path}")
print(f"  Variables incluidas: {count_mapeadas}")
print(f"  Variables excluidas (sin mapeo): {sin_mapeo}")

# Crear reporte de mapeos
print("\n" + "=" * 80)
print("DETALLE DE MAPEOS")
print("=" * 80)

print("\n✓ MAPEADAS EXITOSAMENTE:")
for var_simple in sorted(variables_petroleras.keys()):
    mapeo = mapeos[var_simple]
    if mapeo['nombre_tecnico'] and mapeo['score'] == 1.0:
        print(f"  {var_simple:20} → {mapeo['nombre_tecnico']:40} (EXACTO)")

for var_simple in sorted(variables_petroleras.keys()):
    mapeo = mapeos[var_simple]
    if mapeo['nombre_tecnico'] and mapeo['score'] < 1.0:
        print(f"  {var_simple:20} → {mapeo['nombre_tecnico']:40} (score: {mapeo['score']})")

print("\n✗ SIN MAPEO (requieren revisión manual):")
for var_simple in sorted(variables_petroleras.keys()):
    mapeo = mapeos[var_simple]
    if not mapeo['nombre_tecnico']:
        print(f"  {var_simple:20} - Descripción: {variables_petroleras[var_simple].get('descripcion', 'N/A')[:50]}")

print("\n" + "=" * 80)
