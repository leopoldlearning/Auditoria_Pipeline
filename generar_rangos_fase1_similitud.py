#!/usr/bin/env python3
"""
FASE 1 — Mapeo automático por similitud
Genera un archivo preliminar de rangos sincronizado con tbl_maestra_variables.
"""

import os
import difflib
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import importlib.util

load_dotenv()

# Conexión a BD
DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DEV_DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)

# Cargar archivo original del cliente
spec = importlib.util.spec_from_file_location(
    "variables_ref",
    "inputs_referencial/Rangos_de_validacion_variables_petroleras.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
variables_petroleras = module.VARIABLES_PETROLERAS

print("=" * 80)
print("FASE 1 — MAPEADOR AUTOMÁTICO POR SIMILITUD")
print("=" * 80)

# Obtener variables técnicas reales
with engine.begin() as conn:
    result = conn.execute(text("""
        SELECT variable_id, nombre_tecnico, id_formato1
        FROM referencial.tbl_maestra_variables
        ORDER BY nombre_tecnico
    """))
    variables_bd = {row[1]: {'id': row[0], 'id_fmt': row[2]} for row in result}

print(f"\nVariables en BD: {len(variables_bd)}")
print(f"Variables en archivo original: {len(variables_petroleras)}")

# Función de similitud
def similitud(a, b):
    a = a.lower().replace("_", " ")
    b = b.lower().replace("_", " ")
    return difflib.SequenceMatcher(None, a, b).ratio()

# Preprocesar palabras clave
palabras_bd = {
    nombre: set(nombre.lower().replace("_", " ").split())
    for nombre in variables_bd.keys()
}

# Mapeo preliminar
mapeos = {}
for var_simple, data in variables_petroleras.items():
    descripcion = data.get("descripcion", "").lower()
    palabras_desc = set(descripcion.split())

    mejores = []

    for nombre_tecnico in variables_bd.keys():
        palabras_tecnico = palabras_bd[nombre_tecnico]

        score_keywords = (
            len(palabras_desc & palabras_tecnico)
            / max(len(palabras_desc), len(palabras_tecnico))
            if palabras_desc else 0
        )

        score_nombre = similitud(var_simple, nombre_tecnico)
        score_desc = similitud(descripcion, nombre_tecnico.replace("_", " "))

        score_final = (
            score_keywords * 0.5 +
            score_desc * 0.35 +
            score_nombre * 0.15
        )

        if score_final > 0.25:
            mejores.append((nombre_tecnico, score_final))

    if mejores:
        mejores.sort(key=lambda x: x[1], reverse=True)
        mejor = mejores[0]
        mapeos[var_simple] = {
            "nombre_tecnico": mejor[0],
            "score": round(mejor[1], 3),
            "alternativas": [m[0] for m in mejores[1:3]]
        }
    else:
        mapeos[var_simple] = {
            "nombre_tecnico": None,
            "score": 0.0
        }

# Generar archivo preliminar
nuevo_path = "inputs_referencial/Rangos_validacion_variables_petroleras_sincronizado_fase1.py"
with open(nuevo_path, "w", encoding="utf-8") as f:
    f.write("# Archivo preliminar generado por FASE 1\n\nVARIABLES_PETROLERAS = {\n")

    for var_simple, data in variables_petroleras.items():
        m = mapeos[var_simple]
        if not m["nombre_tecnico"]:
            continue

        nombre_tecnico = m["nombre_tecnico"]
        descripcion = data.get("descripcion", "").replace("'", "\\'")
        tipo_nombre = data.get("tipo", float).__name__
        unidad = data.get("unidad", "")
        ejemplo = data.get("ejemplo")
        rmin = data.get("Rango_Min")
        rmax = data.get("Rango_Max")

        f.write(f"    # Original: {var_simple} (score: {m['score']})\n")
        f.write(f"    '{nombre_tecnico}': {{\n")
        f.write("        'descripcion': '" + descripcion + "',\n")
        f.write(f"        'tipo': {tipo_nombre},\n")
        f.write(f"        'unidad': '{unidad}',\n")
        f.write(f"        'ejemplo': {ejemplo},\n")
        f.write(f"        'Rango_Min': {rmin},\n")
        f.write(f"        'Rango_Max': {rmax},\n")
        f.write("    },\n\n")

    f.write("}\n")


print(f"\n✓ Archivo preliminar generado: {nuevo_path}")
