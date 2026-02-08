#!/usr/bin/env python3
"""
FASE 2B — Mapeo híbrido SCADA + stage + similitud + reglas
Genera el archivo DEFINITIVO de rangos petroleros.
"""

import os
import pandas as pd
import difflib
from sqlalchemy import create_engine
from dotenv import load_dotenv
import importlib.util

load_dotenv()

# Conexión BD
DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DEV_DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)

# === 1) Cargar archivo original del cliente ===
RUTA_ARCHIVO = "inputs_referencial/Rangos_de_validacion_variables_petroleras.py"

if not os.path.exists(RUTA_ARCHIVO):
    raise FileNotFoundError(f"ERROR: No se encontró el archivo {RUTA_ARCHIVO}")

spec = importlib.util.spec_from_file_location("variables_ref", RUTA_ARCHIVO)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

variables_petroleras = module.VARIABLES_PETROLERAS

if not variables_petroleras:
    raise ValueError("ERROR: VARIABLES_PETROLERAS está vacío. Revisa el archivo original.")

print("=" * 80)
print("FASE 2B — MAPEADOR HÍBRIDO SCADA + SIMILITUD")
print("=" * 80)

# === 2) Cargar SCADA → stage → maestra ===
with engine.begin() as conn:
    df_scada = pd.read_sql("""
        SELECT var_id_scada, id_formato1, columna_stage
        FROM referencial.tbl_var_scada_map
    """, conn)

    df_maestra = pd.read_sql("""
        SELECT id_formato1, nombre_tecnico
        FROM referencial.tbl_maestra_variables
    """, conn)

# Normalizar tipos
df_scada["id_formato1"] = df_scada["id_formato1"].astype(str)
df_maestra["id_formato1"] = df_maestra["id_formato1"].astype(str)
df_scada["var_id_scada"] = df_scada["var_id_scada"].astype(str)
df_scada["columna_stage"] = df_scada["columna_stage"].astype(str)

# Unir SCADA → maestra
df_map = df_scada.merge(df_maestra, on="id_formato1", how="left")

# Diccionarios
dic_scada = dict(zip(df_map["var_id_scada"].str.lower(), df_map["nombre_tecnico"]))
dic_stage = dict(zip(df_map["columna_stage"].str.lower(), df_map["nombre_tecnico"]))
dic_tecnico = {row["nombre_tecnico"].lower(): row["nombre_tecnico"] for _, row in df_maestra.iterrows()}

# === 3) Función de similitud ===
def mejor_match(valor, lista):
    valor = valor.lower()
    lista_lower = [x.lower() for x in lista]
    match = difflib.get_close_matches(valor, lista_lower, n=1, cutoff=0.65)
    if match:
        idx = lista_lower.index(match[0])
        return lista[idx]
    return None

# === 4) Mapeo híbrido ===
mapeo_final = {}

for var_simple, data in variables_petroleras.items():

    v = var_simple.lower()

    # 1) SCADA exacto
    if v in dic_scada:
        mapeo_final[var_simple] = dic_scada[v]
        continue

    # 2) stage exacto
    if v in dic_stage:
        mapeo_final[var_simple] = dic_stage[v]
        continue

    # 3) nombre_tecnico exacto
    if v in dic_tecnico:
        mapeo_final[var_simple] = dic_tecnico[v]
        continue

    # 4) substring SCADA
    sub = [k for k in dic_scada.keys() if v in k]
    if sub:
        mapeo_final[var_simple] = dic_scada[sub[0]]
        continue

    # 5) substring stage
    sub = [k for k in dic_stage.keys() if v in k]
    if sub:
        mapeo_final[var_simple] = dic_stage[sub[0]]
        continue

    # 6) similitud SCADA
    match = mejor_match(v, list(dic_scada.keys()))
    if match:
        mapeo_final[var_simple] = dic_scada[match]
        continue

    # 7) similitud stage
    match = mejor_match(v, list(dic_stage.keys()))
    if match:
        mapeo_final[var_simple] = dic_stage[match]
        continue

    # 8) similitud nombre_tecnico
    match = mejor_match(v, list(dic_tecnico.keys()))
    if match:
        mapeo_final[var_simple] = dic_tecnico[match]
        continue

    # 9) Sin mapeo → revisión manual
    mapeo_final[var_simple] = None

# === 5) Generar archivo limpio definitivo ===
nuevo_path = "inputs_referencial/Rangos_validacion_variables_petroleras_limpio.py"

with open(nuevo_path, "w", encoding="utf-8") as f:
    f.write("# Archivo limpio generado por FASE 2B\n\nVARIABLES_PETROLERAS = {\n")

    for var_simple, nombre_tecnico in mapeo_final.items():
        if not nombre_tecnico:
            continue

        d = variables_petroleras[var_simple]

        descripcion = d.get("descripcion", "").replace("'", "\\'")
        tipo_nombre = d.get("tipo", float).__name__
        unidad = d.get("unidad", "")
        ejemplo = d.get("ejemplo")
        rmin = d.get("Rango_Min")
        rmax = d.get("Rango_Max")

        f.write(f"    '{nombre_tecnico}': {{\n")
        f.write("        'descripcion': '" + descripcion + "',\n")
        f.write(f"        'tipo': {tipo_nombre},\n")
        f.write(f"        'unidad': '{unidad}',\n")
        f.write(f"        'ejemplo': {ejemplo},\n")
        f.write(f"        'Rango_Min': {rmin},\n")
        f.write(f"        'Rango_Max': {rmax},\n")
        f.write(f"        'original_var': '{var_simple}',\n")
        f.write("    },\n\n")

    f.write("}\n")

print(f"\n✓ Archivo limpio generado: {nuevo_path}")
