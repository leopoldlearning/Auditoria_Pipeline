#!/usr/bin/env python3
"""
Carga y sincroniza el esquema referencial:
- tbl_maestra_variables
- tbl_ref_unidades
- tbl_dq_rules
- tbl_var_scada_map
Usando:
- Variables_ID_stage.csv
- 'Rangos_validacion_variables_petroleras_limpio.py'
- V1_stage_to_stage.sql
"""

import os
import re
import ast
import re
import importlib.util
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER', 'audit')
DB_PASS = os.getenv('DEV_DB_PASSWORD', 'audit')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5433')
DB_NAME = os.getenv('DB_NAME', 'etl_data')

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Rutas (ajustables según tu repo)
PATH_VARIABLES_STAGE = os.path.join(BASE_DIR, "data", "Variables_ID_stage.csv")
PATH_RANGOS = os.path.join(
    BASE_DIR,
    "inputs_referencial",
    "Rangos_validacion_variables_petroleras_limpio.py"
)
PATH_V1_STAGE = os.path.join(BASE_DIR, "src", "sql", "process", "V1__stage_to_stage.sql")
PATH_REFERENCIAL_SQL = os.path.join("src", "sql", "schema", "V4__referencial_schema_redesign.sql")


def execute_sql_file(path):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with engine.begin() as conn:
        conn.execute(text(sql))


import re
import ast

def load_python_dict_from_file(path, dict_name):
    """
    Extrae un diccionario desde un archivo .txt/.py que contiene código Python,
    comentarios, imports y objetos no evaluables. Solo conserva campos útiles:
    Rango_Min, Rango_Max, unidad.
    """
    # Primero intentar importar el archivo como módulo Python (más robusto)
    try:
        spec = importlib.util.spec_from_file_location("referencial_vars", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if hasattr(module, dict_name):
            raw = getattr(module, dict_name)
            # Normalizar a la forma esperada
            cleaned = {}
            for key, val in raw.items():
                cleaned[key] = {
                    "Rango_Min": val.get("Rango_Min"),
                    "Rango_Max": val.get("Rango_Max"),
                    "unidad": val.get("unidad")
                }
            return cleaned
    except SyntaxError as e:
        # Caeremos a la extracción por texto si el .py tiene errores de sintaxis
        print(f"[WARN] Import falla por SyntaxError en {path}: {e}; intentando extracción textual...")
    except Exception:
        # Cualquier otro fallo nos lleva a la extracción textual
        pass

    # Si la importación falla, proceder con extracción textual + limpieza
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    # 1. Encontrar el bloque del diccionario
    start_pattern = rf"{dict_name}\s*=\s*\{{"
    start_match = re.search(start_pattern, content)

    if not start_match:
        raise ValueError(f"No se encontró el diccionario '{dict_name}' en {path}")

    start_index = start_match.end() - 1
    substring = content[start_index:]

    # 2. Contar llaves para encontrar el final del dict
    brace_count = 0
    end_index = None

    for i, char in enumerate(substring):
        if char == "{":
            brace_count += 1
        elif char == "}":
            brace_count -= 1
            if brace_count == 0:
                end_index = i + 1
                break

    if end_index is None:
        raise ValueError("No se pudo cerrar el diccionario.")

    dict_text = substring[:end_index]

    # 3. Limpiar comentarios y comillas problemáticas comunes
    dict_text = re.sub(r"#.*", "", dict_text)
    # Reemplazar comillas dobles curvas o extrañas
    dict_text = dict_text.replace('“', '"').replace('”', '"')
    # Reducir secuencias de comillas simples dobles a una sola
    dict_text = dict_text.replace("''", "'")

    # 4. Reemplazar tipos Python por strings
    dict_text = dict_text.replace("float", "'float'")
    dict_text = dict_text.replace("int", "'int'")
    dict_text = dict_text.replace("tuple", "'tuple'")

    # 5. Reemplazar tuplas por strings (simple heurística)
    dict_text = re.sub(r"\(([^\)]*)\)", r"'(\1)'", dict_text)

    # 6. Evaluar de forma segura
    try:
        raw_dict = ast.literal_eval(dict_text)
    except Exception as e:
        raise ValueError(f"No se pudo parsear el diccionario desde {path}: {e}")

    # 7. Filtrar solo campos útiles
    cleaned = {}
    for key, val in raw_dict.items():
        cleaned[key] = {
            "Rango_Min": val.get("Rango_Min"),
            "Rango_Max": val.get("Rango_Max"),
            "unidad": val.get("unidad")
        }

    return cleaned




def sync_referencial_schema():
    # Crea var_scada_map + vista vw_variables_scada_stage
    if os.path.exists(PATH_REFERENCIAL_SQL):
        execute_sql_file(PATH_REFERENCIAL_SQL)
    else:
        print(f"[WARN] referencial_master.sql no encontrado en {PATH_REFERENCIAL_SQL}")


def load_maestra_variables():
    df = pd.read_csv(PATH_VARIABLES_STAGE, sep=";")

    df_stage = df[df["Capa"] == "Stage"].copy()

    # Insertar en tbl_maestra_variables solo si no existe id_formato1
    insert_sql = """
        INSERT INTO referencial.tbl_maestra_variables
            (id_formato1, nombre_tecnico, tabla_origen, clasificacion_logica, volatilidad, unidad_id)
        SELECT
            :id_formato1,
            :nombre_tecnico,
            :tabla_origen,
            :clasificacion_logica,
            :volatilidad,
            NULL::INT
        WHERE NOT EXISTS (
            SELECT 1 FROM referencial.tbl_maestra_variables mv
            WHERE mv.id_formato1 = :id_formato1
            OR mv.nombre_tecnico = :nombre_tecnico
        );
    """

    with engine.begin() as conn:
        for _, row in df_stage.iterrows():
            conn.execute(
                text(insert_sql),
                {
                    "id_formato1": int(row["ID"]),
                    "nombre_tecnico": row["Nombre_Variable"],
                    "tabla_origen": row["Nombre_Tabla"],
                    "clasificacion_logica": row["Categoria_Clasificacion"],
                    "volatilidad": "ALTA"  # default razonable; se puede refinar luego
                }
            )

def load_unidades_y_dq():
    """
    Carga unidades y reglas DQ basadas en el archivo limpio
    Rangos_validacion_variables_petroleras_limpio.py.

    - Cada clave del dict es un nombre_tecnico válido.
    - Se inserta/actualiza la unidad en tbl_ref_unidades.
    - Se actualiza unidad_id en tbl_maestra_variables.
    - Se insertan reglas DQ (min/max) si existen.
    """

    # Cargar diccionario limpio
    rangos = load_python_dict_from_file(PATH_RANGOS, "VARIABLES_PETROLERAS")

    with engine.begin() as conn:
        for nombre_tecnico, data in rangos.items():

            unidad = data.get("unidad")
            rango_min = data.get("Rango_Min")
            rango_max = data.get("Rango_Max")

            # ------------------------------------------------------------------
            # 1. Insertar/actualizar unidad en tbl_ref_unidades
            # ------------------------------------------------------------------
            unidad_id = None
            if unidad:
                unidad_id = conn.execute(
                    text("""
                        INSERT INTO referencial.tbl_ref_unidades (simbolo, descripcion)
                        VALUES (:simbolo, :descripcion)
                        ON CONFLICT (simbolo) DO UPDATE
                            SET descripcion = EXCLUDED.descripcion
                        RETURNING unidad_id;
                    """),
                    {"simbolo": unidad, "descripcion": unidad}
                ).scalar()

            # ------------------------------------------------------------------
            # 2. Actualizar unidad_id en la maestra
            # ------------------------------------------------------------------
            conn.execute(
                text("""
                    UPDATE referencial.tbl_maestra_variables
                    SET unidad_id = COALESCE(unidad_id, :unidad_id)
                    WHERE nombre_tecnico = :nombre_tecnico;
                """),
                {"unidad_id": unidad_id, "nombre_tecnico": nombre_tecnico}
            )

            # ------------------------------------------------------------------
            # 3. Insertar regla DQ si existen rangos
            # ------------------------------------------------------------------
            if rango_min is not None and rango_max is not None:
                conn.execute(
                    text("""
                        INSERT INTO referencial.tbl_dq_rules
                            (variable_id, valor_min, valor_max, severidad, origen_regla)
                        SELECT
                            mv.variable_id,
                            :valor_min,
                            :valor_max,
                            'WARNING',
                            'Rangos_validacion_variables_petroleras_limpio.py'
                        FROM referencial.tbl_maestra_variables mv
                        WHERE mv.nombre_tecnico = :nombre_tecnico
                        ON CONFLICT DO NOTHING;
                    """),
                    {
                        "valor_min": float(rango_min),
                        "valor_max": float(rango_max),
                        "nombre_tecnico": nombre_tecnico
                    }
                )



def load_limites_pozo():
    """
    Cargar los rangos min/max desde VARIABLES_PETROLERAS a referencial.tbl_limites_pozo
    como min_warning y max_warning para las variables que coincidan.
    Se mapean nombres simplificados (ql, SPM, etc) a nombres técnicos SQL.
    """
    # Cargar diccionario de rangos
    rangos = load_python_dict_from_file(PATH_RANGOS, "VARIABLES_PETROLERAS")
    
    # Mapeo manual entre nombres simplificados de VARIABLES_PETROLERAS y nombres técnicos SQL
    # Basado en coincidencias semánticas
    mapeo_variables = {
        'ql': 'produccion_fluido_bpd_act',
        'SPM': 'pump_avg_spm_act',
        'SPM_dia': 'spm_promedio_diario_medidor', # Verifica si este cambio es correcto en V4
        'WHP': 'well_head_pressure_psi_act',
        'CHP': 'casing_head_pressure_psi_act',
        'PIP': 'pump_intake_pressure_psi_act',
        'Hp': 'motor_power_hp_act',
        'spm_promedio': 'pump_avg_spm_act',
        'presion_cabezal': 'well_head_pressure_psi_act',
        'presion_casing': 'casing_head_pressure_psi_act',
    }
    
    # Primero, limpiar los límites existentes genéricos (pozo_id NULL o 0)
    delete_sql = """
        DELETE FROM referencial.tbl_limites_pozo 
        WHERE pozo_id IS NULL OR pozo_id = 0;
    """

    # Insertar rangos como entradas 'globales' usando pozo_id = 0
    insert_sql = """
        INSERT INTO referencial.tbl_limites_pozo (pozo_id, variable_id, min_warning, max_warning)
        SELECT
            :pozo_id,
            mv.variable_id,
            :min_warning,
            :max_warning
        FROM referencial.tbl_maestra_variables mv
        WHERE mv.nombre_tecnico = :nombre_tecnico;
    """

    with engine.begin() as conn:
        # Limpiar primero
        conn.execute(text(delete_sql))

        # Luego insertar los nuevos rangos
        for nombre_var, data in rangos.items():
            rango_min = data.get("Rango_Min")
            rango_max = data.get("Rango_Max")

            # Solo insertar si ambos rangos existen
            if pd.notna(rango_min) and pd.notna(rango_max):
                # Buscar nombre técnico (primero intentar mapeo, luego búsqueda directa)
                nombre_tecnico = mapeo_variables.get(nombre_var, nombre_var)

                # Usaremos pozo_id = 0 para rangos genéricos aplicables a todos los pozos
                pozo_id_generic = 0

                # Verificar si existe en la BD
                check_sql = """
                    SELECT COUNT(*) FROM referencial.tbl_maestra_variables 
                    WHERE nombre_tecnico = :nombre_tecnico
                """
                exists = conn.execute(text(check_sql), {"nombre_tecnico": nombre_tecnico}).scalar()

                if exists:
                    params = {
                        "pozo_id": pozo_id_generic,
                        "nombre_tecnico": nombre_tecnico,
                        "min_warning": float(rango_min),
                        "max_warning": float(rango_max)
                    }
                    try:
                        conn.execute(text(insert_sql), params)
                    except Exception as e:
                        print(f"[ERROR] Insertando límite para {nombre_tecnico}: {e}")


def build_tbl_var_scada_map():
    """
    Parsear V1_stage_to_stage.sql para extraer:
    - var_id_scada (IDN)
    - alias columna_stage (nombre real de SQL del AS)
    - id_formato1 (comentario ID: xx)
    - comentario (descripción del comentario)
    """
    if not os.path.exists(PATH_V1_STAGE):
        print(f"[WARN] V1_stage_to_stage.sql no encontrado en {PATH_V1_STAGE}")
        return

    with open(PATH_V1_STAGE, "r", encoding="utf-8") as f:
        lines = f.readlines()

    insert_sql = """
        INSERT INTO referencial.tbl_var_scada_map (var_id_scada, id_formato1, columna_stage, comentario)
        VALUES (:var_id_scada, :id_formato1, :columna_stage, :comentario)
        ON CONFLICT (var_id_scada) DO UPDATE
        SET id_formato1 = EXCLUDED.id_formato1,
            columna_stage = EXCLUDED.columna_stage,
            comentario = EXCLUDED.comentario;
    """

    entries = []

    # Recorre líneas buscando comentarios con ID/IDN
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        # Busca línea comentario con ID y IDN
        id_match = re.match(r"--\s*ID:\s*(\d+)\s*\|\s*IDN:\s*(\d+)\s*\|\s*(.*)", line)
        if id_match:
            id_fmt = id_match.group(1)
            idn = id_match.group(2)
            desc = id_match.group(3).strip()
            
            # Busca la siguiente línea que contiene la expresión SQL con AS
            col_name = None
            for j in range(i + 1, min(i + 3, len(lines))):
                code_line = lines[j]
                # Busca el patrón ) AS nombre_columna al final de la expresión
                as_match = re.search(r"\)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)", code_line, re.IGNORECASE)
                if as_match:
                    col_name = as_match.group(1)
                    break
            
            if col_name:
                entries.append({
                    "id_fmt": int(id_fmt),
                    "idn": int(idn),
                    "col_name": col_name,
                    "desc": desc
                })
        
        i += 1

    # Inserta todas las entradas
    with engine.begin() as conn:
        for entry in entries:
            conn.execute(
                text(insert_sql),
                {
                    "var_id_scada": entry["idn"],
                    "id_formato1": entry["id_fmt"],
                    "columna_stage": entry["col_name"],
                    "comentario": entry["desc"]
                }
            )


def main():
    print("=== SYNC REFERENCIAL MASTER ===")
    sync_referencial_schema()
    load_maestra_variables()
    load_unidades_y_dq()
    load_limites_pozo()
    build_tbl_var_scada_map()
    print("[OK] Referencial sincronizado correctamente.")


if __name__ == "__main__":
    main()
