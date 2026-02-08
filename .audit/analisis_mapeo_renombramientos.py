#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
ANÁLISIS DE MAPEO Y RENOMBRAMIENTOS - DATASET_CURRENT_VALUES
================================================================================

Objetivo: Detectar variables que existen con NOMBRES DIFERENTES pero representan
lo MISMO según hoja_validacion.csv

Ejemplo detectado por usuario:
- ACTUAL en SQL: chp_psi
- ESPERADO según validación: casing_head_pressure_psi_act (CHP)
- ACCIÓN: RENOMBRAR (no agregar como nueva)

Este script busca todos estos casos.

Fecha: 2026-02-08 09:13
Autor: Antigravity
================================================================================
"""

import re
import csv
from pathlib import Path
from typing import Dict, List, Set, Tuple
from difflib import SequenceMatcher
from datetime import datetime

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

BASE_DIR = Path(r"D:\ITMeet\Operaciones\BP010-data-pipelines-auditoria")
SQL_SCHEMA = BASE_DIR / "src" / "sql" / "schema"
DATA_DIR = BASE_DIR / "data"
AUDIT_DIR = BASE_DIR / ".audit"

V3_SQL = SQL_SCHEMA / "V3__reporting_schema_redesign.sql"
HOJA_VALIDACION = DATA_DIR / "hoja_validacion.csv"

OUTPUT_MAPEO = AUDIT_DIR / "MAPEO_RENOMBRAMIENTOS.md"
OUTPUT_RENOMBRAR = AUDIT_DIR / "VARIABLES_A_RENOMBRAR.txt"

# ============================================================================
# DICCIONARIO DE ABREVIACIONES CONOCIDAS
# ============================================================================

ABREVIACIONES = {
    'chp': 'casing_head_pressure',
    'whp': 'well_head_pressure',
    'pip': 'pump_intake_pressure',
    'pdp': 'pump_discharge_pressure',
    'spm': 'strokes_per_minute',
    'bpd': 'barrels_per_day',
    'bbl': 'barrels',
    'pct': 'percent',
    'psi': 'pounds_per_square_inch',
    'ft': 'feet',
    'in': 'inches',
    'lb': 'pounds',
    'hp': 'horsepower',
    'kwh': 'kilowatt_hour',
    'mtbf': 'mean_time_between_failures',
    'kpi': 'key_performance_indicator',
    'dop': 'degree_of_production',
    'tvd': 'true_vertical_depth',
    'ai': 'artificial_intelligence',
    'ipr': 'inflow_performance_relationship'
}

# ============================================================================
# FUNCIONES
# ============================================================================

def extraer_columnas_tabla(sql_content: str, tabla: str) -> Dict[str, str]:
    """Extrae columnas con sus tipos."""
    sql_lower = sql_content.lower()
    pattern = rf"create\s+table\s+(?:if\s+not\s+exists\s+)?reporting\.{tabla}\s*\((.*?)\);"
    match = re.search(pattern, sql_lower, re.DOTALL | re.IGNORECASE)
    
    if not match:
        return {}
    
    table_def = match.group(1)
    columnas = {}
    
    for line in table_def.split('\n'):
        line = line.strip()
        if not line or line.startswith(('--', '/*', '*', 'constraint', 'primary', 'foreign')):
            continue
        
        match_col = re.match(r'^\s*([a-z_][a-z0-9_]*)\s+([A-Z]+(?:\(\d+(?:,\s*\d+)?\))?)', line, re.IGNORECASE)
        if match_col:
            columnas[match_col.group(1)] = match_col.group(2)
    
    return columnas

def leer_csv(filepath: Path, delimiter=';') -> List[Dict]:
    """Lee CSV."""
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return list(reader)

def expandir_abreviaciones(nombre: str) -> str:
    """Expande abreviaciones conocidas en un nombre de variable."""
    palabras = nombre.split('_')
    expandidas = []
    for palabra in palabras:
        expandidas.append(ABREVIACIONES.get(palabra, palabra))
    return '_'.join(expandidas)

def similitud(a: str, b: str) -> float:
    """Calcula similitud entre dos strings."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def encontrar_candidatos(nombre_esperado: str, columnas_actuales: Dict[str, str]) -> List[Tuple[str, float]]:
    """
    Encuentra candidatos de columnas actuales que podrían ser el nombre esperado.
    Retorna lista de (nombre_actual, score_similitud) ordenada por score.
    """
    candidatos = []
    
    # Expandir abreviaciones del nombre esperado
    esperado_expandido = expandir_abreviaciones(nombre_esperado)
    
    for col_actual in columnas_actuales.keys():
        # Expandir abreviaciones de la columna actual
        actual_expandido = expandir_abreviaciones(col_actual)
        
        # Calcular similitud
        score1 = similitud(nombre_esperado, col_actual)
        score2 = similitud(esperado_expandido, actual_expandido)
        score_final = max(score1, score2)
        
        # Si hay similitud significativa (>0.5), agregar como candidato
        if score_final > 0.5:
            candidatos.append((col_actual, score_final))
    
    # Ordenar por score descendente
    return sorted(candidatos, key=lambda x: x[1], reverse=True)

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 80)
    print("ANÁLISIS DE MAPEO Y RENOMBRAMIENTOS - DATASET_CURRENT_VALUES")
    print("=" * 80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    
    # ========================================================================
    # 1. LEER DATOS
    # ========================================================================
    print("[1/4] Leyendo datos...")
    
    with open(V3_SQL, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    col_current = extraer_columnas_tabla(sql_content, 'dataset_current_values')
    col_dim_pozo = extraer_columnas_tabla(sql_content, 'dim_pozo')
    col_horarias = extraer_columnas_tabla(sql_content, 'fact_operaciones_horarias')
    col_diarias = extraer_columnas_tabla(sql_content, 'fact_operaciones_diarias')
    col_mensuales = extraer_columnas_tabla(sql_content, 'fact_operaciones_mensuales')
    
    hoja_data = leer_csv(HOJA_VALIDACION, delimiter=';')
    
    print(f"  ✓ dataset_current_values: {len(col_current)} columnas")
    print(f"  ✓ dim_pozo: {len(col_dim_pozo)} columnas")
    print(f"  ✓ hoja_validacion: {len(hoja_data)} registros\n")
    
    # ========================================================================
    # 2. EXTRAER NOMBRES ESPERADOS POR TABLA
    # ========================================================================
    print("[2/4] Extrayendo nombres esperados por tabla...")
    
    esperados_por_tabla = {
        'dataset_current_values': [],
        'dim_pozo': [],
        'fact_operaciones_horarias': [],
        'fact_operaciones_diarias': [],
        'fact_operaciones_mensuales': []
    }
    
    columnas_por_tabla = {
        'reporting.dataset_current_values': col_current,
        'reporting.dim_pozo': col_dim_pozo,
        'reporting.fact_operaciones_horarias': col_horarias,
        'reporting.fact_operaciones_diarias': col_diarias,
        'reporting.fact_operaciones_mensuales': col_mensuales,
        'reporting.FACT_OPERACIONES_HORARIAS': col_horarias,
        'reporting.FACT_OPERACIONES_DIARIAS': col_diarias,
        'reporting.FACT_OPERACIONES_MENSUALES': col_mensuales,
        'reporting.DIM_POZO': col_dim_pozo
    }
    
    for row in hoja_data:
        nombre_reporting = row.get('Nombre en REPORTING', row.get('Nombre en el esquema REPORTING', '')).strip()
        tabla_reporting = row.get('Tabla reporting', row.get('Tabla en esquema reporting', '')).strip()
        
        if not nombre_reporting or nombre_reporting.upper() in ['N/A', 'NA', '']:
            continue
        
        nombre_lower = nombre_reporting.lower()
        
        # Identificar tabla destino
        tabla_key = None
        if 'current' in tabla_reporting.lower():
            tabla_key = 'dataset_current_values'
        elif 'dim_pozo' in tabla_reporting.lower():
            tabla_key = 'dim_pozo'
        elif 'horaria' in tabla_reporting.lower():
            tabla_key = 'fact_operaciones_horarias'
        elif 'diaria' in tabla_reporting.lower():
            tabla_key = 'fact_operaciones_diarias'
        elif 'mensual' in tabla_reporting.lower():
            tabla_key = 'fact_operaciones_mensuales'
        
        if tabla_key:
            esperados_por_tabla[tabla_key].append({
                'nombre': nombre_lower,
                'tabla_original': tabla_reporting
            })
    
    for tabla, vars_list in esperados_por_tabla.items():
        print(f"  ✓ {tabla}: {len(vars_list)} variables esperadas")
    
    print()
    
    # ========================================================================
    # 3. DETECTAR RENOMBRAMIENTOS
    # ========================================================================
    print("[3/4] Detectando renombramientos necesarios...")
    
    renombramientos = []
    faltantes_reales = []
    
    # Analizar dataset_current_values
    print("\n  Analizando dataset_current_values:")
    for var_esperada in esperados_por_tabla['dataset_current_values']:
        nombre_esp = var_esperada['nombre']
        
        # Búsqueda exacta
        if nombre_esp in col_current:
            continue
        
        # Buscar candidatos similares
        candidatos = encontrar_candidatos(nombre_esp, col_current)
        
        if candidatos and candidatos[0][1] > 0.7:  # Alta similitud
            renombramientos.append({
                'tabla': 'dataset_current_values',
                'nombre_actual': candidatos[0][0],
                'nombre_esperado': nombre_esp,
                'similitud': candidatos[0][1],
                'accion': 'RENOMBRAR'
            })
            print(f"    🔄 RENOMBRAR: {candidatos[0][0]} → {nombre_esp} (similitud: {candidatos[0][1]:.2f})")
        else:
            faltantes_reales.append({
                'tabla': 'dataset_current_values',
                'nombre': nombre_esp,
                'accion': 'AGREGAR'
            })
            print(f"    ➕ AGREGAR: {nombre_esp} (no existe)")
    
    print(f"\n  Total renombramientos detectados: {len(renombramientos)}")
    print(f"  Total variables realmente faltantes: {len(faltantes_reales)}\n")
    
    # ========================================================================
    # 4. GENERAR REPORTES
    # ========================================================================
    print("[4/4] Generando reportes...")
    
    with open(OUTPUT_MAPEO, 'w', encoding='utf-8') as f:
        f.write("# MAPEO Y RENOMBRAMIENTOS - DATASET_CURRENT_VALUES\n\n")
        f.write(f"**Fecha**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## Renombramientos Necesarios\n\n")
        f.write(f"**Total**: {len(renombramientos)}\n\n")
        
        if renombramientos:
            f.write("| Nombre Actual (V3) | Nombre Esperado (Validación) | Similitud | Acción |\n")
            f.write("|-------------------|------------------------------|-----------|--------|\n")
            for r in renombramientos:
                f.write(f"| `{r['nombre_actual']}` | `{r['nombre_esperado']}` | {r['similitud']:.2f} | {r['accion']} |\n")
            f.write("\n")
        
        f.write("## Variables Realmente Faltantes\n\n")
        f.write(f"**Total**: {len(faltantes_reales)}\n\n")
        
        if faltantes_reales:
            for var in faltantes_reales:
                f.write(f"- `{var['nombre']}`\n")
    
    print(f"  ✓ Mapeo guardado: {OUTPUT_MAPEO}")
    
    with open(OUTPUT_RENOMBRAR, 'w', encoding='utf-8') as f:
        f.write(f"VARIABLES A RENOMBRAR EN DATASET_CURRENT_VALUES ({len(renombramientos)} total)\n")
        f.write(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
        for r in renombramientos:
            f.write(f"{r['nombre_actual']} → {r['nombre_esperado']}\n")
    
    print(f"  ✓ Lista renombramientos: {OUTPUT_RENOMBRAR}")
    
    print("\n" + "=" * 80)
    print("ANÁLISIS COMPLETADO")
    print("=" * 80)
    print(f"\n🔄 Variables a RENOMBRAR: {len(renombramientos)}")
    print(f"➕ Variables a AGREGAR: {len(faltantes_reales)}")
    print(f"📁 Reportes en: {AUDIT_DIR}\n")

if __name__ == '__main__':
    main()
