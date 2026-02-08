#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
ANÁLISIS COMPLETO DE VARIABLES - AUDITORÍA BP010 DATA PIPELINES
================================================================================

Este script analiza la coherencia entre:
1. 01_maestra_variables.csv (Especificación BI/Dashboard)
2. hoja_validacion.csv (Validación completa)
3. V3_reporting_schema_redesign.sql (Esquema real)
4. Rangos_validacion_variables_petroleras_limpio.py (Límites cliente)
5. 02_reglas_calidad.csv (Reglas de calidad)
6. 03_reglas_consistencia.csv (Reglas de consistencia)

Objetivo: Identificar variables faltantes, discrepancias de nomenclatura,
y generar plan de acción para completar el esquema reporting.

Autor: Antigravity
Fecha: 2026-02-07
================================================================================
"""

import csv
import re
import json
from pathlib import Path
from typing import Dict, List, Set, Tuple
from collections import defaultdict
from datetime import datetime

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

BASE_DIR = Path(r"D:\ITMeet\Operaciones\BP010-data-pipelines-auditoria")
INPUTS_REF = BASE_DIR / "inputs_referencial"
SQL_SCHEMA = BASE_DIR / "src" / "sql" / "schema"
DATA_DIR = BASE_DIR / "data"
AUDIT_DIR = BASE_DIR / ".audit"

# Archivos de entrada
MAESTRA_VARS = INPUTS_REF / "01_maestra_variables.csv"
HOJA_VALIDACION = DATA_DIR / "hoja_validacion.csv"
REPORTING_SQL = SQL_SCHEMA / "V3__reporting_schema_redesign.sql"
RANGOS_LIMPIO = INPUTS_REF / "Rangos_validacion_variables_petroleras_limpio.py"
REGLAS_CALIDAD = INPUTS_REF / "02_reglas_calidad.csv"
REGLAS_CONSISTENCIA = INPUTS_REF / "03_reglas_consistencia.csv"

# Archivos de salida
OUTPUT_RESUMEN = AUDIT_DIR / "RESUMEN_VARIABLES.md"
OUTPUT_GAPS = AUDIT_DIR / "VARIABLES_FALTANTES.json"
OUTPUT_PANELES = AUDIT_DIR / "PANELES_BI_UNICOS.json"
OUTPUT_MAPEO = AUDIT_DIR / "MAPEO_TRAZABILIDAD.csv"

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def leer_csv_delimitado(filepath: Path, delimiter=';') -> List[Dict]:
    """Lee un CSV con delimitador específico y retorna lista de diccionarios."""
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return list(reader)

def extraer_columnas_sql(sql_content: str, tabla: str) -> Set[str]:
    """
    Extrae nombres de columnas de una tabla específica en el SQL.
    Busca el patrón CREATE TABLE...(...) y extrae nombres de columnas.
    """
    # Normalizar contenido
    sql_normalized = sql_content.lower()
    
    # Buscar la definición de la tabla
    pattern = rf"create\s+table\s+(?:if\s+not\s+exists\s+)?reporting\.{tabla}\s*\((.*?)\);"
    match = re.search(pattern, sql_normalized, re.DOTALL | re.IGNORECASE)
    
    if not match:
        return set()
    
    table_def = match.group(1)
    
    # Extraer nombres de columnas (primera palabra de cada línea que no sea comentario)
    columnas = set()
    for line in table_def.split('\n'):
        line = line.strip()
        if not line or line.startswith('--') or line.startswith('/*') or line.startswith('*'):
            continue
        if line.lower().startswith(('constraint', 'primary', 'foreign', 'unique', 'check')):
            continue
        
        # Extraer nombre de columna (primera palabra antes del espacio)
        match_col = re.match(r'^\s*([a-z_][a-z0-9_]*)', line, re.IGNORECASE)
        if match_col:
            columnas.add(match_col.group(1))
    
    return columnas

def extraer_paneles_unicos(filepath: Path) -> Dict[str, Dict]:
    """
    Extrae paneles únicos de hoja_validacion.csv columna 'Panel(es) de Uso'.
    Retorna diccionario {nombre_panel: {elementos: [...], ids: [...]}}
    """
    data = leer_csv_delimitado(filepath, delimiter=';')
    paneles = defaultdict(lambda: {'elementos': [], 'ids': []})
    
    for idx, row in enumerate(data, start=1):
        panel_col = row.get('Panel(es) de Uso', row.get('Panel(es) de Uso(nombre o título del panel en el dashboard)', ''))
        ident = row.get('Ident Dashboard Element', row.get('Ident Dashboard Element(número del elemento o grafico dentro de cada panel del dashboard', ''))
        nombre_viz = row.get('Nombre de Visualización', row.get('Nombre en el dashboard de Visualización', ''))
        
        if not panel_col:
            continue
        
        # Separar múltiples paneles (separados por / o ,)
        panel_names = re.split(r'[/,]', panel_col)
        
        for panel_name in panel_names:
            panel_name = panel_name.strip()
            if panel_name and panel_name.lower() not in ['n/a', 'na', '']:
                paneles[panel_name]['elementos'].append({
                    'id': ident,
                    'nombre': nombre_viz,
                    'linea': idx
                })
                if ident:
                    paneles[panel_name]['ids'].append(ident)
    
    return dict(paneles)

# ============================================================================
# ANÁLISIS PRINCIPAL
# ============================================================================

def main():
    print("=" * 80)
    print("ANÁLISIS COMPLETO DE VARIABLES - AUDITORÍA BP010")
    print("=" * 80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Asegurar que existe el directorio de auditoría
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    
    # ========================================================================
    # 1. LEER FUENTES DE DATOS
    # ========================================================================
    print("[1/6] Leyendo archivos fuente...")
    
    # Maestra Variables
    maestra_vars = leer_csv_delimitado(MAESTRA_VARS, delimiter=';')
    print(f"  ✓ 01_maestra_variables.csv: {len(maestra_vars)} registros")
    
    # Hoja Validación
    hoja_validacion = leer_csv_delimitado(HOJA_VALIDACION, delimiter=';')
    print(f"  ✓ hoja_validacion.csv: {len(hoja_validacion)} registros")
    
    # SQL Reporting
    with open(REPORTING_SQL, 'r', encoding='utf-8') as f:
        reporting_sql = f.read()
    print(f"  ✓ V3__reporting_schema_redesign.sql: {len(reporting_sql)} caracteres")
    
    # Reglas Calidad
    reglas_calidad = leer_csv_delimitado(REGLAS_CALIDAD, delimiter=';')
    print(f"  ✓ 02_reglas_calidad.csv: {len(reglas_calidad)} registros")
    
    # Reglas Consistencia
    reglas_consist = leer_csv_delimitado(REGLAS_CONSISTENCIA, delimiter=';')
    print(f"  ✓ 03_reglas_consistencia.csv: {len(reglas_consist)} registros")
    
    print()
    
    # ========================================================================
    # 2. EXTRAER VARIABLES DE MAESTRA
    # ========================================================================
    print("[2/6] Extrayendo variables definidas en Maestra...")
    
    vars_reporting = set()
    vars_detalle = []
    
    for row in maestra_vars:
        nombre_reporting = row.get('Nombre en el esquema REPORTING', row.get('Nombre en el esquema de Reporting', ''))
        tabla_reporting = row.get('Tabla en esquema reporting', row.get('Tabla en Reporting', ''))
        nombre_dashboard = row.get('Nombre en el dashboard de Visualización', '')
        
        if not nombre_reporting or nombre_reporting.lower() in ['n/a', 'na', '']:
            continue
        
        vars_reporting.add(nombre_reporting.lower())
        vars_detalle.append({
            'nombre_reporting': nombre_reporting.lower(),
            'tabla': tabla_reporting,
            'nombre_dashboard': nombre_dashboard
        })
    
    print(f"  ✓ Variables únicas en Maestra: {len(vars_reporting)}")
    print()
    
    # ========================================================================
    # 3. EXTRAER COLUMNAS DEL SQL
    # ========================================================================
    print("[3/6] Extrayendo columnas de schema SQL...")
    
    tablas_analizar = ['dataset_current_values', 'dim_pozo', 'fact_operaciones_diarias', 
                       'fact_operaciones_horarias', 'fact_operaciones_mensuales']
    
    columnas_sql = {}
    for tabla in tablas_analizar:
        cols = extraer_columnas_sql(reporting_sql, tabla)
        columnas_sql[tabla] = cols
        print(f"  ✓ reporting.{tabla}: {len(cols)} columnas")
    
    # Consolidar todas las columnas
    todas_columnas_sql = set()
    for cols in columnas_sql.values():
        todas_columnas_sql.update(cols)
    
    print(f"\n  Total columnas únicas en SQL: {len(todas_columnas_sql)}")
    print()
    
    # ========================================================================
    # 4. IDENTIFICAR GAPS
    # ========================================================================
    print("[4/6] Identificando variables faltantes...")
    
    faltantes = vars_reporting - todas_columnas_sql
    print(f"  ⚠ Variables en Maestra pero NO en SQL: {len(faltantes)}")
    
    if faltantes:
        print("  Primeras 10:")
        for var in sorted(list(faltantes))[:10]:
            print(f"    - {var}")
    
    print()
    
    # ========================================================================
    # 5. EXTRAER PANELES BI
    # ========================================================================
    print("[5/6] Extrayendo paneles BI únicos...")
    
    paneles = extraer_paneles_unicos(HOJA_VALIDACION)
    print(f"  ✓ Paneles BI únicos identificados: {len(paneles)}")
    
    for panel_name, info in sorted(paneles.items()):
        print(f"    - {panel_name}: {len(info['elementos'])} elementos")
    
    print()
    
    # ========================================================================
    # 6. GENERAR REPORTES
    # ========================================================================
    print("[6/6] Generando reportes...")
    
    # Reporte Markdown
    with open(OUTPUT_RESUMEN, 'w', encoding='utf-8') as f:
        f.write(f"# RESUMEN DE ANÁLISIS DE VARIABLES\\n\\n")
        f.write(f"**Fecha**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\\n\\n")
        f.write(f"## Estadísticas Generales\\n\\n")
        f.write(f"- Variables definidas en Maestra: **{len(vars_reporting)}**\\n")
        f.write(f"- Columnas en SQL (total): **{len(todas_columnas_sql)}**\\n")
        f.write(f"- Variables FALTANTES en SQL: **{len(faltantes)}**\\n")
        f.write(f"- Paneles BI identificados: **{len(paneles)}**\\n\\n")
        
        f.write(f"## Variables Faltantes\\n\\n")
        if faltantes:
            for var in sorted(faltantes):
                f.write(f"- `{var}`\\n")
        else:
            f.write("✅ Todas las variables están presentes.\\n")
        
        f.write(f"\\n## Paneles BI\\n\\n")
        for panel_name, info in sorted(paneles.items()):
            f.write(f"### {panel_name}\\n")
            f.write(f"Total elementos: {len(info['elementos'])}\\n\\n")
    
    print(f"  ✓ Resumen guardado en: {OUTPUT_RESUMEN}")
    
    # Gaps JSON
    with open(OUTPUT_GAPS, 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total_faltantes': len(faltantes),
            'variables_faltantes': sorted(list(faltantes))
        }, f, indent=2, ensure_ascii=False)
    
    print(f"  ✓ Gaps guardados en: {OUTPUT_GAPS}")
    
    # Paneles JSON
    with open(OUTPUT_PANELES, 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total_paneles': len(paneles),
            'paneles': {k: {
                'total_elementos': len(v['elementos']),
                'elementos': v['elementos']
            } for k, v in paneles.items()}
        }, f, indent=2, ensure_ascii=False)
    
    print(f"  ✓ Paneles guardados en: {OUTPUT_PANELES}")
    
    print()
    print("=" * 80)
    print("ANÁLISIS COMPLETADO")
    print("=" * 80)
    print()
    print(f"📊 Variables faltantes: {len(faltantes)}")
    print(f"📋 Paneles BI: {len(paneles)}")
    print(f"📁 Reportes en: {AUDIT_DIR}")
    print()

if __name__ == '__main__':
    main()
