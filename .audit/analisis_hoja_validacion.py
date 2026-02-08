#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
ANÁLISIS DE VARIABLES - USANDO HOJA_VALIDACION.CSV COMO FUENTE DE VERDAD
================================================================================

Este script compara:
- hoja_validacion.csv (columna "Nombre en REPORTING")
- V3__reporting_schema_redesign.sql (columnas reales en tablas)

Objetivo: Identificar variables que realmente faltan en el esquema SQL

Autor: Antigravity
Fecha: 2026-02-07 19:11
================================================================================
"""

import csv
import re
from pathlib import Path
from typing import Dict, List, Set
from datetime import datetime

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

BASE_DIR = Path(r"D:\ITMeet\Operaciones\BP010-data-pipelines-auditoria")
DATA_DIR = BASE_DIR / "data"
SQL_SCHEMA = BASE_DIR / "src" / "sql" / "schema"
AUDIT_DIR = BASE_DIR / ".audit"

# Archivos
HOJA_VALIDACION = DATA_DIR / "hoja_validacion.csv"
REPORTING_SQL = SQL_SCHEMA / "V3__reporting_schema_redesign.sql"

# Output
OUTPUT_ANALISIS = AUDIT_DIR / "ANALISIS_HOJA_VALIDACION.md"
OUTPUT_FALTANTES = AUDIT_DIR / "VARIABLES_FALTANTES_REAL.txt"

# ============================================================================
# FUNCIONES
# ============================================================================

def leer_csv_delimitado(filepath: Path, delimiter=';') -> List[Dict]:
    """Lee CSV con delimitador y retorna lista de diccionarios."""
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return list(reader)

def extraer_columnas_sql(sql_content: str, tabla: str) -> Set[str]:
    """
    Extrae nombres de columnas de una tabla específica en el SQL.
    """
    sql_normalized = sql_content.lower()
    
    # Buscar CREATE TABLE
    pattern = rf"create\s+table\s+(?:if\s+not\s+exists\s+)?reporting\.{tabla}\s*\((.*?)\);"
    match = re.search(pattern, sql_normalized, re.DOTALL | re.IGNORECASE)
    
    if not match:
        return set()
    
    table_def = match.group(1)
    
    # Extraer columnas
    columnas = set()
    for line in table_def.split('\n'):
        line = line.strip()
        if not line or line.startswith(('--', '/*', '*')):
            continue
        if line.lower().startswith(('constraint', 'primary', 'foreign', 'unique', 'check')):
            continue
        
        match_col = re.match(r'^\s*([a-z_][a-z0-9_]*)', line, re.IGNORECASE)
        if match_col:
            columnas.add(match_col.group(1))
    
    return columnas

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 80)
    print("ANÁLISIS DE VARIABLES - HOJA VALIDACIÓN vs SQL REPORTING")
    print("=" * 80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Crear directorio audit si no existe
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    
    # ========================================================================
    # 1. LEER HOJA VALIDACIÓN
    # ========================================================================
    print("[1/4] Leyendo hoja_validacion.csv...")
    hoja_data = leer_csv_delimitado(HOJA_VALIDACION, delimiter=';')
    print(f"  ✓ Total registros: {len(hoja_data)}\n")
    
    # ========================================================================
    # 2. EXTRAER VARIABLES ESPERADAS EN REPORTING
    # ========================================================================
    print("[2/4] Extrayendo variables esperadas en REPORTING...")
    
    vars_esperadas = set()
    vars_detalle = []
    
    # Posibles nombres de columna (variaciones)
    col_nombres = [
        'Nombre en REPORTING',
        'Nombre en el esquema REPORTING',
        'Nombre REPORTING'
    ]
    
    for row in hoja_data:
        # Buscar la columna correcta
        nombre_reporting = None
        for col_name in col_nombres:
            if col_name in row:
                nombre_reporting = row[col_name]
                break
        
        if not nombre_reporting:
            continue
        
        # Limpiar y normalizar
        nombre_reporting = nombre_reporting.strip()
        
        # Excluir valores vacíos, N/A, calculado genérico, etc.
        if not nombre_reporting or nombre_reporting.upper() in ['N/A', 'NA', '']:
            continue
        
        # Convertir a minúsculas para comparación
        nombre_lower = nombre_reporting.lower()
        
        # Agregar a conjunto
        vars_esperadas.add(nombre_lower)
        
        # Guardar detalle
        tabla_col = row.get('Tabla reporting', row.get('Tabla en esquema reporting', ''))
        vars_detalle.append({
            'nombre': nombre_lower,
            'tabla': tabla_col,
            'nombre_original': nombre_reporting
        })
    
    print(f"  ✓ Variables únicas esperadas en REPORTING: {len(vars_esperadas)}\n")
    
    # ========================================================================
    # 3. EXTRAER COLUMNAS REALES DEL SQL
    # ========================================================================
    print("[3/4] Extrayendo columnas reales del SQL...")
    
    with open(REPORTING_SQL, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    tablas = [
        'dataset_current_values',
        'dim_pozo',
        'dim_tiempo',
        'dim_hora',
        'fact_operaciones_horarias',
        'fact_operaciones_diarias',
        'fact_operaciones_mensuales',
        'dataset_latest_dynacard',
        'dataset_kpi_business'
    ]
    
    columnas_por_tabla = {}
    todas_columnas = set()
    
    for tabla in tablas:
        cols = extraer_columnas_sql(sql_content, tabla)
        if cols:
            columnas_por_tabla[tabla] = cols
            todas_columnas.update(cols)
            print(f"  ✓ {tabla}: {len(cols)} columnas")
    
    print(f"\n  Total columnas únicas en SQL: {len(todas_columnas)}\n")
    
    # ========================================================================
    # 4. IDENTIFICAR FALTANTES
    # ========================================================================
    print("[4/4] Identificando variables faltantes...")
    
    faltantes = vars_esperadas - todas_columnas
    
    print(f"\n  📊 RESULTADO:")
    print(f"  - Variables esperadas (hoja_validacion.csv): {len(vars_esperadas)}")
    print(f"  - Columnas en SQL: {len(todas_columnas)}")
    print(f"  - Variables FALTANTES: {len(faltantes)}\n")
    
    # Clasificar faltantes
    faltantes_sorted = sorted(list(faltantes))
    
    if faltantes:
        print("  ⚠ VARIABLES FALTANTES EN SQL:")
        print("  " + "=" * 76)
        for idx, var in enumerate(faltantes_sorted, 1):
            # Buscar en qué tabla debería estar
            tabla_destino = "?"
            for detalle in vars_detalle:
                if detalle['nombre'] == var:
                    tabla_destino = detalle['tabla']
                    break
            print(f"  {idx:2}. {var:50} → {tabla_destino}")
        print()
    else:
        print("  ✅ Todas las variables esperadas están presentes en SQL!\n")
    
    # ========================================================================
    # 5. GENERAR REPORTES
    # ========================================================================
    print("[5/5] Generando reportes...")
    
    # Markdown
    with open(OUTPUT_ANALISIS, 'w', encoding='utf-8') as f:
        f.write("# ANÁLISIS DE VARIABLES - HOJA VALIDACIÓN vs SQL\n\n")
        f.write(f"**Fecha**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("## Resumen\n\n")
        f.write(f"- Variables esperadas (hoja_validacion.csv): **{len(vars_esperadas)}**\n")
        f.write(f"- Columnas encontradas en SQL: **{len(todas_columnas)}**\n")
        f.write(f"- Variables FALTANTES: **{len(faltantes)}**\n\n")
        
        if faltantes:
            f.write("## Variables Faltantes\n\n")
            f.write("| # | Variable | Tabla Destino |\n")
            f.write("|---|----------|---------------|\n")
            for idx, var in enumerate(faltantes_sorted, 1):
                tabla_destino = "?"
                for detalle in vars_detalle:
                    if detalle['nombre'] == var:
                        tabla_destino = detalle['tabla']
                        break
                f.write(f"| {idx} | `{var}` | {tabla_destino} |\n")
            f.write("\n")
        
        f.write("## Desglose por Tabla SQL\n\n")
        for tabla, cols in sorted(columnas_por_tabla.items()):
            f.write(f"### reporting.{tabla}\n")
            f.write(f"Total columnas: {len(cols)}\n\n")
    
    print(f"  ✓ Reporte guardado: {OUTPUT_ANALISIS}")
    
    # TXT simple
    with open(OUTPUT_FALTANTES, 'w', encoding='utf-8') as f:
        f.write(f"VARIABLES FALTANTES EN SQL ({len(faltantes)} total)\n")
        f.write(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
        for var in faltantes_sorted:
            f.write(f"{var}\n")
    
    print(f"  ✓ Lista faltantes: {OUTPUT_FALTANTES}")
    
    print("\n" + "=" * 80)
    print("ANÁLISIS COMPLETADO")
    print("=" * 80)
    print(f"\n📊 Variables faltantes: {len(faltantes)}")
    print(f"📁 Reportes en: {AUDIT_DIR}\n")

if __name__ == '__main__':
    main()
