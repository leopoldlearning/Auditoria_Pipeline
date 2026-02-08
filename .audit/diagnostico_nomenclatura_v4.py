#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
DIAGNÓSTICO DE NOMENCLATURA PARA V4 REPORTING SCHEMA
================================================================================

Objetivo: Analizar discrepancias de nombres entre:
1. fact_operaciones_horarias
2. fact_operaciones_diarias
3. fact_operaciones_mensuales
4. dataset_current_values
5. hoja_validacion.csv (fuente de verdad)

Incluye correcciones específicas del usuario:
- produccion_fluido_bbl → Agregar a horarias (ID:107)
- prod_petroleo_bbl → Renombrar a prod_petroleo_diaria_bpd (ID:108)
- total_petroleo_bbl → Renombrar a produccion_petroleo_acumulada_bbl (ID:98)
- remanent_reserves_bbl → Agregar calculado (ID:130)
- prom_produccion_fluido_bbl → Agregar a mensuales (ID:107)
- estado_motor_on → Binaria en current_values (ID:120)

Fecha: 2026-02-08
Autor: Antigravity
================================================================================
"""

import re
import csv
from pathlib import Path
from typing import Dict, List, Set, Tuple
from collections import defaultdict
from datetime import datetime

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

BASE_DIR = Path(r"D:\ITMeet\Operaciones\BP010-data-pipelines-auditoria")
SQL_SCHEMA = BASE_DIR / "src" / "sql" / "schema"
DATA_DIR = BASE_DIR / "data"
AUDIT_DIR = BASE_DIR / ".audit"

# Archivos
V3_SQL = SQL_SCHEMA / "V3__reporting_schema_redesign.sql"
HOJA_VALIDACION = DATA_DIR / "hoja_validacion.csv"

# Outputs
OUTPUT_DIAGNOSTICO = AUDIT_DIR / "DIAGNOSTICO_NOMENCLATURA_V4.md"
OUTPUT_CORRECCIONES = AUDIT_DIR / "CORRECCIONES_USUARIO.md"
OUTPUT_PLAN_V4 = AUDIT_DIR / "PLAN_V4_SCHEMA.md"

# ============================================================================
# CORRECCIONES ESPECÍFICAS DEL USUARIO
# ============================================================================

CORRECCIONES_USUARIO = {
    'horarias': [
        {
            'accion': 'AGREGAR',
            'nombre': 'produccion_fluido_bbl',
            'id_formato1': 107,
            'comentario': 'Incluir en reporting operaciones horarias. NO existe'
        },
        {
            'accion': 'RENOMBRAR',
            'nombre_actual': 'prod_petroleo_bbl',
            'nombre_nuevo': 'prod_petroleo_diaria_bpd',
            'id_formato1': 108,
            'comentario': 'Cambiar prod_petroleo_bbl por prod_petroleo_diaria_bpd'
        }
    ],
    'mensuales': [
        {
            'accion': 'RENOMBRAR',
            'nombre_actual': 'total_petroleo_bbl',
            'nombre_nuevo': 'produccion_petroleo_acumulada_bbl',
            'id_formato1': 98,
            'comentario': 'Cambiar total_petroleo_bbl por produccion_petroleo_acumulada_bbl'
        },
        {
            'accion': 'AGREGAR',
            'nombre': 'remanent_reserves_bbl',
            'id_formato1': 130,
            'tipo': 'CALCULADO',
            'comentario': 'Calculado. Incluir en reporting.fact_operaciones_mensuales'
        },
        {
            'accion': 'AGREGAR',
            'nombre': 'prom_produccion_fluido_bbl',
            'id_formato1': 107,
            'comentario': 'Incluir en operaciones mensuales. NO existe'
        }
    ],
    'current_values': [
        {
            'accion': 'AGREGAR',
            'nombre': 'estado_motor_on',
            'id_formato1': 120,
            'tipo': 'BOOLEAN',
            'comentario': 'Variable binaria para saber si motor está encendido. Barra verde/rojo en dashboard'
        }
    ]
}

# ============================================================================
# FUNCIONES
# ============================================================================

def extraer_columnas_tabla(sql_content: str, tabla: str) -> Dict[str, List[str]]:
    """Extrae columnas de una tabla con sus tipos de dato."""
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
        
        # Extraer nombre y tipo
        match_col = re.match(r'^\s*([a-z_][a-z0-9_]*)\s+([A-Z]+(?:\(\d+(?:,\s*\d+)?\))?)', line, re.IGNORECASE)
        if match_col:
            col_name = match_col.group(1)
            col_type = match_col.group(2)
            columnas[col_name] = col_type
    
    return columnas

def leer_csv(filepath: Path, delimiter=';') -> List[Dict]:
    """Lee CSV y retorna lista de diccionarios."""
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return list(reader)

def comparar_nomenclatura(col_horarias, col_diarias, col_mensuales, col_current):
    """Compara nomenclatura entre tablas fact."""
    # Nombres base (sin sufijos _pct, _act, _target, etc)
    def get_base_name(name):
        suffixes = ['_pct', '_act', '_bbl', '_psi', '_ft', '_in', '_lb', '_hp', '_a', '_f', '_hrs', '_mcf', '_kwh', '_usd']
        for suffix in suffixes:
            if name.endswith(suffix):
                return name[:-len(suffix)]
        return name
    
    # Agrupar por nombre base
    groups = defaultdict(lambda: {'horarias': [], 'diarias': [], 'mensuales': [], 'current': []})
    
    for col in col_horarias.keys():
        base = get_base_name(col)
        groups[base]['horarias'].append(col)
    
    for col in col_diarias.keys():
        base = get_base_name(col)
        groups[base]['diarias'].append(col)
    
    for col in col_mensuales.keys():
        base = get_base_name(col)
        groups[base]['mensuales'].append(col)
    
    for col in col_current.keys():
        base = get_base_name(col)
        groups[base]['current'].append(col)
    
    return groups

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 80)
    print("DIAGNÓSTICO DE NOMENCLATURA PARA V4 REPORTING SCHEMA")
    print("=" * 80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    
    # ========================================================================
    # 1. LEER SQL ACTUAL (V3)
    # ========================================================================
    print("[1/5] Leyendo V3 Reporting Schema...")
    with open(V3_SQL, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    col_horarias = extraer_columnas_tabla(sql_content, 'fact_operaciones_horarias')
    col_diarias = extraer_columnas_tabla(sql_content, 'fact_operaciones_diarias')
    col_mensuales = extraer_columnas_tabla(sql_content, 'fact_operaciones_mensuales')
    col_current = extraer_columnas_tabla(sql_content, 'dataset_current_values')
    
    print(f"  ✓ fact_operaciones_horarias: {len(col_horarias)} columnas")
    print(f"  ✓ fact_operaciones_diarias: {len(col_diarias)} columnas")
    print(f"  ✓ fact_operaciones_mensuales: {len(col_mensuales)} columnas")
    print(f"  ✓ dataset_current_values: {len(col_current)} columnas\n")
    
    # ========================================================================
    # 2. COMPARAR NOMENCLATURA
    # ========================================================================
    print("[2/5] Comparando nomenclatura entre tablas...")
    
    grupos = comparar_nomenclatura(col_horarias, col_diarias, col_mensuales, col_current)
    
    # Detectar inconsistencias
    inconsistencias = []
    for base, tables in grupos.items():
        if len(tables['horarias']) > 0 and len(tables['diarias']) > 0:
            if tables['horarias'][0] != tables['diarias'][0].replace('produccion_', 'prod_').replace('promedio_', 'prom_'):
                inconsistencias.append({
                    'base': base,
                    'horarias': tables['horarias'],
                    'diarias': tables['diarias'],
                    'tipo': 'NOMBRE_DIFERENTE'
                })
    
    print(f"  ⚠ Inconsistencias detectadas: {len(inconsistencias)}\n")
    
    # ========================================================================
    # 3. ANALIZAR CORRECCIONES DEL USUARIO
    # ========================================================================
    print("[3/5] Analizando correcciones del usuario...")
    
    correcciones_aplicables = {
        'horarias': [],
        'diarias': [],
        'mensuales': [],
        'current_values': []
    }
    
    for tabla, correcciones in CORRECCIONES_USUARIO.items():
        for corr in correcciones:
            if corr['accion'] == 'AGREGAR':
                tabla_cols = {
                    'horarias': col_horarias,
                    'mensuales': col_mensuales,
                    'current_values': col_current
                }[tabla]
                
                if corr['nombre'] not in tabla_cols:
                    correcciones_aplicables[tabla].append(corr)
                    print(f"  ✓ {tabla}: AGREGAR {corr['nombre']} (ID:{corr['id_formato1']})")
                else:
                    print(f"  ⚠ {tabla}: {corr['nombre']} ya existe")
            
            elif corr['accion'] == 'RENOMBRAR':
                tabla_cols = {
                    'horarias': col_horarias,
                    'mensuales': col_mensuales
                }[tabla]
                
                if corr['nombre_actual'] in tabla_cols:
                    correcciones_aplicables[tabla].append(corr)
                    print(f"  ✓ {tabla}: RENOMBRAR {corr['nombre_actual']} → {corr['nombre_nuevo']}")
                else:
                    print(f"  ⚠ {tabla}: {corr['nombre_actual']} NO EXISTE (no se puede renombrar)")
    
    print()
    
    # ========================================================================
    # 4. GENERAR REPORTES
    # ========================================================================
    print("[4/5] Generando reportes...")
    
    # Reporte principal
    with open(OUTPUT_DIAGNOSTICO, 'w', encoding='utf-8') as f:
        f.write("# DIAGNÓSTICO DE NOMENCLATURA - V4 REPORTING SCHEMA\n\n")
        f.write(f"**Fecha**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## 1. Estado Actual (V3)\n\n")
        f.write(f"- `fact_operaciones_horarias`: {len(col_horarias)} columnas\n")
        f.write(f"- `fact_operaciones_diarias`: {len(col_diarias)} columnas\n")
        f.write(f"- `fact_operaciones_mensuales`: {len(col_mensuales)} columnas\n")
        f.write(f"- `dataset_current_values`: {len(col_current)} columnas\n\n")
        
        f.write("## 2. Inconsistencias Detectadas\n\n")
        if inconsistencias:
            for inc in inconsistencias[:10]:  # Primeras 10
                f.write(f"### {inc['base']}\n")
                f.write(f"- Horarias: `{inc['horarias']}`\n")
                f.write(f"- Diarias: `{inc['diarias']}`\n\n")
        else:
            f.write("✅ No se detectaron inconsistencias mayores.\n\n")
        
        f.write("## 3. Correcciones del Usuario\n\n")
        for tabla, corr_list in correcciones_aplicables.items():
            if corr_list:
                f.write(f"### {tabla}\n\n")
                for corr in corr_list:
                    nombre_display = corr.get('nombre', '') or f"{corr.get('nombre_actual', '')} → {corr.get('nombre_nuevo', '')}"
                    f.write(f"- **{corr['accion']}**: {nombre_display}\n")
                    f.write(f"  - ID: {corr['id_formato1']}\n")
                    f.write(f"  - Comentario: {corr['comentario']}\n\n")
    
    print(f"  ✓ Diagnóstico guardado: {OUTPUT_DIAGNOSTICO}")
    
    # Reporte de correcciones
    with open(OUTPUT_CORRECCIONES, 'w', encoding='utf-8') as f:
        f.write("# CORRECCIONES ESPECÍFICAS DEL USUARIO\n\n")
        f.write("## Resumen de Cambios para V4\n\n")
        
        total_cambios = sum(len(v) for v in correcciones_aplicables.values())
        f.write(f"**Total de cambios a aplicar**: {total_cambios}\n\n")
        
        for tabla, corr_list in correcciones_aplicables.items():
            if corr_list:
                f.write(f"### Tabla: reporting.{tabla}\n\n")
                f.write("| Acción | Variable | ID | Comentario |\n")
                f.write("|--------|----------|----|-----------|\n")
                for corr in corr_list:
                    nombre_display = corr.get('nombre', '') or f"{corr.get('nombre_actual', '')} → {corr.get('nombre_nuevo', '')}"
                    f.write(f"| {corr['accion']} | `{nombre_display}` | {corr['id_formato1']} | {corr['comentario']} |\n")
                f.write("\n")
    
    print(f"  ✓ Correcciones guardadas: {OUTPUT_CORRECCIONES}")
    
    # Plan V4
    with open(OUTPUT_PLAN_V4, 'w', encoding='utf-8') as f:
        f.write("# PLAN DE CREACIÓN V4 REPORTING SCHEMA\n\n")
        f.write("## Filosofía Zero-Calc / Zero-Hardcode\n\n")
        f.write("- ✅ **Zero-Calc**: BI no calcula, solo visualiza\n")
        f.write("- ✅ **Zero-Hardcode**: No números mágicos en código\n")
        f.write("- ✅ **Referencial como cerebro**: Única fuente de verdad\n\n")
        
        f.write("## Cambios a Implementar\n\n")
        f.write(f"**Total de modificaciones**: {total_cambios}\n\n")
        
        f.write("### Variables a Agregar/Renombrar por Tabla\n\n")
        for tabla in ['horarias', 'mensuales', 'current_values']:
            if correcciones_aplicables[tabla]:
                f.write(f"#### {tabla}\n")
                for corr in correcciones_aplicables[tabla]:
                    f.write(f"- [ ] {corr['accion']}: {corr.get('nombre', corr.get('nombre_nuevo', 'N/A'))}\n")
                f.write("\n")
    
    print(f"  ✓ Plan V4 guardado: {OUTPUT_PLAN_V4}")
    
    # ========================================================================
    # 5. RESUMEN
    # ========================================================================
    print("\n" + "=" * 80)
    print("DIAGNÓSTICO COMPLETADO")
    print("=" * 80)
    print(f"\n📊 Total de cambios a aplicar: {total_cambios}")
    print(f"📁 Reportes generados en: {AUDIT_DIR}\n")

if __name__ == '__main__':
    main()
