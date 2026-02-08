#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRIPT DE VERIFICACIÓN V4 REPORTING SCHEMA
Verifica que todos los 24 cambios esperados estén presentes
"""

import re
from pathlib import Path

V4_PATH = Path(r"D:\ITMeet\Operaciones\BP010-data-pipelines-auditoria\src\sql\schema\V4__reporting_schema_redesign.sql")
AUDIT_DIR = Path(r"D:\ITMeet\Operaciones\BP010-data-pipelines-auditoria\.audit")

# Cambios esperados
RENOMBRAMIENTOS = [
    "well_head_pressure_psi_act",
    "casing_head_pressure_psi_act",
    "pump_intake_pressure_psi_act",
    "pump_discharge_pressure_psi_act",
    "gas_fill_monitor_pct_act",
    "rod_weight_buoyant_lb_act",
    "ai_accuracy_act",
    "pump_spm_status_color",
    "pump_avg_spm_act",
    "mtbf_variance_pct",
    "motor_power_hp_act",
    "motor_current_a_act",
    "longitud_carrera_nominal_unidad_in",
    "prod_petroleo_diaria_bpd",
    "produccion_petroleo_acumulada_bbl"
]

NUEVAS_VARIABLES = [
    "current_stroke_length_act_in",
    "fluid_level_tvd_ft",
    "pwf_psi_act",
    "motor_running_flag",
    "produccion_fluido_bbl",
    "estado_motor_fin_dia",
    "remanent_reserves_bbl",
    "prom_produccion_fluido_bbl",
    "pump_stroke_length_act"  # MANTENER (coexiste con current_stroke_length_act_in)
]

def main():
    print("=" * 80)
    print("VERIFICACIÓN V4 REPORTING SCHEMA")
    print("=" * 80)
    
    with open(V4_PATH, 'r', encoding='utf-8') as f:
        content = f.read().lower()
    
    print("\n[1/2] Verificando renombramientos (15 esperados)...")
    missing_renamed = []
    for var in RENOMBRAMIENTOS:
        if var.lower() not in content:
            missing_renamed.append(var)
            print(f"  ❌ FALTA: {var}")
        else:
            print(f"  ✓ {var}")
    
    print(f"\n[2/2] Verificando nuevas variables (9 esperadas)...")
    missing_new = []
    for var in NUEVAS_VARIABLES:
        if var.lower() not in content:
            missing_new.append(var)
            print(f"  ❌ FALTA: {var}")
        else:
            print(f"  ✓ {var}")
    
    print("\n" + "=" * 80)
    print("RESULTADO")
    print("=" * 80)
    
    total_missing = len(missing_renamed) + len(missing_new)
    
    if total_missing == 0:
        print("✅ VERIFICACIÓN EXITOSA")
        print(f"   - Renombramientos: {len(RENOMBRAMIENTOS)}/{len(RENOMBRAMIENTOS)}")
        print(f"   - Nuevas variables: {len(NUEVAS_VARIABLES)}/{len(NUEVAS_VARIABLES)}")
        print(f"   - Total: 24/24 cambios aplicados correctamente")
        return 0
    else:
        print(f"❌ VERIFICACIÓN FALLIDA: {total_missing} cambios faltantes")
        if missing_renamed:
            print(f"\n  Renombramientos faltantes: {missing_renamed}")
        if missing_new:
            print(f"\n  Nuevas variables faltantes: {missing_new}")
        return 1

if __name__ == '__main__':
    exit(main())
