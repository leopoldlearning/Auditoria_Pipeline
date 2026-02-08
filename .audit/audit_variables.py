
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(os.getenv('DATABASE_URL'))

dq_names = [
    'formation_thickness_ft', 'damage_factor', 'formation_volume_factor', 'max_fluid_load_lb',
    'water_specific_gravity', 'absolute_permeability_md', 'vertical_permeability_md', 'horizontal_permeability_md',
    'bubble_point_pressure_psi', 'well_head_pressure_psi_act', 'flowing_bottom_hole_pressure_psi',
    'critical_fbhp_psi', 'casing_head_pressure_psi_act', 'presion_estatica_yacimiento',
    'prod_petroleo_diaria_bpd', 'profundidad_vertical_bomba', 'profundidad_vertical_yacimiento',
    'equivalent_radius_ft', 'drainage_radius_ft', 'wellbore_radius_ft', 'crude_oil_viscosity_cp',
    'well_type', 'horizontal_length_ft', 'corrected_dynamic_fluid_level_ft', 'min_rod_load_lb_act',
    'well_stroke_position_in', 'surface_rod_load_lb', 'downhole_pump_position_in', 'downhole_pump_load_lb',
    'tubing_anchor_depth_ft', 'rod_weight_buoyant_lb_act', 'max_rod_load_lb_act', 'motor_current_a_act',
    'porcentaje_agua', 'pump_fill_monitor_pct'
]

rc_vars = [
    'max_rod_load_lb_act', 'min_rod_load_lb_act', 'rod_weight_buoyant_lb_act',
    'flowing_bottom_hole_pressure_psi', 'well_head_pressure_psi_act',
    'presion_estatica_yacimiento', 'profundidad_vertical_bomba', 'profundidad_vertical_yacimiento',
    'wellbore_radius_ft', 'drainage_radius_ft'
]

def audit():
    with engine.connect() as conn:
        existing = [r[0] for r in conn.execute(text('SELECT nombre_tecnico FROM referencial.tbl_maestra_variables'))]
        
        print("AUDIT RESULT:")
        print("-" * 20)
        found_all_dq = True
        for n in dq_names:
            if n not in existing:
                print(f"MISSING DQ VAR: {n}")
                found_all_dq = False
        if found_all_dq: print("✅ All DQ Vars found.")

        found_all_rc = True
        for n in rc_vars:
            if n not in existing:
                print(f"MISSING RC VAR: {n}")
                found_all_rc = False
        if found_all_rc: print("✅ All RC Vars found.")

if __name__ == "__main__":
    audit()
