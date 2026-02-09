from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

with engine.begin() as conn:
    print("\n--- INICIANDO VERIFICACIÓN DE HERENCIA MULTI-POZO ---")
    
    # 1. Crear un pozo nuevo (ID 100)
    conn.execute(text("INSERT INTO stage.tbl_pozo_maestra (well_id, nombre_pozo) VALUES (100, 'Pozo Auditoria 100') ON CONFLICT (well_id) DO UPDATE SET nombre_pozo = EXCLUDED.nombre_pozo"))
    
    # 2. Consultar la vista pivote para el Pozo 1 (Plantilla) y el Pozo 100 (Nuevo)
    res = conn.execute(text("SELECT pozo_id, whp_max_crit, spm_target FROM referencial.vw_limites_pozo_pivot_v4 WHERE pozo_id IN (1, 100) ORDER BY pozo_id")).fetchall()
    
    if len(res) < 2:
        print(f"ERROR: No se encontraron ambos pozos en la vista. Filas encontradas: {len(res)}")
        for r in res: print(f"  Pozo {r[0]}")
    else:
        p1 = res[0]
        p100 = res[1]
        print(f"Pozo 1   | WHP Max Crit: {p1[1]} | SPM Target: {p1[2]}")
        print(f"Pozo 100 | WHP Max Crit: {p100[1]} | SPM Target: {p100[2]}")
        
        if p1[1] == p100[1] and p1[2] == p100[2]:
            print("\n✅ ÉXITO: El Pozo 100 ha heredado correctamente los límites del Pozo 1 (Plantilla).")
        else:
            print("\n❌ FALLO: Los límites no coinciden.")
