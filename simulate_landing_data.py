#!/usr/bin/env python3
"""
Simulación de datos SCADA para auditoría (fallback cuando no hay datos reales).
Inyecta filas en stage.landing_scada_data con var_id extraídos de V1__stage_to_stage.sql
y valores < 1000 para evitar Numeric Overflow.
Solo usar en entorno de auditoría aislado.
"""
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_URL = (
    f"postgresql://{os.getenv('DB_USER', 'audit')}:{os.getenv('DEV_DB_PASSWORD', 'audit')}"
    f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5433')}/{os.getenv('DB_NAME', 'etl_data')}"
)

# var_id usados en V1__stage_to_stage (pivot). Valores numéricos < 1000; boolean para var_id=3.
VAR_IDS_FLOAT = [
    727, 12058, 12059, 11, 692, 717, 12295, 12277, 321, 728, 714,
    137, 131, 140, 740, 741, 12282, 12285, 772, 12184, 284, 1216, 1217, 286,
    883, 65, 866, 13, 1218, 1219, 298, 733, 793, 715, 716, 1135, 12296,
    282, 293, 292, 8, 766, 12283, 12279, 12280, 12268, 12269, 12281,
    694, 1206, 294, 934, 299, 289, 290, 283, 898, 899, 896, 1188, 288, 10,
]
VAR_ID_BOOLEAN = 3
# well_id debe existir en tbl_pozo_maestra (ej. 5 si se cargó UDF o datos reales)
DEFAULT_WELL_ID = 5


def run_simulation(well_id: int = DEFAULT_WELL_ID):
    engine = create_engine(DB_URL)
    ts = datetime.utcnow()
    moddate = ts.strftime("%Y-%m-%d %H:%M:%S")
    modtime = ts.strftime("%H:%M:%S")

    with engine.connect() as conn:
        # Limpiar landing para este pozo en esta ejecución (opcional: comentar si se desea acumular)
        conn.execute(text("DELETE FROM stage.landing_scada_data WHERE unit_id = :wid"), {"wid": well_id})
        conn.commit()

        for var_id in VAR_IDS_FLOAT:
            value = 500.0 if var_id != 694 else 1000  # entero para contador
            conn.execute(
                text("""
                    INSERT INTO stage.landing_scada_data (unit_id, location_id, var_id, measure, moddate, modtime)
                    VALUES (:unit_id, 1, :var_id, :measure, :moddate::timestamp, :modtime::time)
                """),
                {
                    "unit_id": well_id,
                    "var_id": var_id,
                    "measure": str(int(value) if var_id in (694, 299, 289, 898, 899, 896) else value),
                    "moddate": moddate,
                    "modtime": modtime,
                },
            )
        conn.execute(
            text("""
                INSERT INTO stage.landing_scada_data (unit_id, location_id, var_id, measure, moddate, modtime)
                VALUES (:unit_id, 1, :var_id, :measure, :moddate::timestamp, :modtime::time)
            """),
            {"unit_id": well_id, "var_id": VAR_ID_BOOLEAN, "measure": "true", "moddate": moddate, "modtime": modtime},
        )
        conn.commit()

    print(f"[OK] Simulated landing_scada_data for well_id={well_id} ({len(VAR_IDS_FLOAT) + 1} rows).")


if __name__ == "__main__":
    run_simulation()
