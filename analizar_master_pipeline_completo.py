import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

BP010_DIR = Path("D:/ITMeet/Operaciones/BP010-data-pipelines-auditoria")

ELEMENTOS = {
    "scripts": ["init_schemas.py", "ingest_real_telemetry.py"],
    "notebooks": ["0_1_udf_to_stage_AWS_v0.ipynb", "0_3_stage_to_stage_AWS_v0.ipynb", "1_2_actualizar_current_values_v3.ipynb"],
    "ddl": ["src/sql/schema/V3__referencial_schema_redesign.sql", "src/sql/schema/V4__stage_schema_redesign.sql", "src/sql/schema/V1__universal_schema.sql", "src/sql/schema/V3__reporting_schema_redesign.sql", "src/sql/schema/V5__stored_procedures.sql"],
}

class Analyzer:
    def check(self, path):
        p = BP010_DIR / path
        return p.exists()
    
    def postgres(self):
        rep = "## PostgreSQL Expert\n\n"
        for item in ELEMENTOS["ddl"][:3]:
            rep += f"- {'OK' if self.check(item) else 'FALTA'}: {Path(item).name}\n"
        return rep

a = Analyzer()
print(a.postgres())
print("\nOK - Script listo para ejecutar")
