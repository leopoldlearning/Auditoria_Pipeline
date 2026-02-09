
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

files = [
    "src/sql/schema/V4__referencial_schema_redesign.sql",
    "src/sql/schema/V4__stage_schema_redesign.sql",
    "src/sql/schema/V1__universal_schema.sql",
    "src/sql/schema/V4__reporting_schema_redesign.sql",
    "src/sql/schema/V6.1__historical_reporting_engine_v4.sql",
    "src/sql/schema/V6.2__dq_engine_v4.sql",
    "src/sql/schema/V6.3__sync_dim_pozo_targets_v4.sql",
    "src/sql/schema/V6__stored_procedures_v4_compatible.sql",
    "src/sql/schema/V4__referencial_seed_data.sql"
]

for f in files:
    print(f"Checking {f}...")
    try:
        with open(f, "r", encoding="utf-8") as file:
            sql = file.read()
            if sql.strip():
                with engine.begin() as conn:
                    # Execute script in parts to avoid large transaction issues if needed
                    # but here we just want to find the failing file
                    conn.execute(text(sql))
        print(f"✅ {f} OK")
    except Exception as e:
        print(f"❌ {f} FAILED: {e}")
        break
