from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

with engine.connect() as conn:
    print("--- DEBUGGING DQ ENGINE ---")
    try:
        conn.execute(text("CALL stage.sp_execute_dq_validation('2020-01-01'::DATE, '2030-12-31'::DATE, NULL::INT);"))
        print("SUCCESS!")
    except Exception as e:
        print("\n--- ERROR CAPTURED ---")
        print(str(e))
