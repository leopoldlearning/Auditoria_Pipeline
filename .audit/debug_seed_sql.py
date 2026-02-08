
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER', 'audit')
DB_PASS = os.getenv('DEV_DB_PASSWORD', 'audit')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5433')
DB_NAME = os.getenv('DB_NAME', 'etl_data')
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

file_path = "src/sql/schema/V4__referencial_seed_data.sql"

def debug_sql():
    engine = create_engine(DB_URL)
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            sql_content = f.read()
        
        # Execute each statement separated by semicolon to find exact line
        statements = sql_content.split(';')
        with engine.begin() as conn:
            for i, stmt in enumerate(statements):
                if stmt.strip():
                    try:
                        conn.execute(text(stmt + ';'))
                    except Exception as stmt_error:
                        print(f"Error in Statement {i+1}:")
                        print(stmt.strip())
                        print("-" * 20)
                        print(f"Detail: {stmt_error}")
                        break
        print("Done.")
    except Exception as e:
        print(f"General Error: {e}")

if __name__ == "__main__":
    debug_sql()
