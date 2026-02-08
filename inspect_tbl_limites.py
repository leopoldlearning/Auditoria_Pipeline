from setup import engine
from sqlalchemy import text

with engine.connect() as conn:
    q = text("""
    SELECT column_name, is_nullable, data_type
    FROM information_schema.columns
    WHERE table_schema = 'referencial' AND table_name = 'tbl_limites_pozo'
    ORDER BY ordinal_position;
    """)
    for row in conn.execute(q):
        print(f"{row[0]:30s} | nullable={row[1]:5s} | type={row[2]}")
