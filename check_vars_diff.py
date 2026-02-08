import re

# Paths
sql_path = r"D:\ITMeet\Operaciones\BP010-data-pipelines\src\sql\process\V1__stage_to_stage.sql"
py_path = r"D:\ITMeet\Operaciones\BP010-data-pipelines-auditoria\simulate_scada_data.py"

# Read SQL
with open(sql_path, "r", encoding="utf-8") as f:
    sql_lines = f.readlines()

sql_ids = set()
commented_ids = set()

for line in sql_lines:
    # Find all occurrences of l.var_id = X
    matches = re.findall(r"l\.var_id\s*=\s*(\d+)", line)
    for m in matches:
        vid = int(m)
        sql_ids.add(vid)
        # Check if line is commented
        if line.strip().startswith("--"):
            commented_ids.add(vid)

# Read Python
with open(py_path, "r", encoding="utf-8") as f:
    py_content = f.read()

# Pattern for dictionary keys: 123: (
py_ids = set()
matches = re.findall(r"^\s*(\d+):\s*\(", py_content, re.MULTILINE)
for m in matches:
    py_ids.add(int(m))

# Diff
missing = sql_ids - py_ids
missing_active = missing - commented_ids
missing_commented = missing.intersection(commented_ids)

print(f"Total Unique Vars in SQL: {len(sql_ids)}")
print(f"Total Vars in Simulation: {len(py_ids)}")
print(f"Missing Total: {len(missing)}")
print("-" * 30)
if missing_active:
    print(f"MISSING ACTIVE VARS (Critical): {sorted(list(missing_active))}")
else:
    print("No missing active variables.")

if missing_commented:
    print(f"Missing variables (Commented in SQL): {sorted(list(missing_commented))}")
