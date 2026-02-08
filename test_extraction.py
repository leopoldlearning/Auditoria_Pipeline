#!/usr/bin/env python3
import re

sql_file = r"src\sql\process\V1__stage_to_stage.sql"

with open(sql_file, "r", encoding="utf-8") as f:
    lines = f.readlines()

# Test extraction
for i in range(len(lines)):
    line = lines[i]
    id_pattern = re.match(r"\s*--\s*ID:\s*(\d+)\s*\|\s*IDN:\s*(\d+)\s*\|\s*(.+)$", line)
    if id_pattern and i < 5:  # Solo primeros 5 para test
        id_fmt = id_pattern.group(1)
        idn = id_pattern.group(2)
        desc = id_pattern.group(3).strip()
        print(f"\nEncontrado ID={id_fmt}, IDN={idn}, DESC={desc}")
        
        # Buscar siguiente línea con AS
        for j in range(i + 1, min(i + 5, len(lines))):
            next_line = lines[j]
            print(f"  Línea {j}: {next_line.strip()}")
            as_match = re.search(r"AS\s+([a-zA-Z_][a-zA-Z0-9_]*)", next_line, re.IGNORECASE)
            if as_match:
                col_name = as_match.group(1)
                print(f"  ✓ Encontrado: {col_name}")
                break
