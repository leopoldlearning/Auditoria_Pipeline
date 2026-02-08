import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime
import io

# Force UTF-8
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Notebook sequence
NOTEBOOKS = [
    ("0_0_create_schema_AWS_v0.ipynb", "Schema Initialization"),
    ("3_1_populate_referencial_seed.ipynb", "Referencial Seed Data"),
    ("0_1_udf_to_stage_AWS_v0.ipynb", "Excel/UDF Ingestion"),
    ("0_2_raw_to_stage_AWS_v0.ipynb", "API/Scada Ingestion"),
    ("0_3_stage_to_stage_AWS_v0.ipynb", "Stage Transformations"),
    ("1_1_stage_to_reporting_AWS_v0.ipynb", "Reporting ETL"),
    ("1_2_actualizar_current_values_v3.ipynb", "CurrentValues Update"),
]

def run_notebook(nb_file, desc):
    log_file = f"log_{nb_file.replace('.ipynb', '')}.txt"
    print(f"\n--- Running: {desc} ({nb_file}) ---", flush=True)
    print(f"Logging to: {log_file}", flush=True)
    
    with open(log_file, 'w', encoding='utf-8') as f:
        process = subprocess.Popen(
            [
                sys.executable, "-m", "jupyter", "nbconvert",
                "--to", "notebook",
                "--execute",
                "--inplace",
                "--ExecutePreprocessor.timeout=600",
                nb_file
            ],
            stdout=f,
            stderr=subprocess.STDOUT,
            text=True
        )
        process.wait()
        
    if process.returncode == 0:
        print(f"✅ SUCCESS", flush=True)
        return True
    else:
        print(f"❌ FAILED", flush=True)
        return False

if __name__ == "__main__":
    for nb, desc in NOTEBOOKS:
        success = run_notebook(nb, desc)
        if not success:
            print(f"\nPipeline interrupted at {nb}", flush=True)
            break
