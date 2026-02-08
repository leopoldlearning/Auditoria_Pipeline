#!/usr/bin/env python3
"""
Audit Notebook Executor
Executes the BP010 data pipeline notebooks in the correct sequence for audit purposes.
"""
import os
import sys
import subprocess
import io
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Force UTF-8 encoding for stdout
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Load environment
load_dotenv()

# Define notebook execution sequence
NOTEBOOK_SEQUENCE = [
    ("0_0_create_schema_AWS_v0.ipynb", "Schema Initialization"),
    ("3_1_populate_referencial_seed.ipynb", "Referencial Seed Data"),
    ("0_1_udf_to_stage_AWS_v0.ipynb", "Excel/UDF Ingestion"),
    ("0_2_raw_to_stage_AWS_v0.ipynb", "API/Scada Ingestion"),
    ("0_3_stage_to_stage_AWS_v0.ipynb", "Stage Transformations"),
    ("1_1_stage_to_reporting_AWS_v0.ipynb", "Reporting ETL"),
    ("1_2_actualizar_current_values_v3.ipynb", "CurrentValues Update"),
]

def execute_notebook(notebook_path: Path, description: str) -> bool:
    """Execute a single notebook and return success status."""
    print("\n" + "=" * 80, flush=True)
    print(f"Executing: {description}", flush=True)
    print(f"   Notebook: {notebook_path.name}", flush=True)
    print("=" * 80, flush=True)
    
    try:
        # Use jupyter nbconvert to execute
        process = subprocess.Popen(
            [
                sys.executable, "-m", "jupyter", "nbconvert",
                "--to", "notebook",
                "--execute",
                "--inplace",
                "--ExecutePreprocessor.timeout=600",
                str(notebook_path)
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        if process.stdout:
            for line in process.stdout:
                print(line, end='', flush=True)
            
        process.wait()
        
        if process.returncode == 0:
            print(f"\n✅ {description} - SUCCESS", flush=True)
            return True
        else:
            print(f"\n❌ {description} - FAILED (Code {process.returncode})", flush=True)
            return False
            
    except Exception as e:
        print(f"\n❌ {description} - ERROR: {e}", flush=True)
        return False

def main():
    """Main execution function."""
    print("=" * 80)
    print("AUDIT PIPELINE EXECUTION")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Track results
    results = []
    audit_root = Path(__file__).parent
    
    # Execute each notebook in sequence
    for notebook_file, description in NOTEBOOK_SEQUENCE:
        notebook_path = audit_root / notebook_file
        
        if not notebook_path.exists():
            print(f"⚠️  Notebook not found: {notebook_file}")
            results.append((notebook_file, False, "Not found"))
            continue
        
        success = execute_notebook(notebook_path, description)
        results.append((notebook_file, success, "Success" if success else "Failed"))
        
        # Stop on first failure
        if not success:
            print(f"\n❌ Pipeline execution stopped due to failure in: {description}")
            break
    
    # Summary
    print("\n" + "=" * 80)
    print("EXECUTION SUMMARY")
    print("=" * 80)
    for notebook, success, status in results:
        icon = "[OK]" if success else "[FAIL]"
        print(f"{icon} {notebook}: {status}")
    
    successful = sum(1 for _, success, _ in results if success)
    total = len(results)
    print(f"\nCompleted: {successful}/{total} notebooks")
    print("=" * 80)
    
    return all(success for _, success, _ in results)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
