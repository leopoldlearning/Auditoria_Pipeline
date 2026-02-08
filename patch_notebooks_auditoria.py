#!/usr/bin/env python3
"""
Parches para notebooks en entorno de auditoría (según Guía de Replicación).
1. Path looping: reemplazar lógica que busca recursivamente el directorio raíz por os.chdir(PROJECT_ROOT).
2. Credenciales: reemplazar DB_USER = "..." (o similares) por os.getenv('DB_USER').
Ejecutar desde la raíz de BP010-data-pipelines-auditoria.
"""
import json
import os
from pathlib import Path

AUDIT_ROOT = Path(__file__).parent.resolve()
PROJECT_ROOT_VAR = "PROJECT_ROOT"

def patch_notebook(path: Path) -> bool:
    modified = False
    with open(path, "r", encoding="utf-8") as f:
        nb = json.load(f)

    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = cell.get("source", [])
        if not isinstance(source, list):
            source = [source] if source else []
        new_source = []
        for line in source:
            s = line if isinstance(line, str) else "".join(line)
            # 1) Path looping: mientras ... os.path.basename(...) == 'BP010-data-pipelines' -> os.chdir(PROJECT_ROOT)
            if "os.path.basename" in s and ("BP010-data-pipelines" in s or "BP010-data-pipelines-auditoria" in s):
                if "while" in s or "chdir" not in s:
                    # Reemplazar bloque típico por una sola línea
                    if "chdir" not in s:
                        new_source.append(f"PROJECT_ROOT = r'{AUDIT_ROOT}'\nos.chdir(PROJECT_ROOT)\n")
                        modified = True
                    continue
            # 2) Credenciales hardcodeadas
            if 'DB_USER = "' in s or "DB_USER = '" in s:
                if "os.getenv" not in s:
                    s = s.replace('DB_USER = "', 'DB_USER = os.getenv("DB_USER", "audit")  # was: "')
                    idx = s.find('")  # was')
                    if idx == -1:
                        s = "DB_USER = os.getenv('DB_USER', 'audit')\n"
                    modified = True
            if 'DEV_DB_PASSWORD = "' in s or "db_password = \"hydrog" in s.lower():
                if "os.getenv" not in s:
                    s = "db_password = os.getenv('DEV_DB_PASSWORD', 'audit')\n"
                    modified = True
            new_source.append(s)

        cell["source"] = new_source

    if modified:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(nb, f, indent=1)
    return modified


def main():
    for p in AUDIT_ROOT.glob("*.ipynb"):
        if patch_notebook(p):
            print(f"[OK] Patched: {p.name}")


if __name__ == "__main__":
    main()
