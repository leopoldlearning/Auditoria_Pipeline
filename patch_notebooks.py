import os
import json
from pathlib import Path

audit_dir = 'BP010-data-pipelines-auditoria'
primary_dir = 'BP010-data-pipelines'

for p in Path('.').glob('*.ipynb'):
    try:
        with open(p, 'r', encoding='utf-8') as f:
            nb = json.load(f)
        
        modified = False
        for cell in nb['cells']:
            if cell['cell_type'] == 'code':
                new_source = []
                for line in cell['source']:
                    # Look for the while loop that changes directory
                    if primary_dir in line and 'os.path.basename' in line:
                        # Replace 'BP010-data-pipelines' with a check for both
                        old_pattern = f"'{primary_dir}'"
                        new_pattern = f"['{primary_dir}', '{audit_dir}']"
                        line = line.replace(old_pattern, new_pattern)
                        line = line.replace("==", "in")
                        modified = True
                    new_source.append(line)
                cell['source'] = new_source
                
        if modified:
            with open(p, 'w', encoding='utf-8') as f:
                json.dump(nb, f, indent=1)
            print(f'Patched {p}')
    except Exception as e:
        print(f'Error patching {p}: {e}')
