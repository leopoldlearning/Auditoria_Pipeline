import json
from pathlib import Path
import re
import os

search_keys = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME']

for nb_file in Path('.').glob('*.ipynb'):
    modified = False
    try:
        with open(nb_file, 'r', encoding='utf-8') as f:
            nb = json.load(f)
    except Exception as e:
        print(f'Error reading {nb_file}: {e}')
        continue
    
    for cell in nb['cells']:
        if cell['cell_type'] == 'code':
            new_source = []
            for line in cell['source']:
                # Check for hardcoded credentials pattern like DB_USER = "..."
                stripped = line.strip()
                replaced = False
                for key in search_keys:
                    # Matches regex: ^KEY\s*=\s*["'].*["'] or ^KEY\s*=\s*\d+
                    if re.match(fr'^\s*{key}\s*=\s*[\"\'].*[\"\']', line) or (key == 'DB_PORT' and re.match(fr'^\s*{key}\s*=\s*\d+', line)):
                        if key == 'DB_USER':
                            new_source.append(f'{key} = os.getenv("DB_USER", "audit")\n')
                        elif key == 'DB_PASSWORD':
                            new_source.append(f'{key} = os.getenv("DEV_DB_PASSWORD", "audit")\n')
                        elif key == 'DB_HOST':
                            new_source.append(f'{key} = os.getenv("DB_HOST", "localhost")\n')
                        elif key == 'DB_PORT':
                            new_source.append(f'{key} = os.getenv("DB_PORT", "5432")\n')
                        elif key == 'DB_NAME':
                            new_source.append(f'{key} = os.getenv("DB_NAME", "etl_data")\n')
                        replaced = True
                        modified = True
                        break
                if not replaced:
                    new_source.append(line)
            cell['source'] = new_source
            
    if modified:
        with open(nb_file, 'w', encoding='utf-8') as f:
            json.dump(nb, f, indent=1)
        print(f"Patched {nb_file}")
