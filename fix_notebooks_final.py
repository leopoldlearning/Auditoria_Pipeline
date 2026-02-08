import json
from pathlib import Path

# Fix all notebooks by removing the directory-checking loop
for nb_file in Path('.').glob('*.ipynb'):
    with open(nb_file, 'r', encoding='utf-8') as f:
        nb = json.load(f)
    
    modified = False
    for cell in nb['cells']:
        if cell['cell_type'] == 'code':
            new_source = []
            skip_block = False
            for i, line in enumerate(cell['source']):
                # Skip the entire while loop block
                if 'while os.path.basename' in line:
                    skip_block = True
                    # Just set the working directory once
                    new_source.append("import os\n")
                    new_source.append("os.chdir(r'D:\\ITMeet\\Operaciones\\BP010-data-pipelines-auditoria')\n")
                    new_source.append(f"print(f'Working directory: {{os.getcwd()}}')\n")
                    modified = True
                elif skip_block and line.strip() and not line.strip().startswith('print'):
                    # End of while block
                    skip_block = False
                elif not skip_block:
                    new_source.append(line)
            
            cell['source'] = new_source
    
    if modified:
        with open(nb_file, 'w', encoding='utf-8') as f:
            json.dump(nb, f, indent=1)
        print(f'Fixed {nb_file.name}')
