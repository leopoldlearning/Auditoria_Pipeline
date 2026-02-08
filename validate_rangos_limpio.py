import importlib.util
import sys
from pathlib import Path

path = Path('inputs_referencial') / 'Rangos_validacion_variables_petroleras_limpio.py'
if not path.exists():
    print('MISSING', path)
    sys.exit(2)

spec = importlib.util.spec_from_file_location('rango_mod', str(path))
mod = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(mod)
except Exception as e:
    print('IMPORT_ERROR', e)
    raise

if not hasattr(mod, 'VARIABLES_PETROLERAS'):
    print('MISSING_VARIABLES_PETROLERAS')
    sys.exit(3)

vars_dict = mod.VARIABLES_PETROLERAS
print('VARIABLES_COUNT', len(vars_dict))

missing_ranges = []
for k, v in vars_dict.items():
    rmin = v.get('Rango_Min')
    rmax = v.get('Rango_Max')
    if rmin is None or rmax is None:
        missing_ranges.append(k)
    print(k, 'min=', rmin, 'max=', rmax)

if missing_ranges:
    print('MISSING_RANGES', missing_ranges)
else:
    print('ALL_RANGES_OK')
