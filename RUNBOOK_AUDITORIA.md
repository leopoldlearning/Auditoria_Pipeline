# Runbook: Auditoría BP010 Data Pipelines

## Prerrequisitos

- Docker y Docker Compose instalados.
- Contenedor **bp010-audit-db** en ejecución. El script `init_schemas.py` y el pipeline dependen de él (uso de `docker exec` para aplicar DDL).
- Variables de entorno configuradas (ver [ENV_AUDITORIA.md](ENV_AUDITORIA.md)).
- Entorno Python con dependencias instaladas (`pip install -r requirements.txt`); se recomienda un venv.

## Orden de ejecución

1. Levantar infraestructura:
   ```bash
   docker compose up -d
   docker exec bp010-audit-db pg_isready -U audit -d etl_data
   ```
2. Activar venv (si aplica) e instalar dependencias:
   ```bash
   pip install -r requirements.txt
   ```
3. Ejecutar el pipeline completo:
   ```bash
   python MASTER_PIPELINE_RUNNER.py
   ```
   O por pasos: ejecutar `init_schemas.py`, luego los notebooks en el orden indicado en [GUIA_REPLICACION_AUDITORIA_IA.md](GUIA_REPLICACION_AUDITORIA_IA.md).

## Verificación

- Adminer: `http://localhost:8080` (usuario `audit`, contraseña `audit`, base `etl_data`, servidor `postgres-audit`).
- Consultar `reporting.dataset_current_values` y `stage.tbl_pozo_scada_dq` para validar resultados.

## Solución de problemas

- **No hay tablas en `reporting` o no hay datos en `referencial.tbl_limites_pozo`**  
  Suele deberse a que `init_schemas.py` no llegó a ejecutarse por completo (p. ej. contenedor no levantado o fallo en un paso). **Qué hacer:**  
  1. Asegúrese de que Docker está en marcha y los contenedores levantados: `docker compose up -d`.  
  2. Ejecute de nuevo el init: `python init_schemas.py` (desde la raíz del repo de auditoría).  
  3. Si aparece "No se pudo conectar a la base de datos", compruebe que el contenedor `bp010-audit-db` existe y que el puerto 5433 está accesible.

- **`stage.landing_scada_data` siempre vacía**  
  Los datos entran por (1) archivos en `D:\ITMeet\Operaciones\API Hydrog manual` (CSV/Excel/SQL que inserten en landing), o (2) el script de simulación. El `MASTER_PIPELINE_RUNNER` ejecuta la simulación **siempre que landing esté vacía** tras la ingesta real. Si no hay datos reales y tampoco simulación, revise que `simulate_landing_data.py` se ejecute (debe aparecer el mensaje "landing_scada_data vacía; ejecutando simulación...") y que no falle por conexión a la BD.
