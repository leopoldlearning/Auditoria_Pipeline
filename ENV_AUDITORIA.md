# Variables de entorno para auditoría BP010

## Prerrequisitos (DevOps)

- **Docker y Docker Compose** instalados y en ejecución.
- **Contenedores levantados** antes de ejecutar `init_schemas.py` o `MASTER_PIPELINE_RUNNER.py`:
  ```bash
  cd BP010-data-pipelines-auditoria
  docker compose up -d
  ```
- Opcional: comprobar salud del contenedor antes de correr el pipeline:
  ```bash
  docker exec bp010-audit-db pg_isready -U audit -d etl_data
  ```
- Tanto `init_schemas.py` como el runner asumen que el contenedor **bp010-audit-db** está en ejecución (init_schemas usa `docker exec` para aplicar los DDL).

**CRÍTICO**: No uses credenciales del repositorio original. Crea un `.env` en la raíz de `BP010-data-pipelines-auditoria` con:

```ini
DB_USER=audit
DEV_DB_PASSWORD=audit
DB_HOST=localhost
DB_PORT=5433
DB_NAME=etl_data
```

Opcional: si defines `DATABASE_URL`, `init_schemas.py` la usará; si no, la construye a partir de las variables anteriores.

## Ejemplo .env completo

```ini
DB_USER=audit
DEV_DB_PASSWORD=audit
DB_HOST=localhost
DB_PORT=5433
DB_NAME=etl_data
```

Copiar este bloque a un archivo `.env` en la raíz del directorio de auditoría.

## Consideraciones técnicas (Guía de Replicación)

- **Encoding SQL**: Los scripts SQL se leen en `latin-1` y se envían en `utf-8` en `init_schemas.py`. Mantener codificación consistente en nuevos scripts.
- **Unicode en consola (Windows)**: Los logs del orquestador evitan emojis para no provocar `UnicodeEncodeError` al redirigir a `.log`.
- **Type casting en SQL**: Al llamar procedimientos desde Python usar casting explícito, ej. `'2026-02-01'::DATE`.
- **Entorno Python**: Se recomienda usar un entorno virtual (venv) e instalar dependencias con `pip install -r requirements.txt` para reproducibilidad.
