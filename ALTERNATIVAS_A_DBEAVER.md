# SOLUCIÓN ALTERNATIVA - Usar psql en lugar de DBeaver

Si DBeaver sigue dando problemas, puedes conectarte directamente con psql desde Docker:

## Opción 1: Conectar con psql desde Docker

```powershell
docker exec -it bp010-audit-db psql -U audit -d etl_data
```

Una vez dentro, puedes ejecutar consultas SQL:

```sql
-- Ver todos los esquemas
\dn

-- Ver tablas en stage
\dt stage.*

-- Ver tablas en referencial
\dt referencial.*

-- Ver tablas en reporting
\dt reporting.*

-- Ejecutar consultas
SELECT COUNT(*) FROM referencial.tbl_maestra_variables;

-- Para salir
\q
```

## Opción 2: Ejecutar el script de auditoría desde psql

```powershell
docker exec -i bp010-audit-db psql -U audit -d etl_data < 01_auditoria_esquemas.sql
```

## Opción 3: Usar pgAdmin (Alternativa a DBeaver)

Si quieres una interfaz gráfica:
1. Descargar pgAdmin: https://www.pgadmin.org/download/
2. Conectar con:
   - Host: localhost
   - Port: 5433
   - Database: etl_data
   - Username: audit
   - Password: audit

## SI QUIERES INSISTIR CON DBEAVER

Intenta esto en DBeaver:

1. Al crear la conexión, después de poner los datos básicos
2. Ir a **"Advanced"** o **"Connection initialization SQL"**
3. Agregar este comando:
   ```sql
   SET timezone = 'UTC';
   ```

O en **"Driver Properties"** agregar:
- Name: `options`
- Value: `-c timezone=UTC`
