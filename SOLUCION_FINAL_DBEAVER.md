# ✅ SOLUCIÓN FINAL PARA DBEAVER 25.3.3

El problema es que DBeaver está enviando el timezone del sistema Windows que PostgreSQL no reconoce.

## SOLUCIÓN: Usar "Connection initialization SQL" en DBeaver

### Paso a Paso en DBeaver 25.3.3:

1. **Nueva Conexión PostgreSQL**
2. **Pestaña "Main"** - Llenar:
   ```
   Host: localhost
   Port: 5433
   Database: etl_data
   Username: audit
   Password: audit
   ☑️ Save password
   ```

3. **MUY IMPORTANTE - Ir a la pestaña "Connection details"** o **"Initialization"**
   - Buscar el campo **"Initialization SQL"** o **"SQL to execute on connection"**
   - Agregar esta línea:
     ```sql
     SET timezone = 'UTC';
     ```

4. **Test Connection** → Debería funcionar ✅

---

## ALTERNATIVA 1: Si no encuentras "Initialization SQL"

1. Después de crear la conexión
2. Click derecho en la conexión → **"Edit Connection"**
3. Ir a **"Connection details"** → **"Bootstrap queries"** o **"Initialization SQL"**
4. Agregar: `SET timezone = 'UTC';`

---

## ALTERNATIVA 2: Modificar configuración del Driver

1. Al crear la conexión, click en **"Edit Driver Settings"**
2. En la pestaña **"Advanced"** o **"Connection properties"**
3. Buscar o agregar:
   - Property: `ApplicationName`
   - Value: `DBeaver`
4. Y también:
   - Property: `assumeMinServerVersion`
   - Value: `15.0`

---

## ALTERNATIVA 3: Usar psql (MÁS RÁPIDO PARA VALIDAR)

Ejecutar desde PowerShell:

```powershell
docker exec -it bp010-audit-db psql -U audit -d etl_data
```

Dentro de psql:
```sql
-- Ver esquemas
\dn

-- Ver tablas en stage  
\dt stage.*

-- Ver tablas en referencial
\dt referencial.*

-- Consultar datos
SELECT * FROM referencial.tbl_maestra_variables LIMIT 5;

-- Salir
\q
```

---

## ÚLTIMA OPCIÓN: Descargar cliente psql para Windows

Si DBeaver no funciona, puedes usar psql nativo:

1. Desde PowerShell:
```powershell
# Instalar PostgreSQL client tools
winget install PostgreSQL.PostgreSQL
```

2. Conectar con:
```powershell
psql -h localhost -p 5433 -U audit -d etl_data
```

---

## VERIFICACIÓN

Los esquemas están creados y funcionando:
```
✅ referencial
✅ reporting
✅ stage
✅ universal
```

Puedes ejecutar el script de auditoría desde la terminal:
```powershell
docker exec -i bp010-audit-db psql -U audit -d etl_data < 01_auditoria_esquemas.sql > resultados_auditoria.txt
```

Esto guardará los resultados en `resultados_auditoria.txt`.
