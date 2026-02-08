# GUÍA DEFINITIVA - DBeaver 25.3.3 Conexión PostgreSQL Puerto 5433

## ✅ ANTES DE EMPEZAR - Verificar que el servidor está corriendo

Ejecutar en terminal:
```powershell
docker ps
```
Debes ver `bp010-audit-db` con status `Up`.

---

## PASO 1: Nueva Conexión en DBeaver 25.3.3

1. **Abrir DBeaver 25.3.3**
2. Click en el ícono **"Nueva Conexión"** (plug icon) en la barra de herramientas
   - O: Menu **Database** → **New Database Connection**
3. En el diálogo que aparece:
   - Buscar **"PostgreSQL"** en el campo de búsqueda
   - Seleccionar **"PostgreSQL"**
   - Click **"Next"** (Siguiente)

---

## PASO 2: Configuración de Conexión (Pestaña "Main")

Completar los campos EXACTAMENTE así:

```
Connect by: Host
Host: localhost
Port: 5433
Database: etl_data
```

**Authentication:**
```
Username: audit
Password: audit
☑️ Save password locally
```

**⚠️ MUY IMPORTANTE:**
- El puerto es **5433** NO 5432
- Database es **etl_data** (sin mayúsculas)

---

## PASO 3: Configurar PostgreSQL Driver

1. En la misma ventana, ir a la pestaña **"PostgreSQL"** (a la derecha de "Main")
2. Buscar el campo **"Show all databases"**: 
   - ☑️ Marcarlo para ver todos los schemas

---

## PASO 4: Driver Properties - SOLUCIÓN AL ERROR DE TIMEZONE

Esta es la parte CRÍTICA para DBeaver 25.3.3:

1. En la ventana de conexión, click en **"Edit Driver Settings"** (link en la parte inferior)
2. En la nueva ventana que se abre:
   - Ir a la pestaña **"Connection properties"**
   - Click en **"+"** para agregar una nueva propiedad
   - **Name**: `timezone`
   - **Value**: `UTC`
   - Click **"OK"**

### ALTERNATIVA MÁS RÁPIDA (Si no quieres tocar Driver Settings):

1. En la ventana principal de conexión, buscar la sección **"Advanced"** o **"Advanced settings"**
2. Buscar algo como **"Connection properties"** o **"URL Properties"**
3. Agregar: `timezone=UTC`

### ALTERNATIVA 2 - Editar JDBC URL Directamente:

1. Click en **"Edit Driver Settings"**
2. En la pestaña **"URL Template"**, modificar la URL para que incluya el parámetro:
   ```
   jdbc:postgresql://{host}:{port}/{database}?timezone=UTC
   ```

---

## PASO 5: Test Connection

1. Click en el botón **"Test Connection..."** en la parte inferior
2. **SI SALE ERROR**, copiar EXACTAMENTE el mensaje de error
3. **SI DICE "Connected"**: ✅ ¡Éxito! → Click "Finish"

---

## ERRORES COMUNES Y SOLUCIONES

### Error: "Connection refused"
- **Causa**: Docker no está corriendo o puerto incorrecto
- **Solución**: Verificar `docker ps` y confirmar puerto 5433

### Error: "invalid value for parameter TimeZone"
- **Causa**: Timezone no está configurado
- **Solución**: Agregar `timezone=UTC` en Driver Properties (Paso 4)

### Error: "password authentication failed"
- **Causa**: Usuario/password incorrecto
- **Solución**: Verificar user=`audit` password=`audit`

### Error: "database does not exist"
- **Causa**: Database name incorrecto
- **Solución**: Debe ser exactamente `etl_data` (minúsculas)

---

## VERIFICACIÓN POST-CONEXIÓN

Una vez conectado exitosamente:

1. Expandir la conexión en el árbol
2. Deberías ver: `etl_data` → `Schemas` → 4 schemas:
   - ✅ `referencial`
   - ✅ `reporting`  
   - ✅ `stage`
   - ✅ `universal`

3. Expandir cualquier schema para ver sus tablas

---

## SI NADA FUNCIONA - DEBUGGING

Ejecutar este comando en PowerShell para verificar conectividad:

```powershell
.\auditor\Scripts\python.exe test_db_connection.py
```

Si Python se conecta pero DBeaver no, el problema es SOLO de configuración de DBeaver.

---

## CONFIGURACIÓN RÁPIDA CON CAPTURA DE PANTALLA

Si puedes compartir una captura de:
1. La ventana de "New Connection" de DBeaver
2. El mensaje de error exacto (si lo hay)

Puedo darte una solución más precisa.

---

## ÚLTIMA ALTERNATIVA - Usar DBeaver Connection String

En lugar de llenar campos individualmente:

1. En DBeaver, crear conexión PostgreSQL
2. Ir a **Driver properties** o **Advanced**
3. Pegar esta URL completa:
   ```
   jdbc:postgresql://localhost:5433/etl_data?user=audit&password=audit&timezone=UTC
   ```
