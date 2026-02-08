# Configuración Manual de DBeaver - Paso a Paso

## Paso 1: Crear Nueva Conexión
1. Abrir DBeaver
2. Click derecho en "Database Navigator" (panel izquierdo)
3. Seleccionar **"Create"** → **"Connection"**
4. Buscar y seleccionar **"PostgreSQL"**
5. Click **"Next"**

## Paso 2: Configuración Principal
En la pestaña **"Main"**:
- **Host**: `localhost`
- **Port**: `5433`  ⚠️ **IMPORTANTE: 5433, NO 5432**
- **Database**: `etl_data`
- **Authentication**: Database Native
- **Username**: `audit`
- **Password**: `audit`
- ☑️ Marcar **"Save password"**

## Paso 3: Configurar Timezone (CRÍTICO)
1. En la misma ventana, buscar la pestaña **"Driver properties"** o **"Connection details"**
2. Click en **"Driver properties"**
3. Buscar en la lista o agregar una nueva propiedad:
   - **Name**: `timezone`
   - **Value**: `UTC`

### Alternativa si no encuentras Driver properties:
1. Ir a la pestaña **"Connection details"**
2. En el campo **"JDBC URL"**, asegurarte de que diga:
   ```
   jdbc:postgresql://localhost:5433/etl_data?timezone=UTC
   ```

## Paso 4: Probar Conexión
1. Click en **"Test Connection..."** (botón en la parte inferior)
2. Deberías ver: **"Connected"** ✅
3. Si sale error, verificar:
   - Puerto es 5433
   - Docker está corriendo: `docker ps`
   - Timezone está configurado como UTC

## Paso 5: Finalizar
1. Click **"Finish"**
2. La conexión "PostgreSQL - etl_data" aparecerá en el panel izquierdo

## Verificación Post-Conexión
Una vez conectado, expande la conexión y verifica:
- **Databases** → **etl_data** → **Schemas**
- Deberías ver:
  - ✅ `referencial`
  - ✅ `reporting`
  - ✅ `stage`
  - ✅ `universal`

## Ejecutar Script de Validación
1. Click derecho en la conexión → **"SQL Editor"** → **"New SQL script"**
2. Abrir el archivo `01_auditoria_esquemas.sql`
3. Ejecutar para validar las tablas

## Si Persiste el Error de Timezone
Ejecutar este comando en DBeaver:
```sql
SHOW timezone;
```
Debería mostrar `UTC`.

Si no, agregar al inicio de tus scripts:
```sql
SET timezone = 'UTC';
```
