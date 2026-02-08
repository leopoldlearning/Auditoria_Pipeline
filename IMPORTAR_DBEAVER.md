# Cómo Importar la Conexión en DBeaver

## Método 1: Importar el archivo XML (Recomendado)

1. Abrir DBeaver
2. Ir a **Database** → **Driver Manager**
3. Asegurarse de que el driver PostgreSQL esté instalado
4. Ir a **File** → **Import** → **Database Connections**
5. Seleccionar el archivo `dbeaver_connection.xml`
6. Click en **Finish**

La conexión "BP010 Audit - localhost:5433" aparecerá en el panel izquierdo.

## Método 2: Crear manualmente (Alternativo)

Si el método de importación no funciona, crear manualmente:

1. Click derecho en "Database Navigator" (panel izquierdo)
2. **Create** → **Connection**
3. Seleccionar **PostgreSQL**
4. Configurar:
   - **Host**: `localhost`
   - **Port**: `5433`
   - **Database**: `etl_data`
   - **Username**: `audit`
   - **Password**: `audit`
5. **Test Connection** → **Finish**

## Verificar la Conexión

Una vez conectado, deberías ver estos esquemas:
- `reporting`
- `stage`
- `referencial`
- `universal`

Puedes ejecutar el script `01_auditoria_esquemas.sql` para validar las tablas.
