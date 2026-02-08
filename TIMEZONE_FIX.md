# ✅ Solución al Error de Timezone

## Problema Resuelto
El error `FATAL: invalid value for parameter "TimeZone": "America/Buenos_Aires"` ha sido corregido.

## Cambios Realizados

### 1. Docker Container (docker-compose.yml)
- Agregadas variables de entorno `TZ: UTC` y `PGTZ: UTC`
- Contenedor reiniciado con la nueva configuración

### 2. Configuración de DBeaver (dbeaver_connection.xml)
- Agregada propiedad `timezone: UTC` en la conexión
- Este archivo está listo para importar en DBeaver

## Pasos para Conectar Ahora

1. **Reiniciar DBeaver** si está abierto
2. **Importar la conexión actualizada:**
   - File → Import → Database Connections
   - Seleccionar `dbeaver_connection.xml`
   - Click Finish

3. **O crear manualmente con estos datos:**
   - Host: `localhost`
   - Port: `5433`
   - Database: `etl_data`
   - Username: `audit`
   - Password: `audit`
   - En "Connection settings" → "Connection properties": agregar `timezone=UTC`

La conexión debería funcionar sin problemas ahora. Los esquemas ya están creados y listos para ser explorados.
