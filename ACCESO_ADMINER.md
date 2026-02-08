# Guía de Acceso a Adminer (Interfaz Gráfica)

Para visualizar la base de datos de auditoría de forma gráfica sin errores de TimeZone:

## 1. Acceso
Abre tu navegador y ve a:
👉 [http://localhost:8080](http://localhost:8080)

## 2. Datos de Login
- **Sistema**: PostgreSQL
- **Servidor**: `postgres-audit` (Es el nombre del contenedor interno)
- **Usuario**: `audit`
- **Contraseña**: `audit`
- **Base de datos**: `etl_data`

## 3. Cómo Visualizar las Tablas (IMPORTANTE)
Adminer muestra un esquema a la vez. Por defecto podrías entrar en `public` (que está vacío).

**Para ver las tablas de auditoría:**
1. En la columna de la izquierda, busca el desplegable o enlace que dice **"Esquema"** (Schema).
2. Selecciona el esquema que quieres auditar:
   - 📂 **stage**: Tablas de ingesta inicial.
   - 📂 **referencial**: Reglas de negocio y maestros.
   - 📂 **reporting**: Tablas finales para el dashboard.
   - 📂 **universal**: Modelos matemáticos.
3. Una vez seleccionado el esquema, aparecerá la lista de tablas debajo (ej: `tbl_pozo_maestra`).

## 4. Ver Contenido
1. Haz click en el nombre de la tabla (ej: `tbl_pozo_maestra`).
2. En el menú superior de la tabla, haz click en **"Seleccionar datos"** (Select data).
3. ¡Listo! Verás los registros. (Nota: Al inicio algunas tablas pueden estar vacías hasta que ejecutemos los Notebooks de la Fase 3).

## 4. Solución a Problemas
Si no carga la página:
1. Ejecuta `docker ps` para confirmar que `bp010-audit-ui` está corriendo.
2. Si no aparece, ejecuta `docker compose up -d`.
