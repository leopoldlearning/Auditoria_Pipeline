# Guía de Conexión DBeaver - Auditoría BP010

## Configuración de Conexión

### Paso 1: Abrir DBeaver y crear nueva conexión
1. Abrir DBeaver
2. Click en **Database** → **New Database Connection**
3. Seleccionar **PostgreSQL**
4. Click **Next**

### Paso 2: Configurar los parámetros de conexión

**Connection Settings:**
- **Host**: `localhost`
- **Port**: `5433`
- **Database**: `etl_data`
- **Username**: `audit`
- **Password**: `audit`

### Paso 3: Probar y guardar
1. Click en **Test Connection**
2. Si la conexión es exitosa, dar click en **Finish**

## Esquemas a Validar

### 1. Schema: `stage`
Tablas principales:
- `landing_scada_data`
- `tbl_pozo_maestra`
- `tbl_pozo_reservas`
- `tbl_pozo_produccion`

### 2. Schema: `referencial`
Tablas principales:
- `tbl_maestra_variables`
- `tbl_dq_rules`
- `tbl_reglas_consistencia`
- `tbl_limites_pozo`

### 3. Schema: `reporting`
Tablas principales:
- `dim_pozo`
- `fact_operaciones_diarias`
- `fact_operaciones_horarias`
- `dataset_current_values`

## Consultas de Validación

### Verificar esquemas creados
```sql
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
ORDER BY schema_name;
```

### Contar tablas por esquema
```sql
SELECT 
    schemaname AS schema,
    COUNT(*) AS num_tables
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
GROUP BY schemaname
ORDER BY schemaname;
```

### Verificar datos en referencial
```sql
SELECT COUNT(*) FROM referencial.tbl_maestra_variables;
SELECT COUNT(*) FROM referencial.tbl_dq_rules;
```

### Verificar datos en stage
```sql
SELECT COUNT(*) FROM stage.tbl_pozo_maestra;
SELECT COUNT(*) FROM stage.tbl_pozo_reservas;
```

### Verificar datos en reporting
```sql
SELECT * FROM reporting.dim_pozo LIMIT 5;
SELECT * FROM reporting.fact_operaciones_diarias LIMIT 5;
```

## Notas Importantes

⚠️ **Puerto**: Este entorno de auditoría usa el puerto **5433**, no el 5432 por defecto.

✅ **Credenciales**: Usuario y contraseña son ambos `audit` para facilitar la auditoría local.

🔍 **Validación**: Después de cada fase del pipeline, verificar en DBeaver que las tablas y datos se crearon correctamente.
