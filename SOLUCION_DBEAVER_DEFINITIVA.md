# ✅ SOLUCIÓN DEFINITIVA - DBeaver 25.3.3 con PostgreSQL Puerto 5433

## EL PROBLEMA ESTÁ RESUELTO

He configurado el timezone **directamente en el servidor PostgreSQL** en UTC. Ahora puedes conectarte sin ningún parámetro especial.

---

## PASOS PARA CONECTAR EN DBEAVER 25.3.3

### 1. Descargar Driver PostgreSQL (Si te lo pide)
- Click en **"Download"** cuando DBeaver te pida descargar los drivers
- Esperar a que termine la descarga
- Click **"OK"**

### 2. Configuración Simple de Conexión

**NO uses la URL JDBC compleja**. En su lugar:

1. **Nueva Conexión** → **PostgreSQL**
2. En la pestaña **"Main"**, llenar SOLO estos campos:

```
Host: localhost
Port: 5433
Database: etl_data
Username: audit
Password: audit
☑️ Save password
```

3. **NO agregues NADA en "URL Template" ni "Properties"**
4. Click **"Test Connection"**
5. Debería conectar sin errores ✅
6. Click **"Finish"**

---

## SI PERSISTE EL ERROR DE TIMEZONE

Ejecuta este comando desde PowerShell para verificar que el timezone esté configurado:

```powershell
docker exec bp010-audit-db psql -U audit -d etl_data -c "SHOW timezone;"
```

Debería mostrar: `UTC`

---

## CONFIGURACIÓN PASO A PASO CON IMÁGENES MENTALES

**Ventana "Connect to a database":**

```
┌─────────────────────────────────────────────┐
│ PostgreSQL                                   │
├─────────────────────────────────────────────┤
│ Connection settings:                         │
│                                              │
│ Connect by: [Host ▼]                        │
│                                              │
│ Host: [localhost____________]               │
│ Port: [5433]                                 │
│ Database: [etl_data__________]              │
│                                              │
│ Authentication:                              │
│ Username: [audit_____________]              │
│ Password: [••••••]                          │
│ ☑ Save password locally                     │
│                                              │
│                                              │
│            [Test Connection...]  [Finish]   │
└─────────────────────────────────────────────┘
```

**NO VAYAS A:**
- ❌ Driver Properties
- ❌ PostgreSQL tab
- ❌ Advanced settings
- ❌ URL Template

**SOLO llena los 5 campos básicos y presiona Test Connection**

---

## DESPUÉS DE CONECTAR

Deberías ver en el árbol de DBeaver:

```
📁 PostgreSQL - etl_data
  └─ 📁 Databases
      └─ 📁 etl_data
          └─ 📁 Schemas
              ├─ 📁 referencial
              ├─ 📁 reporting
              ├─ 📁 stage
              └─ 📁 universal
```

Expandir cualquiera para ver sus tablas.

---

## SI AÚN NO FUNCIONA

1. **Verificar que Docker está corriendo:**
   ```powershell
   docker ps
   ```
   Debe aparecer `bp010-audit-db` con status `Up (healthy)`

2. **Test de conectividad Python (para confirmar que el servidor funciona):**
   ```powershell
   .\auditor\Scripts\python.exe test_db_connection.py
   ```
   Debe decir "Connection successful"

3. **Si Python conecta pero DBeaver no:**
   - Copiar el mensaje de error EXACTO de DBeaver
   - Incluir el texto completo del error

---

## CONEXIÓN VERIFICADA ✅

El servidor PostgreSQL:
- ✅ Está corriendo en puerto 5433
- ✅ Database `etl_data` existe
- ✅ Usuario `audit` con password `audit` configurado
- ✅ Timezone configurado a UTC
- ✅ Todos los schemas creados (stage, universal, referencial, reporting)

**La conexión desde Python funciona perfectamente, DBeaver debe conectar con la configuración simple.**
