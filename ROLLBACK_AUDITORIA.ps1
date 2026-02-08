# ============================================================================
# SCRIPT DE ROLLBACK PARA AUDITORÍA BP010-DATA-PIPELINES
# ============================================================================
# Creado: 2026-02-07 16:15:00
# Propósito: Restaurar el repositorio de auditoría al estado inicial
# ============================================================================

param(
    [switch]$Force = $false
)

$ErrorActionPreference = "Stop"

# Configuración
$REPO_AUDITORIA = "D:\ITMeet\Operaciones\BP010-data-pipelines-auditoria"
$BACKUP_DIR = "D:\ITMeet\Operaciones\BP010-data-pipelines-auditoria-BACKUPS"
$TIMESTAMP = "2026-02-07_161500"
$BACKUP_NAME = "BACKUP_INICIAL_${TIMESTAMP}.zip"
$BACKUP_PATH = Join-Path $BACKUP_DIR $BACKUP_NAME

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "ROLLBACK DE AUDITORÍA BP010" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Verificar que existe el backup
if (-not (Test-Path $BACKUP_PATH)) {
    Write-Host "ERROR: No se encontró el backup en: $BACKUP_PATH" -ForegroundColor Red
    Write-Host "Backups disponibles:" -ForegroundColor Yellow
    Get-ChildItem -Path $BACKUP_DIR -Filter "BACKUP_*.zip" | ForEach-Object {
        Write-Host "  - $($_.Name)" -ForegroundColor Yellow
    }
    exit 1
}

# Confirmación
if (-not $Force) {
    Write-Host "Backup encontrado: $BACKUP_NAME" -ForegroundColor Green
    Write-Host "Tamaño: $((Get-Item $BACKUP_PATH).Length / 1MB) MB" -ForegroundColor Green
    Write-Host ""
    Write-Host "ADVERTENCIA: Esta operación eliminará todos los cambios realizados" -ForegroundColor Yellow
    Write-Host "desde el momento del backup." -ForegroundColor Yellow
    Write-Host ""
    $confirmation = Read-Host "¿Desea continuar con el rollback? (SI/NO)"
    
    if ($confirmation -ne "SI") {
        Write-Host "Rollback cancelado por el usuario." -ForegroundColor Yellow
        exit 0
    }
}

Write-Host ""
Write-Host "Iniciando rollback..." -ForegroundColor Cyan

# Paso 1: Crear backup del estado actual (por seguridad)
$CURRENT_TIMESTAMP = Get-Date -Format "yyyyMMdd_HHmmss"
$CURRENT_BACKUP = Join-Path $BACKUP_DIR "BACKUP_PRE_ROLLBACK_$CURRENT_TIMESTAMP.zip"
Write-Host "[1/4] Creando backup del estado actual..." -ForegroundColor Yellow

try {
    Compress-Archive -Path $REPO_AUDITORIA -DestinationPath $CURRENT_BACKUP -Force
    Write-Host "  ✓ Backup actual guardado en: $CURRENT_BACKUP" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Error al crear backup del estado actual: $_" -ForegroundColor Red
    exit 1
}

# Paso 2: Eliminar contenido actual del repositorio
Write-Host "[2/4] Limpiando repositorio de auditoría..." -ForegroundColor Yellow

try {
    Get-ChildItem -Path $REPO_AUDITORIA -Exclude "ROLLBACK_AUDITORIA.ps1" | Remove-Item -Recurse -Force
    Write-Host "  ✓ Contenido eliminado" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Error al limpiar repositorio: $_" -ForegroundColor Red
    exit 1
}

# Paso 3: Restaurar desde backup
Write-Host "[3/4] Restaurando desde backup inicial..." -ForegroundColor Yellow

try {
    Expand-Archive -Path $BACKUP_PATH -DestinationPath $REPO_AUDITORIA -Force
    Write-Host "  ✓ Restauración completada" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Error al restaurar backup: $_" -ForegroundColor Red
    Write-Host "  INTENTO DE RECUPERACIÓN: Restaurando desde backup actual..." -ForegroundColor Yellow
    Expand-Archive -Path $CURRENT_BACKUP -DestinationPath $REPO_AUDITORIA -Force
    exit 1
}

# Paso 4: Verificación
Write-Host "[4/4] Verificando restauración..." -ForegroundColor Yellow

$expectedDirs = @("src", "inputs_referencial", "data", "scripts")
$allOk = $true

foreach ($dir in $expectedDirs) {
    $path = Join-Path $REPO_AUDITORIA $dir
    if (Test-Path $path) {
        Write-Host "  ✓ Directorio encontrado: $dir" -ForegroundColor Green
    } else {
        Write-Host "  ✗ Directorio NO encontrado: $dir" -ForegroundColor Red
        $allOk = $false
    }
}

Write-Host ""
if ($allOk) {
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "ROLLBACK COMPLETADO EXITOSAMENTE" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "El repositorio ha sido restaurado al estado inicial." -ForegroundColor Green
    Write-Host "Backup del estado previo guardado en:" -ForegroundColor Cyan
    Write-Host "  $CURRENT_BACKUP" -ForegroundColor Cyan
} else {
    Write-Host "========================================" -ForegroundColor Red
    Write-Host "ROLLBACK COMPLETADO CON ADVERTENCIAS" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
    Write-Host "Por favor, revise manualmente el repositorio." -ForegroundColor Yellow
}
