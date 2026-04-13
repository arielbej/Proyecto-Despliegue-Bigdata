while ($true) {
    $FECHA_HOY = Get-Date -Format "yyyy-MM-dd"
    $HORA_ACTUAL = Get-Date -Format "HHmm"
    
    $DIR_DATOS = "$PSScriptRoot/../data_temp"
    $FICHERO_LOCAL = "$DIR_DATOS/ibex_$FECHA_HOY.csv"
    
    $PYTHON_ENV = "$PSScriptRoot/../../env/Scripts/python.exe"

    if (!(Test-Path $DIR_DATOS)) {
        New-Item -ItemType Directory -Force -Path $DIR_DATOS
    }

    Write-Host "--- Comprobando estado ($HORA_ACTUAL) ---" -ForegroundColor Yellow
    Write-Host "Iniciando Scraper con Python (env)..." -ForegroundColor Cyan

    & $PYTHON_ENV -u "$PSScriptRoot/../python_scripts/scraper.py" | Out-File -FilePath $FICHERO_LOCAL -Append -Encoding utf8

    if (Test-Path $FICHERO_LOCAL) {
        Write-Host "¡Éxito! Datos guardados en $FICHERO_LOCAL" -ForegroundColor Green
        Get-Content $FICHERO_LOCAL -Tail 2
    } else {
        Write-Host "Error: No se creó el archivo de datos." -ForegroundColor Red
    }

    Write-Host "Esperando 30 segundos..."
    Start-Sleep -Seconds 30
}