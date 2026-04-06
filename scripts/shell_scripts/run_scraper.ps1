while ($true) {
    $FECHA_HOY = Get-Date -Format "yyyy-MM-dd"
    $HORA_ACTUAL = Get-Date -Format "HHmm"
    $FICHERO_LOCAL = "$PSScriptRoot/data_temp/ibex_$FECHA_HOY.csv"

    Write-Host "--- Comprobando estado ($HORA_ACTUAL) ---" -ForegroundColor Yellow

    # PRUEBA FORZADA: Quitamos los IF de tiempo para testear
    Write-Host "Iniciando Scraper de Python..." -ForegroundColor Cyan
    
    # Ejecutamos python. Si hay error, lo veremos en pantalla.
    & python -u "$PSScriptRoot/scraper.py" | Out-File -FilePath $FICHERO_LOCAL -Append -Encoding utf8
    
    if (Test-Path $FICHERO_LOCAL) {
        Write-Host "¡Éxito! Datos guardados en $FICHERO_LOCAL" -ForegroundColor Green
        # Leemos las últimas 2 líneas para verificar
        Get-Content $FICHERO_LOCAL -Tail 2
    } else {
        Write-Host "Error: No se creó el archivo de datos." -ForegroundColor Red
    }

    Write-Host "Esperando 30 segundos para la siguiente prueba..."
    Start-Sleep -Seconds 30
}