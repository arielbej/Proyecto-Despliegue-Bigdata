while ($true) {
    $FECHA_HOY = Get-Date -Format "yyyy-MM-dd"
    $HORA_ACTUAL = Get-Date -Format "HHmm"
    
    # 1. Definimos la carpeta de datos (un nivel arriba de donde está el .ps1)
    $DIR_DATOS = "$PSScriptRoot/../data_temp"
    $FICHERO_LOCAL = "$DIR_DATOS/ibex_$FECHA_HOY.csv"
    
    # 2. Definimos la ruta al Python de tu enviroment (env)
    $PYTHON_ENV = "$PSScriptRoot/../../env/Scripts/python.exe"

    # 3. CREAR LA CARPETA SI NO EXISTE (Esto evita el error que tienes ahora)
    if (!(Test-Path $DIR_DATOS)) {
        New-Item -ItemType Directory -Force -Path $DIR_DATOS
    }

    Write-Host "--- Comprobando estado ($HORA_ACTUAL) ---" -ForegroundColor Yellow
    Write-Host "Iniciando Scraper con Python (env)..." -ForegroundColor Cyan

    # 4. Ejecutamos usando el Python del entorno virtual
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