#!/bin/bash

while true; do
    FECHA_HOY=$(date +"%Y-%m-%d")
    HORA_ACTUAL=$(date +"%H%M")

    # Carpeta de datos (un nivel arriba)
    DIR_DATOS="$(dirname "$0")/../data_temp"
    FICHERO_LOCAL="$DIR_DATOS/ibex_$FECHA_HOY.csv"

    # Python del entorno virtual
    PYTHON_ENV="$(dirname "$0")/../../env/bin/python"

    # Crear carpeta si no existe
    if [ ! -d "$DIR_DATOS" ]; then
        mkdir -p "$DIR_DATOS"
    fi

    echo "--- Comprobando estado ($HORA_ACTUAL) ---"
    echo "Iniciando Scraper con Python (env)..."

    # Ejecutar scraper y guardar salida
    "$PYTHON_ENV" -u "$(dirname "$0")/../python_scripts/scraper.py" >> "$FICHERO_LOCAL"

    if [ -f "$FICHERO_LOCAL" ]; then
        echo "¡Éxito! Datos guardados en $FICHERO_LOCAL"
        tail -n 2 "$FICHERO_LOCAL"
    else
        echo "Error: No se creó el archivo de datos."
    fi

    echo "Esperando 30 segundos..."
    sleep 30
done