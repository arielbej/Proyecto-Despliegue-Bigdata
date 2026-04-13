
# Proyecto-Despliegue-Bigdata

Este proyecto procesa datos del IBEX 35 utilizando **Hadoop** y **MapReduce** (mrjob). Se puede ejecutar de forma local para pruebas rápidas o en el clúster Docker para procesamiento distribuido.

## Requisitos Previos
* Docker
* Python 3.12
* Las librerias necesrias las puedes encontrar en src/requirements.txt o directamente instalarlas ejecutando ```pip install -r requirements.txt``` con el env activado.

## Estructura del proyecto
Proyecto-Despliegue-Bigdata/
├── Data/                        # Archivos CSV origen (SAN.csv, ITX.csv, etc.)
├── src/                         # Código fuente principal
│   ├── env/                     # Entorno virtual de Python
│   ├── scripts/                 # Scripts de ejecución
│   │   ├── python_scripts/      # Lógica MapReduce (mrjob)
│   │   │   ├── data_test/       # Datos para probar funcionamiento 
│   │   │   ├── filtro_ibex.py
│   │   │   ├── listado_mensual_ibex.py
│   │   │   ├── listado_semanal_ibex.py
│   │   │   ├── min_max_accion.py
│   │   │   ├── scraper.py
│   │   │   ├── top_baja_ibex.py
│   │   │   └── top_subida_ibex.py
│   │   └── shell_scripts/      
│   │       ├── run_scraper.ps1  # Script para Windows PowerShell
│   │       └── run_scraper.sh   # Script para entornos Linux/Docker
│   ├── .env                     # Variables de entorno
│   └── requirements.txt         # Dependencias de Python
├── compose-hadoop-cluster-...   # Configuración de Docker Compose
└── README.md                    # Documentación del proyecto

## 📂 Preparación y Rutas del Cluster( Docker)
Los archivos residen en el contenedor `namenode-mr` bajo la ruta `/home/luser`.


### 1. Copia de archivos al contenedor
Desde la terminal de tu máquina local, mueve los datos y scripts al volumen del contenedor:
```powershell
docker cp Data namenode-mr:/home/luser/
docker cp scripts namenode-mr:/home/luser/
```

### 2. Configuración del Entorno (en el clúster)
Accede al contenedor como el usuario `luser` y configura el entorno de Python:
```powershell
docker exec -it namenode-mr /bin/bash
```

**Dentro del contenedor:**
```bash
# Crear y activar el entorno virtual
python3 -m venv env
source env/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

### 3. Carga de datos a HDFS
Para que el clúster pueda trabajar con los archivos, estos deben estar en el sistema de archivos distribuido:
```bash
hdfs dfs -mkdir -p /user/luser/datos_ibex
hdfs dfs -put -f ~/Data/*.csv /user/luser/datos_ibex/
```

---

## Guía de Ejecución de Scripts

Para todos los scripts, asegúrate de estar dentro del entorno virtual (`source env/bin/activate`) y en la ruta de los scripts:
`cd ~/scripts/python_scripts`

### 1. Filtrado por Acción y Fecha (`filtro_ibex.py`)
Filtra los registros de una empresa específica en un rango de fechas determinado.

* **Hadoop:**
    ```bash
    python3 filtro_ibex.py -r hadoop --hadoop-tmp-dir hdfs:///tmp/mrjob_luser hdfs:///user/luser/datos_ibex/ --accion="Inditex" --inicio="2026-04-05" --fin="2026-04-11"
    ```
* **Local:**
    ```bash
    python3 filtro_ibex.py ~/Data/*.csv --accion="Inditex" --inicio="2026-04-05" --fin="2026-04-11"
    ```

### 2. Máximos y Mínimos  (`min_max_accion.py`)
Calcula los valores máximos y mínimos  de cada acción.

* **Hadoop:**
    ```bash
    python3 min_max_accion.py -r hadoop --hadoop-tmp-dir hdfs:///tmp/mrjob_luser hdfs:///user/luser/datos_ibex/
    ```
* **Local:**
    ```bash
    python3 min_max_accion.py ~/Data/*.csv
    ```

### 3. Listados  (`listado_mensual_ibex.py` / `listado_semanal_ibex.py`)
Agrupa los datos para obtener resúmenes por periodos de tiempo.

* **Hadoop (Mensual):**
    ```bash
    python3 listado_mensual_ibex.py -r hadoop --hadoop-tmp-dir hdfs:///tmp/mrjob_luser hdfs:///user/luser/datos_ibex/
    ```
* **Local (Semanal):**
    ```bash
    python3 listado_semanal_ibex.py ~/Data/*.csv
    ```

### 4. Ranking de Variaciones (`top_subida_ibex.py` / `top_baja_ibex.py`)
Identifica las acciones que más han subido o bajado en el dataset.

* **Hadoop (Top Subidas):**
    ```bash
    python3 top_subida_ibex.py -r hadoop --hadoop-tmp-dir hdfs:///tmp/mrjob_luser hdfs:///user/luser/datos_ibex/
    ```
* **Local (Top Bajas):**
    ```bash
    python3 top_baja_ibex.py ~/Data/*.csv
    ```

---
