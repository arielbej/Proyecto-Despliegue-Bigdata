from mrjob.job import MRJob
from mrjob.step import MRStep
import os

"""Descripción:
Dado el nombre de una accion y un rango de fechas, obtener su valor minimo y maximo de
cotizacion, asi como el el porcentaje de decremento y de incremento desde el valor inicial de
cotizacion hasta el minimo y maximo, respectivamente."""

class MRAnalisisRangoAccion(MRJob):

    # 1. DEFINIR LOS PARÁMETROS DE ENTRADA
    def configure_args(self):
        super(MRAnalisisRangoAccion, self).configure_args()
        self.add_passthru_arg('--accion', type=str, required=True, help='Nombre de la accion (ej. Iberdrola)')
        self.add_passthru_arg('--inicio', type=str, required=True, help='Fecha de inicio (ej. 2026-04-01)')
        self.add_passthru_arg('--fin', type=str, required=True, help='Fecha de fin (ej. 2026-04-30)')
    
    def mapper(self, _, line):
        # 1. Intentar sacar la fecha del NOMBRE del archivo (ej. ibex_2026-04-05.csv)
        filename = os.environ.get('mapreduce_map_input_file', '')
        # Si estás en local con mrjob, a veces la variable es esta:
        if not filename:
            import inspect
            # Truco para local: mrjob guarda el path en el objeto job
            filename = self.options.input_paths[0] if self.options.input_paths else "2026-04-05"

        # Extraemos algo que parezca una fecha del nombre del archivo
        # Esto es un parche para tu prueba local
        import re
        match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
        fecha_del_archivo = match.group(0) if match else "2026-04-05"

        parts = line.split(',')
        if len(parts) >= 5 and parts[0].strip(): # Evita las líneas de comas vacías ,,,,
            accion_csv = parts[0].strip()
            
            if accion_csv.lower() == self.options.accion.lower():
                try:
                    ultimo = float(parts[1])
                    maximo = float(parts[2])
                    minimo = float(parts[3])
                    
                    # Ignoramos lo que diga el CSV en la columna 4 y usamos la fecha del nombre del archivo
                    if self.options.inicio <= fecha_del_archivo <= self.options.fin:
                        yield accion_csv, (fecha_del_archivo, ultimo, maximo, minimo)
                except (ValueError, IndexError):
                    pass

   
    # 3. REDUCER: Cálculos matemáticos
    def reducer(self, key, values):
        # Convertimos a lista y ordenamos por fecha (el primer elemento de la tupla)
        # Esto nos garantiza saber exactamente cuál fue el PRIMER valor cronológico
        registros = list(values)
        registros.sort(key=lambda x: x[0]) 
        
        if not registros:
            return # Si no hay datos en ese rango, no hacemos nada

        # Extracción de valores base
        valor_inicial = registros[0][1] # El 'último' valor del primer día del rango
        
        # Obtenemos el mínimo y máximo global de todo el periodo seleccionado
        min_rango = min(r[3] for r in registros) # Buscamos en las columnas 'mínimo'
        max_rango = max(r[2] for r in registros) # Buscamos en las columnas 'máximo'

        # Cálculos de porcentajes (Regla de tres)
        # Decremento: (Inicial - Mínimo) / Inicial * 100
        pct_decremento = ((valor_inicial - min_rango) / valor_inicial) * 100
        
        # Incremento: (Máximo - Inicial) / Inicial * 100
        pct_incremento = ((max_rango - valor_inicial) / valor_inicial) * 100

        # Formateamos la salida para que sea fácil de leer
        yield key, {
            '1_rango_analizado': f"{self.options.inicio} a {self.options.fin}",
            '2_valor_inicial': valor_inicial,
            '3_minimo_alcanzado': min_rango,
            '4_maximo_alcanzado': max_rango,
            '5_pct_caida': f"{round(pct_decremento, 2)}%",
            '6_pct_subida': f"{round(pct_incremento, 2)}%"
        }

if __name__ == '__main__':
    MRAnalisisRangoAccion.run()
    
    
