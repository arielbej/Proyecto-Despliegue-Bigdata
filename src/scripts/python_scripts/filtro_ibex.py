from mrjob.job import MRJob
import re
import os

class MR_filtro_ibex(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg('--accion', required=True)
        self.add_passthru_arg('--inicio', required=True)
        self.add_passthru_arg('--fin', required=True)

    def mapper(self, _, line):
        # Sacar fecha del nombre del archivo
        filename = os.environ.get('mapreduce_map_input_file', '')
        match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
        fecha = match.group(0) if match else None

        acciones = line.split(',')
        if len(acciones) < 4 or not fecha:
            return

        nombre = acciones[0].strip()

        if nombre.lower() != self.options.accion.lower():
            return

        try:
            ultimo = float(acciones[1])
            maximo = float(acciones[2])
            minimo = float(acciones[3])
        except:
            return

        # Filtrar por rango de fechas
        if self.options.inicio <= fecha <= self.options.fin:
            yield nombre, (fecha, ultimo, maximo, minimo)

    def reducer(self, key, values):
        datos = sorted(values, key=lambda x: x[0])

        if not datos:
            return

        valor_inicial = datos[0][1]
        min_rango = min(x[3] for x in datos)
        max_rango = max(x[2] for x in datos)

        pct_caida = ((valor_inicial - min_rango) / valor_inicial) * 100
        pct_subida = ((max_rango - valor_inicial) / valor_inicial) * 100

        yield key, {
            "inicial": valor_inicial,
            "min": min_rango,
            "max": max_rango,
            "caida_%": round(pct_caida, 2),
            "subida_%": round(pct_subida, 2)
        }


if __name__ == "__main__":
    MR_filtro_ibex.run()