from mrjob.job import MRJob
from datetime import datetime

class MRListadoMensualIBEX(MRJob):
    """Descripcion
    Generar un listado mensual (del mes actual) donde se indique, para cada acci´on, su
valor inicial, final, m´ınimo y m´aximo.
    """

    def mapper(self, _, line):
        parts = line.split(',')

        if len(parts) != 5:
            return

        accion = parts[0]
        fecha_str = parts[4].strip()

        try:
            ultimo = float(parts[1])
            maximo = float(parts[2])
            minimo = float(parts[3])

            # Convertir fecha (dd/mm)
            fecha = datetime.strptime(fecha_str, "%d/%m")

            # Mes actual
            hoy = datetime.now()

            if fecha.month == hoy.month:
                yield accion, (fecha.day, ultimo, maximo, minimo)

        except:
            pass

    def reducer(self, key, values):
        # Ordenamos por día
        datos = sorted(values, key=lambda x: x[0])

        if not datos:
            return

        valor_inicial = datos[0][1]
        valor_final = datos[-1][1]

        max_mensual = max(v[2] for v in datos)
        min_mensual = min(v[3] for v in datos)

        yield key, {
            'valor_inicial': valor_inicial,
            'valor_final': valor_final,
            'maximo_mensual': max_mensual,
            'minimo_mensual': min_mensual
        }


if __name__ == '__main__':
    MRListadoMensualIBEX.run()