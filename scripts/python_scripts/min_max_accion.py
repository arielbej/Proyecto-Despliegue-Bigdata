from mrjob.job import MRJob
from datetime import datetime

class MinMaxAccion(MRJob):

    def configure_args(self):
        super(MinMaxAccion, self).configure_args()
        self.add_passthru_arg('--accion', type=str, help='Nombre de la accion')

    def mapper(self, _, line):
        try:
            partes = line.split(",")

            if len(partes) < 5:
                return

            nombre = partes[0]

            # Filtrar solo la acción que queremos
            if nombre != self.options.accion:
                return

            maximo = float(partes[2])
            minimo = float(partes[3])
            tiempo = partes[4].strip()

            ahora = datetime.now()

            # Caso 1: es hora (ej: 10:30)
            if ":" in tiempo:
                hora = datetime.strptime(tiempo, "%H:%M").replace(
                    year=ahora.year, month=ahora.month, day=ahora.day
                )

                diferencia = (ahora - hora).seconds / 3600  # horas

                if diferencia <= 1:
                    yield "hora", (minimo, maximo)

            # Caso 2: es fecha (ej: 02/04)
            else:
                fecha = datetime.strptime(tiempo, "%d/%m").replace(year=ahora.year)

                diferencia = (ahora - fecha).days

                if diferencia <= 30:
                    yield "mes", (minimo, maximo)

                if diferencia <= 7:
                    yield "semana", (minimo, maximo)

        except:
            pass

    def reducer(self, key, values):
        min_total = float("inf")
        max_total = float("-inf")

        for minimo, maximo in values:
            if minimo < min_total:
                min_total = minimo
            if maximo > max_total:
                max_total = maximo

        yield key, (min_total, max_total)


if __name__ == "__main__":
    MinMaxAccion.run()