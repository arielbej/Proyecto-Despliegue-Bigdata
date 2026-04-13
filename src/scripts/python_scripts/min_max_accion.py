from mrjob.job import MRJob
from datetime import datetime

class MinMaxAccion(MRJob):
    """Descripción:
    Dado el nombre de una acci´on, recupera su valor m´ınimo y m´aximo de cotizaci´on de la
    ´ultima hora, semana y mes.
    """

    def configure_args(self):
        super(MinMaxAccion, self).configure_args()
        self.add_passthru_arg('--accion', type=str, help='Nombre de la accion')

    def mapper(self, _, line):
        try:
            acciones = line.split(",")

            if len(acciones) < 5:
                return

            nombre = acciones[0]

            # Filtra solo la acción que queremos
            if nombre != self.options.accion:
                return

            maximo = float(acciones[2])
            minimo = float(acciones[3])
            tiempo = acciones[4].strip()

            ahora = datetime.now()

            # Caso 1: Hora 
            if ":" in tiempo:
                hora = datetime.strptime(tiempo, "%H:%M").replace(
                    year=ahora.year, month=ahora.month, day=ahora.day
                )

                diferencia = (ahora - hora).seconds / 3600  # horas

                if diferencia <= 1:
                    yield "hora", (minimo, maximo)

            # Caso 2: Fecha
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