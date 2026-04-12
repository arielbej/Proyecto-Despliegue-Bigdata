from mrjob.job import MRJob
from datetime import datetime, timedelta

class TopSubidasPro(MRJob):

    def mapper(self, _, line):
        try:
            partes = line.split(",")

            if len(partes) < 5:
                return

            nombre = partes[0]
            apertura = float(partes[1])
            cierre = float(partes[2])
            fecha_str = partes[4].strip()

            # Convertimos fecha (formato: dd/mm)
            fecha = datetime.strptime(fecha_str, "%d/%m")

            # Fecha actual (solo día/mes, año ficticio)
            hoy = datetime.now().replace(year=1900)

            # Diferencia de días
            diferencia = (hoy - fecha).days

            subida = cierre - apertura

            # Últimos 30 días → mes
            if 0 <= diferencia <= 30:
                yield "mes", (subida, nombre)

            # Últimos 7 días → semana
            if 0 <= diferencia <= 7:
                yield "semana", (subida, nombre)

        except:
            pass

    def reducer(self, key, values):
        lista = list(values)

        # Ordenar por subida (descendente)
        lista.sort(reverse=True)

        # Top 5
        for subida, nombre in lista[:5]:
            yield f"{key} - {nombre}", subida


if __name__ == "__main__":
    TopSubidasPro.run()