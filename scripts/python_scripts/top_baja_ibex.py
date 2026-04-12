from mrjob.job import MRJob
from datetime import datetime

class TopBajadasPro(MRJob):

    def mapper(self, _, line):
        try:
            partes = line.split(",")

            if len(partes) < 5:
                return

            nombre = partes[0]
            apertura = float(partes[1])
            cierre = float(partes[2])
            fecha_str = partes[4].strip()

            # Convertir fecha (dd/mm)
            fecha = datetime.strptime(fecha_str, "%d/%m")

            # Fecha actual (año ficticio)
            hoy = datetime.now().replace(year=1900)

            diferencia = (hoy - fecha).days

            bajada = cierre - apertura  # será negativa si baja

            # Últimos 30 días → mes
            if 0 <= diferencia <= 30:
                yield "mes", (bajada, nombre)

            # Últimos 7 días → semana
            if 0 <= diferencia <= 7:
                yield "semana", (bajada, nombre)

        except:
            pass

    def reducer(self, key, values):
        lista = list(values)

        # 🔴 ORDEN ASCENDENTE → más negativo primero
        lista.sort()

        # Top 5 bajadas (más negativas)
        for bajada, nombre in lista[:5]:
            yield f"{key} - {nombre}", bajada


if __name__ == "__main__":
    TopBajadasPro.run()