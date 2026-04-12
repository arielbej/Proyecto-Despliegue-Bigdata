"""
MRJob: Recuperar el valor mínimo y máximo de cotización de una acción
       para la última hora, semana y mes.

Uso:
    python min_max_accion.py <ficheros_csv> --accion=<TICKER> [--periodo=<hora|semana|mes>]

Ejemplo:
    python min_max_accion.py data_temp/*.csv --accion=SAN --periodo=semana

Formato esperado del CSV (sin cabecera o con cabecera):
    ticker,fecha,hora,apertura,cierre,minimo,maximo,volumen
    SAN,2026-04-11,10:30,3.45,3.50,3.40,3.55,100000
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from datetime import datetime, timedelta


# ── Fecha/hora de referencia (NOW) ─────────────────────────────────────────
NOW = datetime(2026, 4, 11, 11, 0, 0)   # Ajustar al momento real de ejecución

LIMITE_HORA   = NOW - timedelta(hours=1)
LIMITE_SEMANA = NOW - timedelta(weeks=1)
LIMITE_MES    = NOW - timedelta(days=30)


def parsear_datetime(fecha_str, hora_str):
    """Convierte 'YYYY-MM-DD' y 'HH:MM' en un objeto datetime."""
    try:
        return datetime.strptime(f"{fecha_str} {hora_str}", "%Y-%m-%d %H:%M")
    except ValueError:
        return None


def determinar_periodos(dt):
    """Devuelve los períodos a los que pertenece un datetime dado."""
    periodos = []
    if dt >= LIMITE_HORA:
        periodos.append("hora")
    if dt >= LIMITE_SEMANA:
        periodos.append("semana")
    if dt >= LIMITE_MES:
        periodos.append("mes")
    return periodos


class MinMaxAccion(MRJob):
    """
    MapReduce con dos pasos:
      1. MAP  → emite (accion, periodo) → (minimo, maximo) por cada fila válida
         REDUCE paso 1 → agrega min/max parciales por (accion, periodo)
      2. MAP  → reenvía los resultados
         REDUCE paso 2 → combina y emite el resultado final formateado
    """

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg(
            "--accion",
            required=True,
            help="Ticker de la acción a consultar (ej: SAN, IBE, TEF)"
        )
        self.add_passthru_arg(
            "--periodo",
            default="todos",
            choices=["hora", "semana", "mes", "todos"],
            help="Período a consultar: hora, semana, mes o todos (default: todos)"
        )

    # ── PASO 1: MAPPER ──────────────────────────────────────────────────────
    def mapper_filtrar(self, _, line):
        """
        Lee cada línea del CSV, filtra por acción y período,
        y emite (accion|periodo) → (minimo, maximo).
        """
        line = line.strip()
        if not line or line.lower().startswith("ticker"):
            return  # salta cabecera o líneas vacías

        try:
            partes = list(csv.reader([line]))[0]
            # Columnas esperadas: ticker, fecha, hora, apertura, cierre, minimo, maximo, volumen
            if len(partes) < 7:
                return

            ticker  = partes[0].strip().upper()
            fecha   = partes[1].strip()
            hora    = partes[2].strip()
            minimo  = float(partes[5])
            maximo  = float(partes[6])

        except (ValueError, IndexError):
            return  # fila malformada, ignorar

        # Filtrar por la acción solicitada
        accion_buscada = self.options.accion.upper()
        if ticker != accion_buscada:
            return

        dt = parsear_datetime(fecha, hora)
        if dt is None:
            return

        periodos = determinar_periodos(dt)
        periodo_filtro = self.options.periodo

        for periodo in periodos:
            if periodo_filtro == "todos" or periodo == periodo_filtro:
                # Clave compuesta: accion|periodo
                clave = f"{ticker}|{periodo}"
                yield clave, (minimo, maximo)

    # ── PASO 1: COMBINER (optimización local) ───────────────────────────────
    def combiner_minmax(self, clave, valores):
        """Agrega min/max localmente en cada mapper antes de enviar al reducer."""
        min_local = float("inf")
        max_local = float("-inf")
        for minimo, maximo in valores:
            if minimo < min_local:
                min_local = minimo
            if maximo > max_local:
                max_local = maximo
        yield clave, (min_local, max_local)

    # ── PASO 1: REDUCER ─────────────────────────────────────────────────────
    def reducer_minmax(self, clave, valores):
        """Calcula el mínimo y máximo global para cada (accion, periodo)."""
        min_global = float("inf")
        max_global = float("-inf")
        for minimo, maximo in valores:
            if minimo < min_global:
                min_global = minimo
            if maximo > max_global:
                max_global = maximo

        if min_global == float("inf"):
            return  # no había datos

        ticker, periodo = clave.split("|")
        yield ticker, (periodo, min_global, max_global)

    # ── PASO 2: REDUCER FINAL ───────────────────────────────────────────────
    def reducer_formato_final(self, ticker, valores):
        """
        Agrupa los resultados de todos los períodos para una acción
        y los emite en un formato legible.
        """
        # Orden preferido de presentación
        orden = {"hora": 0, "semana": 1, "mes": 2}
        resultados = sorted(valores, key=lambda x: orden.get(x[0], 99))

        salida = {}
        for periodo, minimo, maximo in resultados:
            salida[periodo] = {"min": round(minimo, 4), "max": round(maximo, 4)}

        if not salida:
            return

        yield ticker, salida

    # ── DEFINICIÓN DE PASOS ─────────────────────────────────────────────────
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_filtrar,
                combiner=self.combiner_minmax,
                reducer=self.reducer_minmax,
            ),
            MRStep(
                reducer=self.reducer_formato_final,
            ),
        ]


if __name__ == "__main__":
    MinMaxAccion.run()