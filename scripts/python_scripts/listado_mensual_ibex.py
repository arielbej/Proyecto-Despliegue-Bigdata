from mrjob.job import MRJob

class MRListadoMensualIBEX(MRJob):

    def mapper(self, _, line):
        # Formato esperado: Acciona,231.60,232.00,226.40,02/04
        parts = line.split(',')
        if len(parts) == 5:
            accion = parts[0]
            try:
                ultimo = float(parts[1])
                maximo = float(parts[2])
                minimo = float(parts[3])
                # No necesitamos la fecha para los cálculos si el fichero está ordenado,
                # pero la podrías usar para asegurar el orden cronológico.
                yield accion, (ultimo, maximo, minimo)
            except ValueError:
                # Si hay cabeceras o errores de formato, los saltamos
                pass

    def reducer(self, key, values):
        lista_valores = list(values)
        # El valor inicial es el primer 'ultimo' que se registró en el mes
        valor_inicial = lista_valores[0][0]
        
        # El valor final es el último 'ultimo' registrado en el mes
        valor_final = lista_valores[-1][0]
        
        # El máximo global es el mayor de todos los máximos del mes
        max_mensual = max(v[1] for v in lista_valores)
        
        # El mínimo global es el menor de todos los mínimos del mes
        min_mensual = min(v[2] for v in lista_valores)
        
        yield key, {
            'valor_inicial': valor_inicial,
            'valor_final': valor_final,
            'maximo_mensual': max_mensual,
            'minimo_mensual': min_mensual
        }

if __name__ == '__main__':
    MRListadoMensualIBEX.run()