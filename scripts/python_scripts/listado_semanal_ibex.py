from mrjob.job import MRJob

class MRListadoSemanalIBEX(MRJob):

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
        
        # El valor inicial es el primer 'ultimo' que se registró en la semana
        valor_inicial = lista_valores[0][0]
        
        # El valor final es el último 'ultimo' registrado en la semana
        valor_final = lista_valores[-1][0]
        
        # El máximo global es el mayor de todos los máximos de la semana
        max_semanal = max(v[1] for v in lista_valores)
        
        # El mínimo global es el menor de todos los mínimos de la semana
        min_semanal = min(v[2] for v in lista_valores)
        
        yield key, {
            'valor_inicial': valor_inicial,
            'valor_final': valor_final,
            'maximo_semanal': max_semanal,
            'minimo_semanal': min_semanal
        }

if __name__ == '__main__':
    MRListadoSemanalIBEX.run()