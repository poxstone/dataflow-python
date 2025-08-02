# https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/interactive-overview/getting-started.ipynb#scrollTo=PoRd7hlnoOu5
import apache_beam as beam

inputs = [0, 1, 2, 3, 4, 5]

# Create a pipeline.
with beam.Pipeline() as pipeline:
  # Feed it some input elements with `Create`.
  outputs = (
      pipeline
      | 'crea valores iniciales en pcollection' >> beam.Create(inputs)  # 0 1 2 3 4 5
      | 'expande cada Valor inicial en varios valores mas' >> beam.FlatMap(lambda x: [x for i in range(x)])  # 1 2 2 3 3 3 4 4 4 4 5 5 5 5 5
      | 'hace una operacion con expandidos' >> beam.Map(lambda y: y*y)  # 1 4 4 9 9 9 16 16 16 16 25 25 25 25 25
      | 'filtra solo valore pares' >> beam.Filter(lambda x: x % 2 == 0)  # 4 4 16 16 16 16
      | 'suma todos los valores resultantes' >> beam.CombineGlobally(sum)  # 72
      # preparar para grupo
      | 'crear tuplas' >> beam.FlatMap(lambda x: [(f'num_{x}', x) for i in range(0, x)])  # ("num_72", 72) ('num_72', 72) ..
      | 'agrupar por llave' >> beam.GroupByKey()
  )

  # Metodo para imprimir valores paralelos
  outputs | beam.Map(print)
