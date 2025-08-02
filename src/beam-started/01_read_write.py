# https://github.com/apache/beam/blob/master/examples/notebooks/interactive-overview/reading-and-writing-data.ipynb
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from typing import Iterable
import codecs
import csv
from typing import Dict, Iterable, List

input_files = './data/kinglear_part*.txt'
output_file_name_prefix = './data_outputs/file'

# Create a pipeline.
with beam.Pipeline() as pipeline:
  # Feed it some input elements with `Create`.
  outputs = (
      pipeline
      # 00 opcional
      #| 'crear lineas' >> beam.Create([
      #    'Each element must be a string.',
      #    'It writes one element per line.',
      #    'There are no guarantees on the line order.',
      #    'The data might be written into multiple files.',
      #])
      # 01
      | 'leer archivos' >> beam.io.ReadFromText(input_files)
      | 'escribir archivo unico' >> beam.io.WriteToText(
          output_file_name_prefix,
          file_name_suffix='.txt')
      #| 'Print contents' >> beam.Map(print)
  )

  # Metodo para imprimir valores paralelos
  outputs | beam.Map(print)
