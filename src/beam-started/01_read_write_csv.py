import json
import codecs
import csv
from datetime import datetime, timezone
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.io.filesystems import FileSystems as beam_fs
from apache_beam.options.pipeline_options import PipelineOptions
from typing import Dict, Iterable, List


@beam.ptransform_fn
@beam.typehints.with_input_types(beam.pvalue.PBegin)
@beam.typehints.with_output_types(Dict[str, str])
def ReadCsvFiles(pbegin: beam.pvalue.PBegin, file_patterns: List[str]) -> beam.PCollection[Dict[str, str]]:
  def expand_pattern(pattern: str) -> Iterable[str]:
    for match_result in beam_fs.match([pattern])[0].metadata_list:
      yield match_result.path

  def read_csv_lines(file_name: str) -> Iterable[Dict[str, str]]:
    with beam_fs.open(file_name) as f:
      for row in csv.DictReader(codecs.iterdecode(f, 'utf-8')):
        yield dict(row)

  return (
      pbegin
      | 'Create file patterns' >> beam.Create(file_patterns)
      | 'Expand file patterns' >> beam.FlatMap(expand_pattern)
      | 'Read CSV lines' >> beam.FlatMap(read_csv_lines)
  )


def function_mod_dict(row):
  row['id'] = '00' + row['id']
  row['fecha'] = str(datetime.now(timezone.utc))
  return row


input_patterns = ['data/user_part_*.csv']
output_file_name_prefix = './data_outputs/file'
options = PipelineOptions(flags=[], type_check_additional='all')
with beam.Pipeline(options=options) as pipeline:
  (
      pipeline
      | 'leer csv' >> ReadCsvFiles(input_patterns)
      | 'covierte json a string double_quotes' >> beam.Map(json.dumps)  # comillas sencillas a dobles
      | 'modifica caracteresd del string' >> beam.Map(lambda y: f'{y}'.replace('first_name', 'primer_nombre'))
      | 'convierte a dict para manipular' >> beam.Map(json.loads)
      | 'funcion manipular dict' >> beam.Map(function_mod_dict)
      | 'filtra solo ids pares' >> beam.Filter(lambda x: int(x['id']) % 2 == 0)
      | 'lambda mod y add col' >> beam.Map(lambda row: {
          **row,
          'primer_nombre': row['primer_nombre'].upper(),
          'fecha_segunda': str(datetime.now(timezone.utc))
        })
      | 'covierte json 2' >> beam.Map(json.dumps)  # comillas sencillas a dobles
      | 'escribir archivo json' >> beam.io.WriteToText(output_file_name_prefix, file_name_suffix='.jsonl')
      | 'Print elements' >> beam.Map(print)
  )