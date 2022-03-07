import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


from unidecode import unidecode
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from google.cloud.bigtable import row


class WordcountOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      # Use add_value_provider_argument for arguments to be templatable
      # Use add_argument as usual for non-templatable arguments
      parser.add_value_provider_argument(
          '--input',
          default='gs://dataflow-samples/shakespeare/kinglear.txt',
          help='Path of the file to read from')
      parser.add_argument(
          '--output',
          #required=True,
          help='Output file to write results to.')


class WordExtractingDoFn(beam.DoFn):
  def process(self, element):
    return re.findall(r'[\w\']+', element, re.UNICODE)


def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  options = PipelineOptions(pipeline_args, save_main_session=True)
  args = options.view_as(WordcountOptions)

  with beam.Pipeline(options=options) as p:
    lines = p | 'Read' >> ReadFromText(args.input)
    counts = (
        lines
        | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum))

    def format_result(word, count):
      return '%s: %d' % (word, count)

    output = counts | 'Format' >> beam.MapTuple(format_result)
    output | 'Write' >> WriteToText(args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
