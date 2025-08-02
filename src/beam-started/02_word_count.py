#https://beam.apache.org/get-started/wordcount-example/
# pytype: skip-file
import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult


class WordExtractingDoFn(beam.DoFn):
    def process(self, element):
        return re.findall(r'[\w\']+', element, re.UNICODE)


def format_result(word, count):
    return '%s: %d' % (word, count)


def run(argv=None, save_main_session=True) -> PipelineResult:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='./data/kinglear_part_*.txt',
        required=True,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        default='./data_outputs/kinglear_count',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline = beam.Pipeline(options=pipeline_options)
  
    # INIT: Read the text file[pattern] into a PCollection.
    lines = pipeline | 'Lee *.txt' >> ReadFromText(known_args.input)
    counts = (
        lines
        | 'Divide texto en palabras' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))  # chair chair the
        | 'une las palabras con 1' >> beam.Map(lambda x: (x, 1))  # ('chair', 1) ('chair', 1)...
        | 'Agrupar y sumar' >> beam.CombinePerKey(sum)  # ('chair', 2)...
    )
    output = counts | 'Formatea en tuplas (function)' >> beam.MapTuple(format_result)  # chair: 2
    outfile = output | 'Escribe archivo de salida .txt' >> WriteToText(known_args.output, file_name_suffix='.txt')  # ./data_outputs/kinglear_count-00000-of-00001.txt
    outfile | beam.Map(print)
  
    # Execute the pipeline and return the result.
    result = pipeline.run()
    result.wait_until_finish()
    return result


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

# python 02_word_count.py --input="./data/kinglear_part_*.txt" --output="./data_outputs/kinglear_count"