from apache_beam.options.pipeline_options import PipelineOptions

input_file = 'gs://bluetab-colombia-data-qa_dataflow01/shakespeare/kinglear.txt'
output_path = 'gs://bluetab-colombia-data-qa_dataflow01/shakespeare_counts/kinglear.txt'

beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='bluetab-colombia-data-qa',
    job_name='df-count-01',
    temp_location='gs://bluetab-colombia-data-qa_dataflow01/shakespeare_counts_temp/',
)