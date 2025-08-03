import json
import argparse
import logging
import os
import apache_beam as beam
from apache_beam.runners.runner import PipelineResult
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import PipelineOptions
#from google.cloud import firestore
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


output_file = './data_outputs/output.jsonl'


class BqToFsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input_bq_table_id',
            dest='input_bq_table_id',
            default='bluetab-colombia-data-qa.thelook_ecommerce.users',
            required=True,
            help='Bigquery load io read from table')
        parser.add_argument(
            '--output_fs_project_id',
            dest='output_fs_project_id',
            default='bluetab-colombia-data-qa',
            required=True,
            help='Output Firestore GCP Project ID.')
        parser.add_argument(
            '--output_fs_db_id',
            dest='output_fs_db_id',
            default='fs-db-uscentral1',
            required=True,
            help='Output Firestore Database ID.')
        parser.add_argument(
            '--output_fs_collection_name',
            dest='output_fs_collection_name',
            default='users',
            required=True,
            help='Output Firestore Colletion Name.')
        parser.add_argument(
            '--output_fs_id_field',
            dest='output_fs_id_field',
            default='id',
            required=True,
            help='Output Firestore id document Name.')


class WriteToFirestoreDoFn(beam.DoFn):
    def __init__(self, project_id, database_id, collection_name, id_field=None):
        self.project_id = project_id
        self.database_id = database_id
        self.collection_name = collection_name
        self.id_field = id_field
        self.client = None
        self.cred = None

    def setup(self):
        self.cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(self.cred, {'projectId': self.project_id})
        self.client = firestore.client(database_id=self.database_id)
        print("Cliente de Firestore inicializado.")

    def process(self, row):
        collection_ref = self.client.collection(self.collection_name)
        # auto create dictionary id if note defined
        if self.id_field and self.id_field in row:
            doc_id = str(row[self.id_field])
            doc_ref = collection_ref.document(doc_id)
            doc_ref.set(row)
        else:
            collection_ref.add(row)

def format_to_jsonl(row):
    return json.dumps(row)


def run(argv=None) -> PipelineResult:
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    known_args = pipeline_options.view_as(BqToFsOptions)
    pipeline = beam.Pipeline(options=pipeline_options)

    # INIT: Read the text file[pattern] into a PCollection.
    lines = pipeline | 'Traer datos de Bigquery (SQL)' >> beam.io.ReadFromBigQuery(
                    query=f"""
                    SELECT * FROM `{known_args.input_bq_table_id}` LIMIT 10
                    """, use_standard_sql=True)
    #lines = lines_s | 'covierte json a string double_quotes' >> beam.Map(json.dumps)  # comillas sencillas a dobles
    #lines | beam.Map(print)
    #json_lines = lines | 'ConvertToJSONL' >> beam.Map(format_to_jsonl)
    #file = json_lines | 'WriteToJSONL' >> beam.io.WriteToText(output_file, file_name_suffix='.jsonl')
    lines | 'Ecribir en Firestore' >> beam.ParDo(WriteToFirestoreDoFn(
        project_id=known_args.output_fs_project_id,
        database_id=known_args.output_fs_db_id,
        collection_name=known_args.output_fs_collection_name,
        id_field=known_args.output_fs_id_field
        ))
    
    # Execute the pipeline and return the result.
    lines | beam.Map(print)
    result = pipeline.run()
    result.wait_until_finish()
    return result


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()