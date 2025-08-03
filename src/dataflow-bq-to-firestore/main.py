import json
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import firestore


pipeline_options = PipelineOptions(
    runner='DirectRunner',  # 'DirectRunner' para ejecutar localmente. Usa 'DataflowRunner' para GCP.
    project='bluetab-colombia-data-qa',
    temp_location='gs://bluetab-colombia-data-qa_dataflow01/staging', # Requerido si usas DataflowRunner
)

table_spec = 'bluetab-colombia-data-qa.thelook_ecommerce.distribution_centers'
output_file = './data_outputs/output.jsonl'
firestore_project_id = 'bluetab-colombia-data-qa'
firestore_database_id = 'fs-db-uscentral1'
firestore_collection_name = 'distribution_centers'
document_id_field = 'documento_prueba'
bigquery_query = """
    SELECT * FROM `bluetab-colombia-data-qa.thelook_ecommerce.users` LIMIT 10
"""

class WriteToFirestoreDoFn(beam.DoFn):
    def __init__(self, project_id, database_id, collection_name, id_field=None):
        self.project_id = project_id
        self.database_id = database_id
        self.collection_name = collection_name
        self.id_field = id_field
        self.client = None

    def setup(self):
        if self.database_id:
            os.environ['FIRESTORE_DATABASE'] = self.database_id
        self.client = firestore.Client(project=self.project_id)
        print("Cliente de Firestore inicializado.")

    def process(self, row):
        collection_ref = self.client.collection(self.collection_name)
        
        # Si se especifica un campo para el ID, úsalo.
        if self.id_field and self.id_field in row:
            doc_id = str(row[self.id_field])
            doc_ref = collection_ref.document(doc_id)
            doc_ref.set(row)
        else:
            # De lo contrario, deja que Firestore genere el ID automáticamente
            collection_ref.add(row)

def format_to_jsonl(row):
    return json.dumps(row)

# Construir el pipeline
with beam.Pipeline(options=pipeline_options) as p:
    lines_s = p | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(query=bigquery_query, use_standard_sql=True)
    #lines = lines_s | 'covierte json a string double_quotes' >> beam.Map(json.dumps)  # comillas sencillas a dobles
    #lines | beam.Map(print)
    #json_lines = lines | 'ConvertToJSONL' >> beam.Map(format_to_jsonl)
    #file = json_lines | 'WriteToJSONL' >> beam.io.WriteToText(output_file, file_name_suffix='.jsonl')
    firestore = lines_s | 'WriteToFirestore' >> beam.ParDo(
        WriteToFirestoreDoFn(
            firestore_project_id,
            firestore_database_id,
            firestore_collection_name,
            id_field='id'
        )
    )
