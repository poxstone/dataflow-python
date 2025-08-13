import json
import argparse
import logging
import random
import datetime # Importar el módulo datetime
import apache_beam as beam
from apache_beam.runners.runner import PipelineResult
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam import pvalue
from firebase_admin import credentials, firestore, initialize_app

# --- Opciones de la Canalización ---
# No se necesitan cambios aquí, pero se mantiene para la estructura.
class BqToFsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input_bq_table_id',
            dest='input_bq_table_id',
            required=True,
            help='ID de la tabla de BigQuery para leer, ej: proyecto.dataset.tabla.'
        )
        parser.add_argument(
            '--output_fs_collection_name',
            dest='output_fs_collection_name',
            required=True,
            help='Nombre de la colección de Firestore de salida.'
        )
        parser.add_argument(
            '--output_fs_id_field',
            dest='output_fs_id_field',
            help='Campo en la fila a usar como ID del documento de Firestore.'
        )
        parser.add_argument(
            '--output_fs_project_id',
            dest='output_fs_project_id',
            help='ID del proyecto de GCP para Firestore (opcional, por defecto el del job).'
        )
        parser.add_argument(
            '--output_fs_db_id',
            dest='output_fs_db_id',
            default='(default)', # Firestore usa '(default)' como ID de la base de datos por defecto
            help='ID de la base de datos de Firestore (opcional).'
        )

# --- Singleton para el Cliente de Firestore ---
# Se mantiene la lógica del Singleton para una inicialización eficiente.
class FirestoreClientSingleton:
    _instance = None
    _is_initialized = False

    @staticmethod
    def get_client(project_id=None, database_id=None):
        # La inicialización de la app de Firebase se maneja una vez por worker.
        if not FirestoreClientSingleton._is_initialized:
            try:
                # Usa credenciales por defecto del entorno de GCP.
                cred = credentials.ApplicationDefault()
                initialize_app(cred, {'projectId': project_id})
                FirestoreClientSingleton._is_initialized = True
                logging.info("App de Firebase inicializada exitosamente.")
            except ValueError:
                # Es normal que la app ya esté inicializada en un worker.
                logging.info("La app de Firebase ya estaba inicializada.")
        
        # Crea el cliente de Firestore si no existe.
        if FirestoreClientSingleton._instance is None:
            FirestoreClientSingleton._instance = firestore.client(database_id=database_id)
            logging.info("Cliente de Firestore inicializado exitosamente.")
        
        return FirestoreClientSingleton._instance

# --- DoFn para Escribir en Firestore ---
class WriteToFirestoreDoFn(beam.DoFn):
    # Límite de escrituras por lote en Firestore.
    _MAX_BATCH_SIZE = 499 
    # Constantes para la lógica de reintentos
    _MAX_RETRIES = 5
    _INITIAL_BACKOFF_SECONDS = 1

    def __init__(self, collection_name, id_field=None, project_id=None, database_id=None):
        self.collection_name = collection_name
        self.id_field = id_field
        self.project_id = project_id
        self.database_id = database_id
        # Estas variables se inicializarán por worker en start_bundle.
        self.client = None
        self.batch = None
        self.batch_size = 0
    
    @staticmethod
    def _json_serializer(obj):
        """Serializador de JSON para objetos no serializables por defecto, como datetime."""
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        raise TypeError(f"El tipo {type(obj)} no es serializable en JSON")

    def start_bundle(self):
        """Inicializa el cliente y el lote al inicio de cada paquete de trabajo."""
        self.client = FirestoreClientSingleton.get_client(self.project_id, self.database_id)
        self.batch = self.client.batch()
        self.batch_size = 0

    def process(self, row):
        """Procesa cada elemento y lo añade al lote de escritura."""
        try:
            # Convierte la fila a un diccionario. `dict()` funciona
            # tanto para objetos beam.Row como para diccionarios ya existentes.
            row_dict = dict(row)
            collection_ref = self.client.collection(self.collection_name)
            
            # Determina el ID del documento. Si no se especifica, Firestore lo genera.
            doc_id = str(row_dict.get(self.id_field)) if self.id_field else None
            
            if doc_id:
                doc_ref = collection_ref.document(doc_id)
            else:
                doc_ref = collection_ref.document()

            # Firestore maneja objetos datetime de Python directamente, no es necesario convertirlos aquí.
            self.batch.set(doc_ref, row_dict)
            self.batch_size += 1
            
            # Envía el lote cuando alcanza el tamaño máximo.
            if self.batch_size >= self._MAX_BATCH_SIZE:
                self._commit_batch()

            # Emite la fila procesada a la salida de éxito para logging.
            yield pvalue.TaggedOutput('success', json.dumps(row_dict, default=self._json_serializer))
            
        except Exception as e:
            logging.error(f"Error procesando la fila: {row}. Error: {e}")
            # Emite la fila y el error a la salida de fallo para logging.
            yield pvalue.TaggedOutput('failed', json.dumps({'row': dict(row), 'error': str(e)}, default=self._json_serializer))

    def finish_bundle(self):
        """
        Asegura que cualquier registro restante en el lote se escriba en Firestore.
        Este es un paso CRÍTICO para evitar la pérdida de datos.
        """
        if self.batch_size > 0:
            self._commit_batch()

    def _commit_batch(self):
        """Envía el lote actual con reintentos y backoff exponencial."""
        for attempt in range(self._MAX_RETRIES):
            try:
                self.batch.commit()
                logging.info(f"Lote de {self.batch_size} documentos enviado exitosamente a Firestore.")
                break  # Si tiene éxito, sale del bucle de reintentos
            except Exception as e:
                logging.warning(f"Intento {attempt + 1} de {self._MAX_RETRIES} falló al enviar el lote: {e}")
                if attempt + 1 == self._MAX_RETRIES:
                    logging.error(f"No se pudo enviar el lote después de {self._MAX_RETRIES} intentos. Se descartan {self.batch_size} registros.")
                    break # Se rinde después del último intento

                # Calcula el tiempo de espera con backoff exponencial y un factor aleatorio (jitter)
                backoff_time = self._INITIAL_BACKOFF_SECONDS * (2 ** attempt) + random.uniform(0, 1)
                logging.info(f"Esperando {backoff_time:.2f} segundos antes de reintentar.")
                time.sleep(backoff_time)
            finally:
                # Reinicia el lote para el siguiente conjunto de escrituras, incluso si falló.
                self.batch = self.client.batch()
                self.batch_size = 0

# --- Función Principal de la Canalización ---
def run(argv=None):
    parser = argparse.ArgumentParser()
    # El parser de argparse no es necesario aquí, Beam lo maneja internamente.
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    bq_fs_options = pipeline_options.view_as(BqToFsOptions)
    
    # Guardar la sesión principal es crucial para que los workers tengan el contexto.
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Lee los datos desde BigQuery.
        rows_from_bq = pipeline | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
            #table=bq_fs_options.input_bq_table_id,
            query=f"SELECT * FROM `{bq_fs_options.input_bq_table_id}` LIMIT 10", # Descomentar para pruebas
            use_standard_sql=True
        )

        # Escribe en Firestore usando ParDo y maneja salidas de éxito y fallo.
        write_results = rows_from_bq | 'WriteToFirestore' >> beam.ParDo(
            WriteToFirestoreDoFn(
                collection_name=bq_fs_options.output_fs_collection_name,
                id_field=bq_fs_options.output_fs_id_field,
                project_id=bq_fs_options.output_fs_project_id,
                database_id=bq_fs_options.output_fs_db_id
            )
        ).with_outputs('success', 'failed')
        
        # --- Logging de Resultados ---
        # Usa una función lambda para formatear correctamente el mensaje de log.
        (write_results.success 
         | 'Log successful writes' >> beam.Map(lambda x: logging.info(f"Escritura exitosa:: {x}")))
        
        (write_results.failed 
         | 'Log failed writes' >> beam.Map(lambda x: logging.error(f"Fallo de escritura:: {x}")))
    
    # El pipeline se ejecuta al salir del bloque 'with'. No es necesario pipeline.run().
    logging.info("El pipeline de Dataflow ha sido construido y se está ejecutando.")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
