from firebase_admin import credentials, firestore, initialize_app
import logging


logging.getLogger().setLevel(logging.INFO)
instance = None
is_initialized = False


def firestoreInit(project_id, database_id):
    global instance
    global is_initialized

    if not is_initialized:
        try:
            # Usa credenciales por defecto del entorno de GCP.
            cred = credentials.ApplicationDefault()
            initialize_app(cred, {'projectId': project_id})
            is_initialized = True
            logging.info("App de Firebase inicializada exitosamente.")
        except ValueError:
            # Es normal que la app ya esté inicializada en un worker.
            logging.info("La app de Firebase ya estaba inicializada.")

    # Crea el cliente de Firestore si no existe.
    if instance is None:
        instance = firestore.client(database_id=database_id)
        logging.info("Cliente de Firestore inicializado exitosamente.")

    return instance


def get_doc_from_collection(project_id, database_id, collection_name, document_id, db=None):
    try:
        if db is None:
            db = firestoreInit(project_id, database_id)

        doc_ref = db.collection(collection_name).document(document_id)
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict()
    except Exception as e:
        logging.error(f"ERROR {e}")
    return None


def update_doc(project_id, database_id, collection_name, document_id, data_to_update, db=None):
    try:
        if db is None:
            db = firestoreInit(project_id, database_id)

        doc_ref = db.collection(collection_name).document(document_id)
        doc = doc_ref.get()
        if doc.exists:
            doc_data = doc.to_dict()
            for key, value in data_to_update.items():
                doc_data[key] = value
            doc_ref.update(doc_data)

    except Exception as e:
        logging.error(f"ERROR {e}")
    return None


def del_doc(project_id, database_id, collection_name, document_id, db=None):
    try:
        if db is None:
            db = firestoreInit(project_id, database_id)

        doc_ref = db.collection(collection_name).document(document_id)
        doc = doc_ref.get()
        if doc.exists:
            return doc_ref.delete()

    except Exception as e:
        logging.error(f"ERROR {e}")
    return None


def get_all_docs_from_collection(project_id, database_id, collection_name):
    docs_list = []
    try:
        db = firestoreInit(project_id, database_id)
        collection_ref = db.collection(collection_name)
        docs = collection_ref.stream()
        for doc in docs:
            docs_list.append(doc.id)
    except Exception as e:
        print(f"Ocurrió un error al intentar obtener los documentos: {e}")
    return docs_list


#resp = get_all_docs_from_collection('pj-test-01', 'fs-db-chatbot-fact', 'fact_consolidado')
#resp = get_doc_from_collection('pj-test-01', 'fs-db-chatbot-fact', 'fact_consolidado', '000000')
#resp = update_doc('pj-test-01', 'fs-db-chatbot-fact', 'fact_consolidado', '000000', {'denom_cta_contrato':'updated'})
resp = del_doc('pj-test-01', 'fs-db-chatbot-fact', 'fact_consolidado', '000000')

logging.info(resp)
