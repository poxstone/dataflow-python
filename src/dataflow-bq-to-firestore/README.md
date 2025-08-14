# INSTALL

- [WordCount quickstart for Python](https://beam.apache.org/get-started/quickstart-py/)

```bash
python3 -m virtualenv .venv;
source .venv/bin/activate;
pip install -r ./src/dataflow-bq-to-firestore/requirements.txt;

# OPTIONAL
pip install --upgrade 'apache-beam[gcp]'
pip install --upgrade google-cloud-bigquery;
pip install --upgrade google-cloud-firestore;
```

# INSTALL

- [WordCount quickstart for Python](https://beam.apache.org/get-started/quickstart-py/)
- [API Reference](https://beam.apache.org/releases/pydoc/current/)
- [Notebook Basic](https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/interactive-overview/getting-started.ipynb#scrollTo=IblMlbE8_4CJ)
- [LEARN] (https://beam.apache.org/get-started/resources/learning-resources/)

```bash
python3 -m virtualenv .venv;
source .venv/bin/activate;
pip install -r ./src/beam-word-count/requirements.txt;

# OPTIONAL
pip install --upgrade 'apache-beam';
```

## TEST
- Direct
```bash
python -m apache_beam.examples.wordcount --input "./data/kinglear_part*.txt" --output "./kinglear_count.txt";
```
- Direct
```bash
python 00_basics.py
```

## RUN LOCAL - 02_word_count.py
- Local word count
```bash
python 02_word_count.py --input="./data/kinglear_part_*.txt" --output="./data_outputs/kinglear_count";
```
- GCP Cloud Storage Word Count
```bash
gsutil cp -r ./data gs://bluetab-colombia-data-qa_dataflow01/;

pip install --upgrade 'apache-beam[gcp]';

python 02_word_count.py --input="gs://bluetab-colombia-data-qa_dataflow01/data/kinglear_part_*.txt" --output="gs://bluetab-colombia-data-qa_dataflow01/data_outputs/kinglear_count";
```

## RUN GCP - dataflow-bq-to-firestore/bq_to_fs_main.py
```bash
# https://cloud.google.com/sdk/gcloud/reference/dataflow/jobs/run
JOB_NAME="df-bq-to-fs";
DATAFLOW_REGION="us-central1";
STORAGE_BUCKET="bluetab-colombia-data-qa_dataflow01";
PROJECT_ID="bluetab-colombia-data-qa";
SA="sa-dataflow-firestore-test@bluetab-colombia-data-qa.iam.gserviceaccount.com";

# Create template on cloud storage
python "bq_to_fs_main.py" \
    --runner="DataflowRunner" \
    --project="${PROJECT_ID}" \
    --region="${DATAFLOW_REGION}" \
    --template_location="gs://${STORAGE_BUCKET}/templates/bq_to_fs_main2.json" \
    --staging_location="gs://${STORAGE_BUCKET}_staging/staging" \
    --temp_location="gs://${STORAGE_BUCKET}_tmp/temp" \
    --requirements_file="requirements.txt" \
    --input_bq_table_id="${PROJECT_ID}.thelook_ecommerce.users" \
    --output_fs_project_id="${PROJECT_ID}" \
    --output_fs_db_id="fs-db-uscentral1" \
    --output_fs_collection_name="users" \
    --output_fs_id_field="id";


# Execute Job from template
gcloud dataflow jobs run "${JOB_NAME}$(date +%s)" \
    --project="${PROJECT_ID}" \
    --region="${DATAFLOW_REGION}" \
    --service-account-email="${SA}" \
    --gcs-location="gs://${STORAGE_BUCKET}/templates/bq_to_fs_main2.json" \
    --network="default" \
    --subnetwork="https://www.googleapis.com/compute/v1/projects/${PROJECT_ID}/regions/${DATAFLOW_REGION}/subnetworks/default" \
    --parameters="input_bq_table_id=${PROJECT_ID}.thelook_ecommerce.users,output_fs_project_id=${PROJECT_ID},output_fs_db_id=fs-db-uscentral1,output_fs_collection_name=users,output_fs_id_field=id";


```