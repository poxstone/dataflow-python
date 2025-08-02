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

## RUN GCP - 02_word_count_template.py
```bash
# https://cloud.google.com/sdk/gcloud/reference/dataflow/jobs/run
JOB_NAME="df-wcount";
DATAFLOW_REGION="us-central1";
STORAGE_BUCKET="bluetab-colombia-data-qa_dataflow01";
PROJECT_ID="bluetab-colombia-data-qa";
SA="sa-dataflow-firestore-test@bluetab-colombia-data-qa.iam.gserviceaccount.com";

# Create template on cloud storage
python -m "02_word_count_template" \
    --runner="DataflowRunner" \
    --project="${PROJECT_ID}" \
    --region="${DATAFLOW_REGION}" \
    --template_location="gs://${STORAGE_BUCKET}/templates/02_word_count_template" \
    --temp_location="gs://${STORAGE_BUCKET}/temp" \
    --staging_location="gs://${STORAGE_BUCKET}/staging" \
    --input="gs://${STORAGE_BUCKET}/data/kinglear_part_*.txt" \
    --output="gs://${STORAGE_BUCKET}/data_outputs/kinglear_count";


# Execute Job from template
gcloud dataflow jobs run "${JOB_NAME}$(date +%s)" \
    --project="${PROJECT_ID}" \
    --region="${DATAFLOW_REGION}" \
    --gcs-location="gs://${STORAGE_BUCKET}/templates/02_word_count_template" \
    --service-account-email="${SA}" \
    --network="default" \
    --subnetwork="https://www.googleapis.com/compute/v1/projects/${PROJECT_ID}/regions/${DATAFLOW_REGION}/subnetworks/default" \
    --parameters="input=gs://${STORAGE_BUCKET}/data/kinglear_part_*.txt,output=gs://${STORAGE_BUCKET}/data_outputs/kinglear_count";
```