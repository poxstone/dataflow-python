# Dataflow


## Variables
```bash
DATAFLOW_REGION="us-central1";
STORAGE_BUCKET="tribal-octane-342812";
PROJECT_ID="tribal-octane-342812";
JOB_NAME="wordcounttemplate";
```

## Run local
```bash
```

## Run in Dataflow
```bash
python -m wordcount2template \
    --runner DataflowRunner \
    --project $PROJECT_ID \
    --region $DATAFLOW_REGION \
    --temp_location gs://$STORAGE_BUCKET/tmp/ \
    --input gs://dataflow-samples/shakespeare/kinglear.txt \
    --output gs://$STORAGE_BUCKET/results/outputs;
```


## Build Template
[Doc](https://cloud.google.com/dataflow/docs/guides/templates/creating-templates)

```bash
python -m wordcount2template \
    --runner DataflowRunner \
    --project $PROJECT_ID \
    --region $DATAFLOW_REGION \
    --template_location gs://$STORAGE_BUCKET/templates/wordcounttemplate \
    --temp_location gs://$STORAGE_BUCKET/temp \
    --staging_location gs://$STORAGE_BUCKET/staging \
    --input gs://dataflow-samples/shakespeare/kinglear.txt \
    --output gs://$STORAGE_BUCKET/results/outputs;
```

## Run from template
```bash
gcloud dataflow jobs run "${JOB_NAME}$(date +%s)" \
    --project $PROJECT_ID \
    --region $DATAFLOW_REGION \
    --gcs-location gs://$STORAGE_BUCKET/templates/$JOB_NAME \
#    --parameters input=gs://dataflow-samples/shakespeare/kinglear.txt,output=gs://$STORAGE_BUCKET/results/outputs;
```
