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
