# Scripts

## set_composer_variable.sh

Creates or updates Composer Airflow Variable `llm_feedback_composer_config` from a JSON file.

### Required environment variables

- `COMPOSER_ENV`
- `COMPOSER_REGION`

### Optional environment variables

- `PROJECT_ID`
- `VAR_NAME` (default: `llm_feedback_composer_config`)
- `VAR_JSON_PATH` (default: `orchestration/composer/llm_feedback_composer_config.dev.json`)

### Example

```bash
COMPOSER_ENV='your-composer-env' \
COMPOSER_REGION='us-central1' \
PROJECT_ID='your-project-id' \
bash scripts/set_composer_variable.sh
```

## run_full_pipeline_dev.sh

Runs the full dev flow in sequence:

1. Generate sample training files (`CSV` + `JSON`)
2. Upload source files to raw bucket datasource path
3. Ingest local files to raw `JSONL`
4. Run Bronze Dataproc
5. Run Silver Dataproc
6. Run Gold Dataproc
7. Optional BigQuery publish + optional star schema build

### Required environment variables

- `PROJECT_ID`
- `REGION`
- `RAW_BUCKET`
- `BRONZE_BUCKET`
- `SILVER_BUCKET`
- `GOLD_BUCKET`
- `SERVICE_ACCOUNT`

### Optional environment variables

- `PUBLISH_BIGQUERY=true|false` (default: `false`)
- `BQ_PROJECT` (default: `PROJECT_ID`)
- `BQ_DATASET` (default: `gold`)
- `BUILD_STAR_SCHEMA=true|false` (default: `false`)
- `INGEST_DATE_UTC` (default: current UTC date)

### Example

```bash
PROJECT_ID='liquid-layout-413121' \
REGION='us-central1' \
RAW_BUCKET='liquid-layout-413121-llmfb-raw-dev' \
BRONZE_BUCKET='liquid-layout-413121-llmfb-bronze-dev' \
SILVER_BUCKET='liquid-layout-413121-llmfb-silver-dev' \
GOLD_BUCKET='liquid-layout-413121-llmfb-gold-dev' \
SERVICE_ACCOUNT='438895410098-compute@developer.gserviceaccount.com' \
PUBLISH_BIGQUERY='true' \
BQ_PROJECT='liquid-layout-413121' \
BQ_DATASET='gold' \
BUILD_STAR_SCHEMA='true' \
bash scripts/run_full_pipeline_dev.sh
```

## smoke_test_dev.sh

Validates the dev setup end-to-end using values from `conf/dev.yaml`.

### Prerequisites

- `gcloud`, `bq`, and Python 3
- Signed in: `gcloud auth login`
- Active project set: `gcloud config set project <PROJECT_ID>`

### Run

```bash
bash scripts/smoke_test_dev.sh
```

### What it checks

- Prints active gcloud account
- Uploads a tiny JSONL file to `gs://<raw_bucket>/smoke/`
- Lists and reads the uploaded object
- Ensures `ops.smoke_test` exists
- Inserts one row and queries the last 5 rows

### Output

Ends with `PASS` on success or `FAIL` on error.
