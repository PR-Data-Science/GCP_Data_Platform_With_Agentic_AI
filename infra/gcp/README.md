# Dev bootstrap

## Prerequisites

- Google Cloud SDK (`gcloud`), `gsutil`, and `bq`
- Signed in with `gcloud auth login`
- Permissions to enable APIs, create buckets, and create BigQuery datasets

## Run

```bash
export PROJECT_ID="liquid-layout-413121"
export REGION="<YOUR_REGION>"
export BQ_LOCATION="<YOUR_BQ_LOCATION>"

bash infra/gcp/bootstrap_dev.sh
```

## What it does

- Enables APIs: Storage, BigQuery, IAM, Secret Manager
- Creates dev buckets (raw/bronze/silver/gold/deadletter)
- Creates BigQuery datasets (bronze/silver/gold/ops)

## Verification

```bash
gcloud services list --enabled | grep -E "storage|bigquery|iam|secretmanager"

gsutil ls -b "gs://${PROJECT_ID}-llmfb-raw-dev"

gsutil ls -b "gs://${PROJECT_ID}-llmfb-bronze-dev"

gsutil ls -b "gs://${PROJECT_ID}-llmfb-silver-dev"

gsutil ls -b "gs://${PROJECT_ID}-llmfb-gold-dev"

gsutil ls -b "gs://${PROJECT_ID}-llmfb-deadletter-dev"

bq show "${PROJECT_ID}:bronze"

bq show "${PROJECT_ID}:silver"

bq show "${PROJECT_ID}:gold"

bq show "${PROJECT_ID}:ops"
```
