#!/usr/bin/env bash
set -euo pipefail

# Submit the Dataproc Serverless bronze ingest job.
#
# Required env vars:
#   PROJECT_ID, REGION, GCS_BUCKET, SERVICE_ACCOUNT
# Optional env vars:
#   ENV_NAME (default: dev)
#   RAW_PREFIX (default: raw/)
#   BRONZE_PREFIX (default: bronze/)
#   RUN_ID, INGEST_DATE, BATCH_NAME
#   MODE (default: append)
#
# Example (specific run_id):
#   PROJECT_ID=liquid-layout-413121 \
#   REGION=us-central1 \
#   GCS_BUCKET=liquid-layout-413121-llmfb-raw-dev \
#   SERVICE_ACCOUNT=dataproc-runner@liquid-layout-413121.iam.gserviceaccount.com \
#   RUN_ID=1a2b3c4d-0000-1111-2222-abcdefabcdef \
#   bash scripts/run_bronze_dataproc.sh

: "${PROJECT_ID:?PROJECT_ID is required}"
: "${REGION:?REGION is required}"
: "${GCS_BUCKET:?GCS_BUCKET is required}"
: "${SERVICE_ACCOUNT:?SERVICE_ACCOUNT is required}"

ENV_NAME="${ENV_NAME:-dev}"
RAW_PREFIX="${RAW_PREFIX:-raw/}"
BRONZE_PREFIX="${BRONZE_PREFIX:-bronze/}"
MODE="${MODE:-append}"

ARGS=(
  "--env=${ENV_NAME}"
  "--gcs_bucket=${GCS_BUCKET}"
  "--raw_prefix=${RAW_PREFIX}"
  "--bronze_prefix=${BRONZE_PREFIX}"
  "--mode=${MODE}"
)

if [[ -n "${RUN_ID:-}" ]]; then
  ARGS+=("--run_id=${RUN_ID}")
fi
if [[ -n "${INGEST_DATE:-}" ]]; then
  ARGS+=("--ingest_date=${INGEST_DATE}")
fi
if [[ -n "${BATCH_NAME:-}" ]]; then
  ARGS+=("--batch_name=${BATCH_NAME}")
fi

JOB_FILE="gs://${GCS_BUCKET}/jobs/bronze_ingest_dataproc.py"

if ! gcloud storage ls "${JOB_FILE}" >/dev/null 2>&1; then
  echo "Uploading job to ${JOB_FILE}"
  gcloud storage cp src/bronze/bronze_ingest_dataproc.py "${JOB_FILE}"
fi

gcloud dataproc batches submit pyspark "${JOB_FILE}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --service-account="${SERVICE_ACCOUNT}" \
  --args="${ARGS[*]}"
