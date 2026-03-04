#!/usr/bin/env bash
set -euo pipefail

# Submit the Dataproc Serverless bronze ingest job.
#
# Required env vars:
#   PROJECT_ID, REGION, RAW_BUCKET, BRONZE_BUCKET, SERVICE_ACCOUNT
# Optional env vars:
#   ENV_NAME (default: dev)
#   RAW_PREFIX (default: raw/)
#   BRONZE_PREFIX (default: bronze/)
#   RUN_ID, INGEST_DATE, BATCH_NAME
#   MODE (default: append)
#   FORCE_REPROCESS=true|false (default: false)
#   CODE_VERSION (default: unknown)
#   OPS_DATASET (default: ops)
# Backward compatibility:
#   If GCS_BUCKET is provided, it is used as fallback for RAW_BUCKET/BRONZE_BUCKET.
#
# Example (specific run_id):
#   PROJECT_ID=liquid-layout-413121 \
#   REGION=us-central1 \
#   RAW_BUCKET=liquid-layout-413121-llmfb-raw-dev \
#   BRONZE_BUCKET=liquid-layout-413121-llmfb-bronze-dev \
#   SERVICE_ACCOUNT=dataproc-runner@liquid-layout-413121.iam.gserviceaccount.com \
#   RUN_ID=1a2b3c4d-0000-1111-2222-abcdefabcdef \
#   bash scripts/run_bronze_dataproc.sh

: "${PROJECT_ID:?PROJECT_ID is required}"
: "${REGION:?REGION is required}"
: "${SERVICE_ACCOUNT:?SERVICE_ACCOUNT is required}"

RAW_BUCKET="${RAW_BUCKET:-${GCS_BUCKET:-}}"
BRONZE_BUCKET="${BRONZE_BUCKET:-${GCS_BUCKET:-}}"

: "${RAW_BUCKET:?RAW_BUCKET is required (or set GCS_BUCKET)}"
: "${BRONZE_BUCKET:?BRONZE_BUCKET is required (or set GCS_BUCKET)}"

ENV_NAME="${ENV_NAME:-dev}"
RAW_PREFIX="${RAW_PREFIX:-raw/}"
BRONZE_PREFIX="${BRONZE_PREFIX:-bronze/}"
MODE="${MODE:-append}"
DATAPROC_PROPERTIES="${DATAPROC_PROPERTIES:-spark.dynamicAllocation.enabled=false,spark.executor.instances=2,spark.executor.cores=4,spark.driver.cores=4}"
FORCE_REPROCESS="${FORCE_REPROCESS:-false}"
CODE_VERSION="${CODE_VERSION:-unknown}"
OPS_DATASET="${OPS_DATASET:-ops}"

ARGS=(
  "--env=${ENV_NAME}"
  "--raw_bucket=${RAW_BUCKET}"
  "--bronze_bucket=${BRONZE_BUCKET}"
  "--raw_prefix=${RAW_PREFIX}"
  "--bronze_prefix=${BRONZE_PREFIX}"
  "--mode=${MODE}"
  "--code_version=${CODE_VERSION}"
  "--ops_dataset=${OPS_DATASET}"
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
if [[ "${FORCE_REPROCESS}" == "true" ]]; then
  ARGS+=("--force")
fi

JOB_FILE="gs://${RAW_BUCKET}/jobs/bronze_ingest_dataproc.py"

echo "Uploading latest job to ${JOB_FILE}"
gcloud storage cp src/bronze/bronze_ingest_dataproc.py "${JOB_FILE}"

gcloud dataproc batches submit pyspark "${JOB_FILE}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --service-account="${SERVICE_ACCOUNT}" \
  ${DATAPROC_PROPERTIES:+--properties="${DATAPROC_PROPERTIES}"} \
  -- "${ARGS[@]}"
