#!/usr/bin/env bash
set -euo pipefail

# Submit the Dataproc Serverless silver transform job.
#
# Required env vars:
#   PROJECT_ID, REGION, BRONZE_BUCKET, SILVER_BUCKET, SERVICE_ACCOUNT
# Optional env vars:
#   ENV_NAME (default: dev)
#   BRONZE_PREFIX (default: bronze/)
#   SILVER_PREFIX (default: silver/)
#   RUN_ID, INGEST_DATE, BATCH_NAME
#   MODE (default: append)
#   FORCE_REPROCESS=true|false (default: false)
#   CODE_VERSION (default: unknown)
#   OPS_DATASET (default: ops)
# Backward compatibility:
#   If GCS_BUCKET is provided, it is used as fallback for BRONZE_BUCKET/SILVER_BUCKET.

: "${PROJECT_ID:?PROJECT_ID is required}"
: "${REGION:?REGION is required}"
: "${SERVICE_ACCOUNT:?SERVICE_ACCOUNT is required}"

BRONZE_BUCKET="${BRONZE_BUCKET:-${GCS_BUCKET:-}}"
SILVER_BUCKET="${SILVER_BUCKET:-${GCS_BUCKET:-}}"

: "${BRONZE_BUCKET:?BRONZE_BUCKET is required (or set GCS_BUCKET)}"
: "${SILVER_BUCKET:?SILVER_BUCKET is required (or set GCS_BUCKET)}"

ENV_NAME="${ENV_NAME:-dev}"
BRONZE_PREFIX="${BRONZE_PREFIX:-bronze/}"
SILVER_PREFIX="${SILVER_PREFIX:-silver/}"
MODE="${MODE:-append}"
DATAPROC_PROPERTIES="${DATAPROC_PROPERTIES:-spark.dynamicAllocation.enabled=false,spark.executor.instances=2,spark.executor.cores=4,spark.driver.cores=4}"
FORCE_REPROCESS="${FORCE_REPROCESS:-false}"
CODE_VERSION="${CODE_VERSION:-unknown}"
OPS_DATASET="${OPS_DATASET:-ops}"

ARGS=(
  "--env=${ENV_NAME}"
  "--bronze_bucket=${BRONZE_BUCKET}"
  "--silver_bucket=${SILVER_BUCKET}"
  "--bronze_prefix=${BRONZE_PREFIX}"
  "--silver_prefix=${SILVER_PREFIX}"
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

JOB_FILE="gs://${BRONZE_BUCKET}/jobs/silver_transform_dataproc.py"

echo "Uploading latest job to ${JOB_FILE}"
gcloud storage cp src/silver/silver_transform_dataproc.py "${JOB_FILE}"

gcloud dataproc batches submit pyspark "${JOB_FILE}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --service-account="${SERVICE_ACCOUNT}" \
  ${DATAPROC_PROPERTIES:+--properties="${DATAPROC_PROPERTIES}"} \
  -- "${ARGS[@]}"
