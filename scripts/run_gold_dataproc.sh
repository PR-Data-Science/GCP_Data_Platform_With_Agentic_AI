#!/usr/bin/env bash
set -euo pipefail

# Submit the Dataproc Serverless gold transform job.
#
# Required env vars:
#   PROJECT_ID, REGION, SILVER_BUCKET, GOLD_BUCKET, SERVICE_ACCOUNT
# Optional env vars:
#   ENV_NAME (default: dev)
#   SILVER_PREFIX (default: silver/)
#   GOLD_PREFIX (default: gold/)
#   RUN_ID, INGEST_DATE
#   PUBLISH_BIGQUERY=true|false (default: false)
#   BQ_PROJECT, BQ_DATASET (required if PUBLISH_BIGQUERY=true)

: "${PROJECT_ID:?PROJECT_ID is required}"
: "${REGION:?REGION is required}"
: "${SERVICE_ACCOUNT:?SERVICE_ACCOUNT is required}"

SILVER_BUCKET="${SILVER_BUCKET:-${GCS_BUCKET:-}}"
GOLD_BUCKET="${GOLD_BUCKET:-${GCS_BUCKET:-}}"

: "${SILVER_BUCKET:?SILVER_BUCKET is required (or set GCS_BUCKET)}"
: "${GOLD_BUCKET:?GOLD_BUCKET is required (or set GCS_BUCKET)}"

ENV_NAME="${ENV_NAME:-dev}"
SILVER_PREFIX="${SILVER_PREFIX:-silver/}"
GOLD_PREFIX="${GOLD_PREFIX:-gold/}"
PUBLISH_BIGQUERY="${PUBLISH_BIGQUERY:-false}"
DATAPROC_PROPERTIES="${DATAPROC_PROPERTIES:-}"

ARGS=(
  "--env=${ENV_NAME}"
  "--silver_bucket=${SILVER_BUCKET}"
  "--gold_bucket=${GOLD_BUCKET}"
  "--silver_prefix=${SILVER_PREFIX}"
  "--gold_prefix=${GOLD_PREFIX}"
)

if [[ -n "${RUN_ID:-}" ]]; then
  ARGS+=("--run_id=${RUN_ID}")
fi
if [[ -n "${INGEST_DATE:-}" ]]; then
  ARGS+=("--ingest_date=${INGEST_DATE}")
fi

if [[ "${PUBLISH_BIGQUERY}" == "true" ]]; then
  : "${BQ_PROJECT:?BQ_PROJECT is required when PUBLISH_BIGQUERY=true}"
  : "${BQ_DATASET:?BQ_DATASET is required when PUBLISH_BIGQUERY=true}"
  ARGS+=("--publish_bigquery")
  ARGS+=("--bq_project=${BQ_PROJECT}")
  ARGS+=("--bq_dataset=${BQ_DATASET}")
fi

JOB_FILE="gs://${SILVER_BUCKET}/jobs/gold_transform_dataproc.py"

echo "Uploading latest job to ${JOB_FILE}"
gcloud storage cp src/gold/gold_transform_dataproc.py "${JOB_FILE}"

gcloud dataproc batches submit pyspark "${JOB_FILE}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --service-account="${SERVICE_ACCOUNT}" \
  ${DATAPROC_PROPERTIES:+--properties="${DATAPROC_PROPERTIES}"} \
  -- "${ARGS[@]}"
