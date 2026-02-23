#!/usr/bin/env bash
set -euo pipefail

# End-to-end dev orchestration:
# 1) Generate sample training batch files (CSV + JSON)
# 2) Upload source files to raw bucket datasource folder
# 3) Ingest each source file to raw JSONL
# 4) Run Bronze Dataproc for each ingestion run_id
# 5) Run Silver Dataproc for each Bronze run_id
# 6) Run Gold Dataproc for each Silver run_id (optional BigQuery publish)
# 7) Optional star-schema materialization in BigQuery

# Required env vars:
#   PROJECT_ID, REGION, RAW_BUCKET, BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET, SERVICE_ACCOUNT
# Optional env vars:
#   ENV_NAME (default: dev)
#   SOURCE_NAME (default: llm_feedback_eval)
#   POD_NAME (default: Magi_Code_Python)
#   POD_TYPE (default: vertical)
#   TASK_TYPE (default: EAC_NEXT_STEPS_SIMPLIFIED)
#   INGEST_DATE_UTC (default: current UTC date YYYY-MM-DD)
#   DATASOURCE_PREFIX (default: datasource)
#   PUBLISH_BIGQUERY (default: false)
#   BQ_PROJECT (required only if PUBLISH_BIGQUERY=true, default PROJECT_ID)
#   BQ_DATASET (required only if PUBLISH_BIGQUERY=true, default: gold)
#   BUILD_STAR_SCHEMA (default: false)
#   DATAPROC_PROPERTIES (optional spark properties override)

: "${PROJECT_ID:?PROJECT_ID is required}"
: "${REGION:?REGION is required}"
: "${RAW_BUCKET:?RAW_BUCKET is required}"
: "${BRONZE_BUCKET:?BRONZE_BUCKET is required}"
: "${SILVER_BUCKET:?SILVER_BUCKET is required}"
: "${GOLD_BUCKET:?GOLD_BUCKET is required}"
: "${SERVICE_ACCOUNT:?SERVICE_ACCOUNT is required}"

ENV_NAME="${ENV_NAME:-dev}"
SOURCE_NAME="${SOURCE_NAME:-llm_feedback_eval}"
POD_NAME="${POD_NAME:-Magi_Code_Python}"
POD_TYPE="${POD_TYPE:-vertical}"
TASK_TYPE="${TASK_TYPE:-EAC_NEXT_STEPS_SIMPLIFIED}"
INGEST_DATE_UTC="${INGEST_DATE_UTC:-$(date -u +%F)}"
DATASOURCE_PREFIX="${DATASOURCE_PREFIX:-datasource}"
PUBLISH_BIGQUERY="${PUBLISH_BIGQUERY:-false}"
BQ_PROJECT="${BQ_PROJECT:-$PROJECT_ID}"
BQ_DATASET="${BQ_DATASET:-gold}"
BUILD_STAR_SCHEMA="${BUILD_STAR_SCHEMA:-false}"
DATAPROC_PROPERTIES="${DATAPROC_PROPERTIES:-spark.dynamicAllocation.enabled=false,spark.executor.instances=1,spark.executor.cores=4,spark.driver.cores=4}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python"
fi

log() {
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] $*"
}

parse_field() {
  local key="$1"
  awk -F= -v k="$key" '$1==k {print $2}'
}

log "Step 1/7: generating sample training batches"
"$PYTHON_BIN" -m src.tools.generate_sample_batches

AUTO_INPUT="data/drop/team=Magi_Code_Python/task=EAC_NEXT_STEPS_SIMPLIFIED/dt=2026-01-23/batch_id=python_training_version1_LLMrated_batch/python_training_version1_LLMrated_batch.json"
HUMAN_INPUT="data/drop/team=Magi_Code_Python/task=EAC_NEXT_STEPS_SIMPLIFIED/dt=2026-01-23/batch_id=python_training_version1_HUMANrated_batch/python_training_version1_HUMANrated_batch.csv"

if [[ ! -f "$AUTO_INPUT" ]]; then
  echo "Missing auto input file: $AUTO_INPUT" >&2
  exit 1
fi
if [[ ! -f "$HUMAN_INPUT" ]]; then
  echo "Missing human input file: $HUMAN_INPUT" >&2
  exit 1
fi

upload_source_file() {
  local source_type="$1"
  local input_path="$2"
  local batch_name
  batch_name="$(basename "$input_path")"
  local destination="gs://${RAW_BUCKET}/${DATASOURCE_PREFIX}/${SOURCE_NAME}/source_type=${source_type}/dt=${INGEST_DATE_UTC}/${batch_name}"
  log "Uploading source file to ${destination}"
  gcloud storage cp "$input_path" "$destination" >/dev/null
}

run_ingestion_capture() {
  local source_type="$1"
  local input_path="$2"
  log "Running ingestion for ${source_type}: ${input_path}"
  local output
  output="$($PYTHON_BIN -m src.ingestion.batch_to_gcs \
    --config conf/dev.yaml \
    --input "$input_path" \
    --source-name "$SOURCE_NAME" \
    --source-type "$source_type" \
    --pod-name "$POD_NAME" \
    --pod-type "$POD_TYPE" \
    --task-type "$TASK_TYPE")"
  echo "$output"
}

run_dataproc_bronze() {
  local run_id="$1"
  log "Submitting Bronze Dataproc for run_id=${run_id} ingest_date=${INGEST_DATE_UTC}"
  PROJECT_ID="$PROJECT_ID" \
  REGION="$REGION" \
  RAW_BUCKET="$RAW_BUCKET" \
  BRONZE_BUCKET="$BRONZE_BUCKET" \
  SERVICE_ACCOUNT="$SERVICE_ACCOUNT" \
  ENV_NAME="$ENV_NAME" \
  DATAPROC_PROPERTIES="$DATAPROC_PROPERTIES" \
  RUN_ID="$run_id" \
  INGEST_DATE="$INGEST_DATE_UTC" \
  bash scripts/run_bronze_dataproc.sh
}

run_dataproc_silver() {
  local run_id="$1"
  log "Submitting Silver Dataproc for run_id=${run_id} ingest_date=${INGEST_DATE_UTC}"
  PROJECT_ID="$PROJECT_ID" \
  REGION="$REGION" \
  BRONZE_BUCKET="$BRONZE_BUCKET" \
  SILVER_BUCKET="$SILVER_BUCKET" \
  SERVICE_ACCOUNT="$SERVICE_ACCOUNT" \
  ENV_NAME="$ENV_NAME" \
  DATAPROC_PROPERTIES="$DATAPROC_PROPERTIES" \
  RUN_ID="$run_id" \
  INGEST_DATE="$INGEST_DATE_UTC" \
  bash scripts/run_silver_dataproc.sh
}

run_dataproc_gold() {
  local run_id="$1"
  log "Submitting Gold Dataproc for run_id=${run_id} ingest_date=${INGEST_DATE_UTC}"

  if [[ "$PUBLISH_BIGQUERY" == "true" ]]; then
    PROJECT_ID="$PROJECT_ID" \
    REGION="$REGION" \
    SILVER_BUCKET="$SILVER_BUCKET" \
    GOLD_BUCKET="$GOLD_BUCKET" \
    SERVICE_ACCOUNT="$SERVICE_ACCOUNT" \
    ENV_NAME="$ENV_NAME" \
    DATAPROC_PROPERTIES="$DATAPROC_PROPERTIES" \
    RUN_ID="$run_id" \
    INGEST_DATE="$INGEST_DATE_UTC" \
    PUBLISH_BIGQUERY="true" \
    BQ_PROJECT="$BQ_PROJECT" \
    BQ_DATASET="$BQ_DATASET" \
    bash scripts/run_gold_dataproc.sh
  else
    PROJECT_ID="$PROJECT_ID" \
    REGION="$REGION" \
    SILVER_BUCKET="$SILVER_BUCKET" \
    GOLD_BUCKET="$GOLD_BUCKET" \
    SERVICE_ACCOUNT="$SERVICE_ACCOUNT" \
    ENV_NAME="$ENV_NAME" \
    DATAPROC_PROPERTIES="$DATAPROC_PROPERTIES" \
    RUN_ID="$run_id" \
    INGEST_DATE="$INGEST_DATE_UTC" \
    bash scripts/run_gold_dataproc.sh
  fi
}

log "Step 2/7: uploading local source files to raw datasource folder"
upload_source_file "json" "$AUTO_INPUT"
upload_source_file "csv" "$HUMAN_INPUT"

log "Step 3/7: ingesting source files to raw JSONL"
AUTO_INGEST_OUTPUT="$(run_ingestion_capture "json" "$AUTO_INPUT")"
HUMAN_INGEST_OUTPUT="$(run_ingestion_capture "csv" "$HUMAN_INPUT")"

AUTO_RUN_ID="$(echo "$AUTO_INGEST_OUTPUT" | parse_field run_id | tail -n 1)"
HUMAN_RUN_ID="$(echo "$HUMAN_INGEST_OUTPUT" | parse_field run_id | tail -n 1)"

if [[ -z "$AUTO_RUN_ID" || -z "$HUMAN_RUN_ID" ]]; then
  echo "Failed to capture run_id from ingestion output" >&2
  echo "AUTO output:" >&2
  echo "$AUTO_INGEST_OUTPUT" >&2
  echo "HUMAN output:" >&2
  echo "$HUMAN_INGEST_OUTPUT" >&2
  exit 1
fi

log "Captured AUTO run_id=${AUTO_RUN_ID}"
log "Captured HUMAN run_id=${HUMAN_RUN_ID}"

RUN_IDS=("$AUTO_RUN_ID" "$HUMAN_RUN_ID")

log "Step 4/7: running Bronze for each run_id"
for run_id in "${RUN_IDS[@]}"; do
  run_dataproc_bronze "$run_id"
done

log "Step 5/7: running Silver for each run_id"
for run_id in "${RUN_IDS[@]}"; do
  run_dataproc_silver "$run_id"
done

log "Step 6/7: running Gold for each run_id"
for run_id in "${RUN_IDS[@]}"; do
  run_dataproc_gold "$run_id"
done

if [[ "$BUILD_STAR_SCHEMA" == "true" ]]; then
  log "Step 7/7: building BigQuery star schema"
  PROJECT_ID="$BQ_PROJECT" BQ_DATASET="$BQ_DATASET" bash scripts/run_gold_star_schema_bq.sh
else
  log "Step 7/7: skipping star schema build (BUILD_STAR_SCHEMA=false)"
fi

log "Pipeline completed successfully"
log "Run IDs processed: ${RUN_IDS[*]}"
log "Ingest date used: ${INGEST_DATE_UTC}"
