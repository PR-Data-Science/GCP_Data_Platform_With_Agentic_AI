#!/usr/bin/env bash
set -euo pipefail

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "Missing required command: $1" >&2
        exit 1
    fi
}

require_env() {
    if [ -z "${!1:-}" ]; then
        echo "Missing required env var: $1" >&2
        exit 1
    fi
}

require_cmd gcloud
require_cmd bq
require_cmd gsutil

require_env PROJECT_ID
require_env REGION
require_env BQ_LOCATION

apis=(
    storage.googleapis.com
    bigquery.googleapis.com
    iam.googleapis.com
    secretmanager.googleapis.com
)

buckets=(
    "${PROJECT_ID}-llmfb-raw-dev"
    "${PROJECT_ID}-llmfb-bronze-dev"
    "${PROJECT_ID}-llmfb-silver-dev"
    "${PROJECT_ID}-llmfb-gold-dev"
    "${PROJECT_ID}-llmfb-deadletter-dev"
)

datasets=(
    bronze
    silver
    gold
    ops
)

created_apis=()
existing_apis=()
created_buckets=()
existing_buckets=()
created_datasets=()
existing_datasets=()

for api in "${apis[@]}"; do
    if gcloud services list --enabled --project "$PROJECT_ID" --format="value(config.name)" | grep -q "^${api}$"; then
        existing_apis+=("$api")
    else
        gcloud services enable "$api" --project "$PROJECT_ID" >/dev/null
        created_apis+=("$api")
    fi
done

for bucket in "${buckets[@]}"; do
    if gsutil ls -b "gs://${bucket}" >/dev/null 2>&1; then
        existing_buckets+=("$bucket")
    else
        gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://${bucket}" >/dev/null
        created_buckets+=("$bucket")
    fi
done

for dataset in "${datasets[@]}"; do
    if bq show --project_id="$PROJECT_ID" "$PROJECT_ID:$dataset" >/dev/null 2>&1; then
        existing_datasets+=("$dataset")
    else
        bq --location="$BQ_LOCATION" mk -d --project_id="$PROJECT_ID" "$dataset" >/dev/null
        created_datasets+=("$dataset")
    fi
done

echo "Bootstrap summary"
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "BQ location: $BQ_LOCATION"
echo ""
echo "APIs enabled (existing): ${existing_apis[*]:-none}"
echo "APIs enabled (new): ${created_apis[*]:-none}"
echo "Buckets (existing): ${existing_buckets[*]:-none}"
echo "Buckets created: ${created_buckets[*]:-none}"
echo "Datasets (existing): ${existing_datasets[*]:-none}"
echo "Datasets created: ${created_datasets[*]:-none}"
