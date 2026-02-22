#!/usr/bin/env bash
set -euo pipefail

# Build star-schema analytics tables in BigQuery from published Gold tables.
#
# Required env vars:
#   PROJECT_ID
# Optional env vars:
#   BQ_DATASET (default: gold)

: "${PROJECT_ID:?PROJECT_ID is required}"

BQ_DATASET="${BQ_DATASET:-gold}"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
SQL_TEMPLATE="${REPO_ROOT}/sql/gold_star_schema.sql"

if [[ ! -f "${SQL_TEMPLATE}" ]]; then
  echo "Missing SQL template: ${SQL_TEMPLATE}" >&2
  exit 1
fi

tmp_sql=$(mktemp)
cleanup() {
  rm -f "${tmp_sql}"
}
trap cleanup EXIT

sed \
  -e "s/__PROJECT_ID__/${PROJECT_ID}/g" \
  -e "s/__BQ_DATASET__/${BQ_DATASET}/g" \
  "${SQL_TEMPLATE}" > "${tmp_sql}"

echo "Running star-schema DDL in ${PROJECT_ID}.${BQ_DATASET}"
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false < "${tmp_sql}"

echo "Created/updated tables:"
bq --project_id="${PROJECT_ID}" ls "${PROJECT_ID}:${BQ_DATASET}" | grep -E 'dim_|fact_' || true
