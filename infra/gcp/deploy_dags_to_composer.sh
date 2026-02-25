#!/usr/bin/env bash
# Deploy DAG files to the Cloud Composer environment and optionally set the Airflow Variable.
set -euo pipefail

COMPOSER_ENV="${COMPOSER_ENV:-gcpllmevaluationprojautomation}"
COMPOSER_LOCATION="${COMPOSER_LOCATION:-us-central1}"
PROJECT_ID="${PROJECT_ID:-liquid-layout-413121}"

DAGS_DIR="$(cd "$(dirname "$0")/../../orchestration/composer/dags" && pwd)"

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "Missing required command: $1" >&2
        exit 1
    fi
}

require_cmd gcloud

echo "Deploying DAGs to Composer environment..."
echo "  Environment : $COMPOSER_ENV"
echo "  Location    : $COMPOSER_LOCATION"
echo "  Project     : $PROJECT_ID"
echo "  DAGs dir    : $DAGS_DIR"
echo ""

dags=(
    "$DAGS_DIR/llm_feedback_dataproc_orchestration.py"
    "$DAGS_DIR/llm_feedback_full_e2e_composer.py"
)

for dag_file in "${dags[@]}"; do
    if [ ! -f "$dag_file" ]; then
        echo "ERROR: DAG file not found: $dag_file" >&2
        exit 1
    fi

    echo "Uploading: $(basename "$dag_file")"
    gcloud composer environments storage dags import \
        --environment "$COMPOSER_ENV" \
        --location "$COMPOSER_LOCATION" \
        --project "$PROJECT_ID" \
        --source "$dag_file"
done

echo ""
echo "✓ All DAGs uploaded successfully."
echo ""
echo "Next steps:"
echo "  1. Open the Airflow UI and confirm both DAGs appear:"
echo "       llm_feedback_dataproc_orchestration"
echo "       llm_feedback_full_e2e_composer"
echo "  2. Set the Airflow Variable 'llm_feedback_composer_config' in the Airflow UI."
echo "     See orchestration/composer/README.md for the example JSON value."
echo "  3. Trigger a manual run for a specific date, e.g.:"
echo '       {"ingest_date": "2026-02-24"}'
