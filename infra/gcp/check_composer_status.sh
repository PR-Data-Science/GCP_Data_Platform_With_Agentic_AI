#!/usr/bin/env bash
# Check the status of the Cloud Composer environment and print a clear summary.
set -euo pipefail

COMPOSER_ENV="${COMPOSER_ENV:-gcpllmevaluationprojautomation}"
COMPOSER_LOCATION="${COMPOSER_LOCATION:-us-central1}"
PROJECT_ID="${PROJECT_ID:-liquid-layout-413121}"

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "Missing required command: $1" >&2
        exit 1
    fi
}

require_cmd gcloud

echo "Checking Composer environment..."
echo "  Environment : $COMPOSER_ENV"
echo "  Location    : $COMPOSER_LOCATION"
echo "  Project     : $PROJECT_ID"
echo ""

STATE=$(gcloud composer environments describe "$COMPOSER_ENV" \
    --location "$COMPOSER_LOCATION" \
    --project "$PROJECT_ID" \
    --format="value(state)" 2>&1) || {
    echo "ERROR: Could not describe Composer environment '$COMPOSER_ENV'."
    echo "Ensure you are authenticated (gcloud auth login) and have the correct project/location."
    exit 1
}

echo "Composer state: $STATE"

if [ "$STATE" = "RUNNING" ]; then
    echo ""
    echo "✓ Composer environment '$COMPOSER_ENV' is ACTIVE and RUNNING."

    AIRFLOW_URI=$(gcloud composer environments describe "$COMPOSER_ENV" \
        --location "$COMPOSER_LOCATION" \
        --project "$PROJECT_ID" \
        --format="value(config.airflowUri)" 2>/dev/null || echo "")

    if [ -n "$AIRFLOW_URI" ]; then
        echo "  Airflow UI: $AIRFLOW_URI"
    fi
else
    echo ""
    echo "✗ Composer environment '$COMPOSER_ENV' is NOT running. Current state: $STATE"
    echo "If the environment was recently created it may still be provisioning (state = CREATING)."
    echo "Re-run this script in a few minutes to recheck."
    exit 1
fi
