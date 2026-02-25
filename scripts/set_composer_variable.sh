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
require_cmd python3

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

VAR_NAME="${VAR_NAME:-llm_feedback_composer_config}"
VAR_JSON_PATH="${VAR_JSON_PATH:-$REPO_ROOT/orchestration/composer/llm_feedback_composer_config.dev.json}"

require_env COMPOSER_ENV
require_env COMPOSER_REGION

if [ ! -f "$VAR_JSON_PATH" ]; then
  echo "Variable JSON file not found: $VAR_JSON_PATH" >&2
  exit 1
fi

VAR_JSON_MINIFIED="$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1])), separators=(",", ":"), ensure_ascii=False))' "$VAR_JSON_PATH")"

PROJECT_ARG=()
if [ -n "${PROJECT_ID:-}" ]; then
  PROJECT_ARG=(--project "$PROJECT_ID")
fi

echo "Setting Airflow Variable '$VAR_NAME' in Composer environment '$COMPOSER_ENV'..."
gcloud composer environments run "$COMPOSER_ENV" \
  --location "$COMPOSER_REGION" \
  "${PROJECT_ARG[@]}" \
  variables -- \
  --set "$VAR_NAME" "$VAR_JSON_MINIFIED"

echo "Variable set successfully."
