#!/usr/bin/env bash
set -euo pipefail

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "Missing required command: $1" >&2
        exit 1
    fi
}

require_cmd gcloud
require_cmd bq
require_cmd python3

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
config_file="$repo_root/conf/dev.yaml"

if [ ! -f "$config_file" ]; then
    echo "Missing config file: $config_file" >&2
    exit 1
fi

read_config() {
    python3 - "$config_file" <<'PY'
import re
import sys

path = sys.argv[1]
project_id = None
raw_bucket = None

with open(path, "r", encoding="utf-8") as fh:
    for line in fh:
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        if re.match(r"^project_id:\s*", line):
            project_id = line.split(":", 1)[1].strip().strip('"').strip("'")
        if re.match(r"^raw_bucket:\s*", line):
            raw_bucket = line.split(":", 1)[1].strip().strip('"').strip("'")

if not project_id or not raw_bucket:
    sys.stderr.write("Missing project_id or gcs.raw_bucket in conf/dev.yaml\n")
    sys.exit(1)

print(project_id)
print(raw_bucket)
PY
}

config_output=$(read_config)
PROJECT_ID=$(printf '%s\n' "$config_output" | sed -n '1p')
RAW_BUCKET=$(printf '%s\n' "$config_output" | sed -n '2p')

trap 'echo "FAIL: smoke test failed." >&2' ERR

echo "Active gcloud account:"
gcloud auth list --filter=status:ACTIVE --format="value(account)"

tmp_file=$(mktemp)
cleanup() {
    rm -f "$tmp_file"
}
trap cleanup EXIT

timestamp=$(date -u +%Y%m%dT%H%M%SZ)
object_path="gs://${RAW_BUCKET}/smoke/smoke_${timestamp}.jsonl"

echo "{\"smoke\": \"ok\", \"ts\": \"${timestamp}\"}" > "$tmp_file"

echo "Uploading smoke file to $object_path"
gcloud storage cp "$tmp_file" "$object_path" >/dev/null

echo "Listing smoke objects:"
gcloud storage ls "gs://${RAW_BUCKET}/smoke/"

echo "Reading uploaded file:"
gcloud storage cat "$object_path"

bq --project_id="$PROJECT_ID" query --use_legacy_sql=false \
  "CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.ops.smoke_test\` (id STRING, created_at TIMESTAMP)" >/dev/null

bq --project_id="$PROJECT_ID" query --use_legacy_sql=false \
  "INSERT INTO \`${PROJECT_ID}.ops.smoke_test\` (id, created_at) VALUES (GENERATE_UUID(), CURRENT_TIMESTAMP())" >/dev/null

echo "Last 5 rows from ops.smoke_test:"
bq --project_id="$PROJECT_ID" query --use_legacy_sql=false \
  "SELECT id, created_at FROM \`${PROJECT_ID}.ops.smoke_test\` ORDER BY created_at DESC LIMIT 5"

echo "PASS: smoke test completed."
