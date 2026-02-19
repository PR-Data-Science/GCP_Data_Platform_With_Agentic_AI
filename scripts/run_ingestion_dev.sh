#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

AUTO_INPUT="data/drop/team=Magi_Code_Python/task=EAC_NEXT_STEPS_SIMPLIFIED/dt=2026-01-23/batch_id=python_training_version1_LLMrated_batch/python_training_version1_LLMrated_batch.json"
HUMAN_INPUT="data/drop/team=Magi_Code_Python/task=EAC_NEXT_STEPS_SIMPLIFIED/dt=2026-01-23/batch_id=python_training_version1_HUMANrated_batch/python_training_version1_HUMANrated_batch.csv"

cd "$REPO_ROOT"

PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python"
fi

"$PYTHON_BIN" -m src.ingestion.batch_to_gcs \
  --config conf/dev.yaml \
  --input "$AUTO_INPUT" \
  --source-name llm_feedback_eval \
  --source-type json \
  --pod-name Magi_Code_Python \
  --pod-type vertical \
  --task-type EAC_NEXT_STEPS_SIMPLIFIED

"$PYTHON_BIN" -m src.ingestion.batch_to_gcs \
  --config conf/dev.yaml \
  --input "$HUMAN_INPUT" \
  --source-name llm_feedback_eval \
  --source-type csv \
  --pod-name Magi_Code_Python \
  --pod-type vertical \
  --task-type EAC_NEXT_STEPS_SIMPLIFIED
