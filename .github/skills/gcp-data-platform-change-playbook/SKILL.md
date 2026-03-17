---
name: gcp-data-platform-change-playbook
description: 'Implement or fix ingestion, bronze, silver, gold, BigQuery publish, manifests, idempotency, deadletter, or ops table behavior in this GCP data platform with Agentic AI repository. Use when asked to add a pipeline feature, fix a stage bug, enforce contracts, or make reruns safe.'
argument-hint: 'What stage or behavior should change? (raw/bronze/silver/gold/bq/ops)'
---

# GCP Data Platform Change Playbook

## When to Use
- Add or modify behavior in `src/ingestion`, `src/bronze`, `src/silver`, `src/gold`, or `sql/`.
- Fix run-id idempotency, manifest logic, partitioning, deadletter, or DQ behavior.
- Update scripts/tests tied to stage execution in `scripts/` and `tests/`.

## Required Context
1. Read architecture and standards in `../../../workingdocs/END_TO_END_PIPELINE_TECHNICAL_DESIGN.md`.
2. Read stage guide(s):
- `../../../workingdocs/RAW_TO_BRONZE_END_TO_END_GUIDE.md`
- `../../../workingdocs/SILVER_TO_GOLD_BIGQUERY_END_TO_END_GUIDE.md`
3. Confirm acceptance checks in `../../../docs/DEFINITION_OF_DONE.md`.

## Procedure
1. Classify the change type.
- `feature`: new transformation/field/table/metric.
- `bugfix`: incorrect result, stage failure, or idempotency issue.
- `hardening`: ops observability, manifests, replay safety, guardrails.
2. Trace affected contract and data grain.
- Identify required keys (`run_id`, `record_hash`, partition keys).
- Identify impacted outputs (Bronze partitions, Silver tables, Gold marts, BigQuery outputs).
3. Implement minimally scoped code changes.
- Preserve existing stage boundaries (Raw -> Bronze -> Silver -> Gold -> Publish).
- Keep metadata propagation intact (`run_id`, `schema_hash`, `record_hash`, timestamps).
- Avoid introducing write behavior that breaks rerun safety.
4. Update tests in the same PR-sized change.
- Add/adjust unit tests under `../../../tests/` for new logic.
- Prioritize deterministic tests around dedup, DQ routing, and output grain.
5. Validate before finalizing.
- Run targeted tests first, then broader `pytest` when practical.
- Verify changed scripts and SQL references still match current paths/args.
6. Report evidence.
- Summarize what changed, why, and which invariants were preserved.
- Include file references and test outcomes.

## Quality Gates
- Idempotency: rerunning same `run_id` does not duplicate outputs.
- Observability: manifests/ops paths are not silently broken.
- Contract safety: required fields, types, and DQ expectations remain enforceable.
- Scope discipline: avoid unrelated edits and avoid reverting existing unrelated changes.

## Completion Checklist
- Code updated in the right stage module(s).
- Tests added/updated for changed behavior.
- Validation run and results captured.
- Response includes explicit impact on Raw/Bronze/Silver/Gold/BigQuery and ops evidence path.
