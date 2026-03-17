---
name: gcp-data-platform-dq-drift-workflow
description: 'Investigate pipeline failures, data-quality regressions, and schema drift for the GCP Data Platform with Agentic AI using manifests, ops tables, deadletter outputs, and stage code. Use when asked to debug bad labels, null/range failures, dedup issues, row-count drift, or schema evolution impact.'
argument-hint: 'What is failing? Include stage, run_id, ingest_date, and symptom.'
---

# GCP Data Platform DQ and Drift Workflow

## When to Use
- A run fails or quality drops in Bronze/Silver/Gold.
- DQ rule failures spike, deadletter volume increases, or row counts drift unexpectedly.
- Schema drift or field-shape changes break downstream transforms.

## Inputs to Gather
- `run_id`, `ingest_date`, `stage`, source type.
- Manifest paths and counts.
- DQ/deadletter artifacts and relevant transformed table outputs.

## Procedure
1. Frame the incident.
- State expected behavior vs observed behavior.
- Identify first failing stage and blast radius to downstream stages.
2. Check stage evidence in order.
- Raw envelope consistency (`run_id`, `schema_hash`, `record_hash`, payload shape).
- Bronze output and dedup invariants.
- Silver DQ failures/deadletter reasons.
- Gold aggregation/publish anomalies.
3. Classify likely cause.
- `contract mismatch`: missing/renamed required fields.
- `type/shape drift`: nested structure or cast failures.
- `data anomaly`: out-of-range values, duplicates, unexpected nulls.
- `logic regression`: recent transform or mapping change.
4. Propose minimal safe fix.
- Prefer contract-aware normalization and rule adjustments over broad relaxations.
- Preserve lineage metadata and rerun safety.
5. Define validation.
- Add regression tests for the exact failure mode.
- Confirm fixed outputs and deadletter behavior for representative inputs.

## Decision Branches
- If failure is additive schema drift only:
- Route unknown fields to rescued/deadletter-safe handling, keep pipeline running.
- If failure breaks required contract fields:
- Fail clearly with reason and evidence; do not silently coerce critical fields.
- If failure is metric/range DQ-only:
- Keep records in deadletter with rule-specific reason and counts.

## Quality Gates
- Diagnosis includes concrete evidence from artifacts or code paths.
- Proposed fix does not remove idempotency safeguards.
- Tests verify both successful path and failure path behavior.
- Incident summary includes root cause, fix scope, and residual risk.

## Reference Docs
- `../../../workingdocs/END_TO_END_PIPELINE_TECHNICAL_DESIGN.md`
- `../../../workingdocs/PROJECT_MEMORY_cleaning_transformations.md`
- `../../../docs/CLEANING_TRANSFORMATIONS_NOTES.md`
- `../../../docs/SOURCE_CONTRACTS.md`
- `../../../dq/dq_rules.yaml`
