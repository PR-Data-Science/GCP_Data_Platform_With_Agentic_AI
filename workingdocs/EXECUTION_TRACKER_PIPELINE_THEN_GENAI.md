# Execution Tracker: Pipeline Hardening -> GenAI Flow

## Purpose
This tracker drives implementation in strict sequence:
1. Complete pipeline hardening and GenAI-readiness artifacts.
2. Then execute GenAI flow implementation from design docs.

Status legend:
- `NOT_STARTED`
- `IN_PROGRESS`
- `BLOCKED`
- `DONE`

---

## A) Pipeline Hardening Backlog (Execute first)

| ID | Task | Status | Priority | Depends On | Acceptance Criteria |
|---|---|---|---|---|---|
| P0 | Baseline audit + implementation tracker | DONE | P0 | None | Pending gaps documented and execution order approved in this tracker |
| P1 | Create ops control-plane artifacts (`sql/ops_tables.sql`) | DONE | P0 | P0 | DDL exists for `ops.pipeline_runs`, `ops.dq_results`, `ops.schema_registry`, `ops.deadletter_summary`, optional `ops.dq_rule_registry` |
| P2 | Create error taxonomy module (`src/ops/error_taxonomy.py`) | DONE | P0 | P1 | Standard categories + error-code mapping helper available |
| P3 | Create ops writer helper (`src/ops/ops_writer.py`) | DONE | P0 | P1,P2 | Helper functions to write runs/DQ/deadletter/schema rows with stable schema |
| P4 | Create DQ rule registry file (`dq/dq_rules.yaml`) | DONE | P0 | P0 | Registry with required fields and starter rules committed |
| P5 | Create contract snapshot structure (`contracts/silver/`, `contracts/gold/`, `contracts/README.md`) | DONE | P0 | P0 | Versioning policy documented + starter contract examples |
| P6 | Standardize manifest path + schema in Bronze job | DONE | P1 | P1,P2,P3 | Bronze writes `manifests/bronze/dt=<date>/run_id=<run_id>/manifest.json` with standardized keys |
| P7 | Standardize manifest path + schema in Silver job | DONE | P1 | P1,P2,P3 | Silver writes standardized manifest + includes failure metadata |
| P8 | Standardize manifest path + schema in Gold job | DONE | P1 | P1,P2,P3 | Gold writes standardized manifest + includes publish stage pointers |
| P9 | Add raw-stage manifest output in ingestion flow | DONE | P1 | P1,P2,P3 | Ingestion writes raw manifest and updates ops rows |
| P10 | Add optional publish-stage manifest + ops row updates | DONE | P1 | P1,P2,P3,P8 | Publish step emits manifest + ops status |
| P11 | Enforce run_id-scoped partitioning strategy updates | DONE | P1 | P6,P7,P8 | Outputs include `run_id` in partition/path strategy or explicit run keying |
| P12 | Add `--force` override behavior for rerun policy | DONE | P1 | P6,P7,P8,P9 | Existing-manifest default skip; force flag enables controlled reprocess |
| P13 | Strengthen deadletter schema + summary aggregation | DONE | P1 | P3,P4,P7 | Deadletter rows include evidence refs, `rule_id`, `severity`; summary writes to `ops.deadletter_summary` |
| P14 | Persist schema snapshots to `ops.schema_registry` on first-seen hash | DONE | P1 | P3,P5,P6,P7,P8 | First-seen schema hash inserts snapshot with source metadata |
| P15 | Persist lineage/version pointers (`code_version`, input/output paths, partitions) | DONE | P1 | P6,P7,P8,P9,P10 | `code_version` and path pointers in manifests + `ops.pipeline_runs` |
| P16 | Wire DQ results to rule registry IDs | DONE | P1 | P4,P7,P13 | `ops.dq_results.rule_id` and deadletter `rule_id` come from registry |
| P17 | Composer/DAG updates for new manifest paths + force controls | DONE | P2 | P6,P7,P8,P9,P10,P12 | DAG planning logic uses new manifest location and rerun semantics |
| P18 | Tests: add/update unit tests for manifests, ops writer, taxonomy, deadletter contracts | DONE | P1 | P2-P17 | Tests pass locally; key behavior covered |
| P19 | Docs sync (`END_TO_END_PIPELINE_TECHNICAL_DESIGN.md`, diagram, runbooks) | DONE | P2 | P6-P18 | Docs match implemented behavior exactly |
| P20 | Pipeline hardening sign-off checkpoint | DONE | P0 | P1-P19 | All pipeline tasks complete and verified |

---

## B) GenAI Flow Backlog (Start only after P20)

| ID | Task | Status | Priority | Depends On | Acceptance Criteria |
|---|---|---|---|---|---|
| G1 | Finalize Phase 2A scope lock (read-only) | DONE | P0 | P20 | Scope + non-goals signed off |
| G2 | Implement FastAPI Agent Service skeleton + session model | DONE | P0 | G1 | API skeleton with health/session endpoints |
| G3 | Implement Router/Coordinator agent runtime contract | DONE | P0 | G2 | Structured request routing and response schema |
| G4 | Build read-only MCP tools (BQ templates, GCS, Composer, Dataproc, schema diff) | DONE | P0 | G1,G2 | Tool contracts + allowlists + role checks |
| G5 | Add Apigee edge policies (auth, quota, routing) | DONE | P1 | G2 | Protected API edge with versioned routes |
| G6 | Implement retrieval/indexing pipeline + evidence refs | DONE | P0 | G2 | Retrieval returns traceable evidence IDs |
| G7 | Build audit tables + proposal tables integration in service | DONE | P0 | G2 | Full trace logging for sessions/tool calls/responses |
| G8 | Build evaluation harness + CI regression gate | IN_PROGRESS | P0 | G4,G6 | Recall/grounding/unsupported-claim metrics enforced |
| G9 | Build Agent Console MVP integration (Ops/DQ modes first) | NOT_STARTED | P1 | G2,G5,G4 | UI reads from service and displays evidence/tool logs |
| G10 | Implement proposal lifecycle + HITL states (Phase 2B) | NOT_STARTED | P1 | G7 | DRAFT->REVIEW->APPROVED lifecycle works |
| G11 | Transform Designer proposal mode (B2S/S2G playbooks) | NOT_STARTED | P1 | G10,G4 | Proposal artifacts generated, not auto-applied |
| G12 | CI gates + controlled PR automation (Phase 2C) | NOT_STARTED | P2 | G10,G11,G8 | PR creation behind approvals + quality gates |
| G13 | Controlled trigger automations (draft-only notifications) | NOT_STARTED | P2 | G7,G9 | Event-driven drafts, no auto-rerun/merge |
| G14 | Security hardening + RBAC + retention + PII controls | NOT_STARTED | P0 | G2-G13 | Security checklist complete and validated |
| G15 | GenAI phase sign-off checkpoint | NOT_STARTED | P0 | G1-G14 | Phase objectives met and documented |

---

## Active Work Log

| Seq | Task ID | Action | Status | Notes |
|---|---|---|---|---|
| 1 | P1 | Prepare ops table DDL file | DONE | Created `sql/ops_tables.sql` with required ops tables |
| 2 | P2 | Implement taxonomy constants and mapper | DONE | Created `src/ops/error_taxonomy.py` |
| 3 | P3 | Implement unified ops writer helper | DONE | Created `src/ops/ops_writer.py` |
| 4 | P4 | Define DQ rule registry YAML | DONE | Created `dq/dq_rules.yaml` with starter rules |
| 5 | P5 | Add contract snapshots/versioning docs | DONE | Created `contracts/README.md`, `contracts/silver/v1_feedback_step.json`, `contracts/gold/v1_training_supervised_examples.json` |
| 6 | P6 | Standardize Bronze manifest behavior | DONE | Updated `src/bronze/bronze_ingest_dataproc.py` with standardized manifest path/schema and `--force` skip override |
| 7 | P7 | Standardize Silver manifest behavior | DONE | Updated `src/silver/silver_transform_dataproc.py` with standardized manifest path/schema and `--force` skip override |
| 8 | P8 | Standardize Gold manifest behavior | DONE | Updated `src/gold/gold_transform_dataproc.py` with standardized manifest path/schema and `--force` skip override |
| 9 | P9 | Add raw-stage manifest behavior | DONE | Updated `src/ingestion/batch_to_gcs.py` with standardized raw manifest + best-effort ops row write |
| 10 | P10 | Add publish-stage manifest + ops updates | DONE | Updated `src/gold/gold_transform_dataproc.py` to emit publish manifest and write `gold`/`publish` ops rows |
| 11 | P11 | Enforce run_id-scoped partition strategy | DONE | Updated Bronze/Silver/Gold writes to partition by `ingest_date + run_id` (Bronze also keeps `source_type`) |
| 12 | P12 | Expand force-override behavior coverage | DONE | Added `FORCE_REPROCESS` propagation in wrappers (`run_bronze_dataproc.sh`, `run_silver_dataproc.sh`, `run_gold_dataproc.sh`, `run_full_pipeline_dev.sh`) and unified code/ops metadata passthrough |
| 13 | P13 | Strengthen deadletter schema + summary aggregation | DONE | Silver deadletter now emits `failure_reason`, `rule_id`, `severity`, `evidence_ref` and writes aggregated summaries to `ops.deadletter_summary` |
| 14 | P14 | Persist schema snapshots to schema registry | DONE | Added first-seen schema snapshot writes via `write_schema_registry_first_seen` in Bronze/Silver/Gold and propagated `OPS_DATASET` through wrappers |
| 15 | P15 | Persist lineage/version pointers | DONE | Added Bronze/Silver `ops.pipeline_runs` writes with `code_version`, `input_paths`, `output_paths`, and `partition_keys`; validated with focused tests |
| 16 | P16 | Wire DQ results to rule registry IDs | DONE | Added Silver `ops.dq_results` writes keyed by `rule_id` with severity, failed counts, and sample hashes; validated via silver tests |
| 17 | P17 | Composer/DAG updates for manifests + force controls | DONE | Updated Composer DAGs to check new manifest paths and propagate `force_reprocess`, `code_version`, and `ops_dataset` to Dataproc args |
| 18 | P18 | Add/update focused tests | DONE | Added `tests/test_ops_controls.py` (taxonomy, schema-registry first-seen, DQ rule/severity mapping) and ran focused suite: 12 passed |
| 19 | P19 | Sync technical docs | DONE | Updated Composer and scripts docs for daily schedules, manifest conventions, and `force_reprocess`/`code_version`/`ops_dataset` controls |
| 20 | P20 | Pipeline hardening sign-off checkpoint | DONE | Pipeline backlog P1-P19 completed with focused validations and tests passing |
| 21 | G1 | Finalize Phase 2A scope lock | DONE | Added `workingdocs/PHASE2A_SCOPE_LOCK.md` aligned to BRD read-only constraints and non-goals |
| 22 | G2 | FastAPI service skeleton + session model | DONE | Added `src/agent_service/app.py` (`/health`, `/sessions`, `/sessions/{id}`), updated dependencies, and validated with `tests/test_agent_service.py` |
| 23 | G3 | Router/coordinator runtime contract | DONE | Added `/router` contract endpoint with structured request/response models, deterministic `ops|dq` routing, read-only tool/evidence envelopes, and passing API tests |
| 24 | G4 | Read-only MCP tool contracts | DONE | Added `src/agent_service/tools.py` with allowlisted read-only contracts (BQ/GCS/Composer/Dataproc/schema diff) and role checks, with passing tests |
| 25 | G5 | Apigee edge policy scaffolding | DONE | Added `infra/gcp/apigee` OpenAPI v1 contract and starter policies for JWT verify, quota, and version/read-only routing headers |
| 26 | G6 | Retrieval/indexing + evidence refs | DONE | Added `src/agent_service/retrieval.py` with optional OpenAI embeddings + lexical fallback, integrated router evidence retrieval, and passing retrieval/API tests |
| 27 | G7 | Audit/proposal tables service integration | DONE | Added audit/proposal persistence module, wired session/router trace logging, added proposal endpoints, extended ops DDL/writers, and validated with 16 passing agent tests |
| 28 | G8 | Evaluation harness + CI regression gate | IN_PROGRESS | Next implementation task |

---

## Execution Rules
- Work one task at a time.
- Update this tracker status immediately after each task.
- Do not start GenAI tasks before pipeline checkpoint `P20`.
- Keep changes small and verifiable; run focused validations after each code update.
