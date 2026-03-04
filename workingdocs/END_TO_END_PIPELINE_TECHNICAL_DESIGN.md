# End-to-End Technical Design: LLM Feedback Data Pipeline (Ingestion → Gold + Airflow Orchestration)

## Why this document exists
This is the single, detailed technical design for the complete pipeline currently built in this repository.
It consolidates stage-level guides into one end-to-end reference for:
- implementation understanding,
- production-style operations,
- troubleshooting and rerun strategy,
- interview and architecture walkthroughs.

---

## 1) Scope and objectives

### 1.1 In scope
- Multi-source ingestion (CSV + JSON batch flows; API utility support exists).
- Raw landing in GCS as canonical JSONL envelope.
- Bronze Dataproc Serverless normalization and partitioned parquet output.
- Silver Dataproc Serverless contract-driven transforms + DQ + deadletter.
- Gold Dataproc Serverless curated marts + optional BigQuery publish.
- BigQuery star schema materialization for analytics serving.
- Airflow/Composer orchestration for dependency-aware execution and reruns.

### 1.2 Out of scope
- Autonomous production write-actions beyond existing orchestrated jobs.
- Real-time streaming ingestion (current architecture is batch-first).

### 1.3 Design goals
1. Deterministic, replay-friendly data movement.
2. Clear layer boundaries (Raw, Bronze, Silver, Gold, BigQuery serving).
3. Idempotent reruns with run-level guardrails.
4. Practical observability via manifests and Dataproc/Airflow control plane.
5. Interview-grade clarity while remaining production-minded.

---

## 2) End-to-end architecture (current implemented)

```mermaid
flowchart LR
  A[Source Drops\nCSV + JSON] --> B[Ingestion\nsrc/ingestion/batch_to_gcs.py]
  B --> C[GCS Raw\nraw/<source>/dt=<date>/run_id=<id>/batch_id=<id>/part-00000.jsonl]

  C --> D[Dataproc Bronze\nsrc/bronze/bronze_ingest_dataproc.py]
  D --> E[GCS Bronze Parquet\npartition: ingest_date, source_type]

  E --> F[Dataproc Silver\nsrc/silver/silver_transform_dataproc.py]
  F --> G[GCS Silver Parquet Tables\nfeedback_step, ratings_long, execution_steps, violations, deadletter]

  G --> H[Dataproc Gold\nsrc/gold/gold_transform_dataproc.py]
  H --> I[GCS Gold Parquet Marts]
  H --> J[Optional BigQuery publish\ngold_* tables]
  J --> K[Star Schema SQL\ndim_* + fact_*]

   C --> O1[Ops & Audit (BigQuery)\nops.pipeline_runs]
   D --> O1
   F --> O1
   H --> O1
   J --> O1
   F --> O2[ops.dq_results]
   D --> O3[ops.schema_registry]
   F --> O4[ops.deadletter_summary]

  L[Cloud Composer Airflow DAGs] --> D
  L --> F
  L --> H
```

### 2.1 Control-plane architecture
- **Data plane**: GCS + Dataproc Serverless + BigQuery.
- **Orchestration plane**: Cloud Composer (Airflow DAGs).
- **Configuration plane**: Airflow Variable `llm_feedback_composer_config`, shell env vars, YAML config for local ingestion.

### 2.2 Production-Grade Standards (Single Env, Batch)

This document enforces a single-flow, batch-only production hardening standard.

#### A) Control plane ops tables (BigQuery)
Treat the following tables as first-class artifacts:

1. `ops.pipeline_runs`
   - `run_id`, `stage`, `status`, `start_ts`, `end_ts`, `duration_ms`, `input_count`, `output_count`, `deadletter_count`, `schema_hash`, `dataproc_batch_id`, `manifest_path`, `error_category`, `error_code`, `error_summary`, `code_version`
2. `ops.dq_results`
   - `run_id`, `stage`, `table_name`, `rule_id`, `severity`, `failed_count`, `sample_record_hashes`, `dq_pass`, `created_ts`
3. `ops.schema_registry`
   - `schema_hash`, `schema_json`, `first_seen_run_id`, `created_ts`, `source_type`
4. `ops.deadletter_summary`
   - `run_id`, `stage`, `rule_id`, `failure_reason`, `count`, `created_ts`

Mandatory rule:
- Every stage (Raw, Bronze, Silver, Gold, Publish) must write/update ops rows.

#### B) Uniform idempotency and processing scope
Mandatory across all stages:
- Processing scope is always `run_id`.
- Output partitioning includes `run_id` (or `ingest_date + run_id`).
- Each stage writes a stage manifest (`*_manifest.json`).
- Rerun behavior:
  - if manifest exists -> skip stage (default),
  - support `--force` to reprocess (optional controlled override).
- Each stage must update `ops.pipeline_runs` with status, counts, and timings.

#### C) Batch-only execution rule
- No streaming semantics are assumed in this hardening profile.
- All controls (idempotency, schema policy, deadletter, observability) are enforced per batch run.

---

## 3) Source ingestion design (CSV/JSON → Raw JSONL)

### 3.1 Sources and formats
- Human-rated batch: CSV.
- Auto-rated batch: JSON array.
- Existing utility support for API ingestion is present in repo, but validated execution path is file-batch.

### 3.2 Canonical raw envelope
Each ingested record is wrapped as:
- `meta`:
  - `run_id`, `ingest_ts`, `source_type`, `source_name`, `source_file`, `source_uri`,
  - `pod_name`, `pod_type`, `task_type`,
  - `schema_hash`, `record_hash`, `row_number`.
- `payload`: normalized business row.

### 3.3 Raw landing path convention
`raw/<source_name>/dt=<YYYY-MM-DD>/run_id=<UUID>/batch_id=<batch_id>/part-00000.jsonl`

Why this convention matters:
- replay-friendly by run/date,
- supportable in production incidents,
- deterministic lineage keys across downstream layers.

Implementation note:
- In `batch_to_gcs.py`, `dt` is derived from current UTC date (`today_yyyy_mm_dd()`), not passed as a CLI argument.
- In Composer full E2E DAG ingestion task, raw object path uses resolved DAG `ingest_date`.

### 3.4 Ingestion implementation
Primary module:
- `src/ingestion/batch_to_gcs.py`

Key behavior:
1. Validates input file existence.
2. Reads rows by source type (`iter_csv` / `iter_json_array`).
3. Normalizes payload shape (`normalize_payload`).
4. Computes schema and record hashes.
5. Writes local temp JSONL (`tmp/raw_jsonl/part-00000.jsonl`).
6. Uploads JSONL to raw bucket in canonical path.
7. Emits `run_id`, `batch_id`, `record_count`, `final_gcs_uri`.

### 3.5 Raw stage hardening contract (uniform idempotency)
- Processing scope: `run_id`.
- Partitioning: `dt + run_id` path scope is mandatory.
- Manifest: emit `{stage}_manifest.json` (stage=`raw`) using standardized evidence schema.
- Rerun policy: default skip if manifest exists; optional controlled `--force` override.
- Ops update: persist status/count/timing/errors to `ops.pipeline_runs`.
- Lineage/version pointers: persist `code_version` (git SHA), input/output path pointers, and partition keys.

---

## 4) Bronze design (Raw JSONL → Bronze parquet)

### 4.1 Module and runtime
- Transform job: `src/bronze/bronze_ingest_dataproc.py`
- Runtime: Dataproc Serverless PySpark.

### 4.2 Core transform behavior
1. Recursively reads raw JSONL under raw prefix.
2. Flattens `payload` into top-level columns.
3. Ensures mandatory metadata columns exist:
   - `run_id`, `ingest_ts`, `schema_hash`, `record_hash`, `source_type`, `raw_path`.
4. Derives fallback metadata from `meta`/path when required.
5. Adds/derives `ingest_date` from path (`ingest_date=` or `dt=` token).
6. Trims string columns.
7. Deduplicates on `run_id + record_hash`.
8. Writes parquet in append mode, partitioned by:
   - `ingest_date`,
   - `source_type` (if present).

### 4.3 Output location
- Root: `gs://<bronze_bucket>/<bronze_prefix>`
- Partition structure (hardening target): `ingest_date=YYYY-MM-DD/run_id=<run_id>/source_type=<csv|json|unknown>/...`

### 4.4 Idempotency guard
Before write, job checks manifest path:
`manifests/bronze/dt=<date>/run_id=<run_id>/manifest.json`

If manifest exists, stage is skipped by default; optional `--force` can reprocess.

### 4.5 Manifest payload (Bronze)
Standard stage manifest schema (`{stage}_manifest.json`):
- `run_id`, `stage`, `env`, `source_type`
- `input_paths`, `output_paths`
- `schema_hash`, `record_count_in`, `record_count_out`, `deadletter_count`
- `dataproc_batch_id` (if applicable)
- `start_ts`, `end_ts`, `duration_ms`
- `code_version` (git SHA)
- `error_category`, `error_code` (if failed)

Store in GCS:
- `manifests/bronze/dt=<date>/run_id=<run_id>/manifest.json`

Note:
- Manifests are evidence references and idempotency-check artifacts.

### 4.6 Bronze control plane updates
- Write/update `ops.pipeline_runs` with:
   - `run_id`, stage=`bronze`, status,
   - input/output counts,
   - deadletter count (if any),
   - duration/timing,
   - `dataproc_batch_id`, manifest path, error summary.
- Register schema state in `ops.schema_registry` (`schema_hash`, schema JSON snapshot, first seen run).
- Persist lineage/version pointers: `code_version`, input path(s), output path(s), partition keys.

Current implementation note:
- `job_start_ts` and `job_end_ts` are both populated at manifest-write time in current code.

---

## 5) Silver design (Bronze parquet → contract-driven Silver tables)

### 5.1 Module and runtime
- Transform job: `src/silver/silver_transform_dataproc.py`
- Runtime: Dataproc Serverless PySpark.

### 5.2 Silver transform behavior
1. Reads Bronze parquet for date/run scope.
2. Normalizes complex fields to known schemas:
   - ratings structs,
   - auto-rater struct,
   - execution JSON struct,
   - violations array.
3. Casts core numeric fields (step indexes).
4. Resolves final metric scores and label precedence.
5. Applies DQ rules.
6. Splits records into clean path and deadletter path.
7. Produces 5 Silver tables and writes manifest.

### 5.3 Data quality rules
Record fails if one or more of:
- missing `run_id`, `record_hash`, `prompt_id`, `query_text`, `evaluated_step_index`,
- negative `evaluated_step_index`,
- invalid `final_overall_label`,
- any final metric outside `[0, 5]`.

### 5.4 Silver outputs and grains
1. `feedback_step`: one row per evaluated step.
2. `ratings_long`: one row per `(record + metric + rater_type)`.
3. `execution_steps`: one row per execution trajectory step.
4. `violations`: one row per violation event.
5. `deadletter`: one row per failed Silver record with reasons + raw snapshot.

### 5.5 Silver deadletter/quarantine contract (minimum)
Each deadletter record must include:
- `run_id`
- `record_hash`
- `schema_hash`
- `failure_stage`
- `failure_reason`
- `rule_id` (when failure is rule-driven)
- `severity`
- `raw_path_ref` (GCS path/reference to original raw file)
- optional `payload_ref` or `payload_snippet_redacted` (if policy allows)
- `ingest_ts`

Additional requirement:
- Aggregate deadletter distribution must be written to `ops.deadletter_summary` (`run_id`, `stage`, `rule_id`, `failure_reason`, `count`, `created_ts`).

### 5.6 Partitioning and write mode
- Write mode: append.
- Partition (hardening target): `ingest_date + run_id`.

### 5.7 Silver idempotency guard
Manifest path check before write:
`manifests/silver/dt=<date>/run_id=<run_id>/manifest.json`

If manifest exists, stage is skipped by default; optional `--force` can reprocess.

### 5.8 Silver control plane updates
- Persist DQ detail rows to `ops.dq_results`.
- Persist stage summary to `ops.pipeline_runs` (status, counts, timings, errors, manifest path).
- DQ and deadletter outputs must reference `rule_id` from DQ Rule Registry.
- Persist lineage/version pointers: `code_version`, input/output pointers, partition keys.

### 5.9 DQ Rule Registry artifact
Required repository artifact:
- `dq/dq_rules.yaml`

Required fields:
- `rule_id`, `name`, `description`, `severity`, `target_table`, `logic_reference`, `owner`, `enabled`

Optional BigQuery mirror table:
- `ops.dq_rule_registry`

Reference contract:
- `ops.dq_results.rule_id` must map to DQ rule registry.
- Deadletter rows must carry `rule_id` for rule-driven failures.

---

## 6) Gold design (Silver tables → curated marts + BigQuery)

### 6.1 Module and runtime
- Transform job: `src/gold/gold_transform_dataproc.py`
- Runtime: Dataproc Serverless PySpark.

### 6.2 Gold transform behavior
Inputs:
- Silver `feedback_step`, `ratings_long`, `violations`.

Outputs (curated marts):
1. `training_supervised_examples`
2. `model_eval_step_metrics`
3. `model_eval_failure_breakdown`
4. `model_comparison_daily`
5. `rater_agreement`

### 6.3 Core Gold logic highlights
- Training table selects latest evaluated step per `(run_id, prompt_id)` and computes average quality score.
- Step metrics table creates operational/model quality view with `is_bad` flag.
- Failure breakdown combines label failures and violation-derived failures.
- Daily comparison aggregates quality by date/task/model.
- Rater agreement computes human-vs-auto absolute metric deltas where overlap exists.

### 6.4 Gold storage + partitioning
- Root: `gs://<gold_bucket>/<gold_prefix>`
- Write mode: append.
- Partition (hardening target): `ingest_date + run_id`.

### 6.5 Optional BigQuery publish
Enabled by `--publish_bigquery` + `--bq_project` + `--bq_dataset`.

Behavior:
- Writes each non-empty Gold DataFrame to `gold_<table_name>` in BigQuery.
- Uses Spark BigQuery connector with `temporaryGcsBucket=<gold_bucket>`.
- Empty DataFrames are skipped (table may not be created for that run).

### 6.6 Gold idempotency guard
Manifest check before write:
`manifests/gold/dt=<date>/run_id=<run_id>/manifest.json`

If manifest exists, stage is skipped by default; optional `--force` can reprocess.

### 6.7 Gold control plane updates
- Persist stage summary and publish status to `ops.pipeline_runs`.
- Gold outputs must retain `run_id` for traceability.
- Persist lineage/version pointers: `code_version`, input/output pointers, partition keys.

---

## 7) BigQuery serving model (post-Gold)

### 7.1 Layering decision
- Gold in GCS parquet remains transform output/system-of-record for this phase.
- BigQuery native tables are the analytics serving layer.

### 7.2 Star schema
Defined in:
- `sql/gold_star_schema.sql`

Materialized via:
- `scripts/run_gold_star_schema_bq.sh`

Objects:
- Dimensions: `dim_date`, `dim_model`, `dim_task`, `dim_step_type`, `dim_label`.
- Facts: `fact_model_eval_step`, `fact_failure_breakdown`.

### 7.3 Publish stage idempotency and ops updates
- Processing scope: `run_id`.
- Publish output scope/keying includes `ingest_date + run_id`.
- Publish manifest required: `{stage}_manifest.json` (stage=`publish`).
- Default rerun behavior: skip if manifest exists; optional `--force` for republish.
- Persist publish stage status/count/timing/errors into `ops.pipeline_runs`.
- Publish manifest path:
   - `manifests/publish/dt=<date>/run_id=<run_id>/manifest.json`
- Persist lineage/version pointers: `code_version`, input/output pointers, partition keys.

---

## 8) Airflow/Composer orchestration design

### 8.1 Composer DAGs in repo
1. `orchestration/composer/dags/llm_feedback_dataproc_orchestration.py`
   - DAG ID: `llm_feedback_dataproc_orchestration`
   - Purpose: orchestrate Dataproc stages from already-landed raw run_ids.

2. `orchestration/composer/dags/llm_feedback_full_e2e_composer.py`
   - DAG ID: `llm_feedback_full_e2e_composer`
   - Purpose: generate source batches, ingest to raw JSONL, then Bronze → Silver → Gold.

### 8.2 Dependency orchestration pattern
`Bronze -> Silver -> Gold` with dynamic task mapping per run.

Each stage also emits ops/audit updates to BigQuery (`ops.pipeline_runs`, `ops.dq_results`, `ops.schema_registry`, `ops.deadletter_summary`).

### 8.3 Rerun-aware planning (dependency-only DAG)
The DAG discovers run_ids from raw path and computes a stage plan using manifests:
- If Gold manifest exists: skip run.
- Else if Silver manifest exists: run Gold only.
- Else if Bronze manifest exists: run Silver then Gold.
- Else: run Bronze then Silver then Gold.

Override control:
- If `dag_run.conf.force_reprocess=true` (or config `force_reprocess=true`), planner bypasses manifest-based skipping and submits all stages for discovered runs, passing `--force` to stage jobs.

This gives branch-aware catch-up without replaying successful stages.

### 8.4 Full E2E DAG behavior
1. Load config variable.
2. Resolve ingest date.
3. Generate source JSON + CSV files directly to raw bucket `datasource/` path.
4. Convert generated source files to canonical raw JSONL and assign run_ids.
5. Build Dataproc batch submit plan for each run_id.
6. Submit Bronze, then Silver, then Gold jobs in strict dependency order.

### 8.5 Composer configuration variable
- Name: `llm_feedback_composer_config`
- Contains project, region, buckets, service account, prefixes, runtime spark properties, and dataset descriptors.
- Optional execution controls include `code_version`, `ops_dataset`, and `force_reprocess`.

Minimum required keys validated by DAG code:
- `project_id`, `region`, `service_account`,
- `raw_bucket`, `bronze_bucket`, `silver_bucket`, `gold_bucket`.

### 8.6 Execution controls
- `max_active_runs=1` for both DAGs.
- `default_args={"retries": 1}`.
- Dataproc submit uses dynamic mapping (`expand_kwargs`).
- Current DAG schedule for both orchestration DAGs is `0 0 * * *` (daily at 00:00 UTC).

---

## 9) Data contracts and path conventions

### 9.1 Layer contracts
- Raw: envelope contract (`meta + payload`) with lineage metadata.
- Bronze: normalized/trimmed parquet with mandatory metadata and dedupe.
- Silver: contract-driven analytical tables with DQ outcome separation.
- Gold: curated marts for training/analytics + optional serving publish.

### 9.2 Metadata propagation contract (non-negotiable)
Every Bronze/Silver/Gold output record must carry:
- `run_id`
- `ingest_ts`
- `schema_hash`
- `record_hash`
- stable business key(s) when available.

Gold outputs must retain `run_id` for end-to-end traceability.

### 9.3 Path conventions
- Raw:
  - `raw/<source>/dt=<date>/run_id=<run_id>/batch_id=<batch_id>/part-00000.jsonl`
- Standardized stage manifest path:
  - `manifests/<stage>/dt=<date>/run_id=<run_id>/manifest.json`

---

## 10) Schema evolution policy

### 10.1 Allowed automatically
- Additive fields only (nullable/new columns).

### 10.2 Block or quarantine
- Type changes.
- Field removals/renames.
- Breaking nested-structure changes.

### 10.3 Mechanism
1. Compute `schema_hash` for incoming batch scope.
2. Check `ops.schema_registry` for known schema lineage.
3. If drift is breaking:
    - route affected records/stage output to deadletter/quarantine,
    - fail stage by default (configurable),
    - write drift summary into ops tables.

### 10.4 Contract snapshots + versioning artifacts
Contracts stored and versioned in repository:
- `contracts/silver/*.json`
- `contracts/gold/*.json`

Contract governance:
- `contracts/README.md` defines allowed evolution (additive vs breaking).

Per-run requirement:
- compute `schema_hash` for incoming payload,
- when hash is first seen, persist schema snapshot to `ops.schema_registry`.

---

## 11) Idempotency and rerun strategy (end-to-end)

### 11.1 Layer-level controls
1. Ingestion:
   - emits unique `run_id` per ingestion execution,
   - emits raw stage manifest using standardized path/schema,
   - updates `ops.pipeline_runs`.
2. Bronze:
   - row dedupe on `run_id + record_hash`,
   - manifest existence guard for run-level rerun protection,
   - default skip-on-manifest with optional `--force`,
   - updates `ops.pipeline_runs`.
3. Silver:
   - run-level manifest guard before write,
   - default skip-on-manifest with optional `--force`,
   - updates `ops.pipeline_runs`, `ops.dq_results`, `ops.deadletter_summary`.
4. Gold:
   - run-level manifest guard before write,
   - default skip-on-manifest with optional `--force`,
   - updates `ops.pipeline_runs`.
5. Publish:
   - run-level publish manifest guard,
   - default skip-on-manifest with optional `--force`,
   - updates `ops.pipeline_runs`.

### 11.2 Rerun matrix
- Same run retriggered after success → guard prevents duplicate writes.
- Partial pipeline success:
  - Bronze done, Silver/Gold missing → resume from Silver.
  - Silver done, Gold missing → resume Gold only.
- New run same date → append safely under same date partition with different run_id.
- Forced rerun (`--force`) is explicit and auditable through ops tables.

---

## 12) Observability, auditability, and troubleshooting

### 12.1 Operational observability surfaces
- Airflow Graph/Task logs (dependency and retry visibility).
- Dataproc Batch history/logs (stage execution diagnostics).
- Per-stage manifests (fast run-level audit snapshot).
- BigQuery ops tables are the source-of-truth operational layer.

### 12.2 Minimal observability standards (mandatory)
Structured log fields required:
- `run_id`, `stage`, `batch_id`, `record_count`, `duration_ms`, `status`

Required per-stage metrics captured:
- `input_count`, `output_count`, `deadletter_count`, `duration`

Persistence rule:
- Stage summaries must be persisted to `ops.pipeline_runs`; logs alone are not sufficient.

### 12.3 Manifest-first operational checks
For each run_id + ingest_date:
1. Verify expected stage manifest exists.
2. Compare row counts against expectations.
3. Confirm downstream stage input availability.

### 12.4 Common failure classes and response
- Config/variable missing or invalid → fix `llm_feedback_composer_config` and rerun failed branch.
- IAM/permission failures → validate service account roles for GCS/Dataproc/BigQuery.
- Connector/staging issues in BigQuery publish → ensure temp GCS bucket option is set and accessible.
- Schema/contract mismatch → inspect deadletter + parser logic and patch schema handling.

### 12.5 Error taxonomy requirement
Standard error taxonomy categories:
- `AUTH`, `IAM_PERMISSION`, `SCHEMA_DRIFT`, `DQ_FAILURE`, `RUNTIME`, `CONFIG`, `DEPENDENCY`, `NETWORK`

Pipeline must populate `error_category` and `error_code` in:
- `ops.pipeline_runs`
- `{stage}_manifest.json` (if failed)

Mapping source:
- `src/ops/error_taxonomy.py`

---

## 13) GenAI-Readiness Outputs (Required Artifacts)

This section defines the additional pipeline artifacts required so future Phase 2 read-only GenAI workflows can consume trusted evidence.

### 13.1 Ops Control Plane Tables (BigQuery)
Pipeline must write/update these tables per `run_id`:

1. `ops.pipeline_runs`
   - `run_id`, `stage`, `status`, `start_ts`, `end_ts`, `duration_ms`, `input_count`, `output_count`, `deadletter_count`, `schema_hash`, `dataproc_batch_id`, `manifest_path`, `error_category`, `error_code`, `error_summary`, `code_version`

2. `ops.dq_results`
   - `run_id`, `stage`, `table_name`, `rule_id`, `severity`, `failed_count`, `sample_record_hashes`, `dq_pass`, `created_ts`

3. `ops.schema_registry`
   - `schema_hash`, `schema_json`, `first_seen_run_id`, `created_ts`, `source_type`

4. `ops.deadletter_summary`
   - `run_id`, `stage`, `rule_id`, `failure_reason`, `count`, `created_ts`

Mandatory statement:
- Ops tables are source of truth, not logs.

### 13.2 Evidence packaging (standard manifests)
Each stage (Raw/Bronze/Silver/Gold/Publish) emits `{stage}_manifest.json` with consistent schema:
- `run_id`, `stage`, `env`, `source_type`
- `input_paths`, `output_paths`
- `schema_hash`, `record_count_in`, `record_count_out`, `deadletter_count`
- `dataproc_batch_id` (if applicable)
- `start_ts`, `end_ts`, `duration_ms`
- `code_version` (git SHA)
- `error_category`, `error_code` (if failed)

Store in GCS:
- `manifests/<stage>/dt=<date>/run_id=<run_id>/manifest.json`

Manifest note:
- Manifests are used as evidence references and idempotency checks.

### 13.3 Lineage and version pointers
Persist `code_version` (git SHA) per run/stage in:
- `ops.pipeline_runs`
- stage manifests

Include pointers:
- input dataset/path(s)
- output dataset/path(s)
- partition keys (`dt`, `run_id`)

---

## 14) Security and governance boundaries

### 14.1 Identity and access
- Dataproc jobs run with configured service account from Composer variable.
- Buckets are separated by layer (`raw`, `bronze`, `silver`, `gold`) for clearer IAM and blast-radius control.

Secrets and IAM hygiene requirements:
- All secrets are managed in Secret Manager (no hardcoded credentials).
- Least-privilege service accounts per job/stage are required; single-SA mode is allowed only with scoped permissions.
- Airflow/Composer reads secrets via Secret Manager integration (or equivalently secured variable references).

### 14.2 Governance controls in current implementation
- Immutable-style append writes by layer.
- Run-scoped traceability via `run_id` and manifests.
- Deterministic hashes (`schema_hash`, `record_hash`) for integrity and drift support.

### 14.3 Secrets/config handling
- Runtime config in Airflow Variable and shell env vars.
- No hard-coded secrets in transform logic.

---

## 15) Performance and cost design

### 15.1 Current practical controls
- Dataproc Serverless (no persistent cluster overhead).
- Partitioning by `ingest_date` (and `source_type` in Bronze) to reduce scan cost.
- Append-mode writes to avoid full rewrites.
- Composer with controlled active runs and bounded retries.

### 15.2 Runtime tuning knob
`dataproc_properties` from Composer variable controls Spark resources, for example:
- executor instances,
- executor cores,
- driver cores,
- dynamic allocation behavior.

---

## 16) Testing and quality strategy

### 16.1 Test modules
- `tests/test_normalize_row.py`
- `tests/test_silver_transform.py`
- `tests/test_gold_transform.py`
- `tests/test_smoke.py`

### 16.2 Coverage intent
- Ingestion normalization correctness.
- Silver score-resolution and DQ behavior.
- Gold aggregation and agreement logic.
- Basic smoke sanity.

### 16.3 Runtime validation strategy
- Successful Dataproc batch completion per stage.
- Manifest row-count checks.
- BigQuery row-count validation for published tables.

---

## 17) Repo layout additions (required helper artifacts)

Required helper modules/files for standardized ops and evidence outputs:
- `sql/ops_tables.sql` (creates ops control-plane tables)
- `src/ops/ops_writer.py` (helper to write `pipeline_runs`/`dq_results`/`deadletter_summary`)
- `src/ops/error_taxonomy.py`
- `dq/dq_rules.yaml`
- `contracts/silver/`
- `contracts/gold/`
- `contracts/README.md`

---

## 18) End-to-end execution runbooks

### 18.1 Local/dev shell orchestrated run
Script:
- `scripts/run_full_pipeline_dev.sh`

Flow:
1. Generate sample data.
2. Upload source files to raw `datasource/` path.
3. Ingest to raw JSONL.
4. Run Bronze Dataproc.
5. Run Silver Dataproc.
6. Run Gold Dataproc (optional BQ publish).
7. Optional star schema build.

### 18.2 Composer-driven run
1. Deploy DAG(s) to Composer.
2. Set Airflow variable `llm_feedback_composer_config`.
3. Trigger:
   - full E2E DAG for generation+ingestion+transforms, or
   - dependency-only DAG for raw-to-gold orchestration.
4. Validate via Airflow + Dataproc + manifests.

---

## 19) Detailed sequence (single run_id path)

1. Source record lands in CSV/JSON drop.
2. Ingestion normalizes to canonical envelope and writes raw JSONL to GCS.
3. Bronze job reads raw JSONL and normalizes metadata.
4. Bronze deduplicates and writes partitioned parquet.
5. Silver reads Bronze run scope, parses nested structures, resolves scores, applies DQ.
6. Silver writes five curated tables and deadletter.
7. Gold reads Silver outputs, builds training + analytics marts.
8. Gold writes parquet, optionally publishes to BigQuery.
9. Each stage writes: manifest + ops table updates (runs, dq, deadletter, schema registry).
10. Airflow tracks orchestration state and supports targeted rerun on failure.

---

## 20) Known constraints and next hardening steps

### 20.1 Current constraints
- Empty Gold outputs may not create BigQuery tables in publish mode.
- Batch-first only (no streaming path in current design).
- Manifest write pattern is practical but not a full transactional checkpoint framework.

### 20.2 Recommended hardening next
1. Optional explicit empty-table creation policy for BigQuery serving consistency.
2. Automated manifest-vs-BigQuery reconciliation job.
3. CI gates for stage scripts and DAG validation.
4. Enhanced alert routing/SLA checks in Composer.

---

## 21) Quick interview summary

1. **Architecture**: GCS-lake layers + Dataproc transforms + Composer orchestration + optional BigQuery serving.
2. **Contracts**: canonical raw envelope and contract-driven Silver/Gold modeling.
3. **Idempotency**: run-level manifest guards across Bronze/Silver/Gold plus dedupe controls.
4. **Recovery**: branch-aware reruns from failed stage without replaying successful stages.
5. **Production mindset**: layer separation, metadata lineage, observability, and cost-conscious serverless execution.
