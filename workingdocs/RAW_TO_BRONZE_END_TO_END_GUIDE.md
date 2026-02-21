# Raw to Bronze End-to-End Guide (Current Executed Flow)

## Why this document exists
This is your **single source of truth** for what is currently implemented and tested in this project up to Bronze.
It is written in simple language, but uses real engineering terms so you can explain this confidently in interviews.

---

## 1) Big picture in one minute

We currently run a **batch-first data platform flow**:

1. New human-rated (CSV) and auto-rated (JSON) files arrive as batch drops.
2. Ingestion normalizes these files into a common **raw JSONL envelope** (`meta` + `payload`).
3. Raw JSONL is stored in GCS under a replay-friendly folder structure.
4. Dataproc Serverless Bronze job reads raw JSONL, standardizes metadata, deduplicates per run, and writes partitioned Parquet.
5. Bronze outputs are stored in a dedicated Bronze bucket with per-run manifests.

This flow has been executed successfully with both **HUMAN** and **AUTO** batches.

---

## 2) Architecture flow (visual)

```mermaid
flowchart LR
  A[Batch Drop Files\nCSV + JSON] --> B[Ingestion: batch_to_gcs.py]
  B --> C[Local JSONL temp\ntmp/raw_jsonl/part-00000.jsonl]
  C --> D[GCS Raw Bucket\nraw/<source>/dt=<date>/run_id=<uuid>/batch_id=<id>/part-00000.jsonl]

  D --> E[Dataproc Serverless\nbronze_ingest_dataproc.py]
  E --> F[Schema normalization\nmeta/payload flatten + metadata derivation]
  F --> G[Dedup + partition write\nParquet Snappy]

  G --> H[GCS Bronze Bucket\nbronze/ingest_date=YYYY-MM-DD/source_type=<csv|json>/part-*.parquet]
  G --> I[Per-run manifest\nbronze/.../_manifests/run_id=<run_id>.json]

  E --> J[Dataproc staging/temp buckets\nlogs + runtime artifacts]
```

---

## 3) Integration types currently supported

### Implemented and used
- **File batch ingestion**
  - CSV input (human-rated)
  - JSON array input (auto-rated)

### Present in codebase (not the main path you just executed)
- API ingestion utilities also exist, but the validated current run is file-batch driven.

---

## 4) File types across stages

- **Input drop files**: `.csv`, `.json`
- **Raw storage format**: `.jsonl` (line-delimited JSON)
- **Bronze storage format**: `.parquet` with snappy compression
- **Operational metadata**: manifest `.json`

---

## 5) How new data arrives at ingestion

### Business view
A new dataset (human or auto) is treated as a **new run**. Every run gets a unique `run_id` for traceability.

### Technical view
- Source drop files are generated or placed under `data/drop/.../batch_id=.../`
- Ingestion command reads each file and writes a normalized JSONL stream to raw GCS.
- Raw path pattern:

`raw/<source_name>/dt=<YYYY-MM-DD>/run_id=<UUID>/batch_id=<batch_id>/part-00000.jsonl`

This structure makes replay and debugging easy:
- `dt` = ingestion date partition
- `run_id` = unique execution identity
- `batch_id` = business batch identity

---

## 6) What each important code file does

## Ingestion side
- `src/ingestion/batch_to_gcs.py`
  - Reads CSV/JSON input.
  - Normalizes rows to a shared payload format.
  - Adds metadata (`run_id`, `ingest_ts`, `schema_hash`, `record_hash`, etc.).
  - Writes JSONL and uploads to raw bucket.

- `src/ingestion/normalize_row.py`
  - Ensures incoming row shapes become consistent payloads.

- `scripts/run_ingestion_dev.sh`
  - Dev runner that ingests both auto and human sample batches.

- `src/tools/generate_sample_batches.py`
  - Generates deterministic sample HUMAN and AUTO input files.

## Bronze side
- `src/bronze/bronze_ingest_dataproc.py`
  - Dataproc Serverless Bronze transform job.
  - Reads raw JSONL recursively.
  - Flattens `payload` and standardizes metadata.
  - Derives missing metadata from `meta` or path when needed.
  - Applies run/date filters.
  - Deduplicates on (`run_id`, `record_hash`).
  - Writes partitioned Parquet to Bronze bucket.
  - Writes per-run manifest.
  - Enforces rerun guard for idempotency.

- `scripts/run_bronze_dataproc.sh`
  - Submits the Bronze job to Dataproc Serverless.
  - Supports separate `RAW_BUCKET` and `BRONZE_BUCKET`.
  - Uploads latest job script before submit.

---

## 7) Intermediate artifacts created during execution

### Local workspace artifacts
- Temporary JSONL at `tmp/raw_jsonl/part-00000.jsonl` during ingestion.

### Raw bucket artifacts
- JSONL file in run-specific folder under `raw/.../run_id=.../batch_id=.../part-00000.jsonl`.

### Bronze bucket artifacts
- Partitioned parquet files, e.g.:
  - `bronze/ingest_date=2026-02-21/source_type=json/part-....parquet`
  - `bronze/ingest_date=2026-02-21/source_type=csv/part-....parquet`
- Per-run manifest:
  - `bronze/ingest_date=2026-02-21/_manifests/run_id=<run_id>.json`

### Dataproc system artifacts
- Staging and temp buckets store runtime logs, metadata, and job internals.

---

## 8) Bronze partition strategy (current)

Current partition columns:
- `ingest_date` (primary)
- `source_type` (secondary, when available)

Why this is good:
- `ingest_date` supports operational slice-by-day.
- `source_type` separates CSV and JSON workload shape.
- Avoiding `run_id` as a partition reduces high-cardinality partition explosion.

---

## 9) Append behavior at Bronze (how data accumulates)

Write mode is append.

Meaning:
- New runs for same day append into the same day partition.
- If you process both HUMAN and AUTO on same day, you will see two source_type folders.
- This is expected and correct.

---

## 10) Idempotency and rerun safety (current hardening)

Two layers now protect repeatability:

1. **Row-level dedup in run output**
   - Drop duplicates by (`run_id`, `record_hash`) before write.

2. **Run-level rerun guard**
   - Before writing, job checks if per-run manifest already exists:
     - `.../_manifests/run_id=<run_id>.json`
   - If exists, job fails fast to prevent duplicate append writes.

This gives you safer operations for accidental reruns of same run_id.

---

## 11) What is a manifest and why it matters

A manifest is a compact run-summary JSON with fields like:
- `run_id`
- `output_path`
- `row_count`
- `schema_hash`
- `job_start_ts`, `job_end_ts`

Why it helps:
- Fast audit trail without scanning parquet.
- Run-level traceability for support/debug.
- Foundation for future pipeline observability and SLA checks.

Important improvement already done:
- Manifests are now **per run** (not one file overwritten per date).

---

## 12) Example of real executed runs

Validated run pattern from current execution:
- AUTO run produced 162 rows.
- HUMAN run produced 59 rows.
- Both landed in Bronze under same `ingest_date` and different `source_type` partitions.
- Combined partition contains both run_ids as expected.

---

## 13) Operational runbook (current)

1. Generate or receive new batch files (CSV/JSON).
2. Run ingestion to raw JSONL.
3. Submit Bronze Dataproc job with:
   - `RAW_BUCKET`
   - `BRONZE_BUCKET`
   - `RUN_ID`
   - `INGEST_DATE`
   - `BATCH_NAME`
4. Validate:
   - Bronze parquet exists in expected partition.
   - Per-run manifest exists.
   - Row counts and run_id counts look right.

---

## 14) Interview-ready talking points

This section is written as **"what it means" + "why it matters" + "how to say it in interview"**.

### A) If asked: “How is your pipeline production-minded even at MVP stage?”

- **We enforce a canonical raw envelope with metadata and hashes.**
  - **In simple words:** No matter what input file comes (CSV/JSON), we wrap every record in one standard structure (`meta` + `payload`).
  - **Why this is important:** Downstream jobs do not break because they always read the same shape. We also keep `run_id`, timestamps, and hashes for trust.
  - **How to say it:** “We standardized raw records into one contract, so processing remains stable even when source formats vary.”

- **We use immutable raw landing with replay-friendly path keys.**
  - **In simple words:** Once raw data lands in GCS, we do not edit it. We store it by date, run ID, and batch ID.
  - **Why this is important:** If something fails, we can reprocess the exact same raw data. This is critical for debugging and compliance-style audits.
  - **How to say it:** “Raw is append-only and replayable by design, which gives us deterministic reruns and easier root-cause analysis.”

- **Bronze uses partitioned parquet for analytical efficiency.**
  - **In simple words:** We save Bronze in Parquet and split folders by `ingest_date` and `source_type`.
  - **Why this is important:** Query engines read less data and run faster. Storage is also more efficient than plain JSON.
  - **How to say it:** “We optimized Bronze for both compute and storage by using columnar Parquet with practical partitions.”

- **We hardened idempotency with both dedup and rerun guard.**
  - **In simple words:** We prevent duplicate data in two ways: record-level dedup and run-level stop check.
  - **Why this is important:** If someone reruns the same run accidentally, data quality won’t silently degrade.
  - **How to say it:** “We implemented defensive idempotency, so repeated executions don’t create duplicate business records.”

- **We write per-run manifests for auditability and traceability.**
  - **In simple words:** Every run writes a small JSON summary (rows written, schema hash, run ID, output path, timestamps).
  - **Why this is important:** Ops teams can validate outcomes quickly without scanning full parquet.
  - **How to say it:** “Each run produces an auditable control artifact, which improves observability and supportability.”

- **We separated RAW and BRONZE buckets to align with lakehouse layer boundaries.**
  - **In simple words:** Raw data and Bronze transformed data are stored in different buckets.
  - **Why this is important:** Cleaner data governance, clearer ownership, safer permissions, and lower chance of accidental cross-layer impact.
  - **How to say it:** “We enforce physical layer separation, which is a strong data-platform governance practice even at MVP.”

### B) If asked: “How do you handle mixed input schemas?”

- **We normalize records into a standard payload model.**
  - **In simple words:** Different source fields are mapped into one consistent payload model before Bronze processing.
  - **Why this is important:** One transformation job can handle multiple source types without custom branching everywhere.
  - **How to say it:** “We apply schema harmonization early, so downstream logic stays predictable and maintainable.”

- **Bronze can derive mandatory metadata even when source shape differs.**
  - **In simple words:** If metadata is missing in one source, we derive it from `meta`, file path, or controlled defaults.
  - **Why this is important:** The pipeline remains resilient to source variability and still preserves core lineage fields.
  - **How to say it:** “We built metadata fallback logic, so required governance columns are always populated.”

- **We keep schema-driven hashing for consistency checks.**
  - **In simple words:** We generate hash values for schema and records to detect drift/duplicates reliably.
  - **Why this is important:** Hashes are lightweight control signals for quality checks, dedup logic, and change detection.
  - **How to say it:** “We use deterministic hashing as a low-cost integrity control across ingestion and Bronze.”

### C) Common follow-up questions and strong answers

Use these when interviewers ask “one level deeper” questions.

- **Q1) Why did you choose JSONL for Raw instead of parquet directly?**
  - **Answer:** “Raw is our immutable source-of-truth layer. JSONL preserves original payload flexibility and is easy to append, replay, and debug line-by-line. We optimize for analytics at Bronze using parquet.”

- **Q2) Why not partition Bronze by run_id too?**
  - **Answer:** “`run_id` is high-cardinality. Partitioning by it creates too many tiny folders/files and hurts performance. We keep `run_id` as a column and partition by `ingest_date` and `source_type` for better operational and query balance.”

- **Q3) How do you prevent duplicate data when a job reruns?**
  - **Answer:** “Two layers: record-level dedup on (`run_id`, `record_hash`) and run-level rerun guard using per-run manifest existence check. That prevents accidental duplicate appends.”

- **Q4) What if the source schema changes unexpectedly?**
  - **Answer:** “We normalize to a standard payload shape and derive mandatory metadata from `meta` or path when needed. Hashing helps detect structural differences, and the pipeline remains resilient to additive variation.”

#### Deep-dive for Q4 (with practical example)

Use this deeper version when interviewer asks “Okay, but what happens in real runs?”

- **Example scenario**
  - **Today:** source has `prompt_id`, `query_text`, `step_index`.
  - **Tomorrow:** source adds new field `model_latency_ms`.

- **What happens in current flow (Raw → Bronze)**
  - Ingestion still wraps records into the same `meta + payload` envelope.
  - Bronze still works because required metadata is derived/fallback-safe.
  - New field can travel in payload; old rows naturally don’t have it.
  - Result: **pipeline does not break for additive optional fields**.

- **Why this does not break current ingestions**
  - The job does not depend on one rigid source object shape.
  - Mandatory governance fields (`run_id`, `ingest_ts`, `record_hash`, etc.) are protected by normalization + fallback logic.
  - Hashing gives visibility into structural drift.

- **What helps next stages (Silver/Gold) because of this design**
  - Bronze preserves data and lineage even during schema evolution.
  - Silver/Gold teams can evolve contracts with confidence because raw/bronze replay is stable.
  - You can backfill or enrich later without losing source traceability.

- **Will new fields be created automatically in later stages?**
  - **Short answer:** partially.
  - Bronze is flexible for additive changes.
  - Silver/Gold/loading layers are usually more contract-driven:
    - If transform code is generic pass-through, field may appear automatically.
    - If transform code selects fixed columns/business logic, you must update scripts manually.

- **Do we need manual changes for transformations/loading?**
  - For business-critical new columns: **yes, usually manual update is required**.
  - Typical updates include:
    - adding column mapping logic,
    - validation rule updates,
    - model/table schema migration,
    - test updates.

- **What about old historical data?**
  - Old records won’t magically get values for new fields.
  - Standard behavior: old rows show `null` for the new column.
  - If business needs historical comparability, define a backfill strategy:
    - default value,
    - derived value,
    - or explicit `not_available` semantics.

- **When schema change is risky (likely to break)**
  - Required field removed or renamed unexpectedly.
  - Type changes on critical columns used in joins, scoring, or rules.
  - In these cases, treat as contract change and update transform/load scripts before promotion.

- **Interview one-liner**
  - “Additive changes are usually non-breaking due to normalization and fallback metadata derivation, while breaking contract changes are intentionally surfaced and handled through controlled transform/schema updates.”

- **Q5) Why separate RAW and BRONZE buckets?**
  - **Answer:** “It enforces layer boundaries physically. This improves governance, permissioning, blast-radius control, and operational clarity.”

- **Q6) How do you know what happened in each run without scanning all data?**
  - **Answer:** “Each run writes a manifest with run_id, row_count, schema_hash, output_path, and timing. This is our quick operational control plane for audits and troubleshooting.”

- **Q7) How do you replay a failed run?**
  - **Answer:** “Raw data is immutable and organized by date/run/batch path keys. We can reprocess the exact same raw slice deterministically by reusing run metadata and run parameters.”

- **Q8) Why Dataproc Serverless for Bronze?**
  - **Answer:** “It gives Spark scale without cluster management overhead. For a batch-heavy Bronze layer, that’s a strong cost/ops tradeoff while still using standard PySpark code.”

- **Q9) What are current known limits in this stage?**
  - **Answer:** “This stage is up to Bronze only. Silver DQ contracts, deadletter routing, Gold modeling, and serving-layer SLAs are next milestones and are intentionally separated for phased delivery.”

- **Q10) What metrics would you track in production for this flow?**
  - **Answer:** “Per-run row counts, input vs output drift, duplicate counts removed, run latency, failure rate, and manifest completeness. These are practical health indicators before advanced observability.”

- **Q11) How does this design support future agentic transformations?**
  - **Answer:** “Because we stabilized contracts, metadata, manifests, and replayability first. Agent-generated logic works best when execution data has reliable structure and lineage.”

- **Q12) If asked for a short architecture pitch (30–45 sec):**
  - **Answer:** “We ingest mixed CSV/JSON batches, normalize to immutable raw JSONL with strong metadata, transform on Dataproc Serverless into partitioned Bronze parquet, enforce idempotency with dedup + rerun guard, and generate per-run manifests for traceability. This gives us a production-minded base for Silver/Gold expansion.”

---

## 15) Scope boundary (what is not covered yet)

This guide covers only up to Bronze.
Not yet covered in this flow:
- Silver contract/DQ/deadletter pipeline
- Gold modeling
- BigQuery serving layer
- Agentic transformation generation layer

These will be documented in next-stage guides using the same pattern.

---

## 16) Quick glossary (simple language)

- **Batch**: one dropped file set processed together.
- **Run**: one execution instance with unique `run_id`.
- **Raw**: immutable source-traceable landing zone.
- **Bronze**: lightly standardized analytics-ready storage layer.
- **Partition**: folder-based segmentation to speed reads.
- **Idempotency**: rerunning does not create wrong duplicates.
- **Manifest**: run summary metadata JSON.
- **Dataproc Serverless**: managed Spark execution without cluster management.

---

## 17) Recommendation for next stage

Before Silver build-out, keep this Bronze contract stable:
- always emit per-run manifests
- preserve rerun guard behavior
- keep run-level QA checks automated

This will reduce downstream data quality surprises.
