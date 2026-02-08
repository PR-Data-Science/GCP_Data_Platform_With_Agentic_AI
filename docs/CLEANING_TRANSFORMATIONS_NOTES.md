<!-- Placeholder for cleaning transformation notes. -->
# Databricks-on-GCP LLM Feedback Pipeline — Cleaning & Transformation Notes (Project Memory)

**Purpose of this file:** Persist the agreed understanding of *what cleaning + transformations we need* for LLM human-evaluation feedback data, and how this fits into the overall project (Part 1 vs Part 2).  
This document is intended to live in the repo (e.g., `docs/` or `project_notes/`) and act as a “single source of truth” for future steps.

---

## 1) North Star Architecture (Part 1 → Part 2)

### Part 1 — Data Platform (Databricks + GCP)
**Source (REST API / CSV / JSON)** → **GCS Raw (JSONL, immutable)**  
→ Databricks **Bronze (Delta)** → **Silver (Contract + DQ + Deadletter)** → **Gold (Curated)**  
→ Publish **Gold → BigQuery**  
→ Ops tables: `ops.pipeline_runs`, `ops.dq_results`, `ops.schema_registry`  
**Dev-first**. Prod later optional. But we keep **big-project patterns** early: config-driven environments, CI checks, IaC/CI-CD scaffolding.

### Part 2 — Agentic Data Engineering (Mandatory)
Add **RAG + Vertex AI ADK (Agents)** to propose/generate cleaning, validation, and transformation logic for schema-variant batches.  
Generated logic is:
- **Grounded** in contracts + metadata (schema registry, past mappings)
- **Gated** by automated tests + DQ checks
- **HITL-approved** (PR-based review/approval)
- **Versioned** (prompts, retrieval corpus, generated code)

---

## 2) Data Sources (What we expect in real life)

We will support:
1. **Batch files** (CSV/JSON/JSONL drops) — easiest MVP path.
2. **REST API** ingestion — common for continuous evaluation pipelines.
3. (Later) Streaming/event ingestion (Pub/Sub/Kafka) — only if needed; not Day-1.

**MVP ingestion choice:** Start with provided **sample CSV**, convert to **JSONL** for raw landing so downstream stays identical when we add REST API.

---

## 3) Raw Landing Standard (GCS Raw → Bronze)

All ingested records must include batch metadata for replayability, idempotency, and audit:
- `run_id` (UUID)
- `ingest_ts` (UTC)
- `schema_hash` (hash of canonical schema)
- `record_hash` (hash of canonicalized record payload)
- `source_name` / `source_type` (file/api)
- `raw_path` (GCS object path)

**Raw format target:** JSONL (1 record per line), append-only.

---

## 4) Cleaning & Transformations — What “Good” Looks Like

### 4.1 Silver Layer Goals (Contract + Standardization + DQ)
Silver is where we **enforce the schema contract** and create a reliable, consistent representation.

**A) Contract enforcement**
- Validate required fields exist (per contract).
- Enforce data types (string/float/int/bool/timestamp).
- Normalize numeric fields (e.g., ratings) safely.
- Standardize timestamps (UTC, consistent format).
- Keep `schema_hash` & record metadata from Bronze.

**B) Canonical text normalization**
- Trim leading/trailing spaces
- Normalize whitespace (multiple spaces → single)
- Handle encoding issues (non-printable characters)
- Keep original raw text fields where necessary (for traceability)

**C) Identifier normalization**
- Standardize keys like `batch_id`, `set_id`, `User_ID`, `Reviewer_ID`
- Ensure consistent casing conventions if needed (do not destroy meaning)

**D) Deduplication + idempotency**
- Use `record_hash` to deduplicate within a batch.
- Define deterministic merge keys for reruns (e.g., `batch_id + set_id + curator/reviewer id`).
- Ensure rerunning the same input does not create duplicates.

**E) Reviewer override model**
When multiple curators evaluate and a reviewer finalizes:
- Store both curator-level and reviewer-level judgments.
- Create a “final” view/column (reviewer if present, else curator aggregate).
- Track deltas: `curator_score` vs `reviewer_score`, override frequency.

**F) Nested structures normalization**
For arrays / nested objects (e.g., tool execution traces):
- Explode nested arrays into child tables:
  - `tool_execution_steps` (one row per tool step)
  - `violations` (one row per violation)
- Preserve ordering (step index) and linkage keys to parent record.

**G) Data Quality (DQ) gates**
Minimum DQ checks for Silver:
- Null checks on required columns
- Valid range checks (e.g., rating bounds / allowed increments)
- Row-count drift thresholds (batch-to-batch sanity)
- Duplicate checks based on merge keys / record_hash
- Referential integrity between parent and exploded child tables

**H) Deadletter / quarantine**
- Any record failing contract/DQ gets routed to:
  - `deadletter` Delta table
  - GCS quarantine path
- Store `failure_reason`, `failed_rules`, and raw payload pointer.

---

### 4.2 Gold Layer Goals (Curated Outputs for Training + Analytics)
Gold produces **consumer-ready tables** with stable semantics. Two lanes:

**Lane 1 — Training-ready datasets**
Examples (depending on what the source contains):
- **Preference dataset**: query + candidate outputs + chosen label (if present)
- **Supervised eval dataset**: query + final rating + reasoning + violations
- **Tool-trajectory dataset**: query + ordered tool steps + final outcome labels

**Lane 2 — Analytics-ready datasets**
- Batch KPIs: avg rating, violation rate, reviewer override rate
- Rater agreement: curator vs reviewer delta distributions
- Trend tables by batch/time/model/topic (when fields exist)
- Tool usage metrics: tool calls per response, step count, highlighted step stats

**Important:** Gold should avoid raw messiness; the core idea is **stable tables for BI + ML**.

---

## 5) Schema Drift Strategy (Pragmatic Default)

Inputs may arrive with extra fields not in the contract. Default approach:
- Keep unknown fields in `rescued_data` (JSON/map column) in Silver
- Track schema changes via `schema_hash` + `ops.schema_registry`
- Do not break the pipeline on additive drift; do break on incompatible drift for required fields/types.

---

## 6) What we are *not* doing early (scope control)

- No near-real-time streaming until batch pipeline is stable.
- No complex feature store, no heavy ML training orchestration inside this project.
- Part 2 agents will be introduced only after Part 1 runs end-to-end reliably.

---

## 7) Deliverables that prove “Interview-Ready”

- End-to-end run: **Ingest → Bronze → Silver (DQ + deadletter) → Gold → BigQuery**
- Reruns are safe (idempotent)
- Ops tables show run status + DQ outcomes
- Clear explanation of:
  - architecture
  - tradeoffs
  - failure modes + recovery
  - schema evolution strategy
  - agent safety + HITL gating (Part 2)
