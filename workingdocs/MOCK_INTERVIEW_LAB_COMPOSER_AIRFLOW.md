# Mock Interview Lab: Airflow + Composer + Dataproc (Hands-on)

## Why this document exists
This lab is designed to help you practice exactly what interviewers ask for:
- orchestration design in Airflow,
- dependency handling and rerun strategy,
- failure recovery,
- cost awareness,
- and clear communication of trade-offs.

It uses your current project assets and Composer DAGs so your practice is realistic.

---

## Lab goal
By the end of this lab, you should be able to confidently explain and demonstrate:
1. How the DAG executes stage dependencies.
2. How idempotency prevents duplicate processing.
3. How to recover from stage failures.
4. How to run targeted reruns (Silver/Gold only).
5. How to control cloud cost in orchestration design.

---

## Prerequisites
- Cloud Composer environment created and healthy.
- GCP project access for:
  - Composer,
  - Dataproc Serverless,
  - GCS buckets (raw/bronze/silver/gold),
  - optional BigQuery.
- DAG files uploaded:
  - `orchestration/composer/dags/llm_feedback_dataproc_orchestration.py`
  - `orchestration/composer/dags/llm_feedback_full_e2e_composer.py`
- Airflow Variable configured:
  - `llm_feedback_composer_config`

---

## Lab format (recommended 75 minutes)
- Phase A (10 min): setup verification
- Phase B (20 min): baseline successful run
- Phase C (20 min): failure injection + recovery
- Phase D (15 min): rerun/idempotency demo
- Phase E (10 min): interview Q&A drill

---

## Phase A — Setup verification (10 min)

### Task A1: Validate Composer + DAG visibility
- Open Airflow UI.
- Confirm both DAGs are present:
  - `llm_feedback_dataproc_orchestration`
  - `llm_feedback_full_e2e_composer`

### Task A2: Validate config
- Confirm Airflow Variable `llm_feedback_composer_config` exists.
- Verify values for:
  - `project_id`, `region`,
  - `raw_bucket`, `bronze_bucket`, `silver_bucket`, `gold_bucket`,
  - `service_account`,
  - `dataproc_properties`.

### Expected outcome
- DAGs are visible and parse successfully.
- No immediate import/runtime config issues in Airflow.

---

## Phase B — Baseline successful run (20 min)

### Option 1 (best for full story): Run full E2E DAG
Trigger DAG: `llm_feedback_full_e2e_composer`

Suggested run config:
```json
{
  "ingest_date": "2026-02-24"
}
```

What should happen:
1. Source batches generated (CSV + JSON) to raw datasource path.
2. Ingestion creates raw JSONL for each generated batch with separate `run_id`s.
3. Dataproc Bronze runs per `run_id`.
4. Dataproc Silver runs per `run_id`.
5. Dataproc Gold runs per `run_id`.

### Option 2: Dependency-only DAG (raw already exists)
Trigger DAG: `llm_feedback_dataproc_orchestration`

Suggested run config:
```json
{
  "ingest_date": "2026-02-22"
}
```

### Verification checklist
- Airflow Graph: all tasks green.
- Dataproc Batch list: Bronze/Silver/Gold submitted and successful.
- Manifest files exist in:
  - `silver/ingest_date=.../_manifests/`
  - `gold/ingest_date=.../_manifests/`

---

## Phase C — Failure injection + recovery (20 min)

### Task C1: Inject controlled failure
Use one of these methods:
- Temporarily set invalid Dataproc property in Variable (for one run), OR
- Temporarily set wrong bucket name in Variable.

Trigger run and observe failure stage.

### Task C2: Diagnose quickly
Capture:
- failing Airflow task ID,
- Dataproc batch ID,
- error class/message,
- impact scope (one branch or full DAG).

### Task C3: Recover
- Fix bad config.
- Rerun from failed task (or clear failed task and retry).

### Expected interviewer-ready explanation
- “Failure occurred in `<stage>` because `<root cause>`.
- Upstream tasks remained valid; downstream blocked by dependency.
- After config fix, rerun resumed safely without raw/bronze duplication due to manifest guard + run scoping.”

---

## Phase D — Rerun + idempotency demo (15 min)

### Task D1: Trigger same ingest date again
- Re-run with the same `ingest_date` as a completed run.

### Task D2: Explain behavior
- Which tasks are skipped or reduced and why?
- How manifests control rerun scope:
  - Gold already present → skip branch,
  - Silver present + Gold missing → run Gold only,
  - Bronze present + Silver/Gold missing → run Silver then Gold.

### What to say in interview
- “The DAG is rerun-safe and branch-aware by manifest checks at each stage.
  This gives idempotency and efficient recovery without duplicate writes.”

---

## Phase E — Mock interview Q&A drill (10 min)

Use these prompts and answer out loud in 60–90 seconds each.

### Q1: Why Airflow/Composer for this pipeline?
Expected points:
- explicit dependencies,
- retries/alerts,
- operational visibility,
- backfills and reruns,
- managed control plane in Composer.

### Q2: How do you prevent duplicate processing?
Expected points:
- per-run manifest checks in Bronze/Silver/Gold,
- run_id scoping,
- append mode with idempotent guard.

### Q3: How do you recover from failures?
Expected points:
- identify failed task and Dataproc batch,
- fix root cause,
- rerun failed branch only,
- no need to replay successful upstream stages.

### Q4: How do you reduce costs?
Expected points:
- low-frequency schedules,
- constrained concurrency,
- minimal Dataproc profile that is stable,
- stop/pause non-critical DAGs,
- short-retention logs and storage lifecycle policies.

### Q5: How would you productionize this?
Expected points:
- env separation,
- secret management,
- SLAs and alert routing,
- data quality contracts,
- CI/CD for DAGs,
- observability dashboard.

---

## Scoring rubric (self-assessment)
Score each 0–2:
- Architecture clarity
- Dependency reasoning
- Failure diagnosis
- Recovery strategy
- Idempotency explanation
- Cost optimization
- Communication confidence

### Score bands
- 0–6: needs more practice
- 7–10: interview-ready with some gaps
- 11–14: strong interview performance

---

## One-page interview summary template
Use this before interviews:

1. **Pipeline**: Composer DAG orchestrates ingestion -> Bronze -> Silver -> Gold on Dataproc Serverless.
2. **Dependency model**: explicit stage chain with branch handling per `run_id`.
3. **Idempotency**: manifest existence checks at each stage prevent duplicate writes.
4. **Recovery**: rerun from failed task/branch; avoid replaying successful stages.
5. **Cost controls**: low schedule frequency, controlled concurrency, right-sized Dataproc settings.
6. **Hands-on proof**: mention one real incident you resolved (e.g., quota failure in Silver and targeted rerun).

---

## Optional advanced drills
- Add SLA miss callback and alert simulation.
- Add DAG-level guardrail for max run_ids per execution.
- Add data contract check task before Bronze.
- Add BigQuery publish validation task after Gold.

---

## Your next best practice session
Repeat this lab once more with a different failure type:
- IAM permission denial,
- missing source file,
- malformed config.

If you can confidently explain diagnosis and recovery for all three, you are interview-strong.
