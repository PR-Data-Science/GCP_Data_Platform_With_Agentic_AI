# Source Contracts (MVP)

## MVP Sources (Part 1)
- Sources are batch file drops only: CSV or JSON.
- Batches can arrive multiple times per day ("as per batch").
- Ingestion starts when a new batch file appears in the drop folder.
- Note: Our sample file is small, but in real production batches can be much larger (e.g., 10k+ records) and payloads can be large/nested.

## Drop Folder Convention (Input)
- Define: gs://<raw_bucket>/drop/<team>/<yyyy-mm-dd>/<filename>
- Different teams may drop into different <team> folders.

## Raw Landing Convention (System of Record)
- Define: gs://<raw_bucket>/raw/llm_feedback_eval/dt=YYYY-MM-DD/run_id=<uuid>/part-00000.jsonl

## Trigger + Idempotency Notes (high-level)
- Trigger is "new file detected" (polling/orchestration comes later).
- Raw landing includes run_id, schema_hash, record_hash for traceability and safe reruns.

## Part 2 Note (API)
- In Part 2 we will add an hourly API pull job to export completed batches as JSON and land them into the same raw JSONL format (no more detail).
