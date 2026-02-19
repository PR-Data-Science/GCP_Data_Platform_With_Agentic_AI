# Raw Landing Format

## Why JSONL for Raw (3–5 bullets)
- Supports large batches, streaming, and append-only ingestion.
- Works uniformly for CSV/JSON/API inputs.
- Handles nested/variable-length structures in payload.

## Record Schema (meta + payload)
```json
{"meta":{"run_id":"550e8400-e29b-41d4-a716-446655440000","ingest_ts":"2026-02-08T12:34:56Z","source_type":"csv","source_name":"llm_feedback_eval","source_file":"feedback_2026-02-08.csv","source_uri":"gs://example-raw-bucket/drop/team-a/2026-02-08/feedback_2026-02-08.csv","schema_hash":"f5c3c0d8b3f0e5c8a9d2c7b1f0e2d3c4b5a6f7e8d9c0b1a2f3e4d5c6b7a8f9e0","record_hash":"6a9f5c2d1e0b4a8f7c6d5e4b3a2f1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4","row_number":42},"payload":{"user_id":"u-123","ratings":[{"aspect":"helpfulness","score":5},{"aspect":"accuracy","score":4}],"comment":{"text":"Great answer","lang":"en"}}}
```
- `meta.run_id` (uuid4 string)
- `meta.ingest_ts` (UTC ISO8601 ending with Z)
- `meta.source_type` (csv|json|api)
- `meta.source_name` (string, e.g., llm_feedback_eval)
- `meta.source_file` (original filename if file-based)
- `meta.source_uri` (gs:// or file:// or https://)
- `meta.schema_hash` (sha256 hex)
- `meta.record_hash` (sha256 hex)
- `meta.row_number` (int)
- `payload` (original record, may contain nested arrays/objects)

## Allowed Minimal Parsing at Ingestion (do NOT call it cleaning)
- If an input CSV column contains a valid JSON string (starts with { or [ and can be parsed), it may be parsed into a JSON object/array inside payload.
- Otherwise preserve as string/null.
- No dedup, no flattening, no business rules in raw.

## GCS Path Convention
- gs://{raw_bucket}/raw/{source_name}/dt=YYYY-MM-DD/run_id={run_id}/part-00000.jsonl
