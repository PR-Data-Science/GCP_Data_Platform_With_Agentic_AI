# Copilot instructions

## Project overview
- Python package in [src/llm_feedback_pipeline](src/llm_feedback_pipeline) handles ingestion, transformation, and load.
- Ingestion pulls REST/JSON/CSV sources and normalizes them to dict rows.
- Transformations are centralized in [src/llm_feedback_pipeline/transform/cleaning.py](src/llm_feedback_pipeline/transform/cleaning.py).
- BigQuery writes are in [src/llm_feedback_pipeline/load/bigquery.py](src/llm_feedback_pipeline/load/bigquery.py).
- Orchestration entrypoint is [src/llm_feedback_pipeline/orchestration/pipeline.py](src/llm_feedback_pipeline/orchestration/pipeline.py).

## Data flow
1. Load sources from config in [config/settings.yaml](config/settings.yaml).
2. Run `load_all()` to build a unified list of rows.
3. Normalize with `normalize_rows()`.
4. Load curated rows to BigQuery with `load_to_bigquery()`.

## Conventions
- Add new ingestion types by extending `load_source()` in `ingestion/sources.py`.
- Keep transformations pure and deterministic; avoid I/O in `transform/`.
- BigQuery writes should use `insert_rows_json` and raise on errors.

## Developer workflows
- Local ingest: `python scripts/run_local_ingest.py`.
- Lint/test: `make lint` and `make test`.

## Integrations
- GCS bucket and BigQuery dataset/table are configured in [config/settings.yaml](config/settings.yaml).
- Databricks notebooks/jobs live in [databricks](databricks).
