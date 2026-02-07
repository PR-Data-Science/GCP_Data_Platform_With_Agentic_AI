# GCP LLM Feedback Data Pipeline

Production-grade pipeline for ingesting LLM feedback data from REST APIs, JSON, and CSV, landing raw data in GCS, transforming in Databricks, and loading curated outputs into BigQuery.

## Architecture (high level)
- **Ingestion**: Fetch from REST APIs or read JSON/CSV, normalize to a common schema, and write raw records to GCS.
- **Transform**: Databricks job reads raw data from GCS, cleans and enriches, and writes curated datasets.
- **Load**: BigQuery load from curated outputs.

## Key folders
- [src/llm_feedback_pipeline](src/llm_feedback_pipeline)
- [config](config)
- [scripts](scripts)
- [databricks](databricks)

## Local dev (skeleton)
1. Configure environment variables in .env.example and copy to .env.
2. Update [config/settings.yaml](config/settings.yaml).
3. Run local ingest: `python scripts/run_local_ingest.py`.

## CI
Basic lint/test workflow in [.github/workflows/ci.yml](.github/workflows/ci.yml).
