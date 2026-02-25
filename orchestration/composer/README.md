# Cloud Composer Orchestration

This folder contains a Composer-ready Airflow DAG for orchestrating Dataproc Serverless stages with dependency control and rerun safety.

## DAG

- File: `dags/llm_feedback_dataproc_orchestration.py`
- DAG ID: `llm_feedback_dataproc_orchestration`
- Schedule: every 15 minutes
- Behavior:
  - discovers `run_id`s from raw JSONL objects for one `ingest_date`
  - checks Bronze/Silver/Gold manifests
  - runs only missing stages per `run_id`
  - enforces stage dependencies:
    - Bronze -> Silver -> Gold
    - Silver -> Gold
    - Gold-only catch-up

  ## Full E2E DAG (with batch generation)

  - File: `dags/llm_feedback_full_e2e_composer.py`
  - DAG ID: `llm_feedback_full_e2e_composer`
  - Schedule: hourly
  - Behavior:
    - generates sample source batches in both formats:
      - JSON batch (`source_type=json`)
      - CSV batch (`source_type=csv`)
    - stores source files under `datasource/...` in raw bucket
    - ingests generated source files into raw JSONL (`raw/.../run_id=.../batch_id=.../part-00000.jsonl`)
    - runs Dataproc Bronze -> Silver -> Gold with strict task dependency

## Airflow Variable

Create this Airflow Variable in Composer:

- Name: `llm_feedback_composer_config`
- Type: JSON

Example value:

```json
{
  "env": "dev",
  "project_id": "liquid-layout-413121",
  "region": "us-central1",
  "gcp_conn_id": "google_cloud_default",
  "service_account": "438895410098-compute@developer.gserviceaccount.com",
  "source_name": "llm_feedback_eval",
  "datasource_prefix": "datasource",
  "raw_bucket": "liquid-layout-413121-llmfb-raw-dev",
  "bronze_bucket": "liquid-layout-413121-llmfb-bronze-dev",
  "silver_bucket": "liquid-layout-413121-llmfb-silver-dev",
  "gold_bucket": "liquid-layout-413121-llmfb-gold-dev",
  "raw_prefix": "raw",
  "bronze_prefix": "bronze",
  "silver_prefix": "silver",
  "gold_prefix": "gold",
  "pod_name": "Magi_Code_Python",
  "pod_type": "vertical",
  "task_type": "EAC_NEXT_STEPS_SIMPLIFIED",
  "auto_batch_id": "python_training_version1_LLMrated_batch",
  "human_batch_id": "python_training_version1_HUMANrated_batch",
  "record_count_per_batch": 12,
  "dataproc_properties": {
    "spark.dynamicAllocation.enabled": "false",
    "spark.executor.instances": "2",
    "spark.executor.cores": "4",
    "spark.driver.cores": "4"
  }
}
```

## Deploy to Composer

### Environment details

| Setting     | Value                            |
|-------------|----------------------------------|
| Environment | `gcpllmevaluationprojautomation` |
| Location    | `us-central1`                    |
| Project     | `liquid-layout-413121`           |

### Option A — convenience script (recommended)

```bash
bash infra/gcp/deploy_dags_to_composer.sh
```

This uploads both DAGs in one command. Override defaults if needed:

```bash
COMPOSER_ENV=gcpllmevaluationprojautomation \
COMPOSER_LOCATION=us-central1 \
PROJECT_ID=liquid-layout-413121 \
bash infra/gcp/deploy_dags_to_composer.sh
```

### Option B — manual upload

```bash
gcloud composer environments storage dags import \
  --environment gcpllmevaluationprojautomation \
  --location us-central1 \
  --project liquid-layout-413121 \
  --source orchestration/composer/dags/llm_feedback_dataproc_orchestration.py

gcloud composer environments storage dags import \
  --environment gcpllmevaluationprojautomation \
  --location us-central1 \
  --project liquid-layout-413121 \
  --source orchestration/composer/dags/llm_feedback_full_e2e_composer.py
```

### Check Composer status

```bash
bash infra/gcp/check_composer_status.sh
```

2. Set Variable in Airflow UI or CLI.

Optional: if your Composer image does not include required providers, upload `orchestration/composer/requirements.txt` as environment PyPI dependencies.

3. Trigger manual run for a specific date:

```json
{
  "ingest_date": "2026-02-22"
}
```

4. Validate runs in Airflow graph view and Dataproc Batch history.

## Notes

- This DAG starts from the raw layer and orchestrates Bronze/Silver/Gold transforms.
- It relies on per-stage manifest files to avoid duplicate writes.
- It does not invoke local scripts from this repo; Composer calls Dataproc APIs directly.
- The full E2E DAG creates source CSV/JSON files and ingests them before Dataproc stages.
