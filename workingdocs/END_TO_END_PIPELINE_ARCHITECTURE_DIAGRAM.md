# End-to-End Pipeline Architecture Diagram

This diagram represents the implemented end-to-end flow in this repository.
It includes GenAI-readiness artifacts for future read-only consumption (ops control plane + evidence packaging), without introducing agentic/UI runtime changes.

```mermaid
flowchart TB
  subgraph Sources[Data Sources]
    S1[CSV Batch\nHuman Rated]
    S2[JSON Batch\nLLM/Auto Rated]
  end

  subgraph Ingestion[Ingestion Layer]
    I1[src/ingestion/batch_to_gcs.py\nNormalize to meta+payload]
  end

  subgraph Raw[Raw Layer - GCS]
    R1[raw/<source>/dt=<date>/run_id=<id>/batch_id=<id>/part-00000.jsonl]
    RM[manifests/raw/dt=<date>/run_id=<id>/manifest.json]
  end

  subgraph Bronze[Bronze Layer - Dataproc + GCS]
    B1[src/bronze/bronze_ingest_dataproc.py\nflatten + metadata standardize + dedupe]
    B2[bronze parquet\npartition target: ingest_date + run_id + source_type]
    BM[manifests/bronze/dt=<date>/run_id=<id>/manifest.json]
  end

  subgraph Silver[Silver Layer - Dataproc + GCS]
    SV1[src/silver/silver_transform_dataproc.py\ncontract parse + score resolution + DQ]
    SV2[feedback_step]
    SV3[ratings_long]
    SV4[execution_steps]
    SV5[violations]
    SV6[deadletter\nwith evidence refs + rule_id]
    SVM[manifests/silver/dt=<date>/run_id=<id>/manifest.json]
    DQR[dq/dq_rules.yaml\nDQ Rule Registry]
  end

  subgraph Gold[Gold Layer - Dataproc + GCS]
    G1[src/gold/gold_transform_dataproc.py\ncurated marts + optional BQ publish]
    G2[training_supervised_examples]
    G3[model_eval_step_metrics]
    G4[model_eval_failure_breakdown]
    G5[model_comparison_daily]
    G6[rater_agreement]
    GM[manifests/gold/dt=<date>/run_id=<id>/manifest.json]
  end

  subgraph Serving[Serving Layer - BigQuery]
    BQ1[gold_* tables\noptional publish]
    BQ2[Star Schema\ndim_* + fact_*]
    PM[manifests/publish/dt=<date>/run_id=<id>/manifest.json]
  end

  subgraph Contracts[Contract Snapshots + Versioning]
    C1[contracts/silver/*.json]
    C2[contracts/gold/*.json]
    C3[contracts/README.md\nevolution policy]
  end

  subgraph OpsAudit[Ops & Audit Control Plane - BigQuery]
    OP1[ops.pipeline_runs\nstatus + timings + counts + code_version]
    OP2[ops.dq_results\nrule-level DQ evidence]
    OP3[ops.schema_registry\nschema_hash snapshots]
    OP4[ops.deadletter_summary\naggregated deadletter distribution]
    OP5[ops.dq_rule_registry\noptional mirror of rule definitions]
  end

  subgraph Orchestration[Cloud Composer / Airflow]
    O1[llm_feedback_dataproc_orchestration\nrun discovery + branch-aware rerun]
    O2[llm_feedback_full_e2e_composer\ngenerate source + ingest + B/S/G]
    O3[Airflow Variable\nllm_feedback_composer_config]
  end

  S1 --> I1
  S2 --> I1
  I1 --> R1
  I1 --> RM
  I1 --> OP1
  I1 --> OP3

  R1 --> B1 --> B2
  B1 --> BM
  B1 --> OP1
  B1 --> OP3

  B2 --> SV1
  SV1 --> SV2
  SV1 --> SV3
  SV1 --> SV4
  SV1 --> SV5
  SV1 --> SV6
  SV1 --> SVM
  DQR --> SV1
  SV1 --> OP1
  SV1 --> OP2
  SV1 --> OP4

  SV2 --> G1
  SV3 --> G1
  SV5 --> G1
  G1 --> G2
  G1 --> G3
  G1 --> G4
  G1 --> G5
  G1 --> G6
  G1 --> GM
  G1 --> OP1

  G1 --> BQ1 --> BQ2
  BQ1 --> PM
  BQ1 --> OP1

  C1 -. schema contract .-> SV1
  C2 -. schema contract .-> G1
  C3 -. policy .-> C1
  C3 -. policy .-> C2

  DQR -. optional mirror .-> OP5

  O1 -. submits Dataproc batches .-> B1
  O1 -. submits Dataproc batches .-> SV1
  O1 -. submits Dataproc batches .-> G1
  O2 -. full E2E orchestration .-> I1
  O2 -. submits Dataproc batches .-> B1
  O2 -. submits Dataproc batches .-> SV1
  O2 -. submits Dataproc batches .-> G1
  O3 -. runtime config .-> O1
  O3 -. runtime config .-> O2
```

## Rerun logic (implemented)
- If `manifests/gold/dt=<date>/run_id=<run_id>/manifest.json` exists: skip run.
- Else if `manifests/silver/dt=<date>/run_id=<run_id>/manifest.json` exists: run Gold only.
- Else if `manifests/bronze/dt=<date>/run_id=<run_id>/manifest.json` exists: run Silver then Gold.
- Else: run Bronze then Silver then Gold.
- Composer override: set `dag_run.conf.force_reprocess=true` to submit all stages for discovered runs and pass `--force` into stage jobs.

## Evidence packaging and lineage notes
- Every stage emits a standardized `{stage}_manifest.json` at `manifests/<stage>/dt=<date>/run_id=<run_id>/manifest.json`.
- Ops tables are source of truth for run status, DQ outcomes, schema snapshots, and deadletter aggregates.
- Lineage/version pointers (`input_paths`, `output_paths`, partition keys, `code_version`) are stored in manifests and `ops.pipeline_runs`.

## Simplified flow (presentation view)

```mermaid
flowchart LR
  A[Sources\nCSV + JSON] --> B[Ingestion\nRaw JSONL in GCS]
  B --> C[Bronze\nDataproc]
  C --> D[Silver\nDataproc + DQ + Deadletter]
  D --> E[Gold\nDataproc Curated Marts]
  E --> F[BigQuery\nServe + Star Schema]

  B --> M1[Manifest\nraw]
  C --> M2[Manifest\nbronze]
  D --> M3[Manifest\nsilver]
  E --> M4[Manifest\ngold]
  F --> M5[Manifest\npublish]

  C --> O[Ops & Audit\nBQ ops tables]
  D --> O
  E --> O
  F --> O

  R[dq/dq_rules.yaml\nDQ registry] --> D
  S[contracts/silver + contracts/gold\nversioned contracts] --> D
  S --> E
```

### Simplified checkpoints
- Stage execution scope: `run_id`.
- Manifest path (all stages): `manifests/<stage>/dt=<date>/run_id=<run_id>/manifest.json`.
- Rerun default: skip if manifest exists; optional controlled force reprocess.
- Ops source of truth: `ops.pipeline_runs`, `ops.dq_results`, `ops.schema_registry`, `ops.deadletter_summary`.
