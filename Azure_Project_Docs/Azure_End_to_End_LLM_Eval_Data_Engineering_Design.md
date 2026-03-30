# Azure End-to-End Data Engineering Design

## 1. Document Purpose
Build a production-ready Azure data engineering project for LLM evaluation data that demonstrates:
- Medallion architecture (Bronze, Silver, Gold)
- Large-scale ETL/ELT using Azure Data Factory, Databricks, and Synapse
- Data quality, observability, governance, and replay-safe processing
- CI/CD and DevOps operating model aligned to enterprise expectations

This design is based on the target job description and your resume strengths.

## 2. Project Goals and Success Criteria

### 2.1 Business Goal
Create a governed analytics data product from raw LLM feedback/evaluation events to support:
- Model quality analytics
- Prompt/response quality monitoring
- Team-level and model-version-level KPI reporting

### 2.2 Technical Success Criteria
- Ingest from at least 3 source types: API JSON, flat files, and relational snapshots
- Process incremental data with idempotent reruns
- Enforce schema/data quality checks with quarantine handling
- Deliver Synapse-serving layer with star schema and performance tuning
- Automate deployments using Azure DevOps CI/CD
- Meet SLA target: daily batch complete within 60 minutes for baseline dataset

## 3. Scope

### In Scope
- Azure Data Factory for orchestration and ingestion
- ADLS Gen2 as centralized storage
- Azure Databricks (PySpark + Delta) for transformations
- Azure Synapse (serverless/dedicated SQL) for serving curated data
- Power BI semantic model and dashboard starter
- Monitoring with Azure Monitor + Log Analytics
- IaC (Terraform/Bicep optional), release gates, rollback and runbooks

### Out of Scope (for initial version)
- Real-time streaming with Event Hubs/Kafka
- Multi-region active-active architecture
- Advanced MLOps model deployment

## 4. Target Architecture

## 4.1 Logical Flow
1. Source systems:
- LLM evaluation API (JSON)
- Human feedback CSV/Parquet drops
- Metadata from SQL source (model registry/prompt registry)

2. Landing and ingestion:
- ADF copies raw payloads to ADLS landing zone
- Raw immutable storage with ingestion timestamp partitioning

3. Bronze layer:
- Databricks normalizes payloads into Delta Bronze tables
- Add ingestion metadata, source file, batch id, hash keys

4. Silver layer:
- Standardization, deduplication, schema enforcement, PII handling
- Business keys and conformed entities
- Data quality validation and quarantine tables

5. Gold layer:
- Star schema marts in Delta/Synapse: fact_eval_event, dim_model, dim_prompt, dim_reviewer, dim_time
- KPI aggregates: pass_rate, toxicity_rate, latency_p95, annotation_agreement

6. Consumption:
- Synapse views and external tables
- Power BI reports and ad hoc SQL analytics

## 4.2 Medallion Mapping
- Bronze: raw + minimally standardized, append-only
- Silver: cleaned, validated, conformed, replay-safe
- Gold: business-ready dimensional models and KPI views

## 5. Data Model Design

### 5.1 Core Dimensions
- dim_model: model_name, model_version, vendor, release_date, status
- dim_prompt: prompt_id, prompt_category, domain, risk_tier
- dim_reviewer: reviewer_id, team, role, region
- dim_time: date_key, day, week, month, quarter, year

### 5.2 Core Facts
- fact_eval_event:
- surrogate keys to dimensions
- metrics: score, latency_ms, token_in, token_out, cost_usd, pass_flag
- attributes: scenario_type, error_type, source_channel

### 5.3 SCD Strategy
- Type 2 on dim_model and dim_prompt for change history
- Type 1 on non-critical descriptive attributes

## 6. Pipeline Design (End to End)

### 6.1 Ingestion Patterns
- API incremental ingestion with watermark (event_time/id)
- File ingestion using metadata-driven ADF pipeline
- Optional CDC pattern for SQL source (if source supports)

### 6.2 Transformation Patterns
- Bronze: parse/flatten nested JSON, enforce base contract
- Silver: null/range checks, dedup by business key + latest timestamp, standardize enums
- Gold: dimensional joins, surrogate key assignment, aggregate tables

### 6.3 Idempotency and Replay
- Batch control table tracks run_id, source watermark, status
- MERGE-based upserts in Silver/Gold
- Rerun by partition/date/run_id without duplication

## 7. Data Quality and Governance

### 7.1 Data Quality Framework
- Rule categories:
- Completeness (required fields)
- Validity (ranges, regex)
- Uniqueness (business keys)
- Referential integrity (fact-dim relationships)

- Failed records flow to quarantine path with reason codes
- Publish DQ summary table for each run

### 7.2 Governance and Security
- ADLS ACL + RBAC, least privilege access
- PII masking/tokenization for sensitive columns
- Key Vault for secrets and linked services
- Data lineage with ADF + Purview (optional extension)

## 8. CI/CD and DevOps Blueprint

### 8.1 Repository Strategy
- Mono-repo sections:
- adf/ (pipelines, datasets, linked services)
- databricks/ (notebooks/jobs)
- synapse/ (SQL objects)
- infra/ (Terraform/Bicep)
- tests/ (unit, integration, dq)

- Branching:
- main: production-ready
- develop: integration branch
- feature/*: developer branches

### 8.2 Environments
- dev, qa, prod with separate resource groups/workspaces
- Environment-specific parameter files and key vault references

### 8.3 CI Pipeline (Azure DevOps)
- Trigger on PR and develop/main pushes
- Steps:
1. Lint and static checks (Python/SQL)
2. Unit tests (PySpark transforms)
3. Validate ADF/Synapse artifacts
4. Build deployment package (ARM/Terraform + notebooks + SQL)

### 8.4 CD Pipeline (Azure DevOps)
- Stage deployments: dev -> qa -> prod
- Gates:
- manual approval for prod
- smoke tests after each stage
- data quality threshold checks before promotion

### 8.5 Release Safety
- Blue/green or versioned deployment for SQL views/tables where possible
- Rollback by artifact version
- Runbook for partial failures and backfills

## 9. Testing Strategy
- Unit tests for transformation logic and schema mapping
- Contract tests for source payloads
- Integration tests for ADF -> Databricks -> Synapse path
- DQ tests on Silver and Gold outputs
- Performance tests for partition pruning and query SLAs

## 10. Monitoring and Operations
- Operational metrics:
- pipeline duration
- row counts in/out
- failed record count
- DQ pass percentage
- cost per run

- Alerting:
- ADF failures, Databricks job failures, SLA breaches
- Log Analytics dashboards for run health

- Incident process:
- Severity matrix, on-call rotation, root cause template

## 11. Agile Delivery Plan (12 Weeks Example)
- Sprint 1-2: architecture, contracts, infra bootstrap
- Sprint 3-4: ingestion framework and Bronze
- Sprint 5-6: Silver transforms + DQ + quarantine
- Sprint 7-8: Gold model + Synapse serving
- Sprint 9-10: CI/CD and automated tests
- Sprint 11: observability, hardening, runbooks
- Sprint 12: UAT, performance tuning, final demo

## 12. Deliverables Checklist
- Architecture diagram + design doc
- ADF pipelines (metadata-driven)
- Databricks notebooks/jobs for Bronze/Silver/Gold
- Synapse SQL objects and star schema
- DQ framework with quarantine and scorecards
- CI/CD pipelines and infra templates
- Monitoring dashboards + alert rules
- Operations runbook and backfill guide

## 13. Interview Demo Narrative
Use this storyline when presenting:
1. Problem: fragmented LLM evaluation data with schema drift and quality risk
2. Architecture: Azure-native medallion with Synapse serving
3. Reliability: idempotent loads, DQ gates, and quarantine workflow
4. DevOps: multi-stage CI/CD with approvals and smoke tests
5. Impact: faster analytics turnaround, better data trust, lower incident load

## 14. Optional Advanced Extensions
- Near-real-time ingestion with Event Hubs + Structured Streaming
- Feature store style tables for ML quality diagnostics
- Cost optimization automation (auto-cluster, job right-sizing)
- Data product SLA dashboard for platform governance
