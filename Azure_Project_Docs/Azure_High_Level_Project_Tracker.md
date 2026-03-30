# Azure LLM Evaluation Data Platform - High Level Project Tracker

## 1. How to Use This Tracker
- Update Status weekly: Not Started, In Progress, Blocked, Completed
- Assign owner and target date for each item
- Track evidence links (PR, pipeline run, dashboard screenshot)

## 2. Program Status Dashboard

| Workstream | Current Status | Priority | Target Outcome |
|---|---|---|---|
| Architecture and Requirements | In Progress | High | Signed-off architecture and source contracts |
| Infrastructure and Security | Not Started | High | Reproducible dev/qa/prod with secure defaults |
| Ingestion Framework (ADF) | Not Started | High | Metadata-driven ingestion from API/files/DB |
| Bronze Layer (Databricks) | Not Started | High | Raw normalized Delta with lineage metadata |
| Silver Layer + DQ | Not Started | High | Clean conformed data + quarantine framework |
| Gold Layer + Synapse Serving | Not Started | High | Star schema and KPI marts for BI |
| Observability and Operations | Not Started | Medium | Alerting, runbooks, SLA monitoring |
| CI/CD and Release Governance | Not Started | High | Automated build/deploy with approvals |
| Documentation and Interview Artifacts | In Progress | Medium | Design pack + architecture narrative |

## 3. Detailed Step Tracker

| Step ID | Step | Status | Owner | Target Date | Exit Criteria |
|---|---|---|---|---|---|
| 1 | Confirm use cases, KPIs, and source contracts | In Progress | You | Week 1 | Source-to-target mapping approved |
| 2 | Set up Azure resource groups, networking, Key Vault, RBAC | Not Started | You | Week 1 | Secure environment baseline ready |
| 3 | Create ADLS zones and folder conventions | Not Started | You | Week 1 | Landing/Bronze/Silver/Gold paths created |
| 4 | Build ADF metadata tables and parameterized pipelines | Not Started | You | Week 2 | One pipeline handles multi-source ingestion |
| 5 | Implement API ingestion with watermark strategy | Not Started | You | Week 2 | Incremental pull with rerun support |
| 6 | Implement file and SQL snapshot ingestion patterns | Not Started | You | Week 2 | Reliable ingestion for all source types |
| 7 | Build Bronze normalization notebooks/jobs | Not Started | You | Week 3 | Delta Bronze tables with audit columns |
| 8 | Build Silver cleaning, dedup, and schema enforcement | Not Started | You | Week 4 | Conformed Silver tables validated |
| 9 | Implement DQ rules and quarantine workflow | Not Started | You | Week 4 | Failed records isolated with reason codes |
| 10 | Build Gold dimensional model and KPI marts | Not Started | You | Week 5 | Fact/dim model queryable in Synapse |
| 11 | Create Synapse views and optimize SQL performance | Not Started | You | Week 5 | Query SLA met with indexing/partitioning |
| 12 | Add monitoring, alerts, and operational dashboard | Not Started | You | Week 6 | Failure/SLA alerts active |
| 13 | Add unit, integration, and data contract tests | Not Started | You | Week 6 | Test suite required for promotion |
| 14 | Configure CI pipeline (build, lint, tests) | Not Started | You | Week 7 | PR checks enforced |
| 15 | Configure CD pipeline (dev->qa->prod) with approvals | Not Started | You | Week 7 | Controlled release promotion active |
| 16 | Create rollback and backfill runbooks | Not Started | You | Week 8 | Incident response process documented |
| 17 | Build Power BI sample dashboard on Gold layer | Not Started | You | Week 8 | Interview-ready analytics demo |
| 18 | Dry-run end-to-end demo and optimize narrative | Not Started | You | Week 9 | 20-minute project walkthrough ready |

## 4. Critical Concepts to Demonstrate (From JD + Resume)
- Medallion architecture with Delta-based Bronze/Silver/Gold
- Synapse analytics serving model and SQL tuning
- ADF parameterized orchestration and retry/backfill controls
- CDC/incremental patterns with idempotent MERGE
- Data quality gates, reconciliation, and quarantine strategy
- Governance: RBAC, PII controls, secrets in Key Vault
- DevOps maturity: PR checks, CI/CD gates, promotion controls
- Agile execution: sprint planning, demos, incident retrospectives
- Stakeholder communication: design options and tradeoff decisions

## 5. Situational Use Cases to Recreate Hands-On

### Use Case 1: Schema Drift in API Source
- Situation: API adds nested field and changes enum values
- Practice Goal: Handle schema evolution without breaking pipeline
- Expected Solution: permissive Bronze ingestion + controlled Silver mapping + contract alert

### Use Case 2: Duplicate and Late Arriving Events
- Situation: Same event arrives multiple times and out of order
- Practice Goal: Ensure exactly-once business outcome
- Expected Solution: business key hashing + event-time watermark + dedup window + MERGE strategy

### Use Case 3: Data Quality Breach in Production
- Situation: Null rate spikes on critical score column
- Practice Goal: Prevent bad data from reaching Gold
- Expected Solution: DQ thresholds, quarantine isolation, SLA alert, controlled rerun

### Use Case 4: Pipeline Failure During Release
- Situation: New release breaks Synapse view dependency
- Practice Goal: Recover quickly and safely
- Expected Solution: release gate checks, smoke tests, version rollback, post-incident RCA

### Use Case 5: Source System Throttling/Timeout
- Situation: API rate limits cause incomplete daily ingestion
- Practice Goal: Maintain SLA while respecting source limits
- Expected Solution: retry with backoff, pagination checkpointing, partial-load reconciliation

### Use Case 6: Cost Spike in Databricks Jobs
- Situation: Runtime and DBU consumption increase 2x
- Practice Goal: Tune performance and reduce cost
- Expected Solution: partition pruning, join optimization, caching discipline, cluster right-sizing

### Use Case 7: Security and Access Audit
- Situation: Audit requests proof of least-privilege and PII controls
- Practice Goal: Demonstrate governance readiness
- Expected Solution: RBAC matrix, Key Vault usage, masking policy, access logs

### Use Case 8: Backfill for Historical Reprocessing
- Situation: Business requests 6 months historical replay with new logic
- Practice Goal: Reprocess safely without duplicate facts
- Expected Solution: partitioned backfill strategy, run manifest, idempotent MERGE validations

## 6. Weekly Execution Cadence
- Monday: sprint planning and backlog refinement
- Daily: update tracker and blockers
- Wednesday: mid-week architecture/test review
- Friday: demo + retro + next-week priorities

## 7. Status Update Template
Use this format each week:
- Completed this week:
- In progress:
- Blockers and risks:
- Mitigation plan:
- Next week priorities:
- Evidence links:
