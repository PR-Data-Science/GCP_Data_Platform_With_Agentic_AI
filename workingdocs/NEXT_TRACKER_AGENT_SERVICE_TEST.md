# Next Tracker: Agent Service Manual Test + Ops Enablement

Date: 2026-03-18
Project: GCP_ETL_LLMFeedbackData

## Objective
Execute the next 3 steps for agent validation:
1. Start the service and keep it running for manual UI testing.
2. Run a complete API smoke sequence.
3. Create missing ops tables so agent evidence can use real ops data.

## Step 1: Keep Service Running (Manual UI Testing)
Status: DONE

Command used:
- PYTHONPATH=. uvicorn src.agent_service.app:app --host 127.0.0.1 --port 8000

Validation:
- GET /health -> 200, {"status":"ok","service":"llm-feedback-agent"}
- GET /console -> 200

Manual UI URL:
- http://127.0.0.1:8000/console

Runtime note:
- Service is running in background terminal session.

## Step 2: Full API Smoke Sequence
Status: DONE

Sequence executed:
- POST /sessions
- POST /router
- POST /proposals
- GET /security/posture

Observed results:
- session_id created: a85576a6-4247-4f34-8684-7234555a0406
- router route: dq
- router read_only: true
- proposal_id created: a3956a29-579d-47cf-9906-be0c34d8a418
- proposal status: DRAFT
- security posture confirms rbac, pii_redaction, retention_controls, draft_only_automation

## Step 3: Create Missing Ops Tables
Status: DONE

DDL source:
- sql/ops_tables.sql

Command used:
- bq --project_id=liquid-layout-413121 query --use_legacy_sql=false < sql/ops_tables.sql

Verification:
- bq --project_id=liquid-layout-413121 ls ops
- Confirmed tables present:
  - pipeline_runs
  - dq_results
  - schema_registry
  - deadletter_summary
  - dq_rule_registry
  - agent_sessions
  - agent_tool_calls
  - agent_responses
  - agent_proposals

## Post-Setup Validation (Executed)
Status: DONE

Bronze refresh run:
- Stage rerun: bronze
- run_id: 2eed441d-252b-4280-9578-783d0b0444f1
- ingest_date: 2026-03-18
- dataproc batch: 6d659683fbd24b69a0ddcd5afb89699e
- result: SUCCEEDED

Ops data verification:
- ops.pipeline_runs shows fresh row:
  - run_id: 2eed441d-252b-4280-9578-783d0b0444f1
  - stage: bronze
  - status: SUCCEEDED
  - code_version: manual-validation
  - created_ts: 2026-03-19 00:34:30
- ops.schema_registry shows fresh bronze schema snapshot for same run_id.

Post-refresh API smoke sequence:
- session_id: 965a3d41-1beb-422f-b2bb-28d12684cc88
- route returned: ops
- evidence refs returned include:
  - bq://ops.pipeline_runs
  - bq://ops.schema_registry
- proposal_id: 88207cdb-fac1-4a9d-a7d9-16eb36d31c6d
- proposal status: DRAFT
- security posture: rbac=true, pii_redaction=true, retention_controls=true, draft_only_automation=true

## Immediate Next Actions
1. Open UI and run 3 manual prompts in Ops and DQ modes.
2. Run silver and gold stage refresh to append additional live ops records.
3. Validate proposal approval and PR-draft endpoints with role checks.

## Quick Runbook
- Start service:
  - source .venv/bin/activate
  - PYTHONPATH=. uvicorn src.agent_service.app:app --host 127.0.0.1 --port 8000
- Open UI:
  - http://127.0.0.1:8000/console
- Optional tests:
  - PYTHONPATH=. pytest -q tests/test_agent_service.py tests/test_agent_security.py tests/test_agent_tools.py
