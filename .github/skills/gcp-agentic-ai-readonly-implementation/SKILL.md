---
name: gcp-agentic-ai-readonly-implementation
description: 'Build or modify the agent service for read-only operations in the GCP Data Platform with Agentic AI: intent routing, evidence-backed answers, audit logging, and read-only tool contracts for BigQuery, GCS, Composer, and Dataproc. Use for agent_service, retrieval, tools, and governance-safe behavior.'
argument-hint: 'What agent capability is needed? (routing/retrieval/tools/audit/response format)'
---

# GCP Agentic AI Read-Only Implementation

## When to Use
- Implement changes in `src/agent_service/`.
- Add diagnostic capabilities without write/remediation actions.
- Improve evidence-backed responses, tool routing, or auditability.

## Required Context
1. Read `../../../workingdocs/PHASE2A_SCOPE_LOCK.md`.
2. Read `../../../workingdocs/PHASE2_AGENTIC_RAG_BRD_DESIGN.md`.
3. Confirm current service files:
- `../../../src/agent_service/app.py`
- `../../../src/agent_service/retrieval.py`
- `../../../src/agent_service/tools.py`
- `../../../src/agent_service/audit.py`

## Hard Constraints
- Phase 2A is strictly read-only.
- No data-plane mutations, no autonomous reruns, no DAG triggers, no Dataproc submissions.
- Responses must separate evidence-backed facts from recommendations.
- All tool calls and final responses must be auditable.

## Procedure
1. Classify request intent.
- `ops`: run status, failure triage.
- `dq`: rule failures and deadletter insights.
- `drift`: schema or contract drift analysis.
- `governance`: lineage/policy explanation.
2. Map intent to read-only tools.
- Allow only status/read/select-style tool paths.
- Reject or safely downgrade any write-like request.
3. Enforce evidence-first response structure.
- `Answer`
- `Evidence` (artifact/table/path references)
- `Suggested Actions` (non-executing in Phase 2A)
- `Confidence`
4. Implement audit coverage.
- Capture prompt/request context, tool calls, and response summary.
- Preserve traceability IDs where available (`run_id`, `trace_id`, session identifiers).
5. Add/update tests.
- Ensure disallowed actions are blocked.
- Ensure evidence formatting and routing behavior are deterministic.
6. Verify safety posture.
- Re-check there is no path to write operations through tool adapters.

## Quality Gates
- Safety: no write path can be triggered.
- Explainability: evidence references are present for operational claims.
- Determinism: routing and tool usage are predictable for same input intent.
- Auditability: request, tool interactions, and output are logged.

## Completion Checklist
- Agent code updated with read-only guarantees.
- Tests cover allow/deny behavior and output structure.
- Final summary explicitly states why the change remains Phase 2A compliant.
