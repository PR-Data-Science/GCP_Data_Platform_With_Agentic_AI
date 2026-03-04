# Phase 2A Scope Lock (Read-Only)

## Objective
Deliver a **read-only** GenAI assistant experience for pipeline observability and diagnostics without any autonomous write/remediation actions.

## In Scope (Phase 2A)
- FastAPI service skeleton for chat/session endpoints.
- Router/coordinator behavior for intent routing only.
- Read-only tool contracts over:
  - BigQuery ops/audit tables,
  - GCS evidence/artifact reads,
  - Composer/Dataproc status inspection.
- Evidence-backed responses with structured provenance.
- Full auditability for prompts, tool calls, and final responses.

## Out of Scope (Phase 2A)
- No direct data-plane mutations by agents.
- No autonomous reruns, retries, DAG triggers, or Dataproc submissions.
- No PR creation/merge automation.
- No write-enabled MCP tools.

## Safety & Governance Constraints
- Strict read-only execution boundary in runtime and tool adapters.
- Role-based access controls and per-session isolation.
- Sensitive-data handling and audit retention as defined in BRD.

## Exit Criteria
- Read-only assistant can answer ops/DQ lineage questions with evidence links.
- All requests and tool interactions are auditable.
- No capability path exists for autonomous write operations.

## Canonical Reference
This scope lock is aligned with `workingdocs/PHASE2_AGENTIC_RAG_BRD_DESIGN.md`.
