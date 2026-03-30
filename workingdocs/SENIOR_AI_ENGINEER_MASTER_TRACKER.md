# Senior AI Engineer Master Tracker

## 0) Why this document exists

This is a hands-on, execution-first tracker to:
- explain the current end-to-end agent service design in this repository,
- clarify why some agents feel "not working" vs "working but scoped differently",
- map each file/function to its role in runtime,
- quantify retrieval, safety, and governance posture,
- define production deployment on GCP/Vertex AI,
- prepare interview-grade depth across modern agentic AI architectures.

Use this as a sequential workshop. Mark each item as `NOT_STARTED`, `IN_PROGRESS`, `DONE`, or `BLOCKED`.

---

## 1) Current reality check: Are transformation/governance agents working?

### 1.1 What is implemented now (fact)

The running service currently routes only to two runtime routes:
- `ops`
- `dq`

Routing logic is deterministic and implemented in:
- `src/agent_service/app.py` (`resolve_route` + `/router`)

Transform and governance are present as capabilities, but not as separate `/router` route labels yet:
- Transform Designer exists as proposal mode endpoint: `/transform-designer/proposals`
- Governance exists as cross-cutting controls (RBAC, PII redaction, retention purge, draft-only automation), not as a dedicated `governance` route in router dispatch.

### 1.2 Why this feels like "not working"

Because BRD language suggests specialist agents (including transform/governance), but runtime router in Phase 2A is intentionally narrow (`ops|dq`) for read-only MVP safety.

### 1.3 Are they adding value today?

Yes, in different forms:
- Transform Designer value (today): creates structured artifacts and safe drafts for B2S/S2G changes without direct write/apply risk.
- Governance value (today): enforces approval gates, role-based controls, redaction, retention, and draft-only execution boundaries.

### 1.4 Evidence from tests

Agent-focused suite currently passes (`46 passed`):
- router behavior,
- retrieval behavior,
- tools allowlist/validation,
- security controls,
- eval gates.

---

## 2) End-to-end flow: Starting point and internal collaboration

## 2.1 Entry points

Primary runtime entrypoint:
- FastAPI app in `src/agent_service/app.py`

External/API surfaces:
- `/health`
- `/sessions`, `/sessions/{id}`
- `/router`
- `/proposals*`
- `/transform-designer/proposals`
- `/triggers/draft-actions`
- `/admin/audit-retention/purge`
- `/security/posture`
- `/console` (UI)

## 2.2 Request flow for diagnostics

1. User/session created via `/sessions`
2. User query sent to `/router`
3. `resolve_route` chooses `ops` or `dq`
4. Retriever fetches evidence refs from indexed corpus
5. Router composes response + tool calls (read-only contract)
6. Audit store logs session/tool/response events
7. Optional proposal flows capture human-in-the-loop lifecycle

## 2.3 Internal file-level responsibility map

### Core orchestration
- `src/agent_service/app.py`
  - API contract, routing, response envelope, proposal endpoints, trigger drafts, admin/security endpoints.

### Retrieval layer
- `src/agent_service/retrieval.py`
  - evidence corpus model,
  - optional OpenAI embedding use,
  - lexical fallback,
  - ranking blend.

### Tool abstraction layer
- `src/agent_service/tools.py`
  - read-only tool registry,
  - allowlists,
  - role authorization,
  - schema diff utility.

### Audit and governance persistence
- `src/agent_service/audit.py`
  - in-memory event/proposal state,
  - persistence hooks to ops tables,
  - lifecycle transitions,
  - retention purge.

### Security utilities
- `src/agent_service/security.py`
  - role checks,
  - PII/API token redaction.

### Transform design playbooks
- `src/agent_service/transform_designer.py`
  - proposal artifact generation for B2S and S2G.

### Eval and quality gates
- `src/agent_service/eval_harness.py`
- `src/agent_service/ci_gate.py`
- `tests/test_agent_eval.py`
  - retrieval and router metrics enforcement in CI.

### Ops table writers
- `src/ops/ops_writer.py`
  - writes to `ops.*` tables including agent telemetry/proposals.

### UI
- `src/agent_service/console_ui.py`
  - lightweight console for route queries and evidence/tool display.

---

## 3) Agent-by-agent role, data sources, retrieval path

## 3.1 Router/Coordinator Agent

Purpose:
- classify intent,
- dispatch to route,
- return grounded response envelope.

Current route set:
- `ops`, `dq` only.

Inputs:
- user query, optional intent hint, session context.

Data dependencies:
- retriever corpus docs (ops/dq/manifests/rules).

## 3.2 Ops Copilot Agent (route = ops)

Purpose:
- pipeline run visibility,
- schema registry and manifest lineage diagnostics.

Primary data sources:
- `bq://ops.pipeline_runs`
- `bq://ops.schema_registry`
- `gcs://manifests/<stage>/.../manifest.json`

## 3.3 Data Quality Agent (route = dq)

Purpose:
- DQ violation/deadletter diagnostics,
- rule-level evidence and severity context.

Primary data sources:
- `bq://ops.dq_results`
- `bq://ops.deadletter_summary`
- `gcs://dq/dq_rules.yaml`

## 3.4 Transform Designer Agent (proposal mode)

Purpose:
- generate non-executing design artifacts for B2S/S2G changes.

Primary outputs (to GCS path convention):
- mapping spec,
- test plan,
- schema preview.

Notes:
- no auto-apply,
- proposal-first for controlled change.

## 3.5 Governance capability (cross-cutting, not routed yet)

Purpose:
- enforce safe operation via policy checks and controls.

Current controls:
- RBAC checks,
- proposal transition permissions,
- PII/API token redaction,
- retention purge endpoint,
- draft-only PR and trigger automation semantics.

---

## 4) Embeddings, vector store, and retrieval efficiency

## 4.1 Embeddings in current implementation

Current behavior in `src/agent_service/retrieval.py`:
- embedding model string default: `text-embedding-3-small`
- OpenAI embeddings are optional and lazy-loaded
- enabled if:
  - `AGENT_USE_OPENAI_EMBEDDINGS=true` and
  - `OPENAI_API_KEY` is present
- otherwise lexical retrieval still works

## 4.2 Vector store used today

Current state:
- no external vector DB is used.
- document vectors are computed and cached in-memory (`self._doc_embeddings` dict).
- corpus is currently a small in-code default evidence corpus.

Implication:
- excellent for local MVP/testing,
- not production-grade for large corpora, multi-tenant scale, or durable indexing.

## 4.3 Retrieval scoring and efficiency

Scoring formula:
- final score = `0.7 * embedding_score + 0.3 * lexical_score` (+ tiny tie breaker)

Efficiency characteristics now:
- linear scan over candidates,
- no ANN index,
- no persistent embedding cache store,
- suitable for tiny corpus.

Measured quality gate (current):
- recall@k threshold is configured to `>= 0.80`
- route accuracy threshold `== 1.0`
- grounding rate threshold `== 1.0`
- unsupported claim rate `<= 0.0`

---

## 5) Why this design was chosen (and alternatives)

## 5.1 Why this design for Phase 2A

Chosen for:
- strict read-only safety,
- deterministic routing,
- explainability with evidence refs,
- fast implementation and testability,
- low blast radius while building governance muscle.

## 5.2 Alternatives and tradeoffs

Alternative A: full multi-agent graph from day 1
- Pros: richer capability, realistic agent-to-agent orchestration
- Cons: harder safety assurance, harder debugging, slower MVP

Alternative B: pure RAG assistant with no route specialization
- Pros: simpler architecture
- Cons: weak deterministic behavior, poor operational explainability

Alternative C: action-first automation (reruns, fixes, PR merges)
- Pros: high automation value
- Cons: high risk without mature guardrails/approvals

Why current approach wins now:
- aligns with enterprise governance expectations,
- demonstrates production discipline before autonomy.

---

## 6) Productionization roadmap (Vertex AI + online serving)

## 6.1 Target production architecture

1. API Edge: Apigee (auth, quota, request shaping)
2. Serving: Cloud Run hosting FastAPI service
3. Agent runtime: Vertex AI Agent Engine / ADK orchestration (phase-wise)
4. Retrieval store: BigQuery vector search or Vertex AI Vector Search
5. Tool layer: MCP-compatible read-only tool adapters
6. Data plane: BigQuery, GCS, Composer, Dataproc
7. Observability: Cloud Logging/Monitoring + ops tables + traces
8. Security: IAM SA separation, Secret Manager, VPC-SC/CMEK as required

## 6.2 Deployment activities (must-do)

### Service packaging and runtime
- [ ] Containerize `src/agent_service/app.py` with pinned dependencies
- [ ] Add production ASGI server config (gunicorn/uvicorn workers)
- [ ] Add health/readiness probes
- [ ] Add structured logging (JSON)

### CI/CD and quality
- [ ] Enforce agent eval gates in CI before deploy
- [ ] Add integration smoke tests against deployed endpoint
- [ ] Add release versioning and immutable tags

### Secrets/config
- [ ] Move all keys/config to Secret Manager + env-specific config
- [ ] Remove direct dependency on local-only assumptions
- [ ] Add configuration validation at startup

### Retrieval productionization
- [ ] Replace in-code corpus with ingestion/indexing pipeline
- [ ] Choose vector backend:
  - BigQuery vector search (good fit with current stack), or
  - Vertex AI Vector Search (high-scale ANN)
- [ ] Add embedding refresh/version strategy
- [ ] Add retrieval observability (latency, recall drift, stale index)

### Tool deployment
- [ ] Deploy tool adapters as secured services/functions (or inline clients)
- [ ] Apply allowlists and schema validation at tool boundary
- [ ] Add per-tool authz, quotas, and audit logs

### Governance/safety
- [ ] Keep read-only by default for prod phase-1
- [ ] Add policy engine for action gating (OPA or custom)
- [ ] Formalize HITL approvals and change windows

### Reliability/SRE
- [ ] Define SLOs (latency, availability, grounding rate)
- [ ] Add retries/timeouts/circuit breakers to all external calls
- [ ] Add runbooks for retrieval/tool failures and degraded-mode behavior

---

## 7) Senior AI Engineer preparation workshop (end-to-end)

## 7.1 Workshop tracks

Track A: architecture mastery
- [ ] Draw current architecture from memory
- [ ] Explain every component, data contract, and failure mode
- [ ] Compare with at least 3 alternative architectures

Track B: code-level fluency
- [ ] Walk line-by-line through `app.py` request lifecycle
- [ ] Explain each dataclass/model and why it exists
- [ ] Trace router call to retriever/tool/audit outputs

Track C: retrieval and embeddings
- [ ] Explain lexical vs embedding hybrid scoring math
- [ ] Implement pluggable embedder interface with tests
- [ ] Benchmark retrieval latency and recall under larger corpus

Track D: tools and MCP layer
- [ ] Document every tool contract and auth control
- [ ] Add one new read-only tool with schema validation + tests
- [ ] Demonstrate tool failure isolation and fallback paths

Track E: governance and safety
- [ ] Demonstrate RBAC and proposal lifecycle semantics
- [ ] Add policy tests for forbidden transitions/actions
- [ ] Add prompt injection and data exfiltration test cases

Track F: eval and observability
- [ ] Expand eval suite with adversarial and long-tail queries
- [ ] Add dashboards for quality metrics over time
- [ ] Define go/no-go deployment criteria

Track G: production deployment
- [ ] Deploy dev Cloud Run + Apigee integration
- [ ] Deploy retrieval backend with index build jobs
- [ ] Run load test, chaos test, rollback drill

Track H: incident response competency
- [ ] Simulate failure scenarios (tool outage, stale embeddings, auth failure)
- [ ] Produce root cause analysis and remediation plan
- [ ] Practice executive and engineering incident communication

## 7.2 Interview drill packs (practice repeatedly)

Pack 1: whiteboard architecture defense
- [ ] Explain why Phase 2A is read-only and how to evolve to action agent safely
- [ ] Defend route determinism vs model-only planning

Pack 2: deep code walkthrough
- [ ] Explain all endpoints and transitions in `app.py`
- [ ] Explain audit persistence and eventual consistency tradeoffs

Pack 3: retrieval science
- [ ] Justify embedding model choice and vector backend choice
- [ ] Explain evaluation metrics and threshold rationale

Pack 4: governance and safety
- [ ] Explain redaction, RBAC, retention, proposal approvals
- [ ] Explain controls for production incidents and abuse cases

Pack 5: platform production
- [ ] Present Terraform/IAM/deployment strategy
- [ ] Explain multi-env promotion and rollback strategy

---

## 8) Gaps to close next (high priority implementation backlog)

1. Add explicit router routes for `transform` and `governance` intents
   - Keep actions disabled; return diagnosis/proposal only.

2. Externalize evidence corpus and indexing
   - Move from static in-code docs to managed, refreshable knowledge base.

3. Introduce durable vector backend
   - BigQuery vector search first (simpler stack alignment), benchmark against Vertex AI Vector Search.

4. Implement agent-to-agent orchestration abstraction
   - Router invokes specialist interface (not hardcoded branch logic).

5. Add memory strategy
   - session memory + durable memory with retention and privacy controls.

6. Harden observability
   - traces, correlation IDs, metric dashboards, alerting policy.

7. Harden security
   - identity propagation, least-privilege per-tool service accounts, threat modeling.

8. Add production deployment assets
   - Dockerfile, Cloud Run service manifest, CI/CD pipeline definitions, infra modules.

---

## 9) 30-60-90 day execution plan

## 30 days
- [ ] Complete architecture/code/eval workshop tracks A-F
- [ ] Add transform/governance route prototypes
- [ ] Build initial retrieval ingestion/indexing pipeline

## 60 days
- [ ] Deploy cloud dev environment end-to-end
- [ ] Add durable vector backend + quality dashboards
- [ ] Run failure drills and strengthen runbooks

## 90 days
- [ ] Production readiness review
- [ ] Canary rollout with strict safety and SLO monitoring
- [ ] Prepare final interview portfolio (design docs + demos + incident stories)

---

## 10) How to use this tracker weekly

1. Pick one track and one backlog item each week.
2. Implement + test + document the decision and tradeoff.
3. Record failures and what changed in architecture.
4. Rehearse interview explanation for that week’s changes.
5. Update status and evidence links before moving on.

---

## 11) Suggested evidence log template

For each completed item, capture:
- Objective
- Design chosen
- Alternatives considered
- Risks and mitigations
- Metrics before/after
- What failed and why
- What you would change at 10x scale

This transforms project work into senior-level interview narratives.
