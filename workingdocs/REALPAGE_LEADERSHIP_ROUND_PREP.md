# RealPage — Senior Leadership Round Prep
**Role:** Senior Agentic AI Engineer  
**Interviewer:** Yuri Bogdanov (LinkedIn: linkedin.com/in/yuribogdanov/)  
**Format:** 30-minute cultural + leadership fit; also test readiness for technical depth on demand  
**Tone to match:** Technically deep, calm, structured, business-aware, governance-conscious

---

## YOUR POLISHED INTRO (2 versions — pick one)

### Version A — Technically grounded (recommended for AI-focused interviewers)
> "Hi, I'm Praveen. I'm a GenAI and data engineer with 6+ years building production-grade data and AI platforms across GCP, AWS, and Azure. My most recent work has been focused on agentic AI, RAG, and LLM-powered automation — specifically around a high-volume GenAI data platform where we ingest 1–2 TB per day of schema-variant LLM feedback data, then use metadata-grounded retrieval, tool-based orchestration, and PySpark-based processing to produce training-ready datasets and evaluation analytics tables. My role spans architecture and hands-on implementation — particularly around agent workflows, schema governance, data quality, confidence-based routing, and HITL approvals. What excites me about RealPage is that it sits at exactly the intersection I enjoy most: building reusable AI platforms, making sound architectural decisions, and turning GenAI into something that's reliable, measurable, and genuinely useful to the business."

### Version B — Leadership-style (use if they signal they want to go strategic early)
> "Over the last several years I've grown from core data engineering into AI platform engineering, where my focus isn't just building model workflows — it's making those workflows production-safe, reusable, and aligned to business outcomes. In my recent work I've built agentic and RAG-driven systems that automate data transformation and validation at scale. But what I enjoy most is solving the harder production questions: how to make outputs trustworthy, how to handle schema drift, how to introduce human-in-the-loop at the right friction points, how to measure retrieval and model quality, and how to build patterns other teams can reuse. I think that blend of hands-on engineering, architecture thinking, and cross-functional communication is what I'd bring here."

---

## MAIN PROJECT — 5-PART LEADERSHIP STORY

| Layer | What to say |
|---|---|
| **Problem** | Large volumes of LLM evaluation and feedback data from multiple APIs and file-based sources — highly inconsistent formats |
| **Why hard** | Needed two things from the same raw data: training-ready datasets for model improvement, AND clean analytics tables to evaluate LLM performance |
| **Architecture** | GCP-native pipeline — immutable raw ingestion → PySpark normalization → metadata-grounded retrieval → agentic layer for cleaning/mapping/validation logic generation |
| **My role** | Architecture + implementation of the intelligence layer: agent retrieval of metadata, confidence scoring, HITL gating, schema contract versioning, replay-safe idempotency |
| **Impact** | Less manual effort, better transformation consistency, stronger governance, faster turnaround for model and analytics teams |

---

## MOCK Q&A — 15 LEADERSHIP + BEHAVIORAL QUESTIONS

---

### Q1. Tell me about yourself.
**Answer framework:** Background → Current work → What excites you about this role  
**Use Version A or B intro above.** End with: *"I'm excited by what RealPage is building — the Lumina platform and the focus on making AI genuinely operational for property management teams aligns exactly with how I think about production AI."*

---

### Q2. Walk me through a project you're most proud of.
> "The project I'm most proud of recently is implementing an agentic data platform on GCP that handles high-volume schema-variant LLM feedback data. What makes me proud of it isn't the volume or the tooling — it's the combination of automation with trust controls. We built it so that an agent can generate transformation logic, but that output is grounded in internal metadata and schema contracts, scored for confidence, and routed to HITL when confidence is below threshold. That's the kind of system I want to build: one that automates aggressively but doesn't lose control. The production result was a reliable, replay-safe pipeline that reduced manual effort and created consistent outputs for both model training and analytics."

---

### Q3. What was the toughest challenge you faced? How did you solve it?
> "The toughest challenge was balancing automation with trust. It's easy to drop an LLM into a pipeline and generate transformation logic. The hard part is answering the question: is this output safe to auto-promote to production? What we built was a grounding layer — retrieval over internal schema definitions, mapping rules, and prior transformation examples — so the agent wasn't generating blindly. Then we added confidence scoring, and anything below threshold goes to a human approval queue. The lesson was: in production AI, the controls around the model matter more than the model itself."

---

### Q4. How do you decide where AI should be used vs. deterministic logic?
> "Simple rule I follow: use AI where judgment, summarization, flexible mapping, or reasoning under ambiguity is needed — and use deterministic logic where consistency, enforcement, and compliance matter. For example, in my pipeline, model-guided reasoning is useful for schema mapping suggestions and transformation generation. But schema validation rules, access controls, PII detection, and final approval thresholds stay deterministic. If I let an LLM make those decisions, I lose the predictability and auditability that production systems require. That separation is also easier to explain to compliance and legal teams."

---

### Q5. How do you think about reusable AI platforms vs. one-off AI features?
> "My mental model is: common capabilities first. Every AI product needs retrieval, prompt version management, evaluation pipelines, guardrails, observability, and model access wrappers. If each team builds those independently, you get quality divergence and a lot of duplicated work. I prefer building shared primitives — essentially an internal AI SDK — that product teams compose into their own workflows. In my current work I built reusable metadata retrieval, schema contract checking, and evaluation harness components that multiple pipelines use. That approach pays off fast when you need to onboard a second use case."

---

### Q6. How do you handle ambiguity from product or business stakeholders?
> "Three steps. First, I make sure I understand the business decision we're trying to improve, not just the feature that was requested. Second, I define measurable success criteria upfront — for AI that might be retrieval precision, answer satisfaction rate, reduction in manual review volume, or time to turnaround. Third, I break the solution into what can be safely shipped first versus what needs experimentation. That's important with AI specifically because the first version almost never behaves exactly as expected end-to-end. I'd rather make that discovery early with a scoped version than after six months of full build."

---

### Q7. How do you ensure AI systems are reliable in production?
> "I treat AI systems like distributed systems with probabilistic components — which means reliability comes from contracts, not trust. I put structure around inputs and outputs, version both prompts and schemas, add retries and fallbacks, monitor drift in inputs and outputs, log retrieval context alongside model decisions, and define explicit thresholds for when the system should stop and hand off to a human. The model is only one component. The surrounding pipeline — data validation, schema enforcement, audit logging, alerting — is what actually makes it production-ready. I care a lot about that outer shell."

---

### Q8. How do you measure success for an AI initiative?
> "I break it into four layers. **Technical quality** — things like retrieval precision/recall, structured output validity, hallucination rate, safety pass rate. **Operational quality** — latency, uptime, error rate, cost per request. **User value** — adoption rate, time saved, user satisfaction, support deflection. **Business value** — depends on the use case: lower manual workload, faster turnaround, better conversion, improved tenant satisfaction. Mixing those together makes it hard to debug what's actually failing. Keeping them separate also makes it easier to talk to non-technical stakeholders — you can say 'our retrieval quality went up, but user adoption is still low' and that tells a precise story."

---

### Q9. Tell me about a time something failed. What did you do?
> "In the pipeline I worked on, we had early cases where schema drift — new source formats or unexpected payload shapes — could propagate silently into curated layers and corrupt downstream training data. The root cause was that validation was happening too late. What I did was push schema contract checks earlier in the flow, implement quarantine behavior to isolate bad loads rather than letting them advance, and make reruns idempotent so we could safely replay any affected date range without side effects. The lesson I took away was: in AI and data systems, early containment is always cheaper than late recovery. You want the system to be loud and fail fast, not silently wrong."

---

### Q10. How do you mentor engineers and raise team quality?
> "I focus on patterns over code. If I review a piece of code and tell someone what to change, they fix that one thing. If I define a pattern — here's how we version prompts, here's the evaluation checklist, here's what every retrieval component must log — then the team can self-review against it. I also like pair-architecting on tricky problems rather than handing down decisions. For cross-functional work, I try to stay out of jargon with product and business stakeholders — I translate AI tradeoffs into speed, risk, quality, and measurable outcomes. That usually creates alignment faster than a deep technical explanation."

---

### Q11 (Technical depth). How do you choose between Google and OpenAI models?
> "I evaluate across five dimensions: task fit, latency, output structure reliability, cost, and compliance/governance constraints. For a structured backend task where I need schema-valid JSON output reliably — I pick the model that demonstrates that consistently in evaluation, regardless of brand. For long-context reasoning or when I need tight GCP ecosystem integration, that shifts the conversation. I don't make it ideological. I prefer a routing mindset: model choice is workload-driven, and ideally the system abstracts the model so I can swap it out if a better option emerges without rewriting the orchestration layer."

---

### Q12 (Technical depth). How do you evaluate a RAG system?
> "I separate retrieval quality from answer quality because they fail for different reasons. For retrieval I measure Recall@K — are the right documents in the candidate set — plus Precision@K and MRR if ranking matters. Then at generation level I evaluate groundedness: is the answer actually using retrieved context, or hallucinating? I also check citation quality, safety, and task completion rate. If you only measure end-to-end answer quality, you can't tell whether you have a retrieval problem, a prompting problem, or a model problem. Those need to be diagnosed independently."

---

### Q13 (Technical depth). How would you architect an AI copilot for RealPage's property operations domain?
> "I'd start by identifying one narrow, high-value workflow where user intent, system action, and business outcome are all measurable — something like maintenance request handling or lease renewal assistance. Then I'd design three layers: retrieval and enterprise context (tenant and property data, policy docs, prior resolutions), orchestration and tool use (intent routing, tool calling against backend systems, HITL for sensitive actions like financial decisions), and guardrails plus observability (PII redaction, safety filters, audit logging, fallback paths). First version stays modular — one agentic backend, controlled tool surface, clean audit trail, easy feedback loop. Once that's reliable and measured, it becomes the reusable platform for the next copilot."

---

### Q14 (Technical depth). How do you control cost in AI systems?
> "Model routing is the first lever — classify the task and route cheap questions to smaller models, complex reasoning to larger ones. Context budgeting comes second — retrieval quality directly controls how much you put in a prompt, so better retrieval means lower cost. Caching for repeated or near-duplicate queries is high-leverage especially at scale. I also look for places where deterministic logic can replace an LLM call entirely — no point using a model if a regex or lookup table gives the same answer. Then right-sizing the infrastructure and reviewing cost-per-request against business value before scaling up."

---

### Q15. What do you want to build in this role? Why RealPage?
> "I want to build the AI platform layer that turns one-off copilot experiments into reusable, measurable, production-safe products. What draws me to RealPage specifically is the Lumina platform — it's an AI product serving a real operational domain with real users. That means evaluation is grounded in something measurable, not just 'do stakeholders like it.' I also respect that RealPage's public messaging around AI focuses on augmenting on-site teams rather than replacing them. That's exactly the responsible AI framing I believe in — AI should make your best people more effective, not eliminate expertise. I want to help build the evaluation, governance, and reusable architecture layers that make that vision trustworthy at scale."

---

## CHALLENGE STORIES (ready to use)

### Story 1 — Schema Drift and Trust
- **Situation:** Schema-variant LLM feedback data from multiple sources with no consistent format
- **Task:** Automate transformation logic generation without losing accuracy or governance
- **Action:** Metadata-grounded RAG (retrieval over schema contracts + mapping rules), confidence scoring on agent outputs, HITL approval gate for low-confidence cases
- **Result:** Safer automation with 90%+ auto-promotion rate on high-confidence cases, reduced manual mapping effort, more consistent curated outputs

### Story 2 — Reliability and Idempotency at Scale
- **Situation:** High-volume ingestion with mixed API and file-drop sources; partial failures could corrupt curated layers
- **Task:** Ensure reruns are safe, bad loads are quarantined, and audit trail is always present
- **Action:** Immutable raw layer with manifest-based idempotency, validation/reconciliation gates before each stage advances, Airflow retry/backfill support, quarantine path for records failing schema contracts
- **Result:** Pipeline became replay-safe across any run — no reprocessing risk, clean audit in ops tables, reduced on-call incidents

### Story 3 — Stakeholder Delivery
- **Situation:** Downstream analytics teams needed evaluation insights faster; pipeline turnaround was too slow
- **Task:** Reduce time from ingestion to analytics-ready Gold tables without sacrificing quality
- **Action:** Partitioning strategy improvements, clustering on BigQuery analytical tables, SQL optimizations in star schema, better orchestration retry logic
- **Result:** Faster iteration cycle for model evaluation teams

---

## QUESTIONS TO ASK YURI (pick 2–3 max)

1. *"From your perspective, what separates AI projects at RealPage that make it to production from the ones that stay in experimentation?"*

2. *"How centralized do you want core AI capabilities — like retrieval, evals, and guardrails — to be versus embedded within individual product teams?"*

3. *"When you think about success for this role in the first six months, is it more about shipping one or two high-impact AI products, or about establishing architectural patterns the broader organization can build on?"*

4. *"Given the emphasis on responsible AI and evaluation frameworks, how mature is the current eval and observability setup, and where do you want this role to raise the bar?"*

---

## DELIVERY REMINDERS

| What to do | Why it lands with senior leaders |
|---|---|
| Slow down slightly | Shows confidence, not anxiety |
| Frame every answer: context → decision → why → outcome → lesson | Leadership pattern |
| Quantify where possible (TB/day, %, time saved) | Makes you credible |
| Avoid "we just used LangChain" answers | Shows depth |
| Say "I don't know yet, but here's how I'd approach it" when uncertain | Shows maturity |
| Never oversell "agents do everything" | Responsible AI framing they care about |
| Bring up governance, HITL, evaluation unprompted | Matches interviewer's public focus |

---

## JD ALIGNMENT CHECKLIST

| JD Requirement | Your Evidence |
|---|---|
| Multi-agent orchestration patterns | Agentic pipeline — tool-based orchestration, confidence routing, HITL |
| RAG and data retrieval architecture | Metadata-grounded retrieval — schema contracts, hybrid search |
| Reusable AI services and SDKs | Shared retrieval, evaluation harness, schema contract components |
| Prompt design and versioning | Prompt registry in agent service |
| Model and retrieval evaluation | Recall@K, groundedness, structured-output validity |
| AI observability and incident response | Audit logging, ops tables, manifest-based lineage |
| Responsible AI, PII, compliance | HITL gates, PII guardrails, content safety, quarantine flows |
| Cost management, model routing | Model routing, context budgeting, caching strategies |
| Stakeholder management | Translating tradeoffs to product/business in non-jargon terms |
| Mentoring and code quality | Pattern-driven mentoring, design/code reviews |

---

## DEEP TECHNICAL DRILL-DOWN (RECENT AI PROJECT)

Use this when leadership says: "Go deeper technically".

### 1) Full Flow (60–90 sec version)

> "End-to-end, my project has two connected parts: the data platform and the agent service. The data platform ingests multi-format LLM feedback data into immutable raw JSONL on GCS, then Bronze/Silver/Gold Dataproc stages normalize, validate, curate, and optionally publish to BigQuery. Every stage is run-scoped and idempotent through manifests and ops table updates. On top of that, I built a read-only agent service in FastAPI that routes user requests into Ops or DQ diagnostics, retrieves grounded evidence from ops sources, and proposes controlled draft actions. So the system combines deterministic data engineering reliability with an agentic interface that is safe, auditable, and governance-aligned." 

### 2) End-to-end architecture cheat sheet

| Area | What to say in interview |
|---|---|
| Input sources | CSV + JSON batches; API utility support exists; canonical envelope stored as raw JSONL |
| Core data stack | GCS (raw/bronze/silver/gold), Dataproc Serverless PySpark, BigQuery serving layer |
| Orchestration | Cloud Composer/Airflow with dynamic task mapping and rerun-aware planning |
| Agent backend | FastAPI + Pydantic service (`/sessions`, `/router`, `/proposals`, transform-designer, draft actions) |
| Retrieval | Hybrid retrieval over evidence corpus: embedding + lexical overlap fallback |
| Ops storage | `ops.pipeline_runs`, `ops.dq_results`, `ops.schema_registry`, `ops.deadletter_summary` |
| Agent audit storage | `agent_sessions`, `agent_tool_calls`, `agent_responses`, `agent_proposals` via audit store |
| Security controls | RBAC, read-only tool contracts, PII redaction, retention purge, draft-only automation |
| CI gate | GitHub Actions workflow runs agent tests + eval regression gate on PR/push |
| Exposure | Apigee scaffolding for authenticated, rate-limited, read-only API exposure |

### 3) Orchestration framework (Airflow/Composer)

If asked "What orchestration framework did you use?":

> "Cloud Composer (managed Airflow). I use two DAGs: one dependency-aware orchestration DAG from landed raw runs, and one full E2E DAG that also generates source batches before triggering Bronze, Silver, and Gold Dataproc jobs. The planner checks stage manifests per run_id so successful stages are skipped by default, and we can override with force_reprocess for controlled replay. This gave us branch-aware catch-up and idempotent rerun behavior without manual intervention."

### 4) What is an MCP server? (interview-ready answer)

> "MCP, Model Context Protocol, is a standard way for models/agents to discover and call tools through a structured interface instead of brittle custom glue. Conceptually, an MCP server exposes tool schemas, input validation, and execution boundaries. In my project, I implemented MCP-style principles through strict tool contracts: allowlisted tools, role-based authorization, input validation, and read-only execution for production safety. So even if not all integrations are fully MCP-native yet, the architecture is already protocol-friendly and governance-first."

### 5) Tools, models, and RAG architecture (exact talking points)

| Component | Current implementation |
|---|---|
| Tooling surface | Read-only tool registry for BigQuery template queries, GCS object reads, Composer DAG status, Dataproc batch status, schema diff |
| Tool guardrails | Role allowlist + table/DAG allowlist + URI validation + capped query limits |
| Embedding model | `text-embedding-3-small` (OpenAI), behind environment flag + API key |
| Retrieval mode | Hybrid: embedding cosine similarity + lexical score; fallback lexical if embedder unavailable |
| Generation style | Router is deterministic intent+evidence response; proposal text is controlled template-driven |
| RAG evidence sources | Ops BQ tables, DQ rule registry, manifest path conventions |

### 6) Input sources, chunking, context manager, memory

How to answer clearly and honestly:

> "In the current implementation the evidence corpus is a compact, domain-specific set of operational documents and table references, so we use document-level retrieval instead of heavy chunking. The retriever tokenizes and scores lexical overlap, plus embeddings when configured. Context is session-scoped: each request has session_id, mode, route, retrieved evidence refs, and tool call traces. We maintain short-lived in-memory session context for runtime speed, and persistent operational memory in BigQuery ops/audit tables for lineage, governance, and incident reconstruction."

If they ask "How would you scale chunking?":

> "I would move to semantic chunking with stable chunk IDs, metadata tags per source/run/stage, and offline retrieval evals before rollout, so we improve recall without polluting grounding quality."

### 7) Evaluation, confidence, and guardrails (production standards)

Use this as your standards answer:

> "I enforce three layers of standards. First, grounding and retrieval quality through eval gates: recall@k, route accuracy, grounding rate, and unsupported claim rate with strict thresholds in CI. Second, safety controls: RBAC, read-only tool permissions, PII redaction, retention purge, and draft-only automation for risky actions. Third, production reliability controls: run-scoped manifests, idempotent reruns, ops table instrumentation, and schema registry lineage. For transformation proposals, I include confidence scoring and keep low-confidence actions in human approval flow."

### 8) Deployment, CI/CD, observability, backend stack

| Area | What to say |
|---|---|
| Backend stack | Python, FastAPI, Pydantic, PySpark, Google Cloud Storage/BigQuery integrations |
| Deployment model | Dataproc Serverless jobs for data stages; Composer DAGs for orchestration; Agent API positioned behind Apigee policies |
| CI/CD | GitHub Actions workflow `agent-quality-gates` runs test suite + eval regression on PR/push |
| Observability | Stage manifests + ops tables + audit events + Airflow and Dataproc status tracing |
| Operational controls | Force reprocess flags, dynamic mapped retries, draft-only behavior for triggered actions |

### 9) Critical AI/agent challenges (problem -> root cause -> fix)

#### Challenge A: Trustworthy retrieval responses
- Problem (simple): Sometimes the assistant gave an answer that sounded correct, but the answer was not strongly tied to real evidence.
- Problem (technical): We had a grounding risk, where response quality looked fine linguistically, but evidence linkage was weak or missing.
- Root cause (simple): The system occasionally searched the wrong evidence pool or returned too little context.
- Root cause (technical): Sparse retrieval results and route mismatch (ops vs dq) lowered evidence recall and increased weakly grounded outputs.
- Fix (what we changed):
	- We made route selection deterministic first, then retrieval second, so query intent is resolved before evidence lookup.
	- We enforced route tags in retrieval to keep ops queries in ops evidence and dq queries in dq evidence.
	- We added fallback evidence references, so the response is never empty on grounding.
	- We added CI quality gates with strict thresholds: grounding_rate must remain 1.0 and unsupported_claim_rate must remain 0.0.
- Impact to say in interview: "We moved from plausible answers to evidence-backed answers, and we made grounding an enforceable release gate, not a best effort behavior."

#### Challenge B: Schema drift across multi-source feedback data
- Problem (simple): New fields or changed field types from source systems could break downstream tables without immediate visibility.
- Problem (technical): We were exposed to schema drift across heterogeneous ingestion sources, causing contract violations in curated layers.
- Root cause (simple): Different producers changed payload shape over time, and checks were happening too late.
- Root cause (technical): Upstream schema variability plus delayed contract enforcement allowed drift to propagate into Silver/Gold processing paths.
- Agentic flow we added (simple):
	- Step 1 Detect: Ops and DQ signals identify drift quickly (schema hash change, DQ spikes, deadletter increase).
	- Step 2 Diagnose: The DQ diagnostics path retrieves evidence from `ops.schema_registry`, `ops.dq_results`, and `ops.deadletter_summary`.
	- Step 3 Propose: Transform Designer agent generates a B2S or S2G remediation proposal with clear artifacts and confidence score.
	- Step 4 Govern: Proposal moves through `DRAFT -> REVIEW -> APPROVED/REJECTED` with role-gated approvals.
	- Step 5 Execute safely: We apply only approved changes, replay by `run_id`, and validate with manifest + ops updates.
- Agentic flow (technical controls):
	- Schema hash is tracked per run and first-seen snapshots are persisted in schema registry for lineage.
	- Evidence-grounded routing ensures schema decisions are based on operational data, not free-form guesses.
	- Deadletter/quarantine isolates contract-breaking records before they contaminate curated tables.
	- Early DQ checks and run-scoped replays reduce blast radius and recovery time.
- Impact to say in interview: "We moved from manual drift firefighting to an evidence-backed agentic remediation loop: detect, diagnose, propose, approve, replay. That improved reliability and reduced mean time to recovery without sacrificing governance."

#### Challenge C: Safe automation vs. speed
- Problem (business/simple): Product and business teams wanted faster delivery of AI-assisted fixes for pipeline issues, but they also needed confidence that changes would not create downstream incidents.
- Problem (business + technical): We had to improve team velocity and stakeholder responsiveness without compromising governance, auditability, and production reliability.
- Root cause (business/simple): Teams were aligned on speed goals, but there was no shared operational boundary between recommendation and execution.
- Root cause (technical): Proposal-to-action lifecycle controls were too loose, which made approvals, ownership, and traceability unclear across engineering, ops, and leadership.
- Agentic + process fix (what we changed):
	- We introduced an agentic decision-support flow where agents detect/diagnose issues and generate draft proposals, but execution remains human-governed.
	- We formalized a control workflow: DRAFT -> REVIEW -> APPROVED or REJECTED, so business and engineering have explicit decision checkpoints.
	- We enforced role-gated transitions to map responsibilities clearly across operator/engineer/approver roles.
	- We enabled quality-gated draft PR generation only after approval to preserve delivery speed with guardrails.
	- We explicitly blocked auto-merge and auto-deploy in this phase to protect production stability.
- Impact to say in interview: "We turned this into a business-team win: faster response to issues, clearer ownership between product and engineering, and leadership-level confidence that speed would not come at the cost of control."

### 10) Optimization strategy (effectiveness + efficiency)

| Challenge | Optimization strategy | Interview impact line |
|---|---|---|
| Retrieval quality variance | Hybrid scoring + curated evidence corpus + eval harness | "We improved consistency by making retrieval measurable, not subjective." |
| Pipeline replay cost | Manifest-based skip + run_id scoping + selective force reprocess | "We rerun only what failed, not the entire chain." |
| Operational debugging time | Standardized ops/audit rows with evidence refs and stage metadata | "Incidents moved from guesswork to traceable evidence." |
| Risk of unsafe automation | Draft-only controls + RBAC + PII redaction + human approval gates | "We scaled responsibly without overexposing production." |

#### A) Retrieval quality variance

- Simple explanation:
	When users ask similar questions, answers should be consistently grounded in the right evidence. Earlier, quality varied because some queries matched strongly while others matched weakly.
- What we optimized:
	- Hybrid scoring: We combine two signals when ranking evidence.
	- Curated evidence corpus: We keep a clean, domain-specific evidence set instead of noisy mixed content.
	- Eval harness: We continuously test retrieval and grounding quality using fixed benchmark cases.
- Technical terms (short meaning):
	- Hybrid scoring: ranking by combining more than one signal (for us: semantic similarity + lexical overlap).
	- Evidence corpus: the approved set of documents/tables the agent is allowed to retrieve from.
	- Eval harness: an automated test suite that checks model/retrieval behavior against known expectations.
	- Grounding: ensuring the answer is tied to retrieved evidence, not guesswork.
- Why leadership should care:
	Better consistency means fewer surprise answers, higher trust, and easier production support.

#### B) Pipeline replay cost

- Simple explanation:
	In data platforms, failures happen. The expensive way is rerunning everything. The efficient way is rerunning only the failed portion.
- What we optimized:
	- Manifest-based skip: if a stage already finished successfully, we skip it.
	- run_id scoping: each run is isolated, so we can target exactly one failed run.
	- Selective force reprocess: we can intentionally rerun specific stages only when needed.
- Technical terms (short meaning):
	- Manifest: a completion record containing stage status, counts, paths, timestamps, and lineage.
	- run_id: unique identifier for one pipeline execution.
	- Idempotent rerun: rerunning does not create duplicates or corrupt outputs.
	- Force reprocess: controlled override to rerun even when prior completion exists.
- Why leadership should care:
	Lower compute cost, faster recovery, and predictable incident handling.

#### C) Operational debugging time

- Simple explanation:
	During incidents, teams lose time if logs and status are scattered. We optimized for fast root-cause discovery.
- What we optimized:
	- Standardized ops rows: each stage writes consistent operational records.
	- Audit rows: agent actions, tool calls, and responses are recorded in a structured way.
	- Evidence references + stage metadata: every event links to source evidence and run/stage context.
- Technical terms (short meaning):
	- Ops table: structured operational telemetry table (status, counts, timing, error context).
	- Audit trail: chronological, tamper-resistant activity history for investigation/compliance.
	- Metadata: context fields such as run_id, stage, timestamps, manifest path, schema hash.
	- Traceability: ability to connect decision -> evidence -> action -> output.
- Why leadership should care:
	Faster MTTR (Mean Time To Recovery), fewer escalations, and better cross-team coordination.

#### D) Risk of unsafe automation

- Simple explanation:
	Automation increases speed, but uncontrolled automation can create production incidents. We added guardrails so speed does not break safety.
- What we optimized:
	- Draft-only controls: agent can propose, not directly execute risky actions.
	- RBAC: only authorized roles can approve specific transitions.
	- PII redaction: sensitive data is masked in logs and responses.
	- Human approval gates: important actions require explicit review/approval.
- Technical terms (short meaning):
	- RBAC (Role-Based Access Control): permissions tied to role (viewer/operator/approver/admin).
	- PII redaction: automatic masking of personal/sensitive information.
	- Approval gate: required checkpoint before execution can continue.
	- Governance: policy controls that ensure compliant, auditable, safe operation.
- Why leadership should care:
	Stronger compliance posture, lower risk exposure, and safer scaling of agentic workflows.

#### 30-second summary you can say in interview

"We optimized in four practical areas: answer quality, rerun efficiency, debugging speed, and automation safety. Technically that meant hybrid retrieval scoring with eval gates, manifest-driven selective replay, standardized ops and audit traceability, and strict governance controls like RBAC and approval gates. Business outcome was simple: more reliable delivery at lower operational risk and cost."

### 11) Clear impact statements (speak these verbatim)

1. "We converted AI-assisted operations from ad-hoc responses into evidence-backed, auditable workflows."
2. "We reduced reprocessing risk with idempotent manifests and run-scoped orchestration."
3. "We improved incident diagnosability using ops/audit tables that connect queries, evidence, tool calls, and outcomes."
4. "We raised trust by enforcing grounding, role-based access, and draft-only execution for sensitive flows."

### 12) Current agents you can claim confidently

| Agent/Capability | What it does |
|---|---|
| Ops Diagnostics Agent | Routes ops questions, retrieves pipeline and schema evidence, returns read-only operational diagnosis |
| DQ Diagnostics Agent | Routes DQ/deadletter/rule questions, retrieves DQ evidence, suggests controlled follow-up |
| Transform Designer Agent | Generates B2S/S2G transform proposal artifacts for schema drift/mapping/DQ updates with confidence score |
| Trigger Response Agent | Converts runtime events (DAG failure, Dataproc failure, DQ spike, schema change) into draft-only proposals + notifications |
| PR Draft Assistant | Builds quality-gated draft PR payloads from approved proposals, explicitly no auto-merge/deploy |

How to position this to leadership:

> "I intentionally started with constrained, high-trust agents: diagnostic and proposal agents before execution agents. That sequencing increased adoption while keeping risk low."

---

## CULTURAL FIT + PROJECT MANAGEMENT PREP

### 1) Team-fit answers leadership expects

#### Q: How do you handle unclear requirements?
> "I run requirement clarification in two tracks: decision clarity and delivery clarity. Decision clarity means identifying what business decision we are improving. Delivery clarity means locking scope, measurable success criteria, and non-goals. For AI work I explicitly separate what is deterministic from what is probabilistic so expectations are realistic from day one."

#### Q: How do you manage deadlines when uncertainty is high?
> "I use milestone-based delivery with risk slicing. I commit hard on stable components and time-box experiments for uncertain parts. I also define go/no-go checkpoints with product stakeholders so we can pivot early instead of discovering misalignment at the end."

#### Q: How do you communicate bad news or delays?
> "Early, direct, and with options. I communicate the issue, root cause, impact, and two recovery paths with tradeoffs. Leadership generally accepts delay when the communication is transparent and the plan is concrete."

#### Q: What's your leadership style?
> "High ownership, low ego, strong standards. I like to set architecture guardrails and quality bars, but give engineers room to design and own implementation. I coach through design reviews, not micromanagement."

#### Q: How do you mentor junior engineers?
> "I use reusable playbooks: coding patterns, review checklists, runbooks, and eval templates. I pair on high-risk tasks and then progressively transfer ownership. The goal is that they can make good decisions without waiting for me."

### 2) Stakeholder + execution scenarios

#### Scenario: Product asks for a feature by quarter-end, but quality risks are high.
Answer:
> "I'd propose a staged release: phase 1 read-only or advisory mode with full observability, phase 2 controlled write actions behind approval gates once quality metrics hit thresholds. That protects timeline and quality at the same time."

#### Scenario: Team disagreement on architecture.
Answer:
> "I ask for explicit evaluation criteria first: reliability, cost, latency, governance, maintainability. Then we run a short comparison against those criteria and document the decision. This keeps debate objective and avoids opinion-driven deadlocks."

#### Scenario: Cross-functional communication gap.
Answer:
> "I use a single-page operating memo per initiative: goals, metrics, risks, dependencies, timeline, owners. It becomes the shared source for product, engineering, and leadership updates."

### 3) Work-style and collaboration questions (rapid answers)

1. "How do you prioritize?"  
"By impact, risk, and reversibility. High-impact + high-risk items get early design rigor."
2. "How do you run meetings?"  
"Clear objective, pre-read where needed, decisions captured with owners and dates."
3. "How do you handle conflict?"  
"Address it early in 1:1, align on facts and outcomes, escalate only when needed."
4. "How do you balance speed and quality?"  
"Ship thin slices fast, but never compromise on safety, observability, and rollback."
5. "How do you work with remote/hybrid teams?"  
"Async-first documentation, explicit ownership, predictable review SLAs, and short decision loops."

---

## 24-HOUR PREP PLAN (FINAL SHARPENING)

1. Rehearse your 90-second intro 5 times until it sounds conversational.
2. Practice 3 challenge stories using problem -> root cause -> fix -> impact.
3. Memorize 6 hard terms: idempotency, grounding, unsupported claim rate, schema registry, draft-only automation, confidence routing.
4. Prepare 2 numbers you can state confidently (volume, quality, cycle-time, or reliability).
5. End with 2 strategic questions to Yuri (not more than 3).
