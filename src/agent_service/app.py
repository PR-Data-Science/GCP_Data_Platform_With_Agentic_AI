from __future__ import annotations

from datetime import datetime, timezone
from threading import Lock
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field

from src.agent_service.audit import AuditStore, ProposalRecord
from src.agent_service.ci_gate import run_quality_gates
from src.agent_service.console_ui import build_console_html
from src.agent_service.retrieval import EvidenceRetriever, default_evidence_corpus
from src.agent_service.security import OPERATOR_ROLES, PRIVILEGED_ROLES, assert_role_allowed, redact_pii
from src.agent_service.transform_designer import build_transform_design_artifacts

class SessionCreateRequest(BaseModel):
    user_id: str | None = Field(default=None)
    mode: Literal["ops", "dq"] = Field(default="ops")


class SessionRecord(BaseModel):
    session_id: str
    user_id: str | None = None
    mode: Literal["ops", "dq"]
    created_ts: str
    read_only: bool = True


class RouterRequest(BaseModel):
    session_id: str
    query: str
    intent_hint: Literal["ops", "dq"] | None = None


class ToolCall(BaseModel):
    tool_name: str
    arguments: dict[str, str]


class RouterResponse(BaseModel):
    session_id: str
    route: Literal["ops", "dq"]
    response_text: str
    evidence_refs: list[str]
    tool_calls: list[ToolCall]
    read_only: bool = True


class ProposalCreateRequest(BaseModel):
    session_id: str
    route: Literal["ops", "dq"]
    title: str
    proposal_text: str
    evidence_refs: list[str] = Field(default_factory=list)


class ProposalStatusUpdateRequest(BaseModel):
    status: Literal["REVIEW", "APPROVED", "REJECTED"]
    actor_role: Literal["viewer", "operator", "engineer", "approver", "admin"]


class TransformProposalRequest(BaseModel):
    session_id: str
    layer: Literal["B2S", "S2G"]
    change_type: Literal["schema_drift", "new_mapping", "dq_rule_update", "kpi_update", "curation_update"]
    source_table: str
    target_table: str
    problem_statement: str
    evidence_refs: list[str] = Field(default_factory=list)
    run_id: str | None = None
    env: Literal["dev", "prod"] = "dev"


class TransformProposalResponse(BaseModel):
    proposal: ProposalRecord
    layer: Literal["B2S", "S2G"]
    change_type: str
    generated_artifacts: list[str]
    confidence_score: float
    auto_applied: bool = False


class PRDraftRequest(BaseModel):
    actor_role: Literal["viewer", "operator", "engineer", "approver", "admin"]
    target_branch: str = "main"
    labels: list[str] = Field(default_factory=lambda: ["agent-proposal", "draft"])


class PRDraftResponse(BaseModel):
    proposal_id: str
    title: str
    body: str
    target_branch: str
    labels: list[str]
    quality_gate_passed: bool
    quality_metrics: dict[str, float]
    draft_only: bool = True
    created: bool = False


class TriggerDraftRequest(BaseModel):
    session_id: str
    event_type: Literal[
        "composer_dag_failure",
        "dataproc_job_failure",
        "dq_failure_spike",
        "schema_hash_change",
    ]
    actor_role: Literal["viewer", "operator", "engineer", "approver", "admin"]
    summary: str
    evidence_refs: list[str] = Field(default_factory=list)
    run_id: str | None = None


class TriggerDraftResponse(BaseModel):
    event_type: str
    notification_channels: list[str]
    notification_payload: dict[str, str]
    proposal_id: str
    draft_only: bool = True
    auto_execute: bool = False


class RetentionPurgeRequest(BaseModel):
    max_age_seconds: int = Field(ge=1)
    actor_role: Literal["viewer", "operator", "engineer", "approver", "admin"]


class InMemorySessionStore:
    def __init__(self) -> None:
        self._sessions: dict[str, SessionRecord] = {}
        self._lock = Lock()

    def create(self, payload: SessionCreateRequest) -> SessionRecord:
        session = SessionRecord(
            session_id=str(uuid4()),
            user_id=payload.user_id,
            mode=payload.mode,
            created_ts=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            read_only=True,
        )
        with self._lock:
            self._sessions[session.session_id] = session
        return session

    def get(self, session_id: str) -> SessionRecord | None:
        return self._sessions.get(session_id)


app = FastAPI(title="LLM Feedback Agent Service", version="0.1.0")
store = InMemorySessionStore()
retriever = EvidenceRetriever(default_evidence_corpus())
audit_store = AuditStore()


def resolve_route(query: str, intent_hint: str | None) -> Literal["ops", "dq"]:
    if intent_hint in {"ops", "dq"}:
        return intent_hint

    lowered = query.lower()
    dq_keywords = ["dq", "deadletter", "rule", "violation", "quality"]
    if any(keyword in lowered for keyword in dq_keywords):
        return "dq"
    return "ops"


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "llm-feedback-agent"}


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/console")


@app.get("/console", response_class=HTMLResponse)
def console() -> str:
    return build_console_html()


@app.post("/sessions", response_model=SessionRecord)
def create_session(payload: SessionCreateRequest) -> SessionRecord:
    session = store.create(payload)
    audit_store.log_session(
        session_id=session.session_id,
        user_id=session.user_id,
        mode=session.mode,
        read_only=session.read_only,
    )
    return session


@app.get("/sessions/{session_id}", response_model=SessionRecord)
def get_session(session_id: str) -> SessionRecord:
    session = store.get(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session_not_found")
    return session


@app.post("/router", response_model=RouterResponse)
def router(payload: RouterRequest) -> RouterResponse:
    session = store.get(payload.session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session_not_found")

    route = resolve_route(query=payload.query, intent_hint=payload.intent_hint)
    retrieved_docs = retriever.search(payload.query, top_k=3, required_tag=route)
    evidence_refs = [doc.uri for doc in retrieved_docs]

    if not evidence_refs:
        if route == "dq":
            evidence_refs = ["bq://ops.dq_results", "bq://ops.deadletter_summary", "gcs://dq/dq_rules.yaml"]
        else:
            evidence_refs = ["bq://ops.pipeline_runs", "bq://ops.schema_registry"]

    if route == "dq":
        response_text = f"Route selected: dq. Returning read-only DQ diagnostics with {len(evidence_refs)} evidence refs."
        tool_calls = [
            ToolCall(tool_name="bq_query", arguments={"table": "ops.dq_results"}),
            ToolCall(tool_name="bq_query", arguments={"table": "ops.deadletter_summary"}),
        ]
    else:
        response_text = f"Route selected: ops. Returning read-only pipeline operations with {len(evidence_refs)} evidence refs."
        tool_calls = [
            ToolCall(tool_name="bq_query", arguments={"table": "ops.pipeline_runs"}),
            ToolCall(tool_name="bq_query", arguments={"table": "ops.schema_registry"}),
        ]

    response = RouterResponse(
        session_id=payload.session_id,
        route=route,
        response_text=response_text,
        evidence_refs=evidence_refs,
        tool_calls=tool_calls,
        read_only=True,
    )
    audit_store.log_router_trace(
        session_id=payload.session_id,
        route=route,
        query_text=redact_pii(payload.query),
        response_text=redact_pii(response.response_text),
        evidence_refs=response.evidence_refs,
        tool_calls=[{"tool_name": call.tool_name, "arguments": call.arguments} for call in response.tool_calls],
        read_only=response.read_only,
    )
    return response


@app.post("/proposals", response_model=ProposalRecord)
def create_proposal(payload: ProposalCreateRequest) -> ProposalRecord:
    session = store.get(payload.session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session_not_found")
    return audit_store.create_proposal(
        session_id=payload.session_id,
        route=payload.route,
        title=payload.title,
        proposal_text=payload.proposal_text,
        evidence_refs=payload.evidence_refs,
    )


@app.post("/transform-designer/proposals", response_model=TransformProposalResponse)
def create_transform_designer_proposal(payload: TransformProposalRequest) -> TransformProposalResponse:
    session = store.get(payload.session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session_not_found")

    design = build_transform_design_artifacts(
        proposal_id=str(uuid4()),
        layer=payload.layer,
        change_type=payload.change_type,
        source_table=payload.source_table,
        target_table=payload.target_table,
        problem_statement=payload.problem_statement,
        run_id=payload.run_id,
        env=payload.env,
    )

    title_prefix = "B2S" if payload.layer == "B2S" else "S2G"
    proposal = audit_store.create_proposal(
        session_id=payload.session_id,
        route="dq" if payload.layer == "B2S" else "ops",
        title=f"{title_prefix} Transform Proposal: {payload.change_type}",
        proposal_text=design.proposal_text,
        evidence_refs=payload.evidence_refs,
    )

    # Recompute artifacts with the persisted proposal id to keep object paths stable.
    persisted_design = build_transform_design_artifacts(
        proposal_id=proposal.proposal_id,
        layer=payload.layer,
        change_type=payload.change_type,
        source_table=payload.source_table,
        target_table=payload.target_table,
        problem_statement=payload.problem_statement,
        run_id=payload.run_id,
        env=payload.env,
    )

    return TransformProposalResponse(
        proposal=proposal,
        layer=payload.layer,
        change_type=payload.change_type,
        generated_artifacts=persisted_design.generated_artifacts,
        confidence_score=persisted_design.confidence_score,
        auto_applied=False,
    )


@app.post("/proposals/{proposal_id}/pr-draft", response_model=PRDraftResponse)
def create_pr_draft(proposal_id: str, payload: PRDraftRequest) -> PRDraftResponse:
    try:
        assert_role_allowed(actor_role=payload.actor_role, allowed_roles=PRIVILEGED_ROLES, action="pr_draft")
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc

    proposal = audit_store.get_proposal(proposal_id)
    if proposal is None:
        raise HTTPException(status_code=404, detail="proposal_not_found")
    if proposal.status != "APPROVED":
        raise HTTPException(status_code=400, detail=f"proposal_not_approved:{proposal.status}")

    quality = run_quality_gates(retriever)
    if not quality.passed:
        raise HTTPException(status_code=412, detail="quality_gate_failed")

    title = f"[DRAFT] Agent Proposal {proposal.proposal_id}: {proposal.title}"
    body_lines = [
        "This is a controlled draft PR payload generated by the agent service.",
        "",
        f"Proposal ID: {proposal.proposal_id}",
        f"Route: {proposal.route}",
        f"Status: {proposal.status}",
        f"Evidence refs: {', '.join(proposal.evidence_refs) if proposal.evidence_refs else 'none'}",
        "",
        "Proposal text:",
        redact_pii(proposal.proposal_text),
        "",
        "Safety: draft-only, no auto-merge, no auto-deploy.",
    ]
    return PRDraftResponse(
        proposal_id=proposal.proposal_id,
        title=title,
        body="\n".join(body_lines),
        target_branch=payload.target_branch,
        labels=payload.labels,
        quality_gate_passed=quality.passed,
        quality_metrics=quality.metrics,
        draft_only=True,
        created=False,
    )


@app.get("/proposals/{proposal_id}", response_model=ProposalRecord)
def get_proposal(proposal_id: str) -> ProposalRecord:
    proposal = audit_store.get_proposal(proposal_id)
    if proposal is None:
        raise HTTPException(status_code=404, detail="proposal_not_found")
    return proposal


@app.post("/proposals/{proposal_id}/status", response_model=ProposalRecord)
def update_proposal_status(proposal_id: str, payload: ProposalStatusUpdateRequest) -> ProposalRecord:
    try:
        proposal = audit_store.update_proposal_status(
            proposal_id=proposal_id,
            new_status=payload.status,
            actor_role=payload.actor_role,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if proposal is None:
        raise HTTPException(status_code=404, detail="proposal_not_found")
    return proposal


@app.post("/triggers/draft-actions", response_model=TriggerDraftResponse)
def handle_trigger_draft_actions(payload: TriggerDraftRequest) -> TriggerDraftResponse:
    session = store.get(payload.session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session_not_found")

    try:
        assert_role_allowed(actor_role=payload.actor_role, allowed_roles=OPERATOR_ROLES, action="trigger_draft")
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc

    route = "dq" if payload.event_type in {"dq_failure_spike", "schema_hash_change"} else "ops"
    proposal = audit_store.create_proposal(
        session_id=payload.session_id,
        route=route,
        title=f"Auto-draft from trigger: {payload.event_type}",
        proposal_text=(
            "Draft-only trigger proposal created by automation handler. "
            "No rerun/merge/deploy executed. "
            f"Summary: {redact_pii(payload.summary)}"
        ),
        evidence_refs=payload.evidence_refs,
    )

    notification_payload = {
        "event_type": payload.event_type,
        "session_id": payload.session_id,
        "proposal_id": proposal.proposal_id,
        "run_id": payload.run_id or "latest_failed_run",
        "summary": redact_pii(payload.summary),
    }
    return TriggerDraftResponse(
        event_type=payload.event_type,
        notification_channels=["slack", "email", "jira"],
        notification_payload=notification_payload,
        proposal_id=proposal.proposal_id,
        draft_only=True,
        auto_execute=False,
    )


@app.get("/sessions/{session_id}/proposals", response_model=list[ProposalRecord])
def list_session_proposals(session_id: str) -> list[ProposalRecord]:
    session = store.get(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session_not_found")
    return audit_store.list_proposals(session_id)


@app.get("/sessions/{session_id}/audit-summary")
def get_audit_summary(session_id: str) -> dict[str, int]:
    session = store.get(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session_not_found")
    return audit_store.summary(session_id)


@app.post("/admin/audit-retention/purge")
def purge_audit_retention(payload: RetentionPurgeRequest) -> dict[str, int]:
    try:
        assert_role_allowed(actor_role=payload.actor_role, allowed_roles=PRIVILEGED_ROLES, action="retention_purge")
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return audit_store.purge_older_than(max_age_seconds=payload.max_age_seconds)


@app.get("/security/posture")
def security_posture() -> dict[str, object]:
    return {
        "rbac": True,
        "pii_redaction": True,
        "retention_controls": True,
        "draft_only_automation": True,
        "allowed_roles": {
            "pr_draft": sorted(PRIVILEGED_ROLES),
            "trigger_draft": sorted(OPERATOR_ROLES),
        },
    }
