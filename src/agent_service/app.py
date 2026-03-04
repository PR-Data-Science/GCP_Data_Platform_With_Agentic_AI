from __future__ import annotations

from datetime import datetime, timezone
from threading import Lock
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from src.agent_service.audit import AuditStore, ProposalRecord
from src.agent_service.retrieval import EvidenceRetriever, default_evidence_corpus

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
        query_text=payload.query,
        response_text=response.response_text,
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


@app.get("/proposals/{proposal_id}", response_model=ProposalRecord)
def get_proposal(proposal_id: str) -> ProposalRecord:
    proposal = audit_store.get_proposal(proposal_id)
    if proposal is None:
        raise HTTPException(status_code=404, detail="proposal_not_found")
    return proposal


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
