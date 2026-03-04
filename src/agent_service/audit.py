from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Literal
from uuid import uuid4

from src.ops.ops_writer import (
    write_agent_proposals,
    write_agent_responses,
    write_agent_sessions,
    write_agent_tool_calls,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class AuditSessionEvent:
    session_id: str
    user_id: str | None
    mode: Literal["ops", "dq"]
    read_only: bool
    created_ts: str


@dataclass(frozen=True)
class AuditToolCallEvent:
    event_id: str
    session_id: str
    route: Literal["ops", "dq"]
    tool_name: str
    tool_args_json: str
    evidence_refs: list[str]
    created_ts: str


@dataclass(frozen=True)
class AuditResponseEvent:
    event_id: str
    session_id: str
    route: Literal["ops", "dq"]
    query_text: str
    response_text: str
    evidence_refs: list[str]
    read_only: bool
    created_ts: str


@dataclass(frozen=True)
class ProposalRecord:
    proposal_id: str
    session_id: str
    route: Literal["ops", "dq"]
    title: str
    proposal_text: str
    evidence_refs: list[str]
    status: Literal["DRAFT", "REVIEW", "APPROVED", "REJECTED"]
    created_ts: str
    updated_ts: str


class AuditStore:
    def __init__(self, ops_dataset: str | None = None) -> None:
        self._ops_dataset = ops_dataset or os.getenv("AGENT_OPS_DATASET", "ops")
        self._lock = Lock()
        self._sessions: dict[str, AuditSessionEvent] = {}
        self._tool_calls: list[AuditToolCallEvent] = []
        self._responses: list[AuditResponseEvent] = []
        self._proposals: dict[str, ProposalRecord] = {}

    def _persist_sessions(self, rows: list[dict]) -> None:
        try:
            write_agent_sessions(rows, dataset=self._ops_dataset)
        except Exception:
            return

    def _persist_tool_calls(self, rows: list[dict]) -> None:
        try:
            write_agent_tool_calls(rows, dataset=self._ops_dataset)
        except Exception:
            return

    def _persist_responses(self, rows: list[dict]) -> None:
        try:
            write_agent_responses(rows, dataset=self._ops_dataset)
        except Exception:
            return

    def _persist_proposals(self, rows: list[dict]) -> None:
        try:
            write_agent_proposals(rows, dataset=self._ops_dataset)
        except Exception:
            return

    def log_session(self, *, session_id: str, user_id: str | None, mode: Literal["ops", "dq"], read_only: bool) -> None:
        event = AuditSessionEvent(
            session_id=session_id,
            user_id=user_id,
            mode=mode,
            read_only=read_only,
            created_ts=utc_now_iso(),
        )
        with self._lock:
            self._sessions[session_id] = event
        self._persist_sessions([asdict(event)])

    def log_router_trace(
        self,
        *,
        session_id: str,
        route: Literal["ops", "dq"],
        query_text: str,
        response_text: str,
        evidence_refs: list[str],
        tool_calls: list[dict[str, object]],
        read_only: bool,
    ) -> None:
        tool_rows: list[dict] = []
        with self._lock:
            for tool_call in tool_calls:
                event = AuditToolCallEvent(
                    event_id=str(uuid4()),
                    session_id=session_id,
                    route=route,
                    tool_name=str(tool_call.get("tool_name", "unknown")),
                    tool_args_json=json.dumps(tool_call.get("arguments", {}), ensure_ascii=False),
                    evidence_refs=evidence_refs,
                    created_ts=utc_now_iso(),
                )
                self._tool_calls.append(event)
                tool_rows.append(asdict(event))

            response_event = AuditResponseEvent(
                event_id=str(uuid4()),
                session_id=session_id,
                route=route,
                query_text=query_text,
                response_text=response_text,
                evidence_refs=evidence_refs,
                read_only=read_only,
                created_ts=utc_now_iso(),
            )
            self._responses.append(response_event)

        if tool_rows:
            self._persist_tool_calls(tool_rows)
        self._persist_responses([asdict(response_event)])

    def create_proposal(
        self,
        *,
        session_id: str,
        route: Literal["ops", "dq"],
        title: str,
        proposal_text: str,
        evidence_refs: list[str],
    ) -> ProposalRecord:
        now = utc_now_iso()
        proposal = ProposalRecord(
            proposal_id=str(uuid4()),
            session_id=session_id,
            route=route,
            title=title,
            proposal_text=proposal_text,
            evidence_refs=evidence_refs,
            status="DRAFT",
            created_ts=now,
            updated_ts=now,
        )
        with self._lock:
            self._proposals[proposal.proposal_id] = proposal
        self._persist_proposals([asdict(proposal)])
        return proposal

    def get_proposal(self, proposal_id: str) -> ProposalRecord | None:
        return self._proposals.get(proposal_id)

    def list_proposals(self, session_id: str) -> list[ProposalRecord]:
        return [proposal for proposal in self._proposals.values() if proposal.session_id == session_id]

    def summary(self, session_id: str) -> dict[str, int]:
        return {
            "session_events": 1 if session_id in self._sessions else 0,
            "tool_call_events": sum(1 for event in self._tool_calls if event.session_id == session_id),
            "response_events": sum(1 for event in self._responses if event.session_id == session_id),
            "proposal_events": sum(1 for proposal in self._proposals.values() if proposal.session_id == session_id),
        }
