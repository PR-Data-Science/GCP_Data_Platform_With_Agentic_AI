from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Literal
from uuid import uuid4

from src.ops.ops_writer import (
    write_agent_proposals,
    write_agent_responses,
    write_agent_sessions,
    write_agent_tool_calls,
)


PROPOSAL_TRANSITIONS: dict[str, set[str]] = {
    "DRAFT": {"REVIEW"},
    "REVIEW": {"APPROVED", "REJECTED"},
    "APPROVED": set(),
    "REJECTED": set(),
}

REVIEW_ALLOWED_ROLES = {"operator", "engineer", "approver", "admin"}
APPROVE_ALLOWED_ROLES = {"approver", "admin"}


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

    def update_proposal_status(self, *, proposal_id: str, new_status: str, actor_role: str) -> ProposalRecord | None:
        with self._lock:
            proposal = self._proposals.get(proposal_id)
            if proposal is None:
                return None

            allowed_targets = PROPOSAL_TRANSITIONS.get(proposal.status, set())
            if new_status not in allowed_targets:
                raise ValueError(f"invalid_status_transition:{proposal.status}->{new_status}")

            if new_status == "REVIEW" and actor_role not in REVIEW_ALLOWED_ROLES:
                raise PermissionError(f"role_not_allowed_for_review:{actor_role}")

            if new_status in {"APPROVED", "REJECTED"} and actor_role not in APPROVE_ALLOWED_ROLES:
                raise PermissionError(f"role_not_allowed_for_approval:{actor_role}")

            updated = ProposalRecord(
                proposal_id=proposal.proposal_id,
                session_id=proposal.session_id,
                route=proposal.route,
                title=proposal.title,
                proposal_text=proposal.proposal_text,
                evidence_refs=proposal.evidence_refs,
                status=new_status,
                created_ts=proposal.created_ts,
                updated_ts=utc_now_iso(),
            )
            self._proposals[proposal_id] = updated

        self._persist_proposals([asdict(updated)])
        return updated

    def list_proposals(self, session_id: str) -> list[ProposalRecord]:
        return [proposal for proposal in self._proposals.values() if proposal.session_id == session_id]

    def summary(self, session_id: str) -> dict[str, int]:
        return {
            "session_events": 1 if session_id in self._sessions else 0,
            "tool_call_events": sum(1 for event in self._tool_calls if event.session_id == session_id),
            "response_events": sum(1 for event in self._responses if event.session_id == session_id),
            "proposal_events": sum(1 for proposal in self._proposals.values() if proposal.session_id == session_id),
        }

    def purge_older_than(self, *, max_age_seconds: int) -> dict[str, int]:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=max_age_seconds)

        def _ts(value: str) -> datetime:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))

        with self._lock:
            before = {
                "sessions": len(self._sessions),
                "tool_calls": len(self._tool_calls),
                "responses": len(self._responses),
                "proposals": len(self._proposals),
            }

            self._sessions = {
                sid: event
                for sid, event in self._sessions.items()
                if _ts(event.created_ts) >= cutoff
            }
            self._tool_calls = [event for event in self._tool_calls if _ts(event.created_ts) >= cutoff]
            self._responses = [event for event in self._responses if _ts(event.created_ts) >= cutoff]
            self._proposals = {
                pid: proposal
                for pid, proposal in self._proposals.items()
                if _ts(proposal.updated_ts) >= cutoff
            }

            after = {
                "sessions": len(self._sessions),
                "tool_calls": len(self._tool_calls),
                "responses": len(self._responses),
                "proposals": len(self._proposals),
            }

        return {
            "removed_sessions": before["sessions"] - after["sessions"],
            "removed_tool_calls": before["tool_calls"] - after["tool_calls"],
            "removed_responses": before["responses"] - after["responses"],
            "removed_proposals": before["proposals"] - after["proposals"],
        }
