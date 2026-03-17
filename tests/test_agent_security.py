from __future__ import annotations

from fastapi.testclient import TestClient

from src.agent_service.app import app
from src.agent_service.security import redact_pii


client = TestClient(app)


def _create_approved_proposal() -> dict:
    session = client.post("/sessions", json={"user_id": "sec-u1", "mode": "ops"}).json()
    proposal = client.post(
        "/proposals",
        json={
            "session_id": session["session_id"],
            "route": "ops",
            "title": "Security proposal",
            "proposal_text": "Contains email alice@example.com and key sk-1234567890ABCDEFGHIJKL",
            "evidence_refs": ["bq://ops.pipeline_runs"],
        },
    ).json()
    client.post(
        f"/proposals/{proposal['proposal_id']}/status",
        json={"status": "REVIEW", "actor_role": "operator"},
    )
    client.post(
        f"/proposals/{proposal['proposal_id']}/status",
        json={"status": "APPROVED", "actor_role": "approver"},
    )
    return proposal


def test_redact_pii_masks_email_and_keys() -> None:
    text = "contact alice@example.com with Bearer abc.def and sk-1234567890ABCDEFGHIJKL"
    out = redact_pii(text)
    assert "alice@example.com" not in out
    assert "sk-1234567890ABCDEFGHIJKL" not in out
    assert "[REDACTED_EMAIL]" in out
    assert "[REDACTED_API_KEY]" in out
    assert "Bearer [REDACTED_TOKEN]" in out


def test_pr_draft_requires_approved_proposal() -> None:
    session = client.post("/sessions", json={"user_id": "sec-u2", "mode": "ops"}).json()
    proposal = client.post(
        "/proposals",
        json={
            "session_id": session["session_id"],
            "route": "ops",
            "title": "Draft only",
            "proposal_text": "Still in draft",
            "evidence_refs": ["bq://ops.pipeline_runs"],
        },
    ).json()

    response = client.post(
        f"/proposals/{proposal['proposal_id']}/pr-draft",
        json={"actor_role": "approver", "target_branch": "main", "labels": ["agent-proposal"]},
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "proposal_not_approved:DRAFT"


def test_pr_draft_success_with_quality_gate() -> None:
    proposal = _create_approved_proposal()
    response = client.post(
        f"/proposals/{proposal['proposal_id']}/pr-draft",
        json={"actor_role": "approver", "target_branch": "main", "labels": ["agent-proposal", "draft"]},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["proposal_id"] == proposal["proposal_id"]
    assert body["draft_only"] is True
    assert body["created"] is False
    assert body["quality_gate_passed"] is True
    assert "[DRAFT] Agent Proposal" in body["title"]


def test_pr_draft_rejects_disallowed_role() -> None:
    proposal = _create_approved_proposal()
    response = client.post(
        f"/proposals/{proposal['proposal_id']}/pr-draft",
        json={"actor_role": "viewer", "target_branch": "main", "labels": ["agent-proposal"]},
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "role_not_allowed_for_pr_draft:viewer"


def test_trigger_draft_actions_creates_proposal_and_notifications() -> None:
    session = client.post("/sessions", json={"user_id": "sec-u3", "mode": "dq"}).json()
    response = client.post(
        "/triggers/draft-actions",
        json={
            "session_id": session["session_id"],
            "event_type": "dq_failure_spike",
            "actor_role": "operator",
            "summary": "DQ spike for user bob@example.com",
            "evidence_refs": ["bq://ops.dq_results"],
            "run_id": "run-999",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["draft_only"] is True
    assert body["auto_execute"] is False
    assert body["notification_payload"]["run_id"] == "run-999"
    assert "[REDACTED_EMAIL]" in body["notification_payload"]["summary"]


def test_trigger_draft_actions_rejects_disallowed_role() -> None:
    session = client.post("/sessions", json={"user_id": "sec-u4", "mode": "ops"}).json()
    response = client.post(
        "/triggers/draft-actions",
        json={
            "session_id": session["session_id"],
            "event_type": "composer_dag_failure",
            "actor_role": "viewer",
            "summary": "DAG failed",
            "evidence_refs": [],
        },
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "role_not_allowed_for_trigger_draft:viewer"


def test_retention_purge_requires_privileged_role() -> None:
    response = client.post(
        "/admin/audit-retention/purge",
        json={"max_age_seconds": 1, "actor_role": "viewer"},
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "role_not_allowed_for_retention_purge:viewer"


def test_retention_purge_admin_success() -> None:
    response = client.post(
        "/admin/audit-retention/purge",
        json={"max_age_seconds": 1, "actor_role": "admin"},
    )
    assert response.status_code == 200
    body = response.json()
    assert "removed_sessions" in body
    assert "removed_tool_calls" in body
    assert "removed_responses" in body
    assert "removed_proposals" in body


def test_security_posture_endpoint() -> None:
    response = client.get("/security/posture")
    assert response.status_code == 200
    body = response.json()
    assert body["rbac"] is True
    assert body["pii_redaction"] is True
    assert body["retention_controls"] is True
    assert body["draft_only_automation"] is True
