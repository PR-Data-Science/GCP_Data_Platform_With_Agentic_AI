from __future__ import annotations

from fastapi.testclient import TestClient

from src.agent_service.app import app


client = TestClient(app)


def test_health_endpoint() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"


def test_session_create_and_get() -> None:
    create_response = client.post("/sessions", json={"user_id": "u1", "mode": "ops"})
    assert create_response.status_code == 200
    session = create_response.json()

    assert session["user_id"] == "u1"
    assert session["mode"] == "ops"
    assert session["read_only"] is True
    assert "session_id" in session

    get_response = client.get(f"/sessions/{session['session_id']}")
    assert get_response.status_code == 200
    assert get_response.json()["session_id"] == session["session_id"]


def test_session_get_missing() -> None:
    response = client.get("/sessions/missing-session-id")
    assert response.status_code == 404
    assert response.json()["detail"] == "session_not_found"


def test_router_defaults_to_ops_route() -> None:
    session = client.post("/sessions", json={"user_id": "u2", "mode": "ops"}).json()
    response = client.post(
        "/router",
        json={
            "session_id": session["session_id"],
            "query": "show latest pipeline run status",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["route"] == "ops"
    assert body["read_only"] is True


def test_router_detects_dq_intent() -> None:
    session = client.post("/sessions", json={"user_id": "u3", "mode": "dq"}).json()
    response = client.post(
        "/router",
        json={
            "session_id": session["session_id"],
            "query": "why did deadletter counts increase",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["route"] == "dq"
    assert "ops.dq_results" in " ".join(body["evidence_refs"])


def test_router_requires_existing_session() -> None:
    response = client.post(
        "/router",
        json={
            "session_id": "missing-session-id",
            "query": "status",
        },
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "session_not_found"


def test_router_writes_audit_summary() -> None:
    session = client.post("/sessions", json={"user_id": "u4", "mode": "ops"}).json()
    response = client.post(
        "/router",
        json={
            "session_id": session["session_id"],
            "query": "show latest pipeline status",
        },
    )
    assert response.status_code == 200

    summary = client.get(f"/sessions/{session['session_id']}/audit-summary")
    assert summary.status_code == 200
    body = summary.json()
    assert body["session_events"] == 1
    assert body["tool_call_events"] >= 1
    assert body["response_events"] >= 1


def test_proposal_create_get_list() -> None:
    session = client.post("/sessions", json={"user_id": "u5", "mode": "dq"}).json()
    create = client.post(
        "/proposals",
        json={
            "session_id": session["session_id"],
            "route": "dq",
            "title": "Investigate deadletter spike",
            "proposal_text": "Draft proposal for DQ remediation review.",
            "evidence_refs": ["bq://ops.deadletter_summary"],
        },
    )
    assert create.status_code == 200
    proposal = create.json()
    assert proposal["status"] == "DRAFT"
    proposal_id = proposal["proposal_id"]

    get_response = client.get(f"/proposals/{proposal_id}")
    assert get_response.status_code == 200
    assert get_response.json()["proposal_id"] == proposal_id

    list_response = client.get(f"/sessions/{session['session_id']}/proposals")
    assert list_response.status_code == 200
    assert len(list_response.json()) >= 1


def test_proposal_create_requires_existing_session() -> None:
    response = client.post(
        "/proposals",
        json={
            "session_id": "missing-session-id",
            "route": "ops",
            "title": "Any",
            "proposal_text": "Any",
            "evidence_refs": [],
        },
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "session_not_found"
