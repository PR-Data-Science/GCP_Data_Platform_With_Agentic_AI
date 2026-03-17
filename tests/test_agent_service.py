from __future__ import annotations

from fastapi.testclient import TestClient

from src.agent_service.app import app


client = TestClient(app)


def test_health_endpoint() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"


def test_root_redirects_to_console() -> None:
    response = client.get("/", follow_redirects=False)
    assert response.status_code in {302, 307}
    assert response.headers["location"] == "/console"


def test_console_endpoint_renders_ui() -> None:
    response = client.get("/console")
    assert response.status_code == 200
    body = response.text
    assert "Agent Console" in body
    assert "Evidence Used" in body
    assert "Tool Calls" in body


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


def test_proposal_lifecycle_transition_happy_path() -> None:
    session = client.post("/sessions", json={"user_id": "u6", "mode": "ops"}).json()
    proposal = client.post(
        "/proposals",
        json={
            "session_id": session["session_id"],
            "route": "ops",
            "title": "Ops proposal",
            "proposal_text": "Draft ops proposal",
            "evidence_refs": ["bq://ops.pipeline_runs"],
        },
    ).json()

    to_review = client.post(
        f"/proposals/{proposal['proposal_id']}/status",
        json={"status": "REVIEW", "actor_role": "operator"},
    )
    assert to_review.status_code == 200
    assert to_review.json()["status"] == "REVIEW"

    to_approved = client.post(
        f"/proposals/{proposal['proposal_id']}/status",
        json={"status": "APPROVED", "actor_role": "approver"},
    )
    assert to_approved.status_code == 200
    assert to_approved.json()["status"] == "APPROVED"


def test_proposal_lifecycle_rejects_invalid_transition() -> None:
    session = client.post("/sessions", json={"user_id": "u7", "mode": "ops"}).json()
    proposal = client.post(
        "/proposals",
        json={
            "session_id": session["session_id"],
            "route": "ops",
            "title": "Ops proposal",
            "proposal_text": "Draft ops proposal",
            "evidence_refs": ["bq://ops.pipeline_runs"],
        },
    ).json()

    response = client.post(
        f"/proposals/{proposal['proposal_id']}/status",
        json={"status": "APPROVED", "actor_role": "approver"},
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "invalid_status_transition:DRAFT->APPROVED"


def test_proposal_lifecycle_rejects_disallowed_role() -> None:
    session = client.post("/sessions", json={"user_id": "u8", "mode": "dq"}).json()
    proposal = client.post(
        "/proposals",
        json={
            "session_id": session["session_id"],
            "route": "dq",
            "title": "DQ proposal",
            "proposal_text": "Draft dq proposal",
            "evidence_refs": ["bq://ops.dq_results"],
        },
    ).json()

    to_review = client.post(
        f"/proposals/{proposal['proposal_id']}/status",
        json={"status": "REVIEW", "actor_role": "operator"},
    )
    assert to_review.status_code == 200

    response = client.post(
        f"/proposals/{proposal['proposal_id']}/status",
        json={"status": "APPROVED", "actor_role": "viewer"},
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "role_not_allowed_for_approval:viewer"


def test_proposal_lifecycle_missing_proposal() -> None:
    response = client.post(
        "/proposals/missing-proposal/status",
        json={"status": "REVIEW", "actor_role": "operator"},
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "proposal_not_found"


def test_transform_designer_b2s_proposal_artifacts() -> None:
    session = client.post("/sessions", json={"user_id": "u9", "mode": "dq"}).json()
    response = client.post(
        "/transform-designer/proposals",
        json={
            "session_id": session["session_id"],
            "layer": "B2S",
            "change_type": "schema_drift",
            "source_table": "bronze.feedback_step",
            "target_table": "silver.feedback_step",
            "problem_statement": "New nested field in source payload.",
            "evidence_refs": ["bq://ops.schema_registry"],
            "run_id": "run-123",
            "env": "dev",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["layer"] == "B2S"
    assert body["auto_applied"] is False
    assert len(body["generated_artifacts"]) == 3
    assert all(path.startswith("gs://agent-proposals/") for path in body["generated_artifacts"])
    assert body["proposal"]["status"] == "DRAFT"
    assert "NOT auto-applied" in body["proposal"]["proposal_text"]


def test_transform_designer_s2g_proposal_artifacts() -> None:
    session = client.post("/sessions", json={"user_id": "u10", "mode": "ops"}).json()
    response = client.post(
        "/transform-designer/proposals",
        json={
            "session_id": session["session_id"],
            "layer": "S2G",
            "change_type": "kpi_update",
            "source_table": "silver.training_examples",
            "target_table": "gold.training_kpis",
            "problem_statement": "Need weekly agreement rate KPI.",
            "evidence_refs": ["bq://ops.pipeline_runs"],
            "env": "prod",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["layer"] == "S2G"
    assert body["proposal"]["route"] == "ops"
    assert body["auto_applied"] is False
    assert body["proposal"]["status"] == "DRAFT"


def test_transform_designer_requires_existing_session() -> None:
    response = client.post(
        "/transform-designer/proposals",
        json={
            "session_id": "missing-session-id",
            "layer": "B2S",
            "change_type": "new_mapping",
            "source_table": "bronze.a",
            "target_table": "silver.a",
            "problem_statement": "Add field mapping.",
            "evidence_refs": [],
            "env": "dev",
        },
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "session_not_found"
