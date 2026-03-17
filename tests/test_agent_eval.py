"""CI regression gate: evaluation harness tests.

These tests enforce minimum metric thresholds for the agent service and
act as the G8 regression gate.  All tests must pass on every PR.

Metrics enforced:
  recall_at_k         >= 0.80   (retrieval surfaces expected evidence)
  route_accuracy      >= 1.00   (deterministic routing is correct)
  grounding_rate      >= 1.00   (every response has at least one evidence ref)
  unsupported_claim_rate <= 0.00 (no responses without evidence)
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.agent_service.app import app
from src.agent_service.eval_harness import (
    BASELINE_THRESHOLDS,
    EvalHarness,
    EvalReport,
    default_eval_suite,
)
from src.agent_service.retrieval import EvidenceRetriever, default_evidence_corpus

client = TestClient(app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _assert_gate(report: EvalReport) -> None:
    """Raise a descriptive AssertionError if any metric fails its threshold."""
    summary = report.summary()
    failures = report.failing_cases()
    failing_ids = [r.case_id for r in failures]

    assert report.recall_at_k >= BASELINE_THRESHOLDS["recall_at_k"], (
        f"recall_at_k={report.recall_at_k:.3f} below threshold "
        f"{BASELINE_THRESHOLDS['recall_at_k']}. "
        f"Failing cases: {failing_ids}"
    )
    assert report.route_accuracy >= BASELINE_THRESHOLDS["route_accuracy"], (
        f"route_accuracy={report.route_accuracy:.3f} below threshold "
        f"{BASELINE_THRESHOLDS['route_accuracy']}. "
        f"Failing cases: {failing_ids}"
    )
    assert report.grounding_rate >= BASELINE_THRESHOLDS["grounding_rate"], (
        f"grounding_rate={report.grounding_rate:.3f} below threshold "
        f"{BASELINE_THRESHOLDS['grounding_rate']}. "
        f"Failing cases: {failing_ids}"
    )
    assert report.unsupported_claim_rate <= BASELINE_THRESHOLDS["unsupported_claim_rate"], (
        f"unsupported_claim_rate={report.unsupported_claim_rate:.3f} above threshold "
        f"{BASELINE_THRESHOLDS['unsupported_claim_rate']}. "
        f"Failing cases: {failing_ids}"
    )
    assert summary["gate_passed"] is True


# ---------------------------------------------------------------------------
# Unit-level retrieval gate (no HTTP, no embeddings)
# ---------------------------------------------------------------------------


def test_retrieval_recall_gate() -> None:
    """Retrieval recall must meet the baseline threshold without embeddings."""
    retriever = EvidenceRetriever(default_evidence_corpus(), use_openai_embeddings=False)
    harness = EvalHarness()
    report = harness.run_retrieval_only(retriever)
    assert report.recall_at_k >= BASELINE_THRESHOLDS["recall_at_k"], (
        f"recall_at_k={report.recall_at_k:.3f} below threshold. "
        f"Failing cases: {[r.case_id for r in report.failing_cases()]}"
    )


def test_retrieval_route_accuracy_gate() -> None:
    """Route resolution must be correct for every eval case."""
    retriever = EvidenceRetriever(default_evidence_corpus(), use_openai_embeddings=False)
    harness = EvalHarness()
    report = harness.run_retrieval_only(retriever)
    assert report.route_accuracy >= BASELINE_THRESHOLDS["route_accuracy"], (
        f"route_accuracy={report.route_accuracy:.3f} below threshold. "
        f"Wrong routes: {[(r.case_id, r.actual_route) for r in report.failing_cases()]}"
    )


def test_retrieval_grounding_gate() -> None:
    """Every retrieval result must contain at least one evidence ref."""
    retriever = EvidenceRetriever(default_evidence_corpus(), use_openai_embeddings=False)
    harness = EvalHarness()
    report = harness.run_retrieval_only(retriever)
    assert report.grounding_rate >= BASELINE_THRESHOLDS["grounding_rate"], (
        f"grounding_rate={report.grounding_rate:.3f} below threshold. "
        f"Ungrounded cases: {[r.case_id for r in report.failing_cases() if not r.grounded]}"
    )


# ---------------------------------------------------------------------------
# Integration-level router gate (full HTTP stack)
# ---------------------------------------------------------------------------


def test_router_eval_gate_passes() -> None:
    """Full end-to-end: all metrics must meet baseline thresholds via the router."""
    harness = EvalHarness()
    report = harness.run(client)
    _assert_gate(report)


def test_router_eval_recall_meets_baseline() -> None:
    """Router recall@k must be >= 0.80 across the default eval suite."""
    harness = EvalHarness()
    report = harness.run(client)
    assert report.recall_at_k >= BASELINE_THRESHOLDS["recall_at_k"], (
        f"recall_at_k={report.recall_at_k:.3f}. "
        f"Missed cases: {[r.case_id for r in report.failing_cases() if not r.recall_hit]}"
    )


def test_router_eval_route_accuracy_is_perfect() -> None:
    """Router routing must be correct for every case (deterministic rules)."""
    harness = EvalHarness()
    report = harness.run(client)
    wrong = [(r.case_id, r.actual_route, r.expected_route) for r in report.results if not r.route_correct]
    assert not wrong, f"Routing errors: {wrong}"


def test_router_eval_all_responses_grounded() -> None:
    """Every /router response must return at least one evidence ref."""
    harness = EvalHarness()
    report = harness.run(client)
    ungrounded = [r.case_id for r in report.results if not r.grounded]
    assert not ungrounded, f"Ungrounded responses (no evidence refs): {ungrounded}"


def test_router_eval_no_unsupported_claims() -> None:
    """unsupported_claim_rate must be 0.0 — no response without evidence."""
    harness = EvalHarness()
    report = harness.run(client)
    assert report.unsupported_claim_rate == 0.0, (
        f"unsupported_claim_rate={report.unsupported_claim_rate:.3f}. "
        f"Cases with no evidence: {[r.case_id for r in report.results if not r.grounded]}"
    )


# ---------------------------------------------------------------------------
# EvalReport / EvalHarness unit tests
# ---------------------------------------------------------------------------


def test_eval_report_summary_keys() -> None:
    """EvalReport.summary() returns expected keys."""
    harness = EvalHarness()
    report = harness.run(client)
    summary = report.summary()
    for key in ("total_cases", "recall_at_k", "route_accuracy", "grounding_rate",
                "unsupported_claim_rate", "gate_passed", "thresholds"):
        assert key in summary, f"Missing key in summary: {key}"


def test_eval_report_total_cases_matches_suite() -> None:
    """EvalReport.results length must equal the eval suite size."""
    suite = default_eval_suite()
    harness = EvalHarness(cases=suite)
    report = harness.run(client)
    assert report.summary()["total_cases"] == len(suite)


def test_eval_harness_custom_thresholds() -> None:
    """EvalHarness respects custom threshold overrides."""
    # With impossibly high thresholds the gate must fail.
    strict = {"recall_at_k": 2.0, "route_accuracy": 2.0, "grounding_rate": 2.0, "unsupported_claim_rate": -1.0}
    harness = EvalHarness(thresholds=strict)
    report = harness.run(client)
    assert report.gate_passed is False


def test_eval_harness_retrieval_only_corpus_coverage() -> None:
    """Retrieval-only mode returns results for every case."""
    retriever = EvidenceRetriever(default_evidence_corpus(), use_openai_embeddings=False)
    harness = EvalHarness()
    report = harness.run_retrieval_only(retriever)
    assert len(report.results) == len(default_eval_suite())
    assert all(len(r.returned_evidence_uris) > 0 for r in report.results), (
        "Some cases had empty retrieval results"
    )
