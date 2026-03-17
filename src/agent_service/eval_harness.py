"""Evaluation harness for the agent service.

Measures three metrics on a fixed eval suite of (query, expected_route,
expected_evidence_uris) cases:

  recall_at_k
      Fraction of eval cases where at least one expected evidence URI
      appears in the top-k retrieved / returned evidence refs.
      Threshold: >= 0.80

  route_accuracy
      Fraction of cases where the resolved route matches the expected
      route.  The router is deterministic so this must be 1.0.
      Threshold: >= 1.0

  grounding_rate
      Fraction of router responses that contain at least one evidence
      ref.  A response with no evidence ref is an unsupported claim.
      Threshold: >= 1.0

  unsupported_claim_rate  (derived: 1.0 - grounding_rate)
      Fraction of responses with no evidence refs.
      Threshold: <= 0.0

Two run modes:
  EvalHarness.run_retrieval_only(retriever)
      Unit-level: exercises EvidenceRetriever + resolve_route directly.
      Fast, no HTTP stack required.

  EvalHarness.run(client)
      Integration-level: drives the /router endpoint through a FastAPI
      TestClient.  Used as the CI regression gate.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from src.agent_service.retrieval import EvidenceRetriever

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

BASELINE_THRESHOLDS: dict[str, float] = {
    "recall_at_k": 0.80,
    "route_accuracy": 1.0,
    "grounding_rate": 1.0,
    "unsupported_claim_rate": 0.0,
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EvalCase:
    """A single evaluation case."""

    case_id: str
    query: str
    expected_route: Literal["ops", "dq"]
    # Recall is satisfied when *any* of these URIs appears in the response.
    expected_evidence_uris: tuple[str, ...]
    intent_hint: Literal["ops", "dq"] | None = None


@dataclass
class EvalResult:
    """Per-case evaluation outcome."""

    case_id: str
    query: str
    expected_route: Literal["ops", "dq"]
    actual_route: Literal["ops", "dq"]
    expected_evidence_uris: tuple[str, ...]
    returned_evidence_uris: list[str]
    evidence_refs_in_response: list[str]

    @property
    def route_correct(self) -> bool:
        return self.actual_route == self.expected_route

    @property
    def recall_hit(self) -> bool:
        returned_set = set(self.returned_evidence_uris)
        return any(uri in returned_set for uri in self.expected_evidence_uris)

    @property
    def grounded(self) -> bool:
        return len(self.evidence_refs_in_response) > 0


@dataclass
class EvalReport:
    """Aggregate evaluation report with pass/fail gate."""

    results: list[EvalResult]
    thresholds: dict[str, float]

    @property
    def recall_at_k(self) -> float:
        if not self.results:
            return 0.0
        return sum(1 for r in self.results if r.recall_hit) / len(self.results)

    @property
    def route_accuracy(self) -> float:
        if not self.results:
            return 0.0
        return sum(1 for r in self.results if r.route_correct) / len(self.results)

    @property
    def grounding_rate(self) -> float:
        if not self.results:
            return 0.0
        return sum(1 for r in self.results if r.grounded) / len(self.results)

    @property
    def unsupported_claim_rate(self) -> float:
        return 1.0 - self.grounding_rate

    @property
    def gate_passed(self) -> bool:
        return (
            self.recall_at_k >= self.thresholds.get("recall_at_k", 0.80)
            and self.route_accuracy >= self.thresholds.get("route_accuracy", 1.0)
            and self.grounding_rate >= self.thresholds.get("grounding_rate", 1.0)
            and self.unsupported_claim_rate <= self.thresholds.get("unsupported_claim_rate", 0.0)
        )

    def summary(self) -> dict[str, object]:
        return {
            "total_cases": len(self.results),
            "recall_at_k": round(self.recall_at_k, 4),
            "route_accuracy": round(self.route_accuracy, 4),
            "grounding_rate": round(self.grounding_rate, 4),
            "unsupported_claim_rate": round(self.unsupported_claim_rate, 4),
            "gate_passed": self.gate_passed,
            "thresholds": self.thresholds,
        }

    def failing_cases(self) -> list[EvalResult]:
        return [r for r in self.results if not r.recall_hit or not r.route_correct or not r.grounded]


# ---------------------------------------------------------------------------
# Default eval suite
# ---------------------------------------------------------------------------


def default_eval_suite() -> list[EvalCase]:
    """Ten labelled cases covering both ops and dq routes.

    expected_evidence_uris lists all valid evidence sources for the query;
    recall_hit is True when any one of them appears in the returned refs.
    """
    return [
        # --- ops cases ---------------------------------------------------
        EvalCase(
            case_id="ops_01",
            query="show me the latest pipeline run status",
            expected_route="ops",
            expected_evidence_uris=("bq://ops.pipeline_runs",),
        ),
        EvalCase(
            case_id="ops_02",
            query="what are the current schema registry snapshots",
            expected_route="ops",
            expected_evidence_uris=("bq://ops.schema_registry",),
        ),
        EvalCase(
            case_id="ops_03",
            query="which stages completed in the last run",
            expected_route="ops",
            # manifest_convention doc also has "stages"; pipeline_runs is
            # the primary evidence, either satisfies the recall check.
            expected_evidence_uris=(
                "bq://ops.pipeline_runs",
                "gcs://manifests/<stage>/dt=<date>/run_id=<run_id>/manifest.json",
            ),
        ),
        EvalCase(
            case_id="ops_04",
            query="show manifest path and code version for bronze stage",
            expected_route="ops",
            expected_evidence_uris=(
                "bq://ops.pipeline_runs",
                "gcs://manifests/<stage>/dt=<date>/run_id=<run_id>/manifest.json",
            ),
        ),
        EvalCase(
            case_id="ops_05_hint",
            query="diagnose the pipeline run failure",
            expected_route="ops",
            expected_evidence_uris=("bq://ops.pipeline_runs",),
            intent_hint="ops",
        ),
        # --- dq cases ---------------------------------------------------
        EvalCase(
            case_id="dq_01",
            query="why did deadletter counts increase last run",
            expected_route="dq",
            expected_evidence_uris=("bq://ops.deadletter_summary",),
        ),
        EvalCase(
            case_id="dq_02",
            query="which dq rule violations are highest severity",
            expected_route="dq",
            expected_evidence_uris=("bq://ops.dq_results", "gcs://dq/dq_rules.yaml"),
        ),
        EvalCase(
            case_id="dq_03",
            query="show failing quality checks for feedback data",
            expected_route="dq",
            expected_evidence_uris=("bq://ops.dq_results",),
        ),
        EvalCase(
            case_id="dq_04",
            query="list records quarantined by nullability rule",
            expected_route="dq",
            expected_evidence_uris=("bq://ops.deadletter_summary", "bq://ops.dq_results"),
        ),
        EvalCase(
            case_id="dq_05_hint",
            query="investigate quality violation anomaly",
            expected_route="dq",
            expected_evidence_uris=("bq://ops.dq_results",),
            intent_hint="dq",
        ),
    ]


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


class EvalHarness:
    """Runs the eval suite and returns an EvalReport.

    Args:
        cases:      Override the default eval suite.
        thresholds: Override BASELINE_THRESHOLDS.
    """

    def __init__(
        self,
        cases: list[EvalCase] | None = None,
        thresholds: dict[str, float] | None = None,
    ) -> None:
        self._cases = cases if cases is not None else default_eval_suite()
        self._thresholds = thresholds if thresholds is not None else BASELINE_THRESHOLDS.copy()

    def run_retrieval_only(self, retriever: EvidenceRetriever) -> EvalReport:
        """Unit-level eval: tests EvidenceRetriever + resolve_route directly.

        Uses top_k=5 so all corpus docs for the route are considered.
        """
        from src.agent_service.app import resolve_route  # avoid circular at import time

        results: list[EvalResult] = []
        for case in self._cases:
            route = resolve_route(query=case.query, intent_hint=case.intent_hint)
            docs = retriever.search(case.query, top_k=5, required_tag=route)
            returned_uris = [doc.uri for doc in docs]
            results.append(
                EvalResult(
                    case_id=case.case_id,
                    query=case.query,
                    expected_route=case.expected_route,
                    actual_route=route,
                    expected_evidence_uris=case.expected_evidence_uris,
                    returned_evidence_uris=returned_uris,
                    evidence_refs_in_response=returned_uris,
                )
            )
        return EvalReport(results=results, thresholds=self._thresholds)

    def run(self, client: Any) -> EvalReport:
        """Integration-level eval: drives the /router endpoint end-to-end.

        Args:
            client: A ``fastapi.testclient.TestClient`` instance wrapping the
                    agent service app.

        Returns:
            EvalReport with metrics across all cases.
        """
        session_resp = client.post("/sessions", json={"user_id": "_eval_harness", "mode": "ops"})
        session_id = session_resp.json()["session_id"]

        results: list[EvalResult] = []
        for case in self._cases:
            payload: dict[str, Any] = {"session_id": session_id, "query": case.query}
            if case.intent_hint is not None:
                payload["intent_hint"] = case.intent_hint

            resp = client.post("/router", json=payload)
            body = resp.json()

            actual_route: Literal["ops", "dq"] = body.get("route", "ops")
            evidence_refs: list[str] = body.get("evidence_refs", [])

            results.append(
                EvalResult(
                    case_id=case.case_id,
                    query=case.query,
                    expected_route=case.expected_route,
                    actual_route=actual_route,
                    expected_evidence_uris=case.expected_evidence_uris,
                    returned_evidence_uris=evidence_refs,
                    evidence_refs_in_response=evidence_refs,
                )
            )
        return EvalReport(results=results, thresholds=self._thresholds)
