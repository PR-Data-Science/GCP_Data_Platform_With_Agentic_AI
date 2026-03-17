from __future__ import annotations

from dataclasses import dataclass

from src.agent_service.eval_harness import BASELINE_THRESHOLDS, EvalHarness
from src.agent_service.retrieval import EvidenceRetriever


@dataclass(frozen=True)
class QualityGateResult:
    passed: bool
    metrics: dict[str, float]
    thresholds: dict[str, float]


def run_quality_gates(retriever: EvidenceRetriever) -> QualityGateResult:
    harness = EvalHarness()
    report = harness.run_retrieval_only(retriever)
    metrics = {
        "recall_at_k": report.recall_at_k,
        "route_accuracy": report.route_accuracy,
        "grounding_rate": report.grounding_rate,
        "unsupported_claim_rate": report.unsupported_claim_rate,
    }
    return QualityGateResult(
        passed=report.gate_passed,
        metrics=metrics,
        thresholds=BASELINE_THRESHOLDS.copy(),
    )
