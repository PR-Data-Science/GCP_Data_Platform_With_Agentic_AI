from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

LayerType = Literal["B2S", "S2G"]
ChangeType = Literal["schema_drift", "new_mapping", "dq_rule_update", "kpi_update", "curation_update"]


@dataclass(frozen=True)
class TransformDesignArtifacts:
    layer: LayerType
    generated_artifacts: list[str]
    proposal_text: str
    confidence_score: float


def _sanitize(segment: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in segment).strip("_") or "na"


def build_transform_design_artifacts(
    *,
    proposal_id: str,
    layer: LayerType,
    change_type: ChangeType,
    source_table: str,
    target_table: str,
    problem_statement: str,
    run_id: str | None,
    env: Literal["dev", "prod"],
) -> TransformDesignArtifacts:
    safe_layer = _sanitize(layer)
    safe_change = _sanitize(change_type)
    safe_source = _sanitize(source_table)
    safe_target = _sanitize(target_table)
    safe_run = _sanitize(run_id or "latest_failed_run")

    artifact_prefix = (
        "gs://agent-proposals/"
        f"env={env}/layer={safe_layer}/run_id={safe_run}/proposal_id={proposal_id}"
    )

    generated_artifacts = [
        f"{artifact_prefix}/mapping_spec_{safe_change}_{safe_source}_to_{safe_target}.json",
        f"{artifact_prefix}/test_plan_{safe_change}_{safe_source}_to_{safe_target}.md",
        f"{artifact_prefix}/schema_preview_{safe_change}_{safe_source}_to_{safe_target}.json",
    ]

    if layer == "B2S":
        proposal_text = (
            "Transform Designer (B2S) draft proposal generated. "
            "This proposal is read-only and NOT auto-applied. "
            "Playbook focus: contract alignment, normalization, metadata propagation, "
            "deterministic dedup keys, and DQ/deadletter routing. "
            f"Change type: {change_type}. Source: {source_table}. Target: {target_table}. "
            f"Problem: {problem_statement}."
        )
        confidence_score = 0.78
    else:
        proposal_text = (
            "Transform Designer (S2G) draft proposal generated. "
            "This proposal is read-only and NOT auto-applied. "
            "Playbook focus: KPI logic, curated dimensional outputs, publish-readiness checks, "
            "and partition/clustering suitability. "
            f"Change type: {change_type}. Source: {source_table}. Target: {target_table}. "
            f"Problem: {problem_statement}."
        )
        confidence_score = 0.8

    return TransformDesignArtifacts(
        layer=layer,
        generated_artifacts=generated_artifacts,
        proposal_text=proposal_text,
        confidence_score=confidence_score,
    )
