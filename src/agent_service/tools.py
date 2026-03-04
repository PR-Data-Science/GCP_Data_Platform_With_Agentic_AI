from __future__ import annotations

from dataclasses import dataclass
from typing import Any


READ_ONLY_ALLOWED_ROLES = {"viewer", "analyst", "admin"}
ALLOWED_BQ_TABLES = {
    "ops.pipeline_runs",
    "ops.dq_results",
    "ops.schema_registry",
    "ops.deadletter_summary",
}
ALLOWED_DAGS = {
    "llm_feedback_dataproc_orchestration",
    "llm_feedback_full_e2e_composer",
}


@dataclass(frozen=True)
class ToolResult:
    tool_name: str
    payload: dict[str, Any]
    read_only: bool = True


class ToolAccessError(PermissionError):
    pass


class ToolValidationError(ValueError):
    pass


class ReadOnlyToolRegistry:
    def _authorize(self, role: str) -> None:
        if role not in READ_ONLY_ALLOWED_ROLES:
            raise ToolAccessError(f"role_not_allowed:{role}")

    def bq_template_query(self, *, table: str, limit: int, role: str) -> ToolResult:
        self._authorize(role)
        if table not in ALLOWED_BQ_TABLES:
            raise ToolValidationError(f"table_not_allowlisted:{table}")
        safe_limit = max(1, min(limit, 500))
        return ToolResult(
            tool_name="bq_template_query",
            payload={
                "table": table,
                "sql": f"SELECT * FROM `{table}` ORDER BY created_ts DESC LIMIT {safe_limit}",
            },
        )

    def gcs_read_object(self, *, uri: str, role: str) -> ToolResult:
        self._authorize(role)
        if not uri.startswith("gs://"):
            raise ToolValidationError("invalid_gcs_uri")
        return ToolResult(tool_name="gcs_read_object", payload={"uri": uri})

    def composer_dag_status(self, *, dag_id: str, role: str) -> ToolResult:
        self._authorize(role)
        if dag_id not in ALLOWED_DAGS:
            raise ToolValidationError(f"dag_not_allowlisted:{dag_id}")
        return ToolResult(tool_name="composer_dag_status", payload={"dag_id": dag_id})

    def dataproc_batch_status(self, *, batch_id: str, role: str) -> ToolResult:
        self._authorize(role)
        if not batch_id:
            raise ToolValidationError("missing_batch_id")
        return ToolResult(tool_name="dataproc_batch_status", payload={"batch_id": batch_id})

    def schema_diff(self, *, expected_schema: dict[str, str], actual_schema: dict[str, str], role: str) -> ToolResult:
        self._authorize(role)

        expected_keys = set(expected_schema.keys())
        actual_keys = set(actual_schema.keys())

        added = sorted(actual_keys - expected_keys)
        removed = sorted(expected_keys - actual_keys)
        type_changes = sorted(
            key
            for key in (expected_keys & actual_keys)
            if expected_schema[key] != actual_schema[key]
        )

        return ToolResult(
            tool_name="schema_diff",
            payload={
                "added_fields": added,
                "removed_fields": removed,
                "type_changes": [
                    {
                        "field": key,
                        "expected": expected_schema[key],
                        "actual": actual_schema[key],
                    }
                    for key in type_changes
                ],
            },
        )
