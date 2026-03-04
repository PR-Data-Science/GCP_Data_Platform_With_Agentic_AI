from __future__ import annotations

import pytest

from src.agent_service.tools import ReadOnlyToolRegistry, ToolAccessError, ToolValidationError


def test_bq_template_query_allowlist_and_limit() -> None:
    registry = ReadOnlyToolRegistry()
    result = registry.bq_template_query(table="ops.pipeline_runs", limit=9999, role="analyst")
    assert result.read_only is True
    assert "LIMIT 500" in result.payload["sql"]


def test_bq_template_query_rejects_disallowed_table() -> None:
    registry = ReadOnlyToolRegistry()
    with pytest.raises(ToolValidationError):
        registry.bq_template_query(table="random.table", limit=10, role="analyst")


def test_tools_reject_disallowed_role() -> None:
    registry = ReadOnlyToolRegistry()
    with pytest.raises(ToolAccessError):
        registry.gcs_read_object(uri="gs://bucket/path.json", role="editor")


def test_schema_diff_contract() -> None:
    registry = ReadOnlyToolRegistry()
    result = registry.schema_diff(
        expected_schema={"a": "STRING", "b": "INT64"},
        actual_schema={"a": "STRING", "b": "FLOAT64", "c": "BOOL"},
        role="viewer",
    )

    assert result.payload["added_fields"] == ["c"]
    assert result.payload["removed_fields"] == []
    assert result.payload["type_changes"] == [
        {"field": "b", "expected": "INT64", "actual": "FLOAT64"}
    ]
