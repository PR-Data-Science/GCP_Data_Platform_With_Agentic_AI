"""Normalize ingestion rows into payloads."""

from __future__ import annotations

import json
from typing import Any, Dict, List


JSON_COLUMNS = {
    "execution_json",
    "curator_1_rating",
    "curator_2_rating",
    "reviewer_curator_1_rating",
    "reviewer_curator_2_rating",
    "violations",
    "auto_rater",
}


def _convert_empty_to_none(value: Any) -> Any:
    if isinstance(value, str) and value == "":
        return None
    return value


def _parse_json_if_needed(payload: Dict[str, Any], key: str) -> None:
    value = payload.get(key)
    if not isinstance(value, str):
        return
    stripped = value.strip()
    if not stripped or stripped[0] not in "[{":
        return
    try:
        payload[key] = json.loads(stripped)
    except json.JSONDecodeError:
        errors = payload.setdefault("parse_errors", [])
        errors.append(f"json_parse_error:{key}")


def _ensure_int(payload: Dict[str, Any], key: str) -> None:
    value = payload.get(key)
    if value is None:
        return
    if isinstance(value, int):
        return
    if isinstance(value, str) and value.isdigit():
        payload[key] = int(value)
        return
    errors = payload.setdefault("parse_errors", [])
    errors.append(f"int_parse_error:{key}")


def normalize_payload(row: Dict[str, Any], source_type: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {k: _convert_empty_to_none(v) for k, v in row.items()}

    for key in JSON_COLUMNS:
        if key in payload:
            _parse_json_if_needed(payload, key)

    _ensure_int(payload, "step_index")
    _ensure_int(payload, "evaluated_step_index")

    if payload.get("reviewer_curator_1_rating") is None:
        payload["reviewer_curator_1_rating"] = payload.get("curator_1_rating")
    if payload.get("reviewer_curator_2_rating") is None:
        payload["reviewer_curator_2_rating"] = payload.get("curator_2_rating")

    violations = payload.get("violations")
    if violations is None:
        payload["violations"] = []
    elif isinstance(violations, str):
        payload["violations"] = [violations]
    elif not isinstance(violations, list):
        payload["violations"] = [violations]

    return payload
