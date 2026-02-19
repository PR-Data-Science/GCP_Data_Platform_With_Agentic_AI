from __future__ import annotations

import json

from src.ingestion.normalize_row import normalize_payload


def test_json_columns_parsed() -> None:
    row = {
        "execution_json": json.dumps({"a": 1}),
        "curator_1_rating": json.dumps({"primary_intent": 3}),
        "violations": json.dumps(["TOXICITY"]),
    }
    payload = normalize_payload(row, "csv")
    assert payload["execution_json"] == {"a": 1}
    assert payload["curator_1_rating"] == {"primary_intent": 3}
    assert payload["violations"] == ["TOXICITY"]


def test_violations_always_list() -> None:
    row = {"violations": ""}
    payload = normalize_payload(row, "csv")
    assert payload["violations"] == []


def test_reviewer_ratings_present() -> None:
    row = {
        "curator_1_rating": {"primary_intent": 2},
        "curator_2_rating": {"primary_intent": 4},
        "reviewer_curator_1_rating": None,
        "reviewer_curator_2_rating": None,
    }
    payload = normalize_payload(row, "csv")
    assert payload["reviewer_curator_1_rating"] == {"primary_intent": 2}
    assert payload["reviewer_curator_2_rating"] == {"primary_intent": 4}
