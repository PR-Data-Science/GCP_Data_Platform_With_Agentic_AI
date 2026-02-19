"""I/O utilities for pipeline data."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import yaml

try:
    from google.cloud import bigquery
except ImportError:  # pragma: no cover - optional dependency for ingestion tests
    bigquery = None


def normalize_rows(rows: Iterable[dict]) -> list[dict]:
    normalized = []
    for row in rows:
        normalized.append({
            "feedback_id": row.get("feedback_id") or row.get("id"),
            "timestamp": row.get("timestamp") or row.get("created_at"),
            "rating": row.get("rating"),
            "comment": row.get("comment") or row.get("text"),
            "model": row.get("model") or row.get("model_name"),
        })
    return normalized


def load_to_bigquery(rows: list[dict], dataset: str, table: str) -> None:
    if bigquery is None:
        raise RuntimeError("google-cloud-bigquery is not installed")
    client = bigquery.Client()
    table_id = f"{client.project}.{dataset}.{table}"
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def today_yyyy_mm_dd() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")
