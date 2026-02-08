"""I/O utilities for pipeline data."""

from __future__ import annotations

from typing import Iterable

from google.cloud import bigquery


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
    client = bigquery.Client()
    table_id = f"{client.project}.{dataset}.{table}"
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
