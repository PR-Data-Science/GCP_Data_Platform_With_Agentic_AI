from __future__ import annotations

from typing import Iterable


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
