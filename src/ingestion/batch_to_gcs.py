"""Batch ingestion helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
import json


@dataclass
class BatchSource:
    name: str
    path: str
    format: str = "json"


def read_json(source: BatchSource) -> list[dict]:
    with open(source.path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def read_csv(source: BatchSource) -> list[dict]:
    import pandas as pd

    return pd.read_csv(source.path).to_dict(orient="records")


def load_batch(source: BatchSource) -> list[dict]:
    if source.format == "json":
        return read_json(source)
    if source.format == "csv":
        return read_csv(source)
    raise ValueError(f"Unsupported batch format: {source.format}")


def load_all_batch(sources: Iterable[BatchSource]) -> list[dict]:
    rows: list[dict] = []
    for source in sources:
        rows.extend(load_batch(source))
    return rows
