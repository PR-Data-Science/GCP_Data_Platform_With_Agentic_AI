from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
import json
import requests


@dataclass
class Source:
    type: str
    name: str
    url: str | None = None
    path: str | None = None


def read_rest(source: Source) -> list[dict]:
    if not source.url:
        raise ValueError("REST source requires url")
    response = requests.get(source.url, timeout=30)
    response.raise_for_status()
    return response.json()


def read_json(source: Source) -> list[dict]:
    if not source.path:
        raise ValueError("JSON source requires path")
    with open(source.path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def read_csv(source: Source) -> list[dict]:
    if not source.path:
        raise ValueError("CSV source requires path")
    import pandas as pd

    return pd.read_csv(source.path).to_dict(orient="records")


def load_source(source: Source) -> list[dict]:
    if source.type == "rest":
        return read_rest(source)
    if source.type == "json":
        return read_json(source)
    if source.type == "csv":
        return read_csv(source)
    raise ValueError(f"Unsupported source type: {source.type}")


def load_all(sources: Iterable[Source]) -> list[dict]:
    rows: list[dict] = []
    for source in sources:
        rows.extend(load_source(source))
    return rows
