"""API ingestion helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import requests


@dataclass
class ApiSource:
    name: str
    url: str


def read_rest(source: ApiSource) -> list[dict]:
    response = requests.get(source.url, timeout=30)
    response.raise_for_status()
    return response.json()


def load_api(source: ApiSource) -> list[dict]:
    return read_rest(source)


def load_all_api(sources: Iterable[ApiSource]) -> list[dict]:
    rows: list[dict] = []
    for source in sources:
        rows.extend(load_api(source))
    return rows
