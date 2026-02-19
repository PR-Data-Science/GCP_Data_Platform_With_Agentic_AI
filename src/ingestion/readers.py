"""Source readers for local drop files."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, Iterator


def iter_json_array(path: str) -> Iterator[Dict]:
    file_path = Path(path)
    with file_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError(f"Expected JSON array in {path}")
    for row in data:
        yield row


def iter_csv(path: str) -> Iterator[Dict]:
    file_path = Path(path)
    with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row
