"""Hashing helpers for ingestion metadata."""

from __future__ import annotations

import hashlib
import json
from typing import Any, List


def canonical_json(obj: Any) -> str:
	return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(text: str) -> str:
	return hashlib.sha256(text.encode("utf-8")).hexdigest()


def schema_hash_from_keys(keys: List[str]) -> str:
	return sha256_hex(canonical_json(keys))


def record_hash_from_payload(payload: dict) -> str:
	return sha256_hex(canonical_json(payload))
