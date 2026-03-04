"""Standard error taxonomy and mapping helpers for pipeline stages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


CATEGORIES = {
    "AUTH",
    "IAM_PERMISSION",
    "SCHEMA_DRIFT",
    "DQ_FAILURE",
    "RUNTIME",
    "CONFIG",
    "DEPENDENCY",
    "NETWORK",
}


@dataclass(frozen=True)
class ErrorInfo:
    category: str
    code: str
    summary: str


def normalize_category(category: str) -> str:
    normalized = (category or "").strip().upper()
    if normalized in CATEGORIES:
        return normalized
    return "RUNTIME"


def from_exception(exc: Exception) -> ErrorInfo:
    message = str(exc).lower()
    code = exc.__class__.__name__.upper()

    if "permission" in message or "forbidden" in message or "access denied" in message:
        return ErrorInfo(category="IAM_PERMISSION", code=code, summary=str(exc))

    if "auth" in message or "credential" in message or "token" in message:
        return ErrorInfo(category="AUTH", code=code, summary=str(exc))

    if "schema" in message or "column" in message or "field" in message:
        return ErrorInfo(category="SCHEMA_DRIFT", code=code, summary=str(exc))

    if "dq" in message or "quality" in message or "validation" in message:
        return ErrorInfo(category="DQ_FAILURE", code=code, summary=str(exc))

    if "config" in message or "missing" in message or "invalid" in message:
        return ErrorInfo(category="CONFIG", code=code, summary=str(exc))

    if "network" in message or "timeout" in message or "connection" in message:
        return ErrorInfo(category="NETWORK", code=code, summary=str(exc))

    if "dependency" in message or "module" in message or "import" in message:
        return ErrorInfo(category="DEPENDENCY", code=code, summary=str(exc))

    return ErrorInfo(category="RUNTIME", code=code, summary=str(exc))


def build_error_info(category: Optional[str], code: Optional[str], summary: Optional[str]) -> ErrorInfo:
    return ErrorInfo(
        category=normalize_category(category or "RUNTIME"),
        code=(code or "UNKNOWN_ERROR").strip().upper(),
        summary=(summary or "").strip(),
    )
