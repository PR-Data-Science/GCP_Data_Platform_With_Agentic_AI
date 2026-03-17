from __future__ import annotations

import re
from typing import Iterable


PRIVILEGED_ROLES = {"approver", "admin"}
OPERATOR_ROLES = {"operator", "engineer", "approver", "admin"}

_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_OPENAI_KEY_RE = re.compile(r"\bsk-[A-Za-z0-9]{20,}\b")
_BEARER_RE = re.compile(r"\bBearer\s+[A-Za-z0-9._-]+\b", re.IGNORECASE)


def assert_role_allowed(*, actor_role: str, allowed_roles: Iterable[str], action: str) -> None:
    if actor_role not in set(allowed_roles):
        raise PermissionError(f"role_not_allowed_for_{action}:{actor_role}")


def redact_pii(text: str) -> str:
    redacted = _EMAIL_RE.sub("[REDACTED_EMAIL]", text)
    redacted = _OPENAI_KEY_RE.sub("[REDACTED_API_KEY]", redacted)
    redacted = _BEARER_RE.sub("Bearer [REDACTED_TOKEN]", redacted)
    return redacted
