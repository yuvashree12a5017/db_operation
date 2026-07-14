"""Credential placeholder resolution for DB connection URLs (section 7/18).

Resolves tokens like ${SECRET:my-secret} embedded in a sqlalchemy_url from
environment variables. Standalone placeholder for common.creds.
"""
from __future__ import annotations

import os
import re

_PLACEHOLDER_RE = re.compile(r"\$\{SECRET:([A-Za-z0-9_\-]+)\}")


def resolve_connection_url(raw_url: str) -> str:
    def _replace(match: re.Match) -> str:
        secret_name = match.group(1)
        value = os.environ.get(secret_name)
        if value is None:
            raise ValueError(f"Unresolved credential placeholder: '{secret_name}'")
        return value

    return _PLACEHOLDER_RE.sub(_replace, raw_url)
