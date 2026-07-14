"""Resolve ${SECRET:NAME} URL placeholders from environment variables."""
from __future__ import annotations

import os
import re

_PLACEHOLDER_RE = re.compile(r"\$\{SECRET:([A-Za-z0-9_-]+)\}")


def resolve_connection_url(raw_url: str) -> str:
    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        value = os.environ.get(name)
        if value is None:
            raise ValueError(f"Unresolved credential placeholder: '{name}'")
        return value

    return _PLACEHOLDER_RE.sub(replace, raw_url)
