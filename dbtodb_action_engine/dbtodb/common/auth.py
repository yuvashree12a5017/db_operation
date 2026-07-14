"""verify_jwt_or_basic auth dependency (section 18 security requirement).

Standalone placeholder for common.auth.
"""
from __future__ import annotations

import base64
import os

import jwt
from fastapi import Header, HTTPException

JWT_SECRET = os.environ.get("DBTODB_JWT_SECRET", "")
BASIC_USER = os.environ.get("DBTODB_BASIC_USER", "")
BASIC_PASSWORD = os.environ.get("DBTODB_BASIC_PASSWORD", "")


async def verify_jwt_or_basic(authorization: str = Header(default="")) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    if authorization.startswith("Bearer "):
        token = authorization.removeprefix("Bearer ")
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        except jwt.PyJWTError as exc:
            raise HTTPException(status_code=401, detail="Invalid JWT") from exc
        return payload.get("sub", "jwt-user")

    if authorization.startswith("Basic "):
        encoded = authorization.removeprefix("Basic ")
        decoded = base64.b64decode(encoded).decode("utf-8")
        username, _, password = decoded.partition(":")
        if username and username == BASIC_USER and password == BASIC_PASSWORD:
            return username
        raise HTTPException(status_code=401, detail="Invalid credentials")

    raise HTTPException(status_code=401, detail="Unsupported authorization scheme")
