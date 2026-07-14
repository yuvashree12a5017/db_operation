"""JWT or Basic authentication dependency."""
from __future__ import annotations

import base64
import binascii
import os

import jwt
from fastapi import Header, HTTPException


async def verify_jwt_or_basic(authorization: str = Header(default="")) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    if authorization.startswith("Bearer "):
        try:
            payload = jwt.decode(
                authorization.removeprefix("Bearer "),
                os.environ.get("DBTODB_JWT_SECRET", ""),
                algorithms=["HS256"],
            )
            return str(payload.get("sub", "jwt-user"))
        except jwt.PyJWTError as exc:
            raise HTTPException(status_code=401, detail="Invalid JWT") from exc
    if authorization.startswith("Basic "):
        try:
            decoded = base64.b64decode(
                authorization.removeprefix("Basic "), validate=True
            ).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError) as exc:
            raise HTTPException(status_code=401, detail="Invalid Basic credentials") from exc
        username, separator, password = decoded.partition(":")
        if (
            separator
            and username == os.environ.get("DBTODB_BASIC_USER", "")
            and password == os.environ.get("DBTODB_BASIC_PASSWORD", "")
            and username
        ):
            return username
        raise HTTPException(status_code=401, detail="Invalid credentials")
    raise HTTPException(status_code=401, detail="Unsupported authorization scheme")
