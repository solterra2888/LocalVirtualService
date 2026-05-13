# -*- coding: utf-8 -*-
"""JWT or Personal Access Token (PAT) authentication for Agent API."""

from __future__ import annotations

from typing import Callable, List

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from backend.auth.jwt_utils import verify_token
from backend.core.pat_rate_limiter import PATRateLimiter
from backend.schemas.user import TokenData
from backend.services.personal_token_service import PersonalTokenService

security = HTTPBearer(auto_error=True)


def _is_pat(token: str) -> bool:
    return bool(token and token.startswith("ght_"))


async def get_current_user_or_pat(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> TokenData:
    """Accept JWT (web session) or PAT (Agent/MCP). PAT triggers per-token rate limit."""
    token = credentials.credentials.strip()
    if _is_pat(token):
        svc = PersonalTokenService()
        row = svc.verify(token)
        if not row:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired API token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        PATRateLimiter.instance().check_and_consume(row["pat_id"])
        return TokenData(
            username=row["username"],
            user_id=row["user_id"],
            is_admin=False,
            token_version=None,
            auth_kind="pat",
            pat_id=row["pat_id"],
            pat_scopes=row.get("scopes") or [],
        )
    td = verify_token(token)
    return TokenData(
        username=td.username,
        user_id=td.user_id,
        is_admin=td.is_admin,
        token_version=td.token_version,
        auth_kind="jwt",
        pat_id=None,
        pat_scopes=None,
    )


def require_pat_scopes(*required: str) -> Callable[..., TokenData]:
    """JWT users bypass scope checks; PAT must include all required scopes."""

    async def _dep(user: TokenData = Depends(get_current_user_or_pat)) -> TokenData:
        if user.user_id is None:
            raise HTTPException(status_code=401, detail="未授权")
        if user.auth_kind != "pat":
            return user
        have: List[str] = list(user.pat_scopes or [])
        for s in required:
            if s not in have:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"API token missing required scope: {s}",
                )
        return user

    return _dep


def optional_pat_rate_headers(user: TokenData) -> dict:
    if user.auth_kind == "pat" and user.pat_id:
        return PATRateLimiter.instance().get_headers(user.pat_id)
    return {}
