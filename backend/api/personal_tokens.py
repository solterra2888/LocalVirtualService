# -*- coding: utf-8 -*-
"""User Personal Access Token management (JWT only — PAT cannot manage tokens)."""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.services.personal_token_service import ALLOWED_SCOPES, PersonalTokenService
from backend.utils.logger import get_logger

logger = get_logger("personal_tokens_api")

router = APIRouter(prefix="/me/tokens", tags=["Personal API Tokens"])


class CreateTokenRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=80, description="Human-readable label")
    scopes: Optional[List[str]] = Field(
        None,
        description="Subset of daily:read, reports:read; default both",
    )
    expires_days: Optional[int] = Field(
        None,
        ge=0,
        le=3650,
        description="0 or null = no expiry; else N days from now",
    )


@router.get("", summary="List personal API tokens")
async def list_tokens(current_user: TokenData = Depends(get_current_user)):
    if current_user.user_id is None:
        raise HTTPException(status_code=401, detail="未授权")
    svc = PersonalTokenService()
    items = svc.list_tokens(current_user.user_id)
    return {"success": True, "data": items}


@router.post("", summary="Create personal API token")
async def create_token(
    body: CreateTokenRequest,
    current_user: TokenData = Depends(get_current_user),
):
    if current_user.user_id is None:
        raise HTTPException(status_code=401, detail="未授权")
    if body.scopes:
        bad = [s for s in body.scopes if s not in ALLOWED_SCOPES]
        if bad:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid scopes: {bad}. Allowed: {sorted(ALLOWED_SCOPES)}",
            )
    exp_days = body.expires_days
    if exp_days == 0:
        exp_days = None
    svc = PersonalTokenService()
    try:
        plaintext, meta = svc.create_token(
            user_id=current_user.user_id,
            name=body.name.strip(),
            scopes=body.scopes,
            expires_days=exp_days,
        )
    except Exception as e:
        logger.error(f"create_token failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    logger.info(
        f"User {current_user.username} created PAT id={meta.get('id')} prefix={meta.get('prefix')}"
    )
    return {"success": True, "data": {"token": plaintext, "meta": meta}}


@router.delete("/{token_id}", summary="Revoke personal API token")
async def revoke_token(
    token_id: int,
    current_user: TokenData = Depends(get_current_user),
):
    if current_user.user_id is None:
        raise HTTPException(status_code=401, detail="未授权")
    svc = PersonalTokenService()
    ok = svc.revoke(current_user.user_id, token_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Token not found or already revoked")
    return {"success": True, "message": "revoked"}
