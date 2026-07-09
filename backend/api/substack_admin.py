# -*- coding: utf-8 -*-
"""
Substack 管理员 API

仅 admin 用户可访问，提供：
  - 凭证（sid + lli 双值 Cookie）的录入、更新、删除、校验
  - 订阅源并集（源A 订阅 newsletter + 源B 关注作者主刊）的同步与手动维护
  - 关注作者登记查看
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_admin_user
from backend.schemas.user import TokenData
from backend.utils.logger import get_logger

logger = get_logger("substack_admin_api")

router = APIRouter()


# ---------- Pydantic 模型 ----------

class CreateCredentialRequest(BaseModel):
    label: str = Field(..., description="凭证标识（如账号备注名）")
    sid: str = Field(..., description="浏览器 Cookie 中的 substack.sid（必需）")
    lli: str = Field("", description="浏览器 Cookie 中的 substack.lli（建议，可空）")


class UpdateCredentialRequest(BaseModel):
    label: Optional[str] = Field(None, description="新标识名")
    sid: Optional[str] = Field(None, description="更新的 substack.sid（留空则不改）")
    lli: Optional[str] = Field(None, description="更新的 substack.lli（留空则不改）")


class UpsertPublicationRequest(BaseModel):
    publication_url: str = Field(..., description="publication 根 URL，如 https://citrini.substack.com")
    publication_name: str = Field("", description="展示名")
    enabled: bool = Field(True, description="是否对用户开放订阅")


# ---------- 凭证管理 ----------

@router.get("/credentials")
async def list_credentials(
    current_user: TokenData = Depends(get_current_admin_user),
) -> List[Dict[str, Any]]:
    """列出所有凭证（Cookie 不返回明文，仅 masked_cookie/状态/时间/标识）。"""
    from backend.services.substack_credential_service import SubstackCredentialService
    return SubstackCredentialService().list_credentials()


@router.post("/credentials", status_code=201)
async def create_credential(
    req: CreateCredentialRequest,
    current_user: TokenData = Depends(get_current_admin_user),
) -> Dict[str, Any]:
    from backend.services.substack_credential_service import SubstackCredentialService
    try:
        return SubstackCredentialService().create_credential(req.label, req.sid, req.lli)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/credentials/{cred_id}")
async def update_credential(
    cred_id: int,
    req: UpdateCredentialRequest,
    current_user: TokenData = Depends(get_current_admin_user),
) -> Dict[str, Any]:
    from backend.services.substack_credential_service import SubstackCredentialService
    ok = SubstackCredentialService().update_credential(cred_id, req.label, req.sid, req.lli)
    if not ok:
        raise HTTPException(status_code=404, detail=f"凭证 {cred_id} 不存在")
    return {"success": True}


@router.delete("/credentials/{cred_id}", status_code=204)
async def delete_credential(
    cred_id: int,
    current_user: TokenData = Depends(get_current_admin_user),
) -> None:
    from backend.services.substack_credential_service import SubstackCredentialService
    ok = SubstackCredentialService().delete_credential(cred_id)
    if not ok:
        raise HTTPException(status_code=404, detail=f"凭证 {cred_id} 不存在")


@router.post("/credentials/{cred_id}/validate")
async def validate_credential(
    cred_id: int,
    current_user: TokenData = Depends(get_current_admin_user),
) -> Dict[str, Any]:
    """探活 subscriptions API，标 valid/expired。"""
    from backend.services.substack_credential_service import SubstackCredentialService
    return SubstackCredentialService().validate_credential(cred_id)


@router.post("/credentials/{cred_id}/refresh-publications")
async def refresh_publications_from_credential(
    cred_id: int,
    current_user: TokenData = Depends(get_current_admin_user),
) -> Dict[str, Any]:
    """同步订阅源并集：源A subscriptions + 源B following(解析主刊)。"""
    from backend.services.substack_credential_service import SubstackCredentialService
    return SubstackCredentialService().refresh_publications(cred_id)


# ---------- 订阅源 / 关注作者维护 ----------

@router.get("/publications")
async def list_publications_admin(
    current_user: TokenData = Depends(get_current_admin_user),
) -> List[Dict[str, Any]]:
    """列出所有 publication（订阅源并集，含 disabled）。"""
    from backend.services.substack_credential_service import SubstackCredentialService
    return SubstackCredentialService().list_publications(enabled_only=False)


@router.put("/publications")
async def upsert_publication_manual(
    req: UpsertPublicationRequest,
    current_user: TokenData = Depends(get_current_admin_user),
) -> Dict[str, Any]:
    """手动添加/更新 publication（兜底）。"""
    from backend.services.substack_credential_service import SubstackCredentialService
    try:
        return SubstackCredentialService().upsert_publication_manual(
            req.publication_url, req.publication_name, req.enabled
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/followed-authors")
async def list_followed_authors_admin(
    current_user: TokenData = Depends(get_current_admin_user),
) -> List[Dict[str, Any]]:
    """列出所有关注作者（源B 登记）。"""
    from backend.services.substack_credential_service import SubstackCredentialService
    return SubstackCredentialService().list_followed_authors(enabled_only=False)
