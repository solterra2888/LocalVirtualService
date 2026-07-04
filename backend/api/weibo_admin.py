# -*- coding: utf-8 -*-
"""
微博管理员 API

仅 admin 用户可访问，提供：
  - 凭证（Cookie）的录入、更新、删除、校验
  - 博主账户列表与启停

对齐 zsxq_admin.py；差异：博主账户由用户订阅时自动 upsert，
此处仅提供只读列表 + 启停（无 refresh / 手动增删）。
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_admin_user
from backend.schemas.user import TokenData
from backend.utils.logger import get_logger

logger = get_logger("weibo_admin_api")

router = APIRouter()


# ---------- Pydantic 模型 ----------

class CreateCredentialRequest(BaseModel):
    label: str = Field(..., description="凭证标识（如账号备注名）")
    cookie: str = Field(..., description="从 m.weibo.cn 浏览器 F12 复制的完整 Cookie")


class UpdateCredentialRequest(BaseModel):
    label: Optional[str] = Field(None, description="新标识名")
    cookie: Optional[str] = Field(None, description="更新的 Cookie（留空则只改标识）")


class SetAccountEnabledRequest(BaseModel):
    enabled: bool = Field(..., description="是否参与抓取")


# ---------- 凭证管理 ----------

@router.get("/credentials")
async def list_credentials(
    current_user: TokenData = Depends(get_current_admin_user),
) -> List[Dict[str, Any]]:
    """列出所有凭证（Cookie 不返回明文，仅状态/时间/标识）。"""
    from backend.services.weibo_credential_service import WeiboCredentialService
    return WeiboCredentialService().list_credentials()


@router.post("/credentials", status_code=201)
async def create_credential(
    req: CreateCredentialRequest,
    current_user: TokenData = Depends(get_current_admin_user),
) -> Dict[str, Any]:
    """录入新的微博凭证。"""
    from backend.services.weibo_credential_service import WeiboCredentialService
    try:
        return WeiboCredentialService().create_credential(req.label, req.cookie)
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
    """更新凭证标签或替换 Cookie。"""
    from backend.services.weibo_credential_service import WeiboCredentialService
    ok = WeiboCredentialService().update_credential(cred_id, req.label, req.cookie)
    if not ok:
        raise HTTPException(status_code=404, detail=f"凭证 {cred_id} 不存在")
    return {"success": True}


@router.delete("/credentials/{cred_id}", status_code=204)
async def delete_credential(
    cred_id: int,
    current_user: TokenData = Depends(get_current_admin_user),
) -> None:
    """删除凭证。"""
    from backend.services.weibo_credential_service import WeiboCredentialService
    ok = WeiboCredentialService().delete_credential(cred_id)
    if not ok:
        raise HTTPException(status_code=404, detail=f"凭证 {cred_id} 不存在")


@router.post("/credentials/{cred_id}/validate")
async def validate_credential(
    cred_id: int,
    current_user: TokenData = Depends(get_current_admin_user),
) -> Dict[str, Any]:
    """主动校验凭证是否有效（调 m.weibo.cn/api/config 探活）。"""
    from backend.services.weibo_credential_service import WeiboCredentialService
    return WeiboCredentialService().validate_credential(cred_id)


# ---------- 博主账户（只读 + 启停） ----------

@router.get("/accounts")
async def list_accounts(
    current_user: TokenData = Depends(get_current_admin_user),
) -> List[Dict[str, Any]]:
    """列出所有博主账户（含 disabled），供管理员查看抓取状态。"""
    from backend.services.weibo_credential_service import WeiboCredentialService
    return WeiboCredentialService().list_accounts(enabled_only=False)


@router.put("/accounts/{uid}/enabled")
async def set_account_enabled(
    uid: str,
    req: SetAccountEnabledRequest,
    current_user: TokenData = Depends(get_current_admin_user),
) -> Dict[str, Any]:
    """启停某博主账户（停用后不参与抓取）。"""
    from backend.services.weibo_credential_service import WeiboCredentialService
    ok = WeiboCredentialService().set_account_enabled(uid, req.enabled)
    if not ok:
        raise HTTPException(status_code=404, detail=f"博主 {uid} 不存在")
    return {"success": True}
