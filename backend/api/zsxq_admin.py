# -*- coding: utf-8 -*-
"""
知识星球管理员 API

仅 admin 用户可访问，提供：
  - 凭证（Cookie）的录入、更新、删除、校验
  - 星球列表的同步与手动维护
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_admin_user
from backend.schemas.user import TokenData
from backend.utils.logger import get_logger

logger = get_logger("zsxq_admin_api")

router = APIRouter()


# ---------- Pydantic 模型 ----------

class CreateCredentialRequest(BaseModel):
    label: str = Field(..., description="凭证标识（如账号备注名）")
    cookie: str = Field(..., description="从 wx.zsxq.com 浏览器 F12 复制的完整 Cookie")


class UpdateCredentialRequest(BaseModel):
    label: Optional[str] = Field(None, description="新标识名")
    cookie: Optional[str] = Field(None, description="更新的 Cookie（留空则只改标识）")


class UpsertGroupRequest(BaseModel):
    group_id: str = Field(..., description="知识星球 group_id")
    group_name: str = Field(..., description="星球显示名称")
    enabled: bool = Field(True, description="是否对用户开放订阅")


# ---------- 凭证管理 ----------

@router.get("/credentials")
async def list_credentials(
    current_user: TokenData = Depends(get_current_admin_user),
) -> List[Dict[str, Any]]:
    """列出所有凭证（Cookie 不返回明文，仅状态/时间/标识）。"""
    from backend.services.zsxq_credential_service import ZsxqCredentialService
    return ZsxqCredentialService().list_credentials()


@router.post("/credentials", status_code=201)
async def create_credential(
    req: CreateCredentialRequest,
    current_user: TokenData = Depends(get_current_admin_user),
) -> Dict[str, Any]:
    """录入新的知识星球凭证。"""
    from backend.services.zsxq_credential_service import ZsxqCredentialService
    try:
        result = ZsxqCredentialService().create_credential(req.label, req.cookie)
        return result
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
    from backend.services.zsxq_credential_service import ZsxqCredentialService
    ok = ZsxqCredentialService().update_credential(cred_id, req.label, req.cookie)
    if not ok:
        raise HTTPException(status_code=404, detail=f"凭证 {cred_id} 不存在")
    return {"success": True}


@router.delete("/credentials/{cred_id}", status_code=204)
async def delete_credential(
    cred_id: int,
    current_user: TokenData = Depends(get_current_admin_user),
) -> None:
    """删除凭证。"""
    from backend.services.zsxq_credential_service import ZsxqCredentialService
    ok = ZsxqCredentialService().delete_credential(cred_id)
    if not ok:
        raise HTTPException(status_code=404, detail=f"凭证 {cred_id} 不存在")


@router.post("/credentials/{cred_id}/validate")
async def validate_credential(
    cred_id: int,
    current_user: TokenData = Depends(get_current_admin_user),
) -> Dict[str, Any]:
    """主动校验凭证是否有效（向知识星球 API 发请求）。"""
    from backend.services.zsxq_credential_service import ZsxqCredentialService
    return ZsxqCredentialService().validate_credential(cred_id)


@router.post("/credentials/{cred_id}/refresh-groups")
async def refresh_groups_from_credential(
    cred_id: int,
    current_user: TokenData = Depends(get_current_admin_user),
) -> Dict[str, Any]:
    """
    用指定凭证拉取「我加入的星球」列表，写入/更新 zsxq_groups 表。
    """
    from backend.services.zsxq_credential_service import ZsxqCredentialService
    return ZsxqCredentialService().refresh_groups(cred_id)


# ---------- 星球手动维护 ----------

@router.get("/groups")
async def list_groups_admin(
    current_user: TokenData = Depends(get_current_admin_user),
) -> List[Dict[str, Any]]:
    """列出所有星球（含 disabled）供管理员查看。"""
    from backend.services.zsxq_credential_service import ZsxqCredentialService
    return ZsxqCredentialService().list_groups(enabled_only=False)


@router.put("/groups")
async def upsert_group_manual(
    req: UpsertGroupRequest,
    current_user: TokenData = Depends(get_current_admin_user),
) -> Dict[str, Any]:
    """手动添加或更新星球（兜底，不依赖 API 拉取）。"""
    from backend.services.zsxq_credential_service import ZsxqCredentialService
    return ZsxqCredentialService().upsert_group_manual(req.group_id, req.group_name, req.enabled)
