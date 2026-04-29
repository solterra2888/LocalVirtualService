# -*- coding: utf-8 -*-
"""
Feed Summary Settings API (PR-N1)

用户对 Feeds 自动个性化摘要的开关 + 模板偏好.

挂载方式 (main.py):
    app.include_router(
        feed_summary_settings.router,
        prefix="/api/feeds",
        tags=["Feed 摘要设置"],
    )

路由:
    GET    /api/feeds/summary-settings              → 当前用户全部渠道设置
    GET    /api/feeds/summary-settings/{channel}    → 单渠道设置 (twitter|youtube)
    PUT    /api/feeds/summary-settings/{channel}    → 创建/更新单渠道设置
"""
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_user
from backend.database.db_manager import DatabaseManager
from backend.schemas.user import TokenData
from backend.services.user_feed_summary_settings_service import (
    SUPPORTED_CHANNELS,
    UserFeedSummarySettingsService,
)
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("feed_summary_settings_api")

router = APIRouter()


def get_db() -> DatabaseManager:
    return DatabaseManager(get_database_config())


# ─── Schemas ───────────────────────────────────────────────────────


class FeedSummarySettingItem(BaseModel):
    id: Optional[int] = None
    channel_type: str
    enabled: bool
    template_id: Optional[int] = None
    min_length: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class FeedSummarySettingUpdate(BaseModel):
    enabled: bool = Field(..., description="是否开启自动个性化")
    template_id: Optional[int] = Field(
        default=None,
        description="模板 ID; 不传走 LLMTemplateResolver (用户私有 → 系统种子)",
    )
    min_length: Optional[int] = Field(
        default=None,
        ge=0,
        description="用户级长度门槛 (字符), 不传走 env",
    )


# ─── Helpers ───────────────────────────────────────────────────────


def _validate_template_module(
    db: DatabaseManager,
    template_id: int,
    expected_module: str,
    current_user_id: int,
) -> None:
    """校验 template_id 存在 + module_code 正确 + (user-scope) 归属当前用户.

    feed_summary 渠道 → twitter; youtube_summary 渠道 → youtube.
    防止用户传一个 file_analysis 模板进来导致 fan-out 调用时炸掉,
    也防止用户保存别人的私有模板 ID 作为自己的偏好.

    注意: llm_prompt_templates 表的"作者"列叫 created_by (NOT owner_user_id);
    早期 PR 中曾误写, 这里用真实列名.
    """
    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT module_code, scope, created_by, is_active
            FROM   llm_prompt_templates
            WHERE  id = %s
            """,
            (template_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(
                status_code=400,
                detail=f"template_id={template_id} 不存在",
            )
        module_code, scope, created_by, is_active = row
        if not is_active:
            raise HTTPException(
                status_code=400,
                detail=f"template_id={template_id} 已停用",
            )
        if module_code != expected_module:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"模板 module_code={module_code!r} 与渠道不匹配; "
                    f"该渠道需要 {expected_module!r}"
                ),
            )
        # user-scope 模板必须归当前用户; system / public 任何登录用户都可用
        if scope == "user" and created_by != current_user_id:
            raise HTTPException(
                status_code=403,
                detail=f"template_id={template_id} 是其他用户的私有模板",
            )
    finally:
        conn.close()


def _channel_to_module(channel_type: str) -> str:
    return {
        "twitter": "feed_summary",
        "youtube": "youtube_summary",
    }[channel_type]


# ─── Routes ────────────────────────────────────────────────────────


@router.get(
    "/summary-settings",
    summary="获取当前用户对各渠道的自动个性化摘要配置",
)
def list_settings(
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    """
    返回固定 len = len(SUPPORTED_CHANNELS) 的列表;
    未配置过的渠道返回默认 disabled 行 (前端 UI 直接渲染).
    """
    service = UserFeedSummarySettingsService(db)
    items = service.list_for_user(current_user.user_id)
    return {"items": items}


@router.get(
    "/summary-settings/{channel_type}",
    summary="获取当前用户对指定渠道的设置",
)
def get_setting(
    channel_type: str = Path(..., regex="^(twitter|youtube)$"),
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    service = UserFeedSummarySettingsService(db)
    item = service.get(current_user.user_id, channel_type)
    if not item:
        # 与 list 行为一致, 未配置返回默认 disabled
        return {
            "id": None,
            "channel_type": channel_type,
            "enabled": False,
            "template_id": None,
            "min_length": None,
            "created_at": None,
            "updated_at": None,
        }
    return item


@router.put(
    "/summary-settings/{channel_type}",
    summary="创建或更新当前用户对指定渠道的设置 (UPSERT)",
)
def upsert_setting(
    payload: FeedSummarySettingUpdate,
    channel_type: str = Path(..., regex="^(twitter|youtube)$"),
    db: DatabaseManager = Depends(get_db),
    current_user: TokenData = Depends(get_current_user),
):
    if channel_type not in SUPPORTED_CHANNELS:
        raise HTTPException(
            status_code=400,
            detail=f"channel_type 必须为 {SUPPORTED_CHANNELS} 之一",
        )

    # 校验 template_id (若提供) 与渠道匹配 + (user-scope) 归属当前用户
    if payload.template_id is not None:
        _validate_template_module(
            db,
            template_id=payload.template_id,
            expected_module=_channel_to_module(channel_type),
            current_user_id=current_user.user_id,
        )

    service = UserFeedSummarySettingsService(db)
    item = service.upsert(
        user_id=current_user.user_id,
        channel_type=channel_type,
        enabled=payload.enabled,
        template_id=payload.template_id,
        min_length=payload.min_length,
    )
    logger.info(
        f"FeedSummarySettings updated: user_id={current_user.user_id} "
        f"channel={channel_type} enabled={payload.enabled} "
        f"template_id={payload.template_id} min_length={payload.min_length}"
    )
    return item
