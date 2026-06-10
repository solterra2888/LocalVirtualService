# -*- coding: utf-8 -*-
"""
Chat group messages API.

Endpoints:
  POST /chat-group-messages/lark/callback - Lark bot callback receiver
  POST /chat-group-messages/batch         - Batch add group messages
"""

import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, Body, HTTPException
from pydantic import BaseModel, Field

from backend.services.chat_group_messages_service import ChatGroupMessagesService
from backend.utils.logger import get_logger

logger = get_logger("chat_group_messages_api")

router = APIRouter(prefix="/chat-group-messages", tags=["群消息"])


class ChatGroupMessageCreate(BaseModel):
    """Request model for external batch inserts."""

    source: str = Field(default="external", description="消息来源，例如 lark/wechat/external")
    message_id: Optional[str] = Field(default=None, description="外部平台消息 ID，非空时按 source+message_id 去重")
    chat_id: str = Field(..., description="群 ID")
    chat_name: Optional[str] = Field(default=None, description="群名称")
    sender_id: str = Field(..., description="发送者 ID")
    sender_name: Optional[str] = Field(default=None, description="发送者名称")
    send_time: Union[datetime, int, float, str] = Field(..., description="发送时间，支持 ISO8601 或 Unix 秒/毫秒")
    msg_type: str = Field(..., description="消息类型，例如 text/link/image/file/其他")
    content: Optional[str] = Field(default=None, description="消息文本内容")
    link_path: Optional[str] = Field(default=None, description="消息中的链接")
    file_path: Optional[str] = Field(default=None, description="文件 key 或文件路径")


class ChatGroupMessagesBatchCreate(BaseModel):
    """Batch insert request wrapper."""

    messages: List[ChatGroupMessageCreate] = Field(..., min_items=1, description="待添加的群消息列表")


def _get_service() -> ChatGroupMessagesService:
    return ChatGroupMessagesService()


def _expected_lark_token() -> str:
    return os.getenv("LARK_ROBOT_VERIFICATION", "").strip()


def _extract_lark_token(body: Dict[str, Any]) -> Optional[str]:
    if isinstance(body.get("token"), str):
        return body["token"]
    header = body.get("header")
    if isinstance(header, dict) and isinstance(header.get("token"), str):
        return header["token"]
    return None


def _extract_lark_challenge(body: Dict[str, Any]) -> Optional[str]:
    if isinstance(body.get("challenge"), str):
        return body["challenge"]
    event = body.get("event")
    if isinstance(event, dict) and isinstance(event.get("challenge"), str):
        return event["challenge"]
    return None


def _is_lark_url_verification(body: Dict[str, Any]) -> bool:
    if body.get("type") == "url_verification":
        return True
    header = body.get("header")
    if isinstance(header, dict) and header.get("event_type") == "url_verification":
        return True
    event = body.get("event")
    if isinstance(event, dict) and event.get("type") == "url_verification":
        return True
    return False


@router.post("/lark/callback", summary="接收 Lark 群消息回调")
async def receive_lark_callback(payload: Dict[str, Any] = Body(...)):
    """
    Receive Lark message callbacks.

    Only `event.message.chat_type == "group"` messages are normalized and saved.
    """
    try:
        expected_token = _expected_lark_token()
        token = _extract_lark_token(payload)
        if not expected_token or token != expected_token:
            raise HTTPException(status_code=403, detail="invalid_token")

        if _is_lark_url_verification(payload):
            challenge = _extract_lark_challenge(payload)
            if not challenge:
                raise HTTPException(status_code=400, detail="missing_challenge")
            return {"challenge": challenge}

        result = _get_service().save_lark_callback(payload)
        return {"success": True, "data": result}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Lark 群消息回调处理失败: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/batch", summary="批量添加群消息")
async def batch_create_messages(request: ChatGroupMessagesBatchCreate):
    """Batch add messages to `chat_group_messages`."""
    try:
        messages = [item.dict() for item in request.messages]
        result = _get_service().save_messages(messages)
        return {"success": True, "data": result}
    except Exception as exc:
        logger.error(f"批量添加群消息失败: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
