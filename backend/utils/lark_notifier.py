# -*- coding: utf-8 -*-
"""
Lark (Larksuite) 群机器人 webhook 通知工具。

告警发送失败时静默记录日志，不向上抛出，避免拖垮 Celery 任务主流程。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import time
from typing import Optional, Sequence, Union

import requests

logger = logging.getLogger(__name__)

# 服务状态监测群（HK Worker 健康检查默认渠道）
LARK_WEBHOOK_SERVICE_STATUS = (
    "https://open.larksuite.com/open-apis/bot/v2/hook/"
    "8ecad972-c0f6-4c27-8330-0b227a494aa2"
)
# 警报机器人群（备用 / 通用告警）
LARK_WEBHOOK_ALERT_BOT = (
    "https://open.larksuite.com/open-apis/bot/v2/hook/"
    "faf6e6eb-dbcc-4c59-9176-fc068e5d08a9"
)

_DEFAULT_TIMEOUT_SEC = 10


def _default_webhook_url() -> str:
    if "LARK_WEBHOOK_URL" in os.environ:
        return os.getenv("LARK_WEBHOOK_URL", "").strip()
    return LARK_WEBHOOK_SERVICE_STATUS


def _build_signed_payload(body: dict, secret: str) -> dict:
    timestamp = str(int(time.time()))
    string_to_sign = f"{timestamp}\n{secret}"
    sign = base64.b64encode(
        hmac.new(
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()
    ).decode("utf-8")
    payload = dict(body)
    payload["timestamp"] = timestamp
    payload["sign"] = sign
    return payload


def send_text(
    title: str,
    lines: Union[str, Sequence[str], None] = None,
    *,
    webhook_url: Optional[str] = None,
    timeout_sec: float = _DEFAULT_TIMEOUT_SEC,
) -> bool:
    """发送 Lark 文本消息。成功返回 True，失败返回 False（不抛出）。"""
    url = (webhook_url or _default_webhook_url()).strip()
    if not url:
        logger.warning("Lark 通知跳过: 未配置 LARK_WEBHOOK_URL")
        return False

    parts: list[str] = []
    if title:
        parts.append(title.strip())
    if lines is not None:
        if isinstance(lines, str):
            if lines.strip():
                parts.append(lines.strip())
        else:
            for line in lines:
                text = str(line).strip()
                if text:
                    parts.append(text)

    text = "\n".join(parts).strip()
    if not text:
        logger.warning("Lark 通知跳过: 空消息")
        return False

    body = {
        "msg_type": "text",
        "content": {"text": text},
    }
    secret = os.getenv("LARK_WEBHOOK_SECRET", "").strip()
    payload = _build_signed_payload(body, secret) if secret else body

    try:
        resp = requests.post(url, json=payload, timeout=timeout_sec)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and data.get("code") not in (None, 0):
            logger.warning("Lark 通知 API 返回错误: %s", data)
            return False
        return True
    except Exception as exc:
        logger.warning("Lark 通知发送失败: %s", exc)
        return False
