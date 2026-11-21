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


def send_card(
    card: dict,
    *,
    webhook_url: Optional[str] = None,
    timeout_sec: float = _DEFAULT_TIMEOUT_SEC,
) -> bool:
    """发送 Lark 交互卡片(msg_type=interactive)。成功返回 True，失败不抛出。

    自定义机器人 webhook 的文本消息不做链接 unfurl(链接预览是应用级能力，
    机器人收不到消息事件)，需要卡片观感时由此直发 interactive 卡片。
    """
    url = (webhook_url or _default_webhook_url()).strip()
    if not url:
        logger.warning("Lark 卡片通知跳过: 未配置 LARK_WEBHOOK_URL")
        return False
    body = {"msg_type": "interactive", "card": card}
    secret = os.getenv("LARK_WEBHOOK_SECRET", "").strip()
    payload = _build_signed_payload(body, secret) if secret else body
    try:
        resp = requests.post(url, json=payload, timeout=timeout_sec)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and data.get("code") not in (None, 0):
            logger.warning("Lark 卡片通知 API 返回错误: %s", data)
            return False
        return True
    except Exception as exc:
        logger.warning("Lark 卡片通知发送失败: %s", exc)
        return False


def send_zsxq_scheduler_report(result: dict) -> bool:
    """
    知识星球 Scheduler 定时抓取完成后，将运行摘要与错误统计一次性推送到 Lark。

    使用 ZSXQ_LARK_WEBHOOK_URL（默认 LARK_WEBHOOK_ALERT_BOT）。
    可通过 ZSXQ_LARK_NOTIFY_ENABLED=false 关闭。
    """
    if os.getenv("ZSXQ_LARK_NOTIFY_ENABLED", "true").lower() != "true":
        return False

    webhook = os.getenv("ZSXQ_LARK_WEBHOOK_URL", LARK_WEBHOOK_ALERT_BOT).strip()
    if not webhook:
        logger.warning("知识星球 Lark 通知跳过: 未配置 ZSXQ_LARK_WEBHOOK_URL")
        return False

    report = result.get("report") or {}
    success = result.get("success", False)
    error_msg = result.get("error") or result.get("message") or ""

    if not success and error_msg:
        status = "失败"
    elif report.get("failed_groups") or report.get("errors"):
        status = "部分失败"
    elif report.get("api_retry_count", 0) > 0 or report.get("warnings"):
        status = "成功（有告警）"
    else:
        status = "成功"

    lines = [
        f"状态: {status}",
        f"星球处理: {result.get('groups_processed', 0)} 个",
        f"新增话题: {result.get('topics_new', 0)} 条",
        f"更新话题: {result.get('topics_updated', 0)} 条",
    ]

    failed = report.get("failed_groups") or result.get("failed_groups") or []
    if failed:
        lines.append(f"失败星球: {', '.join(failed)}")
    else:
        lines.append("失败星球: 无")

    api_retries = report.get("api_retry_count", 0)
    if api_retries:
        lines.append("")
        lines.append("异常统计:")
        lines.append(f"- API 重试: {api_retries} 次")
        for sample in report.get("api_retry_samples") or []:
            lines.append(f"  · {sample}")
        if api_retries > len(report.get("api_retry_samples") or []):
            lines.append("  · …")

    errors = report.get("errors") or []
    if errors:
        if not api_retries:
            lines.append("")
            lines.append("异常统计:")
        for err in errors[:10]:
            lines.append(f"- {err}")
        if len(errors) > 10:
            lines.append(f"- … 另有 {len(errors) - 10} 条")

    warnings = report.get("warnings") or []
    if warnings:
        lines.append("")
        lines.append("警告:")
        for warn in warnings[:8]:
            lines.append(f"- {warn}")
        if len(warnings) > 8:
            lines.append(f"- … 另有 {len(warnings) - 8} 条")

    group_summaries = report.get("group_summaries") or []
    if group_summaries:
        lines.append("")
        lines.append("各星球:")
        for gs in group_summaries:
            lines.append(
                f"- {gs.get('name', '?')} [{gs.get('mode', '?')}]: 新 {gs.get('new', 0)} 条"
            )

    if not success and error_msg:
        lines.append("")
        lines.append(f"错误: {error_msg}")

    duration = report.get("duration_sec")
    if duration is not None:
        lines.append(f"耗时: {duration:.0f}s")

    finished_at = report.get("finished_at") or ""
    title = f"【知识星球定时抓取】{finished_at}".strip()

    return send_text(title, lines, webhook_url=webhook)


def send_substack_scheduler_report(result: dict) -> bool:
    """
    Substack Scheduler 定时抓取完成后，将运行摘要与错误统计一次性推送到 Lark。

    使用 SUBSTACK_LARK_WEBHOOK_URL（默认 LARK_WEBHOOK_ALERT_BOT）。
    可通过 SUBSTACK_LARK_NOTIFY_ENABLED=false 关闭。
    """
    if os.getenv("SUBSTACK_LARK_NOTIFY_ENABLED", "true").lower() != "true":
        return False

    webhook = os.getenv("SUBSTACK_LARK_WEBHOOK_URL", LARK_WEBHOOK_ALERT_BOT).strip()
    if not webhook:
        logger.warning("Substack Lark 通知跳过: 未配置 SUBSTACK_LARK_WEBHOOK_URL")
        return False

    report = result.get("report") or {}
    success = result.get("success", False)
    error_msg = result.get("error") or result.get("message") or ""

    if not success and error_msg:
        status = "失败"
    elif report.get("failed_publications") or report.get("failed_authors") or report.get("errors"):
        status = "部分失败"
    elif report.get("warnings"):
        status = "成功（有告警）"
    else:
        status = "成功"

    lines = [
        f"状态: {status}",
        f"publication 处理: {result.get('publications_processed', 0)} 个",
        f"新增文章: {result.get('posts_new', 0)} 条",
        f"新增动态: {result.get('notes_new', 0)} 条",
        f"关注作者处理: {result.get('authors_processed', 0)} 个",
    ]

    failed_pubs = report.get("failed_publications") or result.get("failed_publications") or []
    failed_authors = report.get("failed_authors") or result.get("failed_authors") or []
    if failed_pubs:
        lines.append(f"失败 publication: {', '.join(failed_pubs)}")
    if failed_authors:
        lines.append(f"失败作者: {', '.join(failed_authors)}")
    if not failed_pubs and not failed_authors:
        lines.append("失败: 无")

    errors = report.get("errors") or []
    if errors:
        lines.append("")
        lines.append("异常统计:")
        for err in errors[:10]:
            lines.append(f"- {err}")
        if len(errors) > 10:
            lines.append(f"- … 另有 {len(errors) - 10} 条")

    warnings = report.get("warnings") or []
    if warnings:
        lines.append("")
        lines.append("警告:")
        for warn in warnings[:8]:
            lines.append(f"- {warn}")
        if len(warnings) > 8:
            lines.append(f"- … 另有 {len(warnings) - 8} 条")

    post_summaries = report.get("post_summaries") or []
    if post_summaries:
        lines.append("")
        lines.append("各 publication (文章):")
        for ps in post_summaries:
            lines.append(
                f"- {ps.get('name', '?')} [{ps.get('mode', '?')}]: 新 {ps.get('new', 0)} 条"
            )

    note_summaries = report.get("note_summaries") or []
    if note_summaries:
        lines.append("")
        lines.append("各作者 (动态):")
        for ns in note_summaries:
            lines.append(f"- @{ns.get('handle', '?')}: 新 {ns.get('new', 0)} 条")

    if not success and error_msg:
        lines.append("")
        lines.append(f"错误: {error_msg}")

    duration = report.get("duration_sec")
    if duration is not None:
        lines.append(f"耗时: {duration:.0f}s")

    finished_at = report.get("finished_at") or ""
    title = f"【Substack 定时抓取】{finished_at}".strip()

    return send_text(title, lines, webhook_url=webhook)


def send_weibo_scheduler_report(result: dict) -> bool:
    """
    微博 Scheduler 定时抓取完成后，将运行摘要与错误统计一次性推送到 Lark。

    使用 WEIBO_LARK_WEBHOOK_URL（默认 LARK_WEBHOOK_ALERT_BOT）。
    可通过 WEIBO_LARK_NOTIFY_ENABLED=false 关闭。
    """
    if os.getenv("WEIBO_LARK_NOTIFY_ENABLED", "true").lower() != "true":
        return False

    webhook = os.getenv("WEIBO_LARK_WEBHOOK_URL", LARK_WEBHOOK_ALERT_BOT).strip()
    if not webhook:
        logger.warning("微博 Lark 通知跳过: 未配置 WEIBO_LARK_WEBHOOK_URL")
        return False

    report = result.get("report") or {}
    success = result.get("success", False)
    error_msg = result.get("error") or result.get("message") or ""

    if not success and error_msg:
        status = "失败"
    elif report.get("failed_accounts") or report.get("errors"):
        status = "部分失败"
    elif report.get("api_retry_count", 0) > 0 or report.get("warnings"):
        status = "成功（有告警）"
    else:
        status = "成功"

    lines = [
        f"状态: {status}",
        f"博主处理: {result.get('accounts_processed', 0)} 个",
        f"新增博文: {result.get('posts_new', 0)} 条",
        f"更新博文: {result.get('posts_updated', 0)} 条",
    ]

    failed = report.get("failed_accounts") or result.get("failed_accounts") or []
    if failed:
        lines.append(f"失败博主: {', '.join(failed)}")
    else:
        lines.append("失败博主: 无")

    api_retries = report.get("api_retry_count", 0)
    if api_retries:
        lines.append("")
        lines.append("异常统计:")
        lines.append(f"- API 重试: {api_retries} 次")
        for sample in report.get("api_retry_samples") or []:
            lines.append(f"  · {sample}")
        if api_retries > len(report.get("api_retry_samples") or []):
            lines.append("  · …")

    errors = report.get("errors") or []
    if errors:
        if not api_retries:
            lines.append("")
            lines.append("异常统计:")
        for err in errors[:10]:
            lines.append(f"- {err}")
        if len(errors) > 10:
            lines.append(f"- … 另有 {len(errors) - 10} 条")

    warnings = report.get("warnings") or []
    if warnings:
        lines.append("")
        lines.append("警告:")
        for warn in warnings[:8]:
            lines.append(f"- {warn}")
        if len(warnings) > 8:
            lines.append(f"- … 另有 {len(warnings) - 8} 条")

    account_summaries = report.get("account_summaries") or []
    if account_summaries:
        lines.append("")
        lines.append("各博主:")
        for acs in account_summaries:
            lines.append(
                f"- {acs.get('name', '?')} [{acs.get('mode', '?')}]: 新 {acs.get('new', 0)} 条"
            )

    if not success and error_msg:
        lines.append("")
        lines.append(f"错误: {error_msg}")

    duration = report.get("duration_sec")
    if duration is not None:
        lines.append(f"耗时: {duration:.0f}s")

    finished_at = report.get("finished_at") or ""
    title = f"【微博定时抓取】{finished_at}".strip()

    return send_text(title, lines, webhook_url=webhook)
