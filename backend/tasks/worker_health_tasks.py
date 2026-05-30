# -*- coding: utf-8 -*-
"""
HK YouTube Worker 健康检查 Celery 任务。

通过 Celery control.ping 周期性探测家用 VM worker 是否在线；
连续未命中超过阈值时向 Lark 推送告警，恢复时推送恢复通知。
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Set

import redis as redis_lib

from backend.celery_app import app
from backend.utils.lark_notifier import send_text
from backend.utils.logger import get_logger

logger = get_logger("worker_health_tasks")

_REDIS: redis_lib.Redis | None = None

_REDIS_KEY_MISS = "hk_health:miss_count"
_REDIS_KEY_STATE = "hk_health:state"
_REDIS_KEY_LAST_ALERT = "hk_health:last_alert_ts"

_HK_YOUTUBE_QUEUES = (
    "youtube_fetching",
    "youtube_transcription",
    "youtube_transcription_long",
    "youtube_priority",
    "youtube_transcription_priority",
)

_DEFAULT_EXPECTED = "youtube-transcription-worker@localhost001"


def _get_redis() -> redis_lib.Redis:
    global _REDIS
    if _REDIS is None:
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        _REDIS = redis_lib.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
    return _REDIS


def _parse_expected_workers() -> List[str]:
    raw = os.getenv("HK_EXPECTED_WORKERS", _DEFAULT_EXPECTED).strip()
    if not raw:
        return [_DEFAULT_EXPECTED]
    workers = [w.strip() for w in raw.split(",") if w.strip()]
    return workers or [_DEFAULT_EXPECTED]


def _ping_workers(timeout: float) -> Set[str]:
    try:
        replies = app.control.ping(timeout=timeout) or []
    except Exception as exc:
        logger.warning("HK health ping 失败: %s", exc)
        return set()

    online: Set[str] = set()
    for item in replies:
        if isinstance(item, dict):
            online.update(item.keys())
    return online


def _fetch_youtube_queue_depths() -> List[str]:
    r = _get_redis()
    lines: List[str] = []
    try:
        pipe = r.pipeline(transaction=False)
        for q in _HK_YOUTUBE_QUEUES:
            pipe.llen(q)
        depths = pipe.execute()
        for q, depth in zip(_HK_YOUTUBE_QUEUES, depths):
            if isinstance(depth, int) and depth > 0:
                lines.append(f"  {q}: {depth}")
    except Exception as exc:
        logger.warning("读取 YouTube 队列深度失败: %s", exc)
        lines.append(f"  (队列深度读取失败: {exc})")
    return lines


def _now_ts() -> float:
    return time.time()


def _format_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _get_int(key: str, default: int = 0) -> int:
    r = _get_redis()
    try:
        val = r.get(key)
        return int(val) if val is not None else default
    except Exception:
        return default


def _get_state() -> str:
    r = _get_redis()
    try:
        return (r.get(_REDIS_KEY_STATE) or "up").strip().lower()
    except Exception:
        return "up"


def _get_last_alert_ts() -> float:
    r = _get_redis()
    try:
        val = r.get(_REDIS_KEY_LAST_ALERT)
        return float(val) if val is not None else 0.0
    except Exception:
        return 0.0


def _set_health_state(*, miss_count: int, state: str, last_alert_ts: float | None = None) -> None:
    r = _get_redis()
    pipe = r.pipeline(transaction=False)
    pipe.set(_REDIS_KEY_MISS, miss_count)
    pipe.set(_REDIS_KEY_STATE, state)
    if last_alert_ts is not None:
        pipe.set(_REDIS_KEY_LAST_ALERT, last_alert_ts)
    pipe.execute()


@app.task(bind=True, name="check_hk_worker_health")
def check_hk_worker_health_task(self) -> Dict[str, Any]:
    """
    探测 HK YouTube Worker 是否在线。

    默认每 10 分钟由 Celery Beat 触发；也可手动：
    check_hk_worker_health_task.delay()
    """
    webhook = os.getenv("LARK_WEBHOOK_URL", "").strip()
    if not webhook:
        logger.debug("HK health check: LARK_WEBHOOK_URL 为空，跳过 Lark 推送")

    timeout = float(os.getenv("HK_HEALTH_PING_TIMEOUT_SEC", "5"))
    miss_threshold = int(os.getenv("HK_HEALTH_MISS_THRESHOLD", "2"))
    cooldown_min = int(os.getenv("HK_HEALTH_ALERT_COOLDOWN_MIN", "60"))

    expected = _parse_expected_workers()
    online = _ping_workers(timeout)
    missing = [w for w in expected if w not in online]
    all_online = len(missing) == 0

    prev_state = _get_state()
    miss_count = _get_int(_REDIS_KEY_MISS, 0)
    last_alert_ts = _get_last_alert_ts()
    now = _now_ts()
    alerted = False
    recovered = False

    if all_online:
        if prev_state == "down":
            recovered = True
            if webhook:
                send_text(
                    "✅ HK YouTube Worker 已恢复",
                    [
                        f"时间: {_format_utc_now()}",
                        f"期望 worker: {', '.join(expected)}",
                        "状态: 全部在线",
                    ],
                )
        _set_health_state(miss_count=0, state="up")
        result = {
            "ok": True,
            "status": "up",
            "expected": expected,
            "online_count": len(online),
            "recovered": recovered,
        }
        logger.info("HK health check: 全部在线 expected=%s", expected)
        return result

    miss_count += 1
    should_alert = miss_count >= miss_threshold
    cooldown_elapsed = (now - last_alert_ts) >= (cooldown_min * 60)

    if should_alert and (prev_state != "down" or cooldown_elapsed):
        queue_lines = _fetch_youtube_queue_depths()
        alert_lines = [
            f"时间: {_format_utc_now()}",
            f"连续未命中: {miss_count} 次 (阈值 {miss_threshold})",
            f"缺失 worker: {', '.join(missing)}",
            f"期望 worker: {', '.join(expected)}",
            f"当前在线 ({len(online)}): {', '.join(sorted(online)) or '(无)'}",
            "",
            "YouTube 队列积压:",
        ]
        alert_lines.extend(queue_lines or ["  (无积压或读取失败)"])
        alert_lines.extend(
            [
                "",
                "建议: 登录 HK VM 执行 systemctl status yt-worker",
            ]
        )
        title = (
            "🔴 HK YouTube Worker 离线"
            if prev_state != "down"
            else "🔴 HK YouTube Worker 仍离线 (提醒)"
        )
        if webhook:
            send_text(title, alert_lines)
            alerted = True
        _set_health_state(miss_count=miss_count, state="down", last_alert_ts=now)
        logger.warning(
            "HK health alert: missing=%s miss_count=%s",
            missing,
            miss_count,
        )
    else:
        _set_health_state(miss_count=miss_count, state=prev_state if prev_state == "down" else "up")

    return {
        "ok": False,
        "status": "down" if miss_count >= miss_threshold else "degraded",
        "expected": expected,
        "missing": missing,
        "miss_count": miss_count,
        "miss_threshold": miss_threshold,
        "alerted": alerted,
    }
