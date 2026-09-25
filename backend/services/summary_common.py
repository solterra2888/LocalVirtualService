# -*- coding: utf-8 -*-
"""
摘要生成共享常量与工具

供 YouTube、Twitter、Zhishixingqiu 两阶段摘要服务以及
Bloomberg The Close、CNBC Closing Bell 单阶段服务使用。
"""

import asyncio
from datetime import date, datetime, timedelta
from typing import Any, Callable, Coroutine, Optional, TypeVar
from zoneinfo import ZoneInfo

from backend.utils.datetime_utils import UTC, utcnow

T = TypeVar("T")

# ── 两阶段批量 Phase A 调度参数 ────────────────────────────────────────────────

BATCH_CONCURRENT = 4
"""generate_all_* 中同时处理的最大 item 数（频道 / 作者 / 星球）。"""

PER_ITEM_TIMEOUT_SEC = 600
"""单 item 的最外层 asyncio.wait_for 超时（秒）。
从原 180s 提高至 600s，以容纳内部 chunk 并发执行的实际耗时。"""

# ── Phase A 内部 LLM chunk 参数 ────────────────────────────────────────────────

CHUNK_CONCURRENT = 3
"""同一 item 内部 LLM chunk 的最大并发数（Semaphore）。"""

CHUNK_LLM_TIMEOUT_SEC = 120
"""单个 LLM chunk 调用的超时（秒）。超时后最多重试 CHUNK_LLM_RETRY 次。"""

SUMMARY_LLM_READ_TIMEOUT_SEC = CHUNK_LLM_TIMEOUT_SEC
"""传给 llm_service 的 HTTP 读超时（秒）。
DB 中 provider 默认常为 30s，不足以处理长字幕 chunk；调用方应显式传入此值。"""

CHUNK_LLM_RETRY = 1
"""单个 LLM chunk 失败/超时后的最大重试次数。全部尝试失败后降级为 title-only 摘要。"""

# ── 输入预算 ────────────────────────────────────────────────────────────────────

MAX_VIDEO_INPUT_CHARS = 2000
"""YouTube Phase A 每个视频字幕的最大字符数（原 4000，减半以降低 LLM 输入量）。"""

# ── 单阶段服务参数 ─────────────────────────────────────────────────────────────

SINGLE_LLM_TIMEOUT_SEC = 600
"""The Close / CNBC 等单次大 LLM 调用的超时（秒）。"""

SINGLE_LLM_RETRY = 1
"""单阶段 LLM 调用失败后的最大重试次数。"""

# ── 孤儿记录清理 ───────────────────────────────────────────────────────────────

ORPHAN_THRESHOLD_MINUTES = 30
"""超过该阈值仍处于 'generating' 状态的记录视为孤儿，重置为 'failed'。"""


# ── 工具函数 ───────────────────────────────────────────────────────────────────

def should_defer_no_video_placeholder(
    report_date: date,
    now: Optional[datetime] = None,
    typical_hour_bj: int = 22,
    timezone_name: str = "Asia/Shanghai",
) -> bool:
    """交易日、且仍早于该节目的经验入库点时，记 episode_not_yet_published。

    The Close、Closing Bell、Surveillance 共用。各节目传入自己的 typical_hour_bj。
    """
    current = now or utcnow()
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    beijing = current.astimezone(ZoneInfo(timezone_name))
    return beijing.date() == report_date and beijing.hour < typical_hour_bj


def orphan_cutoff_dt() -> datetime:
    """返回孤儿记录的截止时间（UTC naive）。"""
    return datetime.utcnow() - timedelta(minutes=ORPHAN_THRESHOLD_MINUTES)


async def call_with_timeout_and_retry(
    coro_factory: Callable[[], Coroutine[Any, Any, T]],
    timeout_sec: float,
    retries: int,
    logger=None,
    label: str = "",
) -> tuple[T, bool]:
    """
    以超时 + 重试方式调用异步协程工厂。

    Args:
        coro_factory: 无参数可调用对象，每次调用返回一个新的协程。
        timeout_sec:  单次超时（秒）。
        retries:      最大重试次数（0 = 只尝试一次）。
        logger:       可选 logger，用于输出警告。
        label:        日志标识前缀。

    Returns:
        (result, degraded) — degraded=True 表示所有尝试均失败，result 来自调用方的降级逻辑。

    Raises:
        将所有异常重新抛给调用方以便其决定如何降级。
    """
    last_exc: BaseException | None = None
    for attempt in range(retries + 1):
        try:
            result = await asyncio.wait_for(coro_factory(), timeout=timeout_sec)
            return result, False
        except (asyncio.TimeoutError, Exception) as exc:
            last_exc = exc
            if logger and attempt < retries:
                logger.warning(
                    f"{label} attempt {attempt + 1}/{retries + 1} failed "
                    f"({type(exc).__name__}: {exc}), retrying"
                )
    raise last_exc  # type: ignore[misc]
