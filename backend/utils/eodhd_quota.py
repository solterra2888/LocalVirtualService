# -*- coding: utf-8 -*-
"""
EODHD API 配额熔断（Redis 共享）与 fan-out 统计计数.

背景:
  EODHD 付费计划按 UTC 日结算配额（北京时间 08:00 重置），耗尽时所有端点
  返回 402 Payment Required。此前熔断标志（api_quota_exhausted）只存在于
  单个服务实例内存里，而 rotation/T2 fan-out 每天提交数千个独立任务、
  各自新建服务实例，实例级标志完全失效——配额耗尽后剩余任务仍逐个空打 API。

  2026-10 排查结论: 实时行情(全天候) + rotation(30k) + T2(8k) 日耗 ~124k
  超过 100k 限额, 每天凌晨出现 402 窗口。P0 已通过调度时段治理把日耗压到
  ~68k; 本模块是兜底: 任何调用方收到 402 → 写 Redis 熔断键, 所有 EODHD
  请求入口先查键, 命中直接快速失败, 不再消耗连接与时间。

熔断键:
  eodhd:quota:exhausted_until = 下个 UTC 零点(北京 08:00)的 unix 秒, 自带 TTL,
  到点自动失效; Redis 不可用时一切查询返回"未熔断", 不影响正常流程。

统计计数 (P1-2 可观测):
  eodhd:fund:stats:<UTC日期> hash: updated / quota_failed / failed —— 单票
  fundamentals 任务按结果分类累加, 保留 48h, 供观察 rotation/T2 真实成功率。
"""

import os
import time
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger("universal_glance")

_BREAKER_KEY = "eodhd:quota:exhausted_until"
_STATS_KEY_PREFIX = "eodhd:fund:stats:"
_STATS_TTL = 172800  # 48h

_client = None
_client_failed = False


class EODHDQuotaExhausted(Exception):
    """EODHD 日配额已耗尽（402）。消息含 API_QUOTA_EXHAUSTED 标记,
    兼容 eodhd_fundamentals_service.concurrent_fetch_companies_data 里
    按字符串识别配额异常的既有逻辑。"""

    def __init__(self, message: str = "API_QUOTA_EXHAUSTED: EODHD 日配额已耗尽 (402 Payment Required)"):
        super().__init__(message)


def _get_client():
    """惰性获取 Redis 客户端; 不可用时返回 None 且熔断功能退化为关闭."""
    global _client, _client_failed
    if _client is not None:
        return _client
    if _client_failed:
        return None
    try:
        import redis
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        client = redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
        client.ping()
        _client = client
        return _client
    except Exception as e:
        _client_failed = True
        logger.warning(f"EODHD 熔断: Redis 不可用, 熔断功能退化关闭: {e}")
        return None


def _next_utc_midnight_ts() -> float:
    """下个 UTC 零点 (= 北京时间 08:00, EODHD 配额重置时刻) 的 unix 秒."""
    now = datetime.now(timezone.utc)
    reset = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    return reset.timestamp()


def is_quota_exhausted() -> bool:
    """当前是否处于 402 熔断窗口. Redis 异常一律返回 False (不熔断)."""
    client = _get_client()
    if client is None:
        return False
    try:
        until = client.get(_BREAKER_KEY)
        return bool(until and float(until) > time.time())
    except Exception:
        return False


def trip_quota_breaker() -> float:
    """收到 402 后调用: 写入熔断键, 有效期至下个北京 08:00 (+60s 缓冲)."""
    reset_ts = _next_utc_midnight_ts()
    client = _get_client()
    if client is not None:
        try:
            ttl = max(int(reset_ts - time.time()) + 60, 60)
            client.set(_BREAKER_KEY, reset_ts, ex=ttl)
            logger.error(
                f"🚨 EODHD API 配额已耗尽 (402), 熔断至北京时间 "
                f"{datetime.fromtimestamp(reset_ts, timezone.utc).astimezone(timezone(timedelta(hours=8))):%H:%M}"
            )
        except Exception as e:
            logger.warning(f"EODHD 熔断: 写入熔断键失败: {e}")
    return reset_ts


def raise_if_quota_exhausted() -> None:
    """EODHD 请求入口统一调用; 熔断中则快速失败抛 EODHDQuotaExhausted."""
    if is_quota_exhausted():
        logger.warning("EODHD 配额熔断中, 跳过请求 (北京时间 08:00 重置)")
        raise EODHDQuotaExhausted()


def _stats_key() -> str:
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"{_STATS_KEY_PREFIX}{day}"


def incr_fund_stat(field: str, by: int = 1) -> None:
    """单票 fundamentals 任务结果计数: updated / quota_failed / failed."""
    client = _get_client()
    if client is None:
        return
    try:
        pipe = client.pipeline()
        pipe.hincrby(_stats_key(), field, by)
        pipe.expire(_stats_key(), _STATS_TTL)
        pipe.execute()
    except Exception as e:
        logger.debug(f"EODHD 统计计数失败(忽略): {e}")


def get_fund_stats(day_offset: int = 0) -> dict:
    """读取某 UTC 日(0=今天, -1=昨天)的统计; Redis 不可用返回空 dict."""
    client = _get_client()
    if client is None:
        return {}
    try:
        day = (datetime.now(timezone.utc) + timedelta(days=day_offset)).strftime("%Y%m%d")
        data = client.hgetall(f"{_STATS_KEY_PREFIX}{day}")
        if isinstance(data, dict):
            return {k.decode() if isinstance(k, bytes) else k:
                    int(v.decode() if isinstance(v, bytes) else v)
                    for k, v in data.items()}
        return {}
    except Exception:
        return {}
