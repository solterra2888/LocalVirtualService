# -*- coding: utf-8 -*-
"""
LLMRateLimiter — Per-user × Per-provider 调用限速 (PR-F)

设计目标:
    避免某个用户的 Re-summarize / Re-tag 风暴打满团队共享的 LLM 配额. 5-8
    人内部团队场景下, 限速主要是"防误操作 + 防失控循环", 而不是商业意义上
    的 SLA 控制.

策略:
    - 实现: 固定 1 分钟窗口 (Redis INCR + EXPIRE), 简单可靠, 误差 ≤1 分钟
    - 拦截点: LLMService.chat_completion_with_template 主入口 (带 user_id 的调用)
    - 默认 limit: env LLM_USER_RATE_LIMIT_PER_MIN (default 30 calls/min/provider/user)
    - 系统调用 (user_id=None) 跳过限速, 避免影响 daily report 聚合等批处理流程

异常:
    - LLMRateLimitExceeded(retry_after_seconds, current, limit)
      调用方应转化为 HTTP 429 + 友好提示.

幂等性:
    - 即使 Redis 不可达也不应阻塞业务: log warning 并放行 (fail-open)
    - 这里限速是辅助保护, 而非核心契约
"""
from __future__ import annotations

import os
import time
from typing import Optional

from backend.utils.logger import get_logger

logger = get_logger("llm_rate_limiter")

# Redis 同步客户端: 操作 <1ms, 在 async 上下文里直接调用是可接受的
try:
    import redis  # type: ignore
except ImportError:  # pragma: no cover
    redis = None


class LLMRateLimitExceeded(Exception):
    """用户在某 provider 上的每分钟调用数超出限制时抛出."""

    def __init__(
        self,
        user_id: int,
        provider: str,
        current: int,
        limit: int,
        retry_after_seconds: int,
    ) -> None:
        self.user_id = user_id
        self.provider = provider
        self.current = current
        self.limit = limit
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            f"LLM rate limit exceeded for user {user_id} on provider '{provider}': "
            f"{current}/{limit} per minute. Retry in {retry_after_seconds}s."
        )


class LLMRateLimiter:
    """Per-user per-provider 固定 1 分钟窗口限速."""

    KEY_PREFIX = "llm:rl"
    DEFAULT_LIMIT_PER_MIN = 30
    KEY_TTL_SECONDS = 70  # 略大于 60s, 防止边界丢失

    _instance: Optional["LLMRateLimiter"] = None

    def __init__(self, redis_url: Optional[str] = None, limit_per_min: Optional[int] = None) -> None:
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.limit_per_min = int(
            limit_per_min
            if limit_per_min is not None
            else os.getenv("LLM_USER_RATE_LIMIT_PER_MIN", self.DEFAULT_LIMIT_PER_MIN)
        )
        self._client = None  # 懒初始化
        self._init_failed = False

    @classmethod
    def instance(cls) -> "LLMRateLimiter":
        """单例 — 复用 Redis 连接池."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _get_client(self):
        if self._init_failed or redis is None:
            return None
        if self._client is not None:
            return self._client
        try:
            self._client = redis.Redis.from_url(
                self.redis_url,
                socket_connect_timeout=2,
                socket_timeout=2,
                decode_responses=True,
            )
            # 简单 ping 确认可达
            self._client.ping()
            return self._client
        except Exception as e:
            logger.warning(f"LLMRateLimiter Redis init failed (fail-open): {e}")
            self._init_failed = True
            self._client = None
            return None

    def check_and_consume(
        self,
        user_id: Optional[int],
        provider: str,
        limit_per_min: Optional[int] = None,
    ) -> None:
        """检查 (user_id, provider) 当前分钟桶下的调用数; 超额抛 LLMRateLimitExceeded.

        Args:
            user_id   : 触发方用户 id; None 表示系统级调用, 直接放行
            provider  : LLM provider 名 (qwen / doubao / glm)
            limit_per_min: 本次调用单独的 limit 覆盖; None 走全局默认

        Behavior:
            - Redis 不可达 → 静默放行 (fail-open)
            - 当前桶 INCR 后值 > limit → 抛 LLMRateLimitExceeded
            - 否则正常返回
        """
        if not user_id:
            return  # 系统调用免限速

        if not provider:
            provider = "default"

        client = self._get_client()
        if client is None:
            return  # fail-open

        now = int(time.time())
        bucket = now // 60  # 当前 60 秒窗口
        key = f"{self.KEY_PREFIX}:{user_id}:{provider}:{bucket}"
        effective_limit = int(limit_per_min) if limit_per_min is not None else self.limit_per_min

        try:
            pipe = client.pipeline(transaction=False)
            pipe.incr(key, 1)
            pipe.expire(key, self.KEY_TTL_SECONDS)
            count, _ = pipe.execute()
        except Exception as e:
            logger.warning(f"LLMRateLimiter Redis op failed (fail-open): {e}")
            return

        try:
            count_int = int(count)
        except (TypeError, ValueError):
            return

        if count_int > effective_limit:
            retry_after = max(1, 60 - (now % 60))
            logger.warning(
                f"LLM rate limit triggered: user={user_id} provider={provider} "
                f"count={count_int} limit={effective_limit} retry_after={retry_after}s"
            )
            raise LLMRateLimitExceeded(
                user_id=user_id,
                provider=provider,
                current=count_int,
                limit=effective_limit,
                retry_after_seconds=retry_after,
            )

    def get_current_usage(self, user_id: int, provider: str) -> int:
        """获取当前分钟桶的使用量 (用于监控/前端展示, 不写入)."""
        client = self._get_client()
        if client is None or not user_id:
            return 0
        bucket = int(time.time()) // 60
        key = f"{self.KEY_PREFIX}:{user_id}:{provider}:{bucket}"
        try:
            v = client.get(key)
            return int(v) if v else 0
        except Exception:
            return 0
