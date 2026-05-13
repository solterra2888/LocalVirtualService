# -*- coding: utf-8 -*-
"""Per-PAT HTTP request rate limit (Redis INCR + 60s window, fail-open)."""

from __future__ import annotations

import os
import time
from typing import Optional

from backend.utils.logger import get_logger

logger = get_logger("pat_rate_limiter")

try:
    import redis  # type: ignore
except ImportError:  # pragma: no cover
    redis = None


class PATRateLimitExceeded(Exception):
    def __init__(
        self,
        pat_id: int,
        current: int,
        limit: int,
        retry_after_seconds: int,
    ) -> None:
        self.pat_id = pat_id
        self.current = current
        self.limit = limit
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            f"PAT rate limit exceeded for pat_id={pat_id}: {current}/{limit} per minute. "
            f"Retry in {retry_after_seconds}s."
        )


class PATRateLimiter:
    KEY_PREFIX = "agent:pat:rl"
    KEY_TTL_SECONDS = 70
    _ENV_KEY = "AGENT_PAT_RATE_LIMIT_PER_MIN"
    _instance: Optional["PATRateLimiter"] = None

    def __init__(self, redis_url: Optional[str] = None, limit_per_min: Optional[int] = None) -> None:
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.limit_per_min = self._parse_limit(limit_per_min)
        self._client = None
        self._init_failed = False

    @classmethod
    def _parse_limit(cls, override: Optional[int]) -> int:
        if override is not None:
            try:
                v = int(override)
                return v if v > 0 else 0
            except (TypeError, ValueError):
                return 0
        raw = os.getenv(cls._ENV_KEY, "60")
        try:
            v = int(raw)
            return v if v > 0 else 0
        except ValueError:
            logger.warning(f"Invalid {cls._ENV_KEY}='{raw}', PAT rate limit disabled")
            return 0

    @property
    def enabled(self) -> bool:
        return self.limit_per_min > 0

    @classmethod
    def instance(cls) -> "PATRateLimiter":
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
            self._client.ping()
            return self._client
        except Exception as e:
            logger.warning(f"PATRateLimiter Redis init failed (fail-open): {e}")
            self._init_failed = True
            self._client = None
            return None

    def check_and_consume(self, pat_id: Optional[int], limit_per_min: Optional[int] = None) -> None:
        if not pat_id:
            return
        effective = self._parse_limit(limit_per_min) if limit_per_min is not None else self.limit_per_min
        if effective <= 0:
            return
        client = self._get_client()
        if client is None:
            return
        now = int(time.time())
        bucket = now // 60
        key = f"{self.KEY_PREFIX}:{pat_id}:{bucket}"
        try:
            pipe = client.pipeline(transaction=False)
            pipe.incr(key, 1)
            pipe.expire(key, self.KEY_TTL_SECONDS)
            count, _ = pipe.execute()
            count_int = int(count)
        except Exception as e:
            logger.warning(f"PATRateLimiter Redis op failed (fail-open): {e}")
            return
        if count_int > effective:
            retry_after = max(1, 60 - (now % 60))
            raise PATRateLimitExceeded(
                pat_id=pat_id,
                current=count_int,
                limit=effective,
                retry_after_seconds=retry_after,
            )

    def get_headers(self, pat_id: Optional[int], limit_per_min: Optional[int] = None) -> dict:
        """Best-effort rate limit headers for responses."""
        effective = self._parse_limit(limit_per_min) if limit_per_min is not None else self.limit_per_min
        if not pat_id or effective <= 0:
            return {}
        client = self._get_client()
        if client is None:
            return {}
        now = int(time.time())
        bucket = now // 60
        key = f"{self.KEY_PREFIX}:{pat_id}:{bucket}"
        try:
            v = client.get(key)
            used = int(v) if v else 0
            remaining = max(0, effective - used)
            reset = bucket * 60 + 60
            return {
                "X-RateLimit-Limit": str(effective),
                "X-RateLimit-Remaining": str(remaining),
                "X-RateLimit-Reset": str(reset),
            }
        except Exception:
            return {}
