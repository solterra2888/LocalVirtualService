# -*- coding: utf-8 -*-
"""
轻量级数据库管理器 — 仅包含 Worker 所需的连接管理功能。

⚠️ 时区契约 (与主仓库 backend/utils/datetime_utils.py 对齐)
====================================================================
家用虚拟机 / 远程主服务器 / PostgreSQL 实例 全部部署在 HK (UTC+8).

PostgreSQL `subscription_content.fetched_at` / `published_at` 等列均为
`timestamp WITHOUT time zone`. 服务器实例的默认 `TimeZone` 为 `Asia/Shanghai`,
若不在每条连接上 `SET TIME ZONE 'UTC'`, 则:

    - `CURRENT_TIMESTAMP::timestamp` 会落入 HK 本地数值 (而非 UTC).
    - psycopg2 把 aware datetime (例如 dateutil 解析 `2026-04-25T10:01:18Z`)
      传入 timestamp without time zone 列时, postgres 会先用 session 时区
      把它换算成 HK 本地, 再丢掉 tzinfo —— 写入 `18:01` (HK) 而非 `10:01` (UTC).

这两条都会导致前端按 UTC 解读时整体偏移 +8 小时, 出现 "明天" 错觉.

修复策略 (双重保险):
  1. 连接层: `options='-c TimeZone=UTC'` 强制 session 时区 = UTC.
  2. 应用层: 注册 psycopg2 adapter, 把所有 naive datetime 视为 HK 本地,
     转换为 UTC 之后再写入 (兜底任何遗漏 `datetime.now()` / 第三方库返回的
     naive 值).

详细背景:
  - docs/instructions/TIMEZONE_OPTIMIZATION.md (主仓库)
  - docs/instructions/DAILY_MODULE.md §13 "时区契约"
"""

import os
import time
import contextlib
import logging
from datetime import datetime, timezone, timedelta

import psycopg2
from psycopg2.extensions import adapt, AsIs, register_adapter
from psycopg2.extras import Json

logger = logging.getLogger(__name__)

# 服务器物理部署时区 (用于把"无 tz 的本地时间"还原为 UTC)
HK_TZ = timezone(timedelta(hours=8))


def _utc_datetime_adapter(dt: datetime):
    """
    psycopg2 适配器: 让所有 datetime 在写入数据库时都以 **UTC naive** 形式落库.

    规则:
      - aware datetime  → 转换到 UTC, 去掉 tzinfo, 写入.
      - naive datetime  → 视为 HK 本地时间 (服务器物理时区), 加上 HK tz 后转 UTC.

    与连接层 `SET TIMEZONE='UTC'` 配合使用, 确保 timestamp without time zone
    列里存的永远是 "UTC 时刻的数值".
    """
    if dt.tzinfo is not None:
        utc_dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    else:
        # naive: 假定是 HK 本地 (家用虚拟机的物理时钟)
        utc_dt = dt.replace(tzinfo=HK_TZ).astimezone(timezone.utc).replace(tzinfo=None)
    # 通过 ISO 字符串交给 psycopg2 默认机制
    return AsIs(f"'{utc_dt.isoformat()}'::timestamp")


def install_psycopg2_utc_adapter() -> None:
    """注册全局 datetime → UTC naive 适配器. 进程内只需调用一次."""
    register_adapter(datetime, _utc_datetime_adapter)
    logger.info("[timezone] psycopg2 UTC datetime adapter installed (HK→UTC)")


# 模块导入时立即注册, 保证 worker 进程任何 DB 写入都受适配器保护.
install_psycopg2_utc_adapter()


def get_db_config() -> dict:
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB_NAME", ""),
        "user": os.getenv("DB_USER", ""),
        "password": os.getenv("DB_PASSWORD", ""),
    }


class DB:
    """极简 PostgreSQL 连接管理 (强制 UTC session)."""

    def __init__(self, params: dict | None = None):
        self._params = params or get_db_config()

    def get_connection(self, max_retries=5, retry_delay=2):
        last_err = None
        for attempt in range(max_retries):
            try:
                return psycopg2.connect(
                    **self._params,
                    connect_timeout=30,
                    # 系统级时区契约: 全部 timestamp 列以 UTC 解释 / 写入.
                    # 服务器实际部署在 HK (UTC+8); 不显式 SET TIME ZONE 会导致
                    # CURRENT_TIMESTAMP 落入 HK 本地数值, 与远程主仓库前后端
                    # "naive=UTC" 的契约不一致, 导致 Daily/Feeds 时间偏移 +8h.
                    options="-c statement_timeout=600000 -c TimeZone=UTC",
                    keepalives_idle=30,
                    keepalives_interval=5,
                    keepalives_count=6,
                )
            except Exception as e:
                last_err = e
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 1.5, 10)
        raise ConnectionError(f"无法连接数据库: {last_err}")

    @contextlib.contextmanager
    def connection(self):
        conn = self.get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
