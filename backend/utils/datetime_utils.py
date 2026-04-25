# -*- coding: utf-8 -*-
"""时区相关统一工具.

设计契约（系统级）:
  1. **存储**: 数据库所有 timestamp 列 (含 naive `timestamp without time zone`) 一律存 UTC.
     - 通过 `db_manager.get_connection()` 在每条连接上设置 `SET TIME ZONE 'UTC'` 保证
       `NOW()` / 默认值 / Python naive datetime 进库后的解释口径一致.
  2. **传输**: 后端 API 输出 datetime 时统一带 UTC tzinfo, Pydantic 序列化为 ISO 8601 (`...Z`).
  3. **展示**: 前端按用户浏览器或偏好时区渲染 (`web-frontend/src/utils/datetime.ts`).
  4. **"今天 / 昨天" 计算**: 永远基于"用户时区", 不能用 `date.today()` (取服务器本地).
     服务器实际部署在香港 (UTC+8) — 若用 `date.today()`, 美国用户会看到错位 1 天.

本文件提供 4 类工具:
  - `utcnow()` / `utc_today()` — 替代危险的 `datetime.now()` / `date.today()`
  - `ensure_utc(dt)` — 把任何来源的 datetime 规范为 UTC-aware
  - `to_naive_utc(dt)` — 写入 naive 列前的最后一道保险
  - `today_in_tz(tz)` / `yesterday_in_tz(tz)` — 按用户时区算"自然日"
  - `parse_user_tz(tz_str)` — 解析前端传入的 IANA 时区, 非法时回退 UTC

参考: `docs/instructions/DAILY_MODULE.md` 的 "时区契约" 一节.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

UTC = timezone.utc


# ─── now / today ────────────────────────────────────────────────────────


def utcnow() -> datetime:
    """带 UTC tzinfo 的当前时间. 替代 `datetime.utcnow()` (3.12 已 deprecated) 与
    `datetime.now()` (取服务器本地, 部署在 HK = UTC+8 → 写库会偏移 8h).
    """
    return datetime.now(UTC)


def utcnow_naive() -> datetime:
    """UTC 时间, 但 tzinfo 剥离 — 用于需要写入 `timestamp without time zone` 列且
    不希望 driver 自动转换的场景. 与 `utcnow()` 在数值上完全相等.
    """
    return datetime.now(UTC).replace(tzinfo=None)


def utc_today() -> date:
    """UTC 当前自然日 (date)."""
    return datetime.now(UTC).date()


def utc_yesterday() -> date:
    return utc_today() - timedelta(days=1)


# ─── tz 规范化 ──────────────────────────────────────────────────────────


def ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """把任意 datetime 规范为 UTC-aware.

    - None → None
    - naive → 视为 UTC (与全系统契约一致), 加 tz
    - aware → 转换到 UTC

    Daily 模块所有从 DB 读出的 naive datetime 都应过这道.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def to_naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """把任意 datetime 转为 naive UTC, 用于写入 `timestamp without time zone` 列.

    与 `ensure_utc` 配对: ensure_utc 用于读, to_naive_utc 用于写.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(UTC).replace(tzinfo=None)


# ─── 用户时区 (per-request) ─────────────────────────────────────────────


def parse_user_tz(tz_str: Optional[str], default: str = "UTC") -> ZoneInfo:
    """解析前端传入的 IANA 时区字符串 (e.g. 'Asia/Shanghai', 'America/New_York').

    非法 / 空 → 回退 default (默认 UTC). 不抛异常, 失败要静默以保证 endpoint 鲁棒.
    """
    if not tz_str:
        return ZoneInfo(default)
    try:
        return ZoneInfo(tz_str)
    except Exception:
        return ZoneInfo(default)


def today_in_tz(tz_str: Optional[str]) -> date:
    """指定时区的"今天" (自然日). 用于计算 Daily Report 的 report_date."""
    tz = parse_user_tz(tz_str)
    return datetime.now(tz).date()


def yesterday_in_tz(tz_str: Optional[str]) -> date:
    """指定时区的"昨天" — Daily Report 的常用默认值.

    示例:
      - 北京时间 2026-04-25 02:00 调用 yesterday_in_tz('Asia/Shanghai') → 2026-04-24
      - 同一时刻 (= 美西 2026-04-24 11:00) 调用 yesterday_in_tz('America/Los_Angeles') → 2026-04-23

    若不指定 tz, 回退 UTC (避免依赖服务器本地 TZ).
    """
    return today_in_tz(tz_str) - timedelta(days=1)


def day_window_utc(target_date: date, tz_str: Optional[str]) -> tuple[datetime, datetime]:
    """把"指定时区的某一自然日"换算为 UTC 起止区间 [start, end), naive.

    用于 SQL: `WHERE published_at >= %s AND published_at < %s`
    (published_at 列存的就是 UTC naive).

    示例:
      day_window_utc(date(2026,4,25), 'Asia/Shanghai')
        → (2026-04-24 16:00:00, 2026-04-25 16:00:00)  # 北京 4-25 = UTC 4-24 16:00 ~ 4-25 16:00
    """
    tz = parse_user_tz(tz_str)
    start_local = datetime.combine(target_date, datetime.min.time(), tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return (
        start_local.astimezone(UTC).replace(tzinfo=None),
        end_local.astimezone(UTC).replace(tzinfo=None),
    )


__all__ = [
    "UTC",
    "utcnow",
    "utcnow_naive",
    "utc_today",
    "utc_yesterday",
    "ensure_utc",
    "to_naive_utc",
    "parse_user_tz",
    "today_in_tz",
    "yesterday_in_tz",
    "day_window_utc",
]
