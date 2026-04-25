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
    """**[DEPRECATED, 不要在 DB 写入路径用]**

    曾用于 `INSERT ... VALUES (utcnow_naive())`. 现在 psycopg2 已注册全局适配器
    (见 install_psycopg2_utc_adapter), 适配器把 naive datetime 当作**HK 本地**
    再转 UTC. 而 utcnow_naive() 返回的是 naive **UTC**, 会被适配器再减 8h —
    存进 DB 比真实时间早 8h. 这正是 2026-04-25 重测时发现的 "刚上传文件
    Daily 显示比 Sources 早 8h" BUG 的根因.

    **替代方案** (任选其一):
      - `utcnow()` (aware UTC): 推荐, 语义最明确
      - `datetime.now()` (naive HK 本地, 服务器在 HK): 适配器会把它转成正确 UTC
      - SQL `NOW()` 默认值: 服务器连接已 SET TIMEZONE='UTC', 直接是 UTC
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
    """**[DEPRECATED, 不要在 DB 写入路径用]**

    曾经的设计是 "naive UTC 入库". 自 2026-04-25 起 psycopg2 已注册全局适配器,
    把所有 naive datetime 当 HK 本地处理. 因此 naive UTC 入库会被适配器再减 8h.
    详见 `utcnow_naive` 的 docstring.

    **替代方案**: 用 `ensure_utc(dt)` 拿到 aware UTC, 直接传给 psycopg2; 适配器
    会自动把 aware datetime 写为正确的 naive UTC.
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
    """把"指定时区的某一自然日"换算为 UTC 起止区间 [start, end), **aware UTC**.

    用于 SQL: `WHERE published_at >= %s AND published_at < %s`
    (published_at 列存的是 UTC naive, 但 psycopg2 适配器会把 aware UTC 正确序列化.)

    示例:
      day_window_utc(date(2026,4,25), 'Asia/Shanghai')
        → (2026-04-24 16:00:00+00:00, 2026-04-25 16:00:00+00:00)
        # 北京 4-25 = UTC 4-24 16:00 ~ 4-25 16:00

    注意: 返回 aware UTC (不是 naive). 否则会被 psycopg2 适配器误判 HK→UTC 再减 8h.
    """
    tz = parse_user_tz(tz_str)
    start_local = datetime.combine(target_date, datetime.min.time(), tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return (
        start_local.astimezone(UTC),
        end_local.astimezone(UTC),
    )


__all__ = [
    "UTC",
    "SERVER_TZ",
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
    "install_psycopg2_utc_adapter",
    "UtcDateTime",
]


# ─── 服务器物理时区 (用于安全网适配器) ───────────────────────────────
# 服务器实际部署在香港 (UTC+8). 所有遗留代码用 `datetime.now()` 拿到的是 HK 本地时间.
# 这个常量只用于一处: psycopg2 适配器把 naive datetime 视为 HK 本地, 转 UTC 后写库.
# 不要在业务代码里用它 — 业务代码请用 utcnow() (aware UTC) 显式.
# 注意: 适配器把 *任何* naive datetime 都当 HK, 所以 utcnow_naive() (naive UTC) 也会被误转!
# 因此 utcnow_naive() 已 DEPRECATED, 见其 docstring.
SERVER_TZ = ZoneInfo("Asia/Hong_Kong")


# ─── psycopg2 安全网: 任何 naive datetime 写入自动转 UTC ────────────


def install_psycopg2_utc_adapter() -> None:
    """注册 psycopg2 datetime 适配器, 全局拦截任何 naive datetime 写入.

    背景:
      项目内有 200+ 处历史代码用 `datetime.now()` 写 DB. 服务器在 HK (UTC+8),
      `datetime.now()` 返回 naive 本地时间 → 直接被写入 `timestamp without time zone`
      列 → 比 UTC 大 8h, 当前端按 UTC 解释这些值时偏移 8h.

    适配器行为:
      - 收到 **naive** datetime: 视为 HK 本地, 转 UTC, 以 naive UTC 字符串发往 PG
      - 收到 **aware** datetime: 转 UTC, 以 naive UTC 字符串发往 PG (与原 psycopg2 行为一致)

    效果:
      - 新写入永远是 UTC, 不依赖代码是否记得用 utcnow()
      - 历史 `datetime.now()` 调用无需逐个改, 都被自动校正
      - 对 timestamptz 列也 OK: PG 接收 naive UTC 字符串时按 session TimeZone 解释 (我们已设 UTC)

    注意:
      - 必须在 import psycopg2 之后、首次连接前调用 (在 main.py / celery worker 启动时)
      - 若 future 把列迁到 timestamptz, 此适配器仍正确工作
    """
    import psycopg2.extensions

    def _adapter(dt: datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=SERVER_TZ).astimezone(UTC).replace(tzinfo=None)
        else:
            dt = dt.astimezone(UTC).replace(tzinfo=None)
        # 输出 ISO 字符串, psycopg2 会包成 timestamp 字面量
        return psycopg2.extensions.AsIs(f"'{dt.isoformat()}'::timestamp")

    psycopg2.extensions.register_adapter(datetime, _adapter)


# ─── Pydantic 工具: 让所有 datetime 字段自动以 UTC tz 序列化 ─────────


def _serialize_utc(dt: Optional[datetime]) -> Optional[str]:
    """Pydantic field_serializer 帮手: naive → 视为 UTC 加 tz, aware → 转 UTC, 输出 ISO."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.isoformat()


# 用法 (Pydantic v2):
#   from backend.utils.datetime_utils import UtcDateTime
#   class MyResp(BaseModel):
#       created_at: UtcDateTime
# 即可自动以 UTC ISO 8601 (...+00:00) 序列化.
try:
    from typing import Annotated
    from pydantic import PlainSerializer

    UtcDateTime = Annotated[
        datetime,
        PlainSerializer(_serialize_utc, return_type=str, when_used="json"),
    ]
except Exception:  # pragma: no cover - pydantic 不可用时 fallback
    UtcDateTime = datetime  # type: ignore
