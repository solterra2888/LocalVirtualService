# -*- coding: utf-8 -*-
"""各市场的应有交易日。

Daily Market Report 在北京时间 06:00 生成。同一个钟点，美股已经收盘，
A 股还没开盘，外汇日线则按美东 17:00 换日。抓取任务和温度卡片共用这里的日期，
避免把休市、未开盘和数据源故障看成同一件事。
"""

from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Dict, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

US_CASH_INDEX_CODES = frozenset({
    "GSPC.INDX",
    "IXIC.INDX",
    "NDX.INDX",
    "SOX.INDX",
    "RUT.INDX",
})

# 温度卡片里的外汇。官方日线经常晚于实时价发布，允许用实时价补一根未完成 K 线。
FX_CODES = frozenset({
    "USDJPY.FOREX",
    "USDCNH.FOREX",
    "GBPUSD.FOREX",
    "EURUSD.FOREX",
})

# 失败品种占比超过该值时，整次抓取记为失败。
FAILURE_RATIO_LIMIT = 0.20
AUTH_STATUSES = frozenset({401, 403})

# 上交所 2026 年已公告的休市。来源：上证公告〔2026〕22 号
# （中秋 9 月 25–27 日，国庆 10 月 1–7 日）。其余年份的临时休市需要在这里补。
_SSE_CLOSURES = set()


def _add_closure_range(start: date, end: date) -> None:
    cursor = start
    while cursor <= end:
        _SSE_CLOSURES.add(cursor)
        cursor += timedelta(days=1)


_add_closure_range(date(2026, 9, 25), date(2026, 9, 27))
_add_closure_range(date(2026, 10, 1), date(2026, 10, 7))

_EXPLICIT_SESSIONS = {
    "SSE50.INDX": "cn",
    "CSI300.INDX": "cn",
    "CSI500.INDX": "cn",
    "CSI1000.INDX": "cn",
    "N225.INDX": "jp",
    "KS11.INDX": "kr",
    "FTSE.INDX": "uk",
    "GDAXI.INDX": "de",
    "2800.HK": "hk",
}

_HOLIDAY_CACHE: Dict[Tuple[str, int], frozenset] = {}


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    shift = (weekday - first.weekday()) % 7
    return first + timedelta(days=shift + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        last = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    shift = (last.weekday() - weekday) % 7
    return last - timedelta(days=shift)


def _easter_sunday(year: int) -> date:
    """Anonymous Gregorian algorithm."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    month = (h + ell - 7 * m + 114) // 31
    day = ((h + ell - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _observe_us(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _nyse_holidays(year: int) -> frozenset:
    key = ("nyse", year)
    cached = _HOLIDAY_CACHE.get(key)
    if cached is not None:
        return cached
    holidays = frozenset({
        _observe_us(date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter_sunday(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observe_us(date(year, 6, 19)),
        _observe_us(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observe_us(date(year, 12, 25)),
    })
    _HOLIDAY_CACHE[key] = holidays
    return holidays


def _is_weekday(day: date) -> bool:
    return day.weekday() < 5


def _is_nyse(day: date) -> bool:
    return _is_weekday(day) and day not in _nyse_holidays(day.year)


def _is_sse(day: date) -> bool:
    return _is_weekday(day) and day not in _SSE_CLOSURES


def session_for_code(code: str) -> str:
    explicit = _EXPLICIT_SESSIONS.get(code)
    if explicit:
        return explicit
    if code.endswith(".FOREX"):
        return "fx"
    if code.endswith(".HK"):
        return "hk"
    if code.endswith(".COMMODITY"):
        return "us_fut"
    return "us"


# timezone, official close, trading-day predicate
_SESSIONS: Dict[str, Tuple[str, time, Callable[[date], bool]]] = {
    "us": ("America/New_York", time(16, 0), _is_nyse),
    "us_fut": ("America/New_York", time(17, 0), _is_nyse),
    "cn": ("Asia/Shanghai", time(15, 0), _is_sse),
    "jp": ("Asia/Tokyo", time(15, 0), _is_weekday),
    "kr": ("Asia/Seoul", time(15, 30), _is_weekday),
    "uk": ("Europe/London", time(16, 30), _is_weekday),
    "de": ("Europe/Berlin", time(17, 30), _is_weekday),
    "fx": ("America/New_York", time(17, 0), _is_weekday),
    "hk": ("Asia/Hong_Kong", time(16, 0), _is_weekday),
}


def _aware(now: datetime) -> datetime:
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now


def _previous_trading_day(day: date, is_trading: Callable[[date], bool]) -> date:
    cursor = day - timedelta(days=1)
    while not is_trading(cursor):
        cursor -= timedelta(days=1)
    return cursor


def expected_session_date(code: str, now: datetime) -> date:
    """返回 now 这一刻已经收盘的最近交易日。"""
    tz_name, close_at, is_trading = _SESSIONS[session_for_code(code)]
    local = _aware(now).astimezone(ZoneInfo(tz_name))
    if is_trading(local.date()) and local.time() >= close_at:
        return local.date()
    return _previous_trading_day(local.date(), is_trading)


def _as_date(value: Any) -> Optional[date]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def evaluate_fetch_health(
    assets: Sequence[Tuple[str, str]],
    eod_outcomes: Dict[str, Dict[str, Any]],
    realtime_outcomes: Dict[str, Dict[str, Any]],
    latest_dates: Dict[str, Any],
    now: datetime,
    store_failed: int = 0,
    check_eod: bool = True,
) -> Dict[str, Any]:
    """判断一次抓取能不能算成功。

    休市窗口内没有新 K 线、但库里的最后日期已经覆盖应有交易日：算成功。
    401/403、失败占比超过 20%、或温度卡片里的美股指数缺了应有交易日：算失败。
    """
    codes = []
    seen = set()
    for _name, code in assets:
        if code not in seen:
            seen.add(code)
            codes.append(code)

    failed = set()
    auth = set()
    closed = set()
    stale_critical = set()

    for code, outcome in realtime_outcomes.items():
        if outcome.get("http_status") in AUTH_STATUSES:
            auth.add(code)

    if check_eod:
        for code in codes:
            outcome = eod_outcomes.get(code) or {
                "ok": False,
                "empty": False,
                "http_status": None,
                "error": "missing",
            }
            status = outcome.get("http_status")
            if status in AUTH_STATUSES:
                auth.add(code)
                failed.add(code)
                if code in US_CASH_INDEX_CODES:
                    stale_critical.add(code)
                continue

            latest = _as_date(latest_dates.get(code))
            expected = expected_session_date(code, now)
            fresh = latest is not None and latest >= expected

            if outcome.get("ok"):
                if code in US_CASH_INDEX_CODES and not fresh:
                    failed.add(code)
                    stale_critical.add(code)
                continue

            if outcome.get("empty") and not outcome.get("error") and fresh:
                closed.add(code)
                continue

            failed.add(code)
            if code in US_CASH_INDEX_CODES and not fresh:
                stale_critical.add(code)

    total = len(codes)
    ratio = (len(failed) / total) if total else 0.0
    success = (
        not auth
        and not stale_critical
        and store_failed <= 0
        and ratio <= FAILURE_RATIO_LIMIT
    )

    reasons = []
    if auth:
        shown = ", ".join(sorted(auth)[:8])
        reasons.append(f"认证失败（{shown}）")
    if stale_critical:
        reasons.append(
            "美国指数缺少应有交易日（" + ", ".join(sorted(stale_critical)) + "）"
        )
    if store_failed > 0:
        reasons.append(f"入库失败 {store_failed} 条")
    if ratio > FAILURE_RATIO_LIMIT:
        reasons.append(f"失败占比 {ratio:.0%}（{len(failed)}/{total}）")

    return {
        "success": success,
        "reason": "；".join(reasons),
        "failed_codes": sorted(failed),
        "auth_codes": sorted(auth),
        "closed_codes": sorted(closed),
        "stale_critical": sorted(stale_critical),
        "failure_ratio": ratio,
        "store_failed": store_failed,
    }


def build_fx_provisional_bar(
    code: str,
    bars: Sequence[Dict[str, Any]],
    quote: Optional[Dict[str, Any]],
    now: datetime,
) -> Optional[Dict[str, Any]]:
    """官方外汇日线还没到应有交易日时，用实时价补一根未完成 K 线。

    不写入 market_data_eod。实时价本身还停在上一交易日时返回 None。
    """
    if code not in FX_CODES or not quote or quote.get("close") is None:
        return None

    expected = expected_session_date(code, now)
    last_time = bars[-1]["time"] if bars else None
    if last_time and str(last_time) >= expected.isoformat():
        return None

    quoted_at = quote.get("data_datetime")
    if quoted_at is None:
        return None
    if not isinstance(quoted_at, datetime):
        quoted_at = datetime.fromisoformat(str(quoted_at))
    if expected_session_date(code, quoted_at) < expected:
        return None

    close = float(quote["close"])
    open_ = quote.get("open")
    if open_ is None:
        open_ = quote.get("previous_close")
    if open_ is None:
        open_ = close
    open_ = float(open_)
    high = float(quote["high"]) if quote.get("high") is not None else max(open_, close)
    low = float(quote["low"]) if quote.get("low") is not None else min(open_, close)
    volume = quote.get("volume") or 0
    return {
        "time": expected.isoformat(),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": int(volume),
        "provisional": True,
    }
