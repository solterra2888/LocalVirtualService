# -*- coding: utf-8 -*-
"""应有交易日、抓取成败和外汇未完成 K 线。"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

from backend.services.market_session import (
    build_fx_provisional_bar,
    evaluate_fetch_health,
    expected_session_date,
)

HKT = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 10, 8, 6, 0, tzinfo=HKT)


def test_expected_dates_at_beijing_0600_during_national_day_reopen():
    assert expected_session_date("GSPC.INDX", NOW) == date(2026, 10, 7)
    assert expected_session_date("RUT.INDX", NOW) == date(2026, 10, 7)
    assert expected_session_date("VIX.INDX", NOW) == date(2026, 10, 7)
    assert expected_session_date("SSE50.INDX", NOW) == date(2026, 9, 30)
    assert expected_session_date("CSI300.INDX", NOW) == date(2026, 9, 30)
    assert expected_session_date("GBPUSD.FOREX", NOW) == date(2026, 10, 7)
    assert expected_session_date("EURUSD.FOREX", NOW) == date(2026, 10, 7)
    assert expected_session_date("N225.INDX", NOW) == date(2026, 10, 7)
    assert expected_session_date("GDAXI.INDX", NOW) == date(2026, 10, 7)
    assert expected_session_date("FTSE.INDX", NOW) == date(2026, 10, 7)


def test_china_expected_date_after_the_close():
    later = datetime(2026, 10, 8, 19, 0, tzinfo=HKT)
    assert expected_session_date("SSE50.INDX", later) == date(2026, 10, 8)


def test_us_observed_holiday_rolls_back():
    # 2026-07-04 是周六，独立日休市提前到 07-03。
    holiday_morning = datetime(2026, 7, 4, 6, 0, tzinfo=HKT)
    assert expected_session_date("GSPC.INDX", holiday_morning) == date(2026, 7, 2)


def test_auth_failure_fails_the_fetch_even_when_other_assets_succeed():
    assets = [("spy", "SPY.US"), ("gbp", "GBPUSD.FOREX")]
    health = evaluate_fetch_health(
        assets,
        eod_outcomes={
            "SPY.US": {"ok": True, "empty": False, "http_status": 200, "error": None},
            "GBPUSD.FOREX": {"ok": False, "empty": False, "http_status": 401, "error": "unauth"},
        },
        realtime_outcomes={},
        latest_dates={"SPY.US": date(2026, 10, 7), "GBPUSD.FOREX": date(2026, 10, 6)},
        now=NOW,
    )
    assert health["success"] is False
    assert "GBPUSD.FOREX" in health["auth_codes"]


def test_closed_china_window_is_not_a_failure():
    health = evaluate_fetch_health(
        [("sse", "SSE50.INDX")],
        eod_outcomes={
            "SSE50.INDX": {"ok": False, "empty": True, "http_status": 200, "error": None},
        },
        realtime_outcomes={},
        latest_dates={"SSE50.INDX": date(2026, 9, 30)},
        now=NOW,
    )
    assert health["success"] is True
    assert health["failed_codes"] == []
    assert health["closed_codes"] == ["SSE50.INDX"]


def test_stale_us_index_fails_even_below_the_ratio_limit():
    health = evaluate_fetch_health(
        [("spx", "GSPC.INDX"), ("spy", "SPY.US")],
        eod_outcomes={
            "GSPC.INDX": {"ok": True, "empty": False, "http_status": 200, "error": None},
            "SPY.US": {"ok": True, "empty": False, "http_status": 200, "error": None},
        },
        realtime_outcomes={},
        latest_dates={"GSPC.INDX": date(2026, 10, 6), "SPY.US": date(2026, 10, 7)},
        now=NOW,
    )
    assert health["success"] is False
    assert health["stale_critical"] == ["GSPC.INDX"]


def test_small_failure_ratio_still_succeeds():
    assets = [(f"n{i}", f"C{i}.US") for i in range(10)]
    outcomes = {
        f"C{i}.US": {"ok": i != 0, "empty": False, "http_status": 200 if i else 500, "error": None if i else "boom"}
        for i in range(10)
    }
    latest = {f"C{i}.US": date(2026, 10, 7) for i in range(10)}
    health = evaluate_fetch_health(
        assets, outcomes, {}, latest, NOW,
    )
    assert health["success"] is True


def test_large_failure_ratio_fails():
    assets = [(f"n{i}", f"C{i}.US") for i in range(4)]
    outcomes = {
        f"C{i}.US": {"ok": False, "empty": False, "http_status": 500, "error": "boom"}
        for i in range(4)
    }
    health = evaluate_fetch_health(assets, outcomes, {}, {}, NOW)
    assert health["success"] is False


def test_fx_provisional_bar_uses_a_quote_from_the_new_session():
    bars = [{"time": "2026-10-06", "open": 1.32, "high": 1.33, "low": 1.31, "close": 1.32666, "volume": 0}]
    quote = {
        "open": 1.3266,
        "high": 1.328,
        "low": 1.321,
        "close": 1.3217,
        "previous_close": 1.32666,
        "volume": 0,
        "data_datetime": NOW,
    }
    bar = build_fx_provisional_bar("GBPUSD.FOREX", bars, quote, NOW)
    assert bar is not None
    assert bar["time"] == "2026-10-07"
    assert bar["provisional"] is True
    assert bar["close"] == 1.3217


def test_fx_provisional_bar_skips_a_quote_from_the_previous_session():
    bars = [{"time": "2026-10-06", "close": 1.32666}]
    # 10 月 7 日 19:00 北京时间仍是美东上午，外汇日还没换到 10 月 7 日。
    quote = {
        "close": 1.33,
        "data_datetime": datetime(2026, 10, 7, 19, 0, tzinfo=HKT),
    }
    assert build_fx_provisional_bar("EURUSD.FOREX", bars, quote, NOW) is None


def test_fx_provisional_bar_skips_when_official_bar_is_already_current():
    bars = [{"time": "2026-10-07", "close": 1.32}]
    quote = {"close": 1.33, "data_datetime": NOW}
    assert build_fx_provisional_bar("USDJPY.FOREX", bars, quote, NOW) is None
