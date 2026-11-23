# -*- coding: utf-8 -*-
"""Morning Brief 涨跌榜 K 线取数测试 —— Yahoo bar 自洽校验

2026-11-18 实测:Yahoo 当日未收盘 bar 的 open 回填成数日前旧值
(300308 拿到 11-13 开盘价 520,高于当日最高 404),open>high 的
物理不可能蜡烛必须整根丢弃,不能进弹窗走势图。
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

from backend.services.morning_brief_report_service import MorningBriefReportService

# 三个连续交易日,数值取自 300308 2026-11 真实行情;时间戳由本地日期
# 动态生成(当日 15:00),与生产代码 datetime.fromtimestamp().date() 往返一致
def _ts(day):
    return int(datetime(2026, 11, day, 15, 0).timestamp())


TS_1116, TS_1117, TS_1118 = _ts(16), _ts(17), _ts(18)
D_1116 = datetime.fromtimestamp(TS_1116).date().isoformat()
D_1117 = datetime.fromtimestamp(TS_1117).date().isoformat()


def _chart_response(timestamps, o, h, lo, c):
    body = {"chart": {"result": [{
        "timestamp": list(timestamps),
        "indicators": {"quote": [{
            "open": list(o), "high": list(h), "low": list(lo), "close": list(c)}]},
    }]}}
    resp = MagicMock()
    resp.json.return_value = body
    return resp


def _bar_dates(bars):
    return [b[0] for b in bars]


def test_drops_inconsistent_latest_bar():
    """open>high 的未收盘 bar(520>404)整根丢弃,保留历史有效 bar。"""
    resp = _chart_response(
        [TS_1116, TS_1117, TS_1118],
        [450.0, 398.88, 520.0],      # 最后一根 open 脏值 = 11-13 开盘价
        [455.0, 410.96, 404.67],
        [406.0, 383.0, 385.72],
        [408.97, 393.0, 393.89],
    )
    with patch("requests.get", return_value=resp):
        d = MorningBriefReportService._yahoo_daily_ohlc("300308.SZ")
    assert d is not None
    assert _bar_dates(d["h"]) == [
        datetime.fromtimestamp(TS_1116).date().isoformat(),
        datetime.fromtimestamp(TS_1117).date().isoformat(),
    ]
    assert d["a"] == d["h"][-1][0]           # 最新日回退到最后一根有效 bar
    assert d["h"][-1] == [D_1117, 398.88, 410.96, 383.0, 393.0]


def test_keeps_valid_bars_untouched():
    """自洽 bar(含 open==low 的涨跌停式一字 bar)原样保留。"""
    resp = _chart_response(
        [TS_1116, TS_1117],
        [450.0, 398.88], [455.0, 410.96], [406.0, 383.0], [408.97, 393.0])
    with patch("requests.get", return_value=resp):
        d = MorningBriefReportService._yahoo_daily_ohlc("300308.SZ")
    assert d["h"] == [
        [D_1116, 450.0, 455.0, 406.0, 408.97],
        [D_1117, 398.88, 410.96, 383.0, 393.0],
    ]


def test_zero_price_and_all_bad_return_none():
    """零价 bar / 全部不自洽 → None(前端显示"走势数据暂不可用")。"""
    zero = _chart_response([TS_1116], [0.0], [0.0], [0.0], [0.0])
    with patch("requests.get", return_value=zero):
        assert MorningBriefReportService._yahoo_daily_ohlc("X.SZ") is None
    bad = _chart_response(
        [TS_1116, TS_1117], [450.0, 209.96], [440.0, 163.35],
        [406.0, 155.3], [408.97, 158.53])
    with patch("requests.get", return_value=bad):
        assert MorningBriefReportService._yahoo_daily_ohlc("300502.SZ") is None


# ── 截止日(2026-11-23):弹窗 K 线与榜单行止于同一交易日 ──────────────────
# 「当日」= 报告日 D(北京,= T+1):美股行/弹窗止于美股交易日 T,港股/A股止于 D。
# 此前弹窗每次点击实时拉最新日线、不设截止,旧报告里图比行多出后面几个交易日。


def test_until_cut_drops_later_bars():
    """until(榜单行截止日)之后的 bar 丢弃。"""
    resp = _chart_response(
        [TS_1116, TS_1117, TS_1118],
        [450.0, 398.88, 395.0], [455.0, 410.96, 399.0],
        [406.0, 383.0, 380.0], [408.97, 393.0, 393.89])
    with patch("requests.get", return_value=resp):
        d = MorningBriefReportService._yahoo_daily_ohlc("300308.SZ", until=D_1117)
    assert _bar_dates(d["h"]) == [D_1116, D_1117]
    assert d["a"] == D_1117           # 最新日回退到截止日内最后一根


def _svc_with_metadata(meta):
    import backend.services.morning_brief_report_service as mod

    cursor = MagicMock()
    cursor.fetchone.return_value = (meta,)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    db = MagicMock()
    db.get_connection.return_value = conn
    with patch.object(mod, "DatabaseManager", return_value=db):
        return mod.MorningBriefReportService()


def _capture_cuts(seen):
    import backend.services.morning_brief_report_service as mod

    mod._kline_cache.clear()
    return patch.object(
        MorningBriefReportService, "_yahoo_daily_ohlc",
        side_effect=lambda sym, until=None: seen.append((sym, until))
        or {"h": [], "a": None})


def test_kline_cut_per_market_from_rank_cut():
    """新报告:美股截至 T,港股/A股截至报告日 D(= T+1)。"""
    meta = {"session_date": "2026-11-19",
            "rank_kline": {"us": ["NVDA"], "hk": ["0700.HK"], "cn": ["000001.SZ"]},
            "rank_cut": {"us": "2026-11-19", "hk": "2026-11-20", "cn": "2026-11-20"}}
    seen = []
    with _capture_cuts(seen):
        svc = _svc_with_metadata(meta)
        for market in ("us", "hk", "cn"):
            svc.get_rank_kline_by_token("tok", market, 0)
    assert seen == [("NVDA", "2026-11-19"), ("0700.HK", "2026-11-20"),
                    ("000001.SZ", "2026-11-20")]


def test_kline_cut_falls_back_to_session_date_for_old_reports():
    """没有 rank_cut 的旧报告:按美股交易日截断(旧行本就冻结在 T)。"""
    meta = {"session_date": "2026-11-19", "rank_kline": {"hk": ["0700.HK"]}}
    seen = []
    with _capture_cuts(seen):
        _svc_with_metadata(meta).get_rank_kline_by_token("tok", "hk", 0)
    assert seen == [("0700.HK", "2026-11-19")]


def test_kline_cache_keyed_by_cut():
    """同一 symbol 不同截止日不共用缓存:换截止日必须回源,同截止日命中。"""
    seen = []
    with _capture_cuts(seen):
        old = {"session_date": "2026-11-19", "rank_kline": {"hk": ["0700.HK"]}}
        _svc_with_metadata(old).get_rank_kline_by_token("tok", "hk", 0)
        _svc_with_metadata(old).get_rank_kline_by_token("tok", "hk", 0)
        new = dict(old, rank_cut={"hk": "2026-11-20"})
        _svc_with_metadata(new).get_rank_kline_by_token("tok", "hk", 0)
    assert seen == [("0700.HK", "2026-11-19"), ("0700.HK", "2026-11-20")]
