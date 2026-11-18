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
