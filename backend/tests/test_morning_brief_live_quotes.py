# -*- coding: utf-8 -*-
"""Morning Brief 实时行情层单测 —— pv 锚与冻结层临门重拉

2026-11-20 事故背景:EODHD realtime 的 previousClose 早晨不滚动一天,
加密/FX 行的实时"当日"错算成两日涨跌(SOL 显示 -1.39% 而真实 -0.64%);
冻结层 08:15 行情门后无人取数,次主流币 bar 在冻结后 ~1h 才发布。
修复:① ft_live spec 预埋 pv(=T 收盘),_window_changes 锚定日 > T 时
优先作昨收;② us_close_chart 冻结前 _rescue_stale_rows 临门重拉。
"""

import sys
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tools" / "facts-report"))

from backend.services.morning_brief_report_service import (  # noqa: E402
    MorningBriefReportService,
)
import us_close_chart as u  # noqa: E402

T = date(2026, 11, 19)
T_NEXT = date(2026, 11, 20)


def _ts(d, h=6):
    return int(datetime(d.year, d.month, d.day, h, 0,
                        tzinfo=timezone.utc).timestamp())


# refs:pv = 61901.82(T 收盘);迷你序列升序,末位即 T 收盘
REFS_PV = {
    "pv": 61901.82,
    "s": [["2026-11-17", 64893.9453], ["2026-11-18", 62987.2656],
          ["2026-11-19", 61901.8203]],
}
# 无 pv 的旧 spec
REFS_OLD = {"s": REFS_PV["s"]}


class TestWindowChangesPv:
    def _wc(self, refs, q, unit="%", fb=T):
        return MorningBriefReportService._window_changes(refs, q, unit, fb)

    def test_pv_overrides_stale_previousclose(self):
        """锚定日 > T 且带 pv:用 pv 而非滞后的 previousClose(11-18 收盘)。"""
        q = {"last": 61988.15, "prev": 62987.2656, "ts": _ts(T_NEXT)}
        wc = self._wc(REFS_PV, q)
        # 61988.15/61901.82-1 = +0.14%(vs pv);若错用 previousClose 则 -1.59%
        assert wc["m1d"] == round((61988.15 / 61901.8203 - 1) * 100, 2)
        assert wc["m1d"] != round((61988.15 / 62987.2656 - 1) * 100, 2)

    def test_no_pv_falls_back_to_previousclose(self):
        """旧 spec(无 pv)维持原逻辑:当日 = last/previousClose。"""
        q = {"last": 61988.15, "prev": 62987.2656, "ts": _ts(T_NEXT)}
        wc = self._wc(REFS_OLD, q)
        assert wc["m1d"] == round((61988.15 / 62987.2656 - 1) * 100, 2)

    def test_anchor_equals_t_keeps_previousclose(self):
        """DB 降级行(锚定日 == T,最新收盘仍是 T):不用 pv,否则恒 0%。"""
        q = {"last": 61901.8203, "prev": 62987.2656, "d": T.isoformat()}
        wc = self._wc(REFS_PV, q)
        assert wc["m1d"] == round((61901.8203 / 62987.2656 - 1) * 100, 2)
        assert wc["m1d"] != 0.0

    def test_pv_bp_unit(self):
        """bp 口径(利率行):pv 锚按价差 ×100 出 bp。"""
        refs = {"pv": 4.50, "s": [["2026-11-18", 4.48], ["2026-11-19", 4.50]]}
        q = {"last": 4.55, "prev": 4.48, "ts": _ts(T_NEXT)}
        wc = self._wc(refs, q, unit="bp")
        assert wc["m1d"] == 5.0   # (4.55-4.50)*100

    def test_prev_ref_unchanged_without_quote_prev(self):
        """行情源无 prev(realtime NA 剥离后):pv 仍生效,不落 prev_ref。"""
        q = {"last": 61988.15, "prev": None, "ts": _ts(T_NEXT)}
        wc = self._wc(REFS_PV, q)
        assert wc["m1d"] == round((61988.15 / 61901.8203 - 1) * 100, 2)


class TestRescueStaleRows:
    """_rescue_stale_rows:monkeypatch _eodhd_eod,零网络。"""

    def _sections(self):
        return [
            {"title": "加密货币", "rows": [
                {"name": "Solana", "ticker": "SOL", "last": 73.6228,
                 "last_str": "73.62", "unit": "%", "as_of": "2026-11-18",
                 "src": None, "hist": [], "code": "SOL-USD.CC",
                 "refs": {}, "m1d": -1.0, "m1w": None, "m1m": None, "ytd": None},
                {"name": "Bitcoin", "ticker": "BTC", "last": 61901.82,
                 "last_str": "61,901.82", "unit": "%", "as_of": "2026-11-19",
                 "src": None, "hist": [], "code": "BTC-USD.CC",
                 "refs": {}, "m1d": -1.7, "m1w": None, "m1m": None, "ytd": None},
                {"name": "Sector ETF", "ticker": "XLE", "last": 80.0,
                 "last_str": "80.00", "unit": "%", "as_of": "2026-11-18",
                 "src": "finviz", "hist": [], "code": "XLE.US",
                 "refs": {}, "m1d": -0.5, "m1w": None, "m1m": None, "ytd": None},
            ]},
        ]

    def test_rescue_rebuilds_stale_row_with_t_bar(self):
        import us_close_chart as u
        sol = [(T, 73.0660), (date(2026, 11, 18), 73.6228),
               (date(2026, 11, 17), 75.1052)]
        with patch.object(u, "_eodhd_eod", return_value=(sol, None)):
            sections = self._sections()
            n = u._rescue_stale_rows(sections, T)
        assert n == 1
        row = sections[0]["rows"][0]
        assert row["as_of"] == T.isoformat()
        assert row["last"] == 73.0660
        assert row.get("dp", 2) == 2
        # 1D = vs 前一 UTC 日收盘(73.6228);_changes 不做展示层舍入
        assert abs(row["m1d"] - (73.0660 / 73.6228 - 1) * 100) < 1e-6

    def test_rescue_skips_when_no_t_bar(self):
        import us_close_chart as u
        old = [(date(2026, 11, 18), 73.6228), (date(2026, 11, 17), 75.1052)]
        with patch.object(u, "_eodhd_eod", return_value=(old, None)):
            sections = self._sections()
            n = u._rescue_stale_rows(sections, T)
        assert n == 0
        assert sections[0]["rows"][0]["as_of"] == "2026-11-18"
        assert sections[0]["rows"][0]["last"] == 73.6228

    def test_rescue_low_price_crypto_gets_dp4(self):
        import us_close_chart as u
        doge = [(T, 0.0581), (date(2026, 11, 18), 0.0588)]
        sections = self._sections()
        sections[0]["rows"][0].update(
            {"name": "Dogecoin", "code": "DOGE-USD.CC", "last": 0.0588,
             "last_str": "0.0588"})
        with patch.object(u, "_eodhd_eod", return_value=(doge, None)):
            n = u._rescue_stale_rows(sections, T)
        assert n == 1
        row = sections[0]["rows"][0]
        assert row["dp"] == 4
        assert row["last_str"] == "0.0581"
