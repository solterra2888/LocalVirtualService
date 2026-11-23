#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""成交额榜取数的离线单测:拆股日修正、单市场容错、按市场重算。

不连库、不联网 —— EODHD 接口与腾讯行情全部 mock。样例取自 2026-11-20 SNDK
8:1 拆股事故(榜单当日 -89.51%,真实 -16.05%)。

跑法: python -I tools/facts-report/test_rank_split_fix.py
"""

import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_snapshot as bs  # noqa: E402

FAILURES = []
T = date(2026, 11, 20)


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


# ── 1. 拆股日修正 _split_adjust ─────────────────────────────────────────
SNDK_SPLIT = {"splits/SNDK.US": [{"date": "2026-11-19", "split": "1.000000/1.000000"},
                                 {"date": "2026-11-20", "split": "8.000000/1.000000"}]}
row = {"code": "SNDK", "exchange": "US", "price": 79.24,
       "prev_close": 755.16, "change_percent": -89.51}
with patch.object(bs, "_eodhd_api", side_effect=lambda p, _: SNDK_SPLIT.get(p, [])):
    new = bs._split_adjust(row, T, None)
check("SNDK 8:1 拆股:当日 -89.51% → -16.05%,昨收 755.16 → 94.395",
      new == -16.05 and abs(row["prev_close"] - 94.395) < 1e-9,
      f"new={new} prev_close={row['prev_close']}")
with patch.object(bs, "_eodhd_api") as api:
    again = bs._split_adjust(row, T, -16.05)
check("同一行不二次折算(极端值/Yahoo 核对均不再触发)",
      again is None and row["change_percent"] == -16.05
      and abs(row["prev_close"] - 94.395) < 1e-9 and not api.called)

row2 = {"code": "XXX", "exchange": "US", "price": 50.0,
        "prev_close": 100.0, "change_percent": -50.0}
with patch.object(bs, "_eodhd_api",
                  return_value=[{"date": "2026-11-13", "split": "2.000000/1.000000"}]):
    check("可疑但 T 日无拆股记录(拆股在别日)→ 保持原值",
          bs._split_adjust(row2, T, None) is None
          and row2["change_percent"] == -50.0 and row2["prev_close"] == 100.0)

row3 = {"code": "NVDA", "exchange": "US", "price": 125.53,
        "prev_close": 145.98, "change_percent": -14.01}
with patch.object(bs, "_eodhd_api") as api:
    check("正常行(-14%)不查拆股接口",
          bs._split_adjust(row3, T, -14.0) is None and not api.called)

row4 = {"code": "YYY", "exchange": "HK", "price": 95.0,
        "prev_close": 100.0, "change_percent": -5.0}
with patch.object(bs, "_eodhd_api",
                  return_value=[{"date": "2026-11-20", "split": "4.000000/5.000000"}]):
    new4 = bs._split_adjust(row4, T, -24.0)
check("Yahoo 末段相差 >1pp 触发:4:5 拆股 → 昨收 125、当日 -24.0%",
      new4 == -24.0 and row4["prev_close"] == 125.0,
      f"new4={new4} prev_close={row4['prev_close']}")

row5 = {"code": "ZZZ", "exchange": "US", "price": 50.0,
        "prev_close": 100.0, "change_percent": -50.0}
with patch.object(bs, "_eodhd_api", side_effect=RuntimeError("boom")):
    check("拆股接口失败不改数",
          bs._split_adjust(row5, T, None) is None
          and row5["change_percent"] == -50.0 and row5["prev_close"] == 100.0)

check("缺昨收的行不处理",
      bs._split_adjust({"code": "NODATA", "exchange": "US", "price": 1.0,
                        "prev_close": None, "change_percent": None}, T, None)
      is None)

check("拆股接口符号映射",
      bs._eodhd_split_symbol({"code": "AAPL", "exchange": "US"}) == "AAPL.US"
      and bs._eodhd_split_symbol({"code": "700", "exchange": "HK"}) == "0700.HK"
      and bs._eodhd_split_symbol({"code": "600519", "exchange": "SHG"}) == "600519.SHG"
      and bs._eodhd_split_symbol({"code": "000001", "exchange": "SHE"}) == "000001.SHE")

# ── 2. _volume_leaders_eodhd:单市场容错 / markets / prev_close ───────────
LISTINGS = {
    "US": [{"Code": "AAPL", "Name": "Apple Inc", "Type": "Common Stock"},
           {"Code": "SPY", "Name": "SPDR S&P 500", "Type": "ETF"}],
    "HK": [{"Code": "0700", "Name": "TENCENT HOLDINGS", "Type": "Common Stock"}],
    "SHG": [{"Code": "600519", "Name": "KWEICHOW MOUTAI", "Type": "Common Stock"}],
    "SHE": [{"Code": "000001", "Name": "PING AN BANK", "Type": "Common Stock"}],
}
PREV, DAY = "2026-11-19", "2026-11-20"
BULK = {
    ("US", DAY): [{"code": "AAPL", "close": 342.56, "volume": 79046218}],
    ("US", PREV): [{"code": "AAPL", "close": 349.21, "volume": 70000000}],
    ("HK", DAY): [],                       # 港股当日休市:接口回空文件
    ("HK", PREV): [{"code": "0700", "close": 344.0, "volume": 41094469}],
    ("SHG", DAY): [{"code": "600519", "close": 1439.0, "volume": 5697589}],
    ("SHG", PREV): [{"code": "600519", "close": 1485.75, "volume": 6722940}],
    ("SHE", DAY): [{"code": "000001", "close": 12.34, "volume": 242461232}],
    ("SHE", PREV): [{"code": "000001", "close": 12.22, "volume": 213498896}],
}


def _fake_api(calls, fail=()):
    def _api(path, params):
        calls.append((path, params.get("date")))
        if path in fail:
            raise RuntimeError(f"simulated outage: {path}")
        if path.startswith("exchange-symbol-list/"):
            return LISTINGS[path.split("/", 1)[1]]
        if path.startswith("eod-bulk-last-day/"):
            return BULK.get((path.split("/", 1)[1], params.get("date")), [])
        return []
    return _api


calls, diag = [], {}
with patch.object(bs, "_eodhd_api", side_effect=_fake_api(calls)), \
        patch.object(bs, "_tencent_names") as tn:
    out = bs._volume_leaders_eodhd(T, diag=diag)
check("港股当日休市:美股/A股照常出榜(此前任一所无数据会让三市榜单一起消失)",
      set(out) == {"us", "hk", "cn"} and len(out["us"]["rows"]) == 1
      and len(out["cn"]["rows"]) == 2 and out["hk"]["rows"] == [],
      f"keys={set(out)} us={len(out.get('us', {}).get('rows', []))} "
      f"cn={len(out.get('cn', {}).get('rows', []))}")
check("休市备注如实(空文件 ≠ 抓取失败)",
      out["hk"]["note"] == "港股当日休市或无成交额达标个股。", out["hk"]["note"])
check("diag 记录各所当日是否取到日线",
      diag == {"US": True, "HK": False, "SHG": True, "SHE": True}, str(diag))
aapl = out["us"]["rows"][0]
check("行内带 prev_close,当日 = 收盘/昨收 - 1",
      aapl["prev_close"] == 349.21
      and aapl["change_percent"] == round((342.56 / 349.21 - 1) * 100, 2),
      str(aapl))
check("A股榜含沪深两所且按成交额降序(茅台 ~82 亿 > 平安银行 ~30 亿)",
      [r["code"] for r in out["cn"]["rows"]] == ["600519", "000001"],
      str([r["code"] for r in out["cn"]["rows"]]))
check("港/A股行补中文名,美股不调腾讯", tn.call_count == 2, f"calls={tn.call_count}")

calls2 = []
with patch.object(bs, "_eodhd_api", side_effect=_fake_api(calls2)), \
        patch.object(bs, "_tencent_names"):
    out2 = bs._volume_leaders_eodhd(T, markets=("hk", "cn"))
check("markets 限定:只返回指定市场、不请求美股文件",
      set(out2) == {"hk", "cn"}
      and not any("US" in p for p, _ in calls2), f"keys={set(out2)}")

calls3, diag3 = [], {}
with patch.object(bs, "_eodhd_api",
                  side_effect=_fake_api(calls3, fail=("eod-bulk-last-day/SHG",))), \
        patch.object(bs, "_tencent_names"):
    out3 = bs._volume_leaders_eodhd(T, diag=diag3)
check("单所抓取失败只丢该所:深交所个股仍在 A股榜",
      [r["code"] for r in out3["cn"]["rows"]] == ["000001"]
      and out3["cn"]["note"] is None and diag3.get("SHG") is False,
      f"cn={[r['code'] for r in out3['cn']['rows']]} note={out3['cn']['note']} diag={diag3}")
check("美股与深交所不受上交所失败影响",
      len(out3["us"]["rows"]) == 1 and diag3.get("SHE") is True)

# ── 3. _rank_sparks:markets 限定 + 临时字段不进快照 ─────────────────────
CANNED = {"spark": [1.0] * 63, "w1": 1.0, "m1": 2.0, "ytd": 3.0,
          "refs": {"w1": 1.0}, "hist": [[DAY, 1.0], [DAY, 1.0]],
          "ytd_ref": 3.0, "spliced": False}
vl = {"us": {"rows": [{"code": "AAPL", "exchange": "US", "price": 342.56,
                       "change_percent": -1.9, "prev_close": 349.21}]},
      "hk": {"rows": [{"code": "0700", "exchange": "HK", "price": 377.2,
                       "change_percent": -3.38, "prev_close": 390.4}]}}
with patch.object(bs, "_rank_perf_spark", return_value=CANNED), \
        patch.object(bs, "_eodhd_api", return_value=[]) as api4:
    bs._rank_sparks(vl, T, markets=("hk",))
check("_rank_sparks 只处理指定市场",
      "spark" in vl["hk"]["rows"][0] and "spark" not in vl["us"]["rows"][0])
check("走势/周期涨跌/带日期序列已补,临时台账字段(_spliced/_split)不进快照",
      vl["hk"]["rows"][0]["spark"] == CANNED["spark"]
      and vl["hk"]["rows"][0]["perf"]["w1"] == 1.0
      and vl["hk"]["rows"][0]["perf_refs"]["s"] == CANNED["hist"]
      and not {"_spliced", "_split"} & set(vl["hk"]["rows"][0]),
      str(sorted(vl["hk"]["rows"][0])))

# ── 4. 价位口径对齐:重算旧工作区时把 Yahoo 复权序列折回榜单行的价位 ──────
from datetime import datetime, timedelta, timezone  # noqa: E402


class _Resp:
    """假 Yahoo chart 响应:给定收盘序列,日期为截至 end 的连续日(末点 = end)。"""

    def __init__(self, closes, end):
        self._c, self._end = closes, end

    def json(self):
        ts = [int(datetime((self._end - timedelta(days=n)).year,
                           (self._end - timedelta(days=n)).month,
                           (self._end - timedelta(days=n)).day, 13, 30,
                           tzinfo=timezone.utc).timestamp())
              for n in range(len(self._c) - 1, -1, -1)]
        return {"chart": {"result": [{
            "timestamp": ts, "indicators": {"quote": [{"close": self._c}]}},
        ]}}


T19 = date(2026, 11, 19)
ADJ = [110.6263] * 30 + [97.0275, 94.395]        # Yahoo 现值:已按 11-20 的 8:1 复权
with patch.object(bs.requests, "get", return_value=_Resp(ADJ, T19)):
    d = bs._rank_perf_spark("SNDK", T19, anchor_price=755.16, anchor_chg=-2.71)
check("重算拆股前的工作区:序列折回行价位(SNDK 末点 94.395 → 755.16)",
      d is not None and d["spark"][-1] == 755.16
      and d["spark"][0] == round(110.6263 * 8, 4)
      and d["hist"][-1][1] == 755.16 and d["w1"] is not None,
      f"last={d and d['spark'][-1]} first={d and d['spark'][0]}")

T20 = date(2026, 11, 20)
SAME = [94.395] * 30 + [80.0, 79.24]
with patch.object(bs.requests, "get", return_value=_Resp(SAME, T20)):
    d2 = bs._rank_perf_spark("SNDK", T20, anchor_price=79.24, anchor_chg=-16.05)
check("正常构建(同一口径):序列原样不动",
      d2 is not None and d2["spark"] == [round(c, 4) for c in SAME[-63:]])

with patch.object(bs.requests, "get", return_value=_Resp(SAME, T20)):
    d3 = bs._rank_perf_spark("SNDK", T20, anchor_price=83.2, anchor_chg=-10.0)
check("两源收盘价相差 5%(非拆股):不折算",
      d3 is not None and d3["spark"][-1] == 79.24, f"last={d3 and d3['spark'][-1]}")

# ── 5. 行内容指纹(收盘补数的幂等判定) ───────────────────────────────────
import refresh_rank_rows as rr  # noqa: E402

check("行内容指纹对代码/价格/当日/成交额敏感,对无关字段不敏感",
      rr._sig([{"code": "A", "price": 1.0, "change_percent": 1.0, "turnover": 5}])
      == rr._sig([{"code": "A", "price": 1.0, "change_percent": 1.0, "turnover": 5,
                   "name": "x", "spark": [1, 2]}])
      and rr._sig([{"code": "A", "price": 1.0, "change_percent": 1.0, "turnover": 5}])
      != rr._sig([{"code": "A", "price": 1.01, "change_percent": 1.0, "turnover": 5}]))

print()
if FAILURES:
    print(f"FAILED {len(FAILURES)}: {FAILURES}")
    raise SystemExit(1)
print("ALL PASS")
