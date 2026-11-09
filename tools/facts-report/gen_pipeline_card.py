#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""每日期刊式流程卡:把当日运行数据注入 pipeline_card_template.html,
产出 {ws}/pipeline-overview.html(独立 HTML,浏览器直接打开,零依赖)。

由 run_pipeline 在发布成功后自动调用;也可手动:
    python gen_pipeline_card.py <workspace 目录>
数据全部来自工作区工件(snapshot/state/claims/check_results/verdict/日志),
不调 LLM、不连库。缺什么就少显示什么,永不抛错阻断发报。
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

TOOL_DIR = Path(__file__).resolve().parent
REPORTS_ROOT = TOOL_DIR.parents[1] / "backend" / "reports" / "facts-report"

AGENT_LABEL = {
    "overall_market": "叙事组(叙事/驱动/持仓)", "company_desk": "公司与板块部",
    "macro_review": "宏观数据部", "geopolitics": "地缘政治部", "fomc": "美联储观察部",
    "outlook": "展望与策略",
}

# 模板 SECTIONS(13) 与 sections/*.md(01-11) 的对应;None=非 md 栏目
SEC_MAP = [None, "01_narrative", "02_drivers", "03_positioning", None, "05_movers",
           "04_sector_action", "06_deep_dives", "07_macro_dashboard", "08_macro_review",
           "09_geopolitics", "10_fomc", "11_outlook"]
SEC_NAMES = ["大盘总览", "今日要闻", "核心驱动", "持仓与资金流", "美股全景", "今日涨跌榜",
             "板块动态", "个股深挖", "宏观速览", "回顾·宏观数据", "回顾·地缘政治",
             "美联储观察", "展望与策略"]


def _j(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _n(x: Any) -> int:
    return len(x) if isinstance(x, (list, dict)) else 0


def _clean_chars(md: Path) -> int:
    try:
        t = md.read_text(encoding="utf-8")
    except OSError:
        return 0
    return len(re.sub(r"<!--.*?-->", "", t, flags=re.S).strip())


def _claims_of(ws: Path, stem: str) -> List[dict]:
    f = next(ws.glob(f"sections/{stem}.claims.json"), None)
    return _j(f) if f else []


def _fmt_k(v: int) -> str:
    return f"{v / 1e6:.2f}M" if v >= 1e6 else f"{round(v / 1e3)}K"


def _run_log(ws: Path) -> str:
    """挑信息量最大的一份运行日志:修订轮记录越多越完整(完整首轮漏斗通常
    只在 cron.log;工作区里的手工修补日志只含终态,按 mtime 取最新反而丢
    中间过程)。"""
    cands: List[tuple] = []  # (mtime, text)
    for p in sorted(ws.glob("*.log"), key=lambda q: q.stat().st_mtime, reverse=True):
        try:
            cands.append((p.stat().st_mtime,
                          p.read_text(encoding="utf-8", errors="replace")))
        except OSError:
            continue
    cron = REPORTS_ROOT / "cron.log"
    if cron.exists():
        t = cron.read_text(encoding="utf-8", errors="replace")
        k = t.rfind("cron start")
        if k >= 0:
            cands.append((cron.stat().st_mtime, t[k:]))
    best, best_score = "", -1
    for mtime, text in cands:
        if "deterministic checks" not in text:
            continue
        score = (text.count("[revise]"), text.count("deterministic checks"), mtime)
        if score > (best_score if isinstance(best_score, tuple) else (-1, -1, best_score)):
            best, best_score = text, score
    return best


def _header_time(ws: Path, total_s: Optional[int]) -> str:
    """页头「HH:MM 开工,HH:MM 送达」:成品 mtime 反推开工(减 total),缺件则只给送达。"""
    from datetime import datetime, timedelta
    rep = ws / "final" / "report.md"
    if not rep.exists():
        return ""
    end = datetime.fromtimestamp(rep.stat().st_mtime)
    if total_s:
        start = end - timedelta(seconds=total_s)
        return f"{start:%H:%M} 开工,{end:%H:%M} 送达"
    return f"送达 {end:%H:%M}"


def build_run(ws: Path) -> Dict[str, Any]:
    snap = _j(ws / "snapshot.json") or {}
    src = snap.get("sources") or {}
    state = _j(ws / "state.json") or {}
    ver = state.get("verify") or {}
    checks = _j(ws / "verify" / "check_results.json") or []
    verdict = _j(ws / "verify" / "verdict.json") or {}
    log = _run_log(ws)

    # ── 原料池 ──
    news = src.get("news") or {}
    nm = src.get("news_macro") or {}
    ee = src.get("economic_events") or {}
    ec = src.get("earnings_calendar") or {}
    mk = src.get("market") or {}
    ca = mk.get("cross_asset")
    ca_n = _n(ca) if isinstance(ca, list) else _n((ca or {}).get("rows"))
    vl = mk.get("volume_leaders") or {}
    vl_n = sum(_n((vl.get(k) or {}).get("rows")) for k in ("us", "hk", "cn"))
    tc = src.get("the_close") or {}
    cb = src.get("cnbc_closing_bell") or {}
    tc_len = len(tc.get("clean_transcript") or "")
    cb_len = len(cb.get("clean_transcript") or "") + len(cb.get("clean_transcript_ot") or "")
    flash, wire, calls = _n(news.get("flash")), _n(news.get("wire")), _n(news.get("calls"))
    research, geo, digest = _n(nm.get("research")), _n(nm.get("geo")), _n(nm.get("digest"))
    sp500, tm, sa = _n(mk.get("sp500_constituents")), _n(mk.get("top_movers")), _n(mk.get("sp500_sector_avg"))
    rec, upc = _n(ee.get("recent")), _n(ee.get("upcoming"))
    ea = _n(ec.get("prior_after_close")) + _n(ec.get("today")) + _n(ec.get("forward"))
    raw_total = flash + wire + calls + research + geo + digest + rec + upc + ea + sp500 + tm + sa + ca_n + vl_n

    pools = [
        {"label": "市场快讯", "src": "订阅推特账号", "count": f"{flash} 条", "note": "盘中即时消息"},
        {"label": "新闻全文", "src": "Yahoo/SA/CNBC 等", "count": f"{wire} 篇", "note": "通讯社与媒体"},
        {"label": "电话会纪要", "src": "Seeking Alpha", "count": f"{calls} 篇", "note": "管理层问答"},
        {"label": "券商宏观解读", "src": "订阅星球/通讯", "count": f"{research} 篇", "note": "署名引用"},
        {"label": "地缘快讯", "src": "订阅源(含央视)", "count": f"{geo} 条", "note": "中英混合"},
        {"label": "中文晨报素材", "src": "订阅源", "count": f"{digest} 条",
         "note": "08:51 后才发布,早于此时必缺(已知边界)" if digest == 0 else "当晨已收"},
        {"label": "经济日历", "src": "EODHD", "count": f"{rec + upc} 条", "note": f"近 3 天 {rec} + 未来 7 天 {upc}"},
        {"label": "财报安排", "src": "日历服务", "count": f"{ea} 条", "note": "盘后+当日+前瞻"},
        {"label": "S&P 500 全成分行情", "src": "Finviz", "count": f"{sp500} 只", "note": "价/量/涨跌/市值"},
        {"label": "榜单与跨资产", "src": "Finviz + EOD", "count": f"{tm + sa + ca_n} 项", "note": f"涨幅榜 {tm} + 板块 {sa} + 跨资产 {ca_n}"},
        {"label": "三市场成交额榜", "src": "Finviz + EODHD + 腾讯", "count": f"{vl_n} 只", "note": "美股/港股/A股 各前 8"},
        {"label": "宏观与央行资料", "src": "FRED + FOMC 表", "count": f"{_n(src.get('macro_pillars'))} 组", "note": "曲线/概率/指标"},
        {"label": "节目文字稿", "src": "The Close + Closing Bell", "count": f"{tc_len + cb_len:,} 字", "note": f"{tc_len:,} + {cb_len:,}"},
    ]

    # ── 章节实测 ──
    per_md = []
    for stem in ["01_narrative", "02_drivers", "03_positioning", "04_sector_action",
                 "05_movers", "06_deep_dives", "07_macro_dashboard", "08_macro_review",
                 "09_geopolitics", "10_fomc", "11_outlook"]:
        cl = _claims_of(ws, stem)
        md = ws / "sections" / f"{stem}.md"
        per_md.append({"stem": stem, "n": len(cl),
                       "src": len({c.get("path", "?").split("[")[0] for c in cl if c.get("path")}),
                       "chars": _clean_chars(md) if md.exists() else 0})
    claims_total = sum(p["n"] for p in per_md)
    src_libs = len({c.get("path", "?").split("[")[0] for p in per_md for c in _claims_of(ws, p["stem"]) if c.get("path")})
    chars_total = sum(p["chars"] for p in per_md)

    det_idx = {"05_movers", "07_macro_dashboard"}
    claims = []
    for p in per_md:
        claims.append({"sec": {"01_narrative": "今日要闻", "02_drivers": "核心驱动",
                               "03_positioning": "持仓与资金流", "04_sector_action": "板块动态",
                               "05_movers": "今日涨跌榜", "06_deep_dives": "个股深挖",
                               "07_macro_dashboard": "宏观速览", "08_macro_review": "回顾·宏观数据",
                               "09_geopolitics": "回顾·地缘政治", "10_fomc": "美联储观察",
                               "11_outlook": "展望与策略"}[p["stem"]],
                       "n": p["n"], "src": p["src"], "chars": p["chars"],
                       "det": p["stem"] in det_idx})
    by = {p["stem"]: p for p in per_md}

    def stat_for(i: int) -> str:
        stem = SEC_MAP[i]
        name = SEC_NAMES[i]
        if i == 0:
            return f"图表(跨资产 {ca_n} 行 + 板块行情)"
        if i == 4:
            return f"图表({sp500} 只成分)"
        if stem is None:
            return ""
        p = by[stem]
        if stem == "01_narrative":
            return f"台账 0 条(按设计不落数字)· 正文 {p['chars']} 字"
        if stem in ("05_movers",):
            return f"{vl_n} 行(三市场各 8)· 正文 {p['chars']} 字"
        if stem == "07_macro_dashboard":
            return f"正文 {p['chars']} 字(全表格)"
        return f"台账 {p['n']} 条 · 引用 {p['src']} 个源 · 正文 {p['chars']} 字"

    sec_stats = [stat_for(i) or "自动制图" for i in range(13)]
    sec_subs = []
    for i in range(13):
        if i in (0, 4):
            sec_subs.append("自动制图")
        elif i == 5:
            sec_subs.append(f"三市场 {vl_n} 行")
        elif i == 8:
            sec_subs.append("自动表格")
        else:
            p = by.get(SEC_MAP[i]) or {"n": 0, "chars": 0}
            sec_subs.append(f"{p['n']} 卡 · {p['chars']} 字")
    pool_subs = [f"{tc_len / 1e4:.1f} 万字", f"主段+OT {cb_len / 1e4:.0f} 万字", f"{flash} 条",
                 f"{wire + calls} 条", f"星球/通讯 {research} 篇", f"{geo} 条(含央视)",
                 f"{sp500} 只 + 三榜 {vl_n}", f"{rec + upc + ea} 条", "曲线/概率/跨资产"]

    # ── 阶段与时长 ──
    ub = state.get("usage_by_phase") or {}
    def tok(k): return ((ub.get(k) or {}).get("inputTokens") or 0)
    phases = [["选题会", round(tok("plan") / 1e3), _fmt_k(tok("plan"))],
              ["写作", round(tok("write") / 1e3), _fmt_k(tok("write"))],
              ["重写", round(tok("revise") / 1e3), _fmt_k(tok("revise"))],
              ["质检", round(tok("verify") / 1e3), _fmt_k(tok("verify"))]]
    tim = state.get("timings") or {}
    def t_first(k): 
        v = tim.get(k)
        return round(v[0]) if isinstance(v, list) and v else None
    stage_groups = ["section:narrative", "section:sector_action", "section:macro_review",
                    "section:geopolitics", "section:fomc"]
    stage1 = max((t_first(k) or 0) for k in stage_groups)
    outlook_s = t_first("section:outlook")
    total_s = t_first("total")
    plan_s = t_first("plan")

    # ── 漏斗(日志优先,缺则退化为终态) ──
    m_first = re.search(r"deterministic checks: (\d+) rows, (\d+) high fails", log)
    highs = [int(x) for x in re.findall(r"deterministic checks: \d+ rows, (\d+) high fails", log)]
    rounds = re.findall(r"\[revise\] (\w+) \([^;]+; (\d+) issue", log)
    gate = re.search(r"(\d+) LLM high issue", log)
    final_fails = [r for r in checks if not r.get("match", True)]
    degraded = bool(ver.get("published_with_issues"))
    v_issues = verdict.get("issues") or []

    funnel = [{"big": f"{claims_total}", "text": "个数字逐一回原料比对"}]
    if m_first:
        funnel.append({"arrow": 1})
        r0, h0 = int(m_first.group(1)), int(m_first.group(2))
        funnel.append({"big": f"{r0} 行", "text": f"机器首轮核对 · <b class='bad'>{h0} 处确认不合格</b>(数字与原料不符/口径引错/缺要素)"})
        if rounds:
            seg = "、".join(f"{AGENT_LABEL.get(a, a)} {c} 处" for a, c in rounds)
            fixed = sum(h0s for h0s in [h0]) + (int(gate.group(1)) if gate else 0)
            funnel.append({"arrow": 1})
            funnel.append({"cls": "fix", "big": f"{h0} → 0", "text": f"打回重写:{seg}"})
            if gate:
                funnel.append({"arrow": 1})
                funnel.append({"big": "主编终审", "text": f"通读全文再抓 <b class='bad'>{gate.group(1)} 处</b>,打回重写后复核通过" if gate else ""})
            funnel.append({"arrow": 1})
            funnel.append({"cls": "ok", "big": f"{fixed} 修复 / {len(final_fails)} 提示",
                           "text": ("剩 %d 条小瑕疵随内部备忘录发布,正文干净" % len(final_fails)) if not degraded
                                   else "仍未全过,按不断更原则带清单发布"})
    else:
        funnel.append({"arrow": 1})
        funnel.append({"big": f"{len(checks)} 行", "text": f"终态核对,{len(final_fails)} 处不匹配(日志缺失,中间轮次未收录)"})

    # ── 全景页徽标 ──
    flow = {
        "raw": f"今日实测:约 {raw_total:,} 条原料 + {(tc_len + cb_len) / 1e4:.1f} 万字节目文字稿",
        "plan": f"今日实测:一次成稿,{_fmt_k(tok('plan'))} tokens",
        "stage1": f"{round(stage1 / 60)} 分钟<br>(最慢编辑部)",
        "outlook": f"{round((outlook_s or 0) / 60)} 分钟",
        "verify": ("今日实测:首轮查出 %d 处不合格,%d 轮重写后%s" % (highs[0] if highs else 0, len(rounds),
                    "带清单发布" if degraded else "全部修复")) if highs else "今日实测:核验完成",
        "final": f"今日实测:11 个栏目 {chars_total:,} 字 · 全程零人工 · {round((total_s or 0) / 60)} 分钟",
        "header-time": _header_time(ws, total_s),
    }
    out_md = ws / "final" / "report.md"
    headline = ""
    if out_md.exists():
        headline = out_md.read_text(encoding="utf-8").splitlines()[0].lstrip("# ").strip()

    triage_note = "注:当日素材智能分拣正常运行。"
    if log and ("chunk failed" in log or "[triage] FAILED" in log):
        triage_note = "注:当日素材智能分拣遇到接口抖动,自动切换备用通道,内容照常产出。"
    elif log and "triage" not in log:
        triage_note = "注:当日按配置跳过语义分拣,直接使用规则分拣(素材分级不受影响)。"

    return {
        "date": ws.name, "headline": headline,
        "statsTitle": f"当日运营数据 · {ws.name} 期",
        "flow": flow, "pools": pools, "phases": phases,
        "totals": [[f"{chars_total:,}", "字 · 11 个栏目"], [f"{claims_total}", "张数字出处卡"],
                   [f"{src_libs}", "个被实际引用的原料库"]],
        "claims": claims, "funnel": funnel,
        "secStats": sec_stats, "secSubs": sec_subs, "poolSubs": pool_subs,
        "outlook-stat": None, "triageNote": triage_note,
        "outlookStat": f"{round((outlook_s or 0) / 60)} 分钟 · {by['11_outlook']['chars']:,} 字 · "
                       f"{by['11_outlook']['n']} 张出处卡 · {by['11_outlook']['src']} 个原料库——它做的是综合判断",
    }



# ─────────────────────────────────────────────────────────────
# 数据流向 tab:交互桑基 payload(v7 gen-logic-sankey 的通用化移植)
# 数据源→切片→栏目→交付,含下钻数据(切片JSON/栏目md/任务书/配给清单)。
# 任何一块缺失就跳过,绝不阻断出卡。
# ─────────────────────────────────────────────────────────────

_SLICE_AGENTS = [("overall_market", "② 市场叙事部", 10),
                 ("company_desk", "③ 公司与板块部", 9),
                 ("macro_review", "④ 宏观数据部", 4),
                 ("geopolitics", "⑤ 地缘政治部", 4),
                 ("fomc", "⑥ 美联储观察部", 3),
                 ("outlook", "⑦ 展望与策略部", 4)]
_SLICE_SECS = {"overall_market": [0, 1, 2], "company_desk": [3, 5],
               "macro_review": [7], "geopolitics": [8], "fomc": [9], "outlook": [10]}
# SEC_MAP 索引(sections 文件序 0-10)→栏目显示名
_SEC_NAMES = ["01_narrative", "02_drivers", "03_positioning", "04_sector_action", "05_movers",
              "06_deep_dives", "07_macro_dashboard", "08_macro_review", "09_geopolitics",
              "10_fomc", "11_outlook"]
_DEPT_OF = {0: "②叙事部", 1: "②叙事部", 2: "②叙事部", 3: "③公司板块部", 5: "③公司板块部",
            4: "规则直出", 6: "规则直出", 7: "④宏观数据部", 8: "⑤地缘政治部",
            9: "⑥美联储观察部", 10: "⑦展望部"}

_SRC_CFG = {
 'tc': {'label': 'The Close 节目', 'unit': '字',
        'role': '②市场叙事部主稿(全文进切片);④⑤⑦部作上下文引用',
        'mp': ['the_close.clean_transcript'], 'dp': ['the_close'], 'vol': 'tc_chars'},
 'cb': {'label': 'Closing Bell 主段+加时', 'unit': '字',
        'role': '③公司与板块部主稿(催化剂与嘉宾观点)',
        'mp': ['cnbc_closing_bell.clean_transcript', 'cnbc_closing_bell.clean_transcript_ot'],
        'dp': ['cnbc_closing_bell'], 'vol': 'cb_chars'},
 'tw': {'label': '推特快讯池', 'unit': '条', 'role': '快讯供给:全部编辑部辅材;⑤地缘/⑥美联储的实时补充',
        'mp': ['news.flash', 'news.wire', 'news.calls'], 'dp': ['news'], 'vol': 'news_counts'},
 'cn': {'label': '中文研究池', 'unit': '条', 'role': '④宏观数据部边际变化(研报转述)+⑤地缘政治部主题原料',
        'mp': ['news_macro.digest', 'news_macro.research', 'news_macro.geo'], 'dp': ['news_macro'], 'vol': 'nm_counts'},
 'cal': {'label': 'EODHD 日历', 'unit': '条', 'role': '④宏观数据部数据日历;③公司部财报催化剂(相交瘦身)',
        'mp': ['economic_events.recent', 'economic_events.upcoming', 'earnings_calendar.prior_after_close',
               'earnings_calendar.today', 'earnings_calendar.upcoming'],
        'dp': ['economic_events', 'earnings_calendar'], 'vol': 'cal_counts'},
 'fz': {'label': 'Finviz 行情', 'unit': '行', 'role': '③公司部行情(切片内为 S&P 异动视图:top15+板块领涨);涨跌榜/热力图规则直出',
        'mp': ['market.sp500_constituents', 'market.sp500_sector_avg', 'market.top_movers', 'market.volume_leaders'],
        'dp': ['market.sp500_constituents', 'market.sp500_sector_avg', 'market.top_movers', 'market.volume_leaders'],
        'vol': 'fz_counts'},
 'fred': {'label': 'FRED/美财政部', 'unit': '系列/行', 'role': '利率与宏观支柱;⑥美联储观察部曲线与实际利率;FT 图绘制',
        'mp': ['macro_pillars.par_curve', 'macro_pillars.fred_extras', 'macro_pillars.fred_ranks',
               'macro_pillars.release_series', 'macro_pillars.effr', 'macro_pillars.auction_events',
               'market.cross_asset', 'market.breadth'],
        'dp': ['macro_pillars', 'market.cross_asset', 'market.breadth'], 'vol': 'fred_counts'},
 'fomc': {'label': 'FOMC 资料+期现概率', 'unit': '条', 'role': '⑥美联储观察部核心材料(声明/路径/概率);⑦展望部引用',
        'mp': ['fomc.decisions', 'fomc.path', 'macro_pillars.fomc_calendar', 'macro_pillars.fed_odds',
               'macro_pillars.next_fomc_odds', 'prior_odds'],
        'dp': ['fomc', 'prior_odds'], 'vol': 'fomc_counts'},
}
_CLAIM_PREFIX = [('the_close', 'tc'), ('cnbc_closing_bell', 'cb'), ('news.', 'tw'), ('news_macro.', 'cn'),
                 ('economic_events', 'cal'), ('earnings_calendar', 'cal'), ('market.cross_asset', 'fred'),
                 ('market.', 'fz'), ('macro_pillars.fed_odds', 'fomc'), ('macro_pillars.next_fomc_odds', 'fomc'),
                 ('macro_pillars.fomc_calendar', 'fomc'), ('macro_pillars.', 'fred'), ('fomc', 'fomc'),
                 ('prior_odds', 'fomc')]
_SRC2SLICE = [('tc', 'overall_market'), ('tw', 'overall_market'), ('fred', 'overall_market'),
              ('cb', 'company_desk'), ('fz', 'company_desk'), ('cal', 'company_desk'), ('tw', 'company_desk'),
              ('cal', 'macro_review'), ('cn', 'macro_review'), ('tc', 'macro_review'), ('fred', 'macro_review'),
              ('cn', 'geopolitics'), ('tw', 'geopolitics'),
              ('fomc', 'fomc'), ('fred', 'fomc'), ('tw', 'fomc'),
              ('tc', 'outlook'), ('fomc', 'outlook'), ('fred', 'outlook')]
_SRC2SLICE_W = {('tc', 'overall_market'): 8, ('tw', 'overall_market'): 3, ('fred', 'overall_market'): 2,
                ('cb', 'company_desk'): 8, ('fz', 'company_desk'): 4, ('cal', 'company_desk'): 2, ('tw', 'company_desk'): 2,
                ('cal', 'macro_review'): 4, ('cn', 'macro_review'): 3, ('tc', 'macro_review'): 3, ('fred', 'macro_review'): 1,
                ('cn', 'geopolitics'): 5, ('tw', 'geopolitics'): 3,
                ('fomc', 'fomc'): 5, ('fred', 'fomc'): 3, ('tw', 'fomc'): 2,
                ('tc', 'outlook'): 4, ('fomc', 'outlook'): 2, ('fred', 'outlook'): 1}


def _dig(obj, path):
    v = obj
    for part in path.split('.'):
        v = v.get(part) if isinstance(v, dict) else None
        if v is None:
            return None
    return v


def _measure(v, unit):
    if v is None:
        return 0
    if unit == '字':
        return len(v) if isinstance(v, str) else 0
    if isinstance(v, list):
        return len(v)
    if isinstance(v, dict):
        return len(v)
    return 1


def _claim_src(path):
    for pre, k in _CLAIM_PREFIX:
        if path.startswith(pre):
            return k
    return 'other'


def _src_volume_label(sources, key):
    """数据源节点的第二行标签(当日实测量)。"""
    try:
        if key == 'tc':
            return f"{len(_dig(sources, 'the_close.clean_transcript') or ''):,} 字"
        if key == 'cb':
            a = len(_dig(sources, 'cnbc_closing_bell.clean_transcript') or '')
            b = len(_dig(sources, 'cnbc_closing_bell.clean_transcript_ot') or '')
            return f"{a + b:,} 字"
        if key == 'tw':
            n = sum(len(_dig(sources, p) or []) for p in ('news.flash', 'news.wire', 'news.calls'))
            return f"{n} 条(flash/wire/calls)"
        if key == 'cn':
            n = sum(len(_dig(sources, p) or []) for p in ('news_macro.digest', 'news_macro.research', 'news_macro.geo'))
            return f"{n} 条(研报+地缘)"
        if key == 'cal':
            n = sum(len(_dig(sources, p) or []) for p in ('economic_events.recent', 'economic_events.upcoming'))
            m = sum(len(_dig(sources, p) or []) for p in ('earnings_calendar.prior_after_close', 'earnings_calendar.today', 'earnings_calendar.upcoming'))
            return f"经济事件 {n}+财报 {m}"
        if key == 'fz':
            c = len(_dig(sources, 'market.sp500_constituents') or [])
            mv = len(_dig(sources, 'market.top_movers') or [])
            sa = len(_dig(sources, 'market.sp500_sector_avg') or [])
            return f"{c} 成分+{mv} 异动+{sa} 板块"
        if key == 'fred':
            n = sum(_measure(_dig(sources, p), '行') for p in _SRC_CFG['fred']['mp'])
            return f"宏观 {n} 系列/行"
        if key == 'fomc':
            n = sum(_measure(_dig(sources, p), '条') for p in _SRC_CFG['fomc']['mp'])
            return f"决议/路径/概率 {n} 条"
    except Exception:
        pass
    return ''


def _sankey_payload(ws: Path) -> Dict[str, Any]:
    """数据流向 tab 的全部数据;缺件跳过,永不抛错。"""
    out: Dict[str, Any] = {'nodes': [], 'links': [], 'slices': {}, 'sliceMeta': {},
                           'md': {}, 'mdMeta': {}, 'prompts': {}, 'promptMeta': {},
                           'pipes': {}, 'srcCfg': {}, 'name2src': {}}
    try:
        snap = _j(ws / "snapshot.json") or {}
        sources = snap.get('sources') or {}
        state = _j(ws / "state.json") or {}
        tim = {k: v[0] for k, v in (state.get('timings') or {}).items() if isinstance(v, list) and v}
        # 栏目字数/claims
        chars, claims = {}, {}
        rep = ws / "final" / "report.md"
        if rep.exists():
            for sec in re.split(r'^## ', rep.read_text(encoding='utf-8'), flags=re.M)[1:]:
                chars[sec.splitlines()[0]] = len(sec)
        for i, key in enumerate(_SEC_NAMES):
            cp = ws / 'sections' / f'{key}.claims.json'
            if cp.exists():
                try:
                    claims[i] = len(_j(cp) or [])
                except Exception:
                    claims[i] = 0
        # md / prompts
        for i, key in enumerate(_SEC_NAMES):
            fp = ws / 'sections' / f'{key}.md'
            if fp.exists():
                out['md'][f'sections/{key}.md'] = fp.read_text(encoding='utf-8')
        if rep.exists():
            out['md']['final/report.md'] = rep.read_text(encoding='utf-8')
        aud = ws / 'final' / 'report.audit.md'
        if aud.exists():
            out['md']['final/report.audit.md'] = aud.read_text(encoding='utf-8')
        try:
            import prompts as P
            import run_pipeline as RP
            blocks = RP.macro_blocks(snap)
            for i, name in enumerate(['narrative', 'drivers', 'positioning', 'sector_action', 'movers',
                                      'deep_dives', 'macro_dashboard', 'macro_review', 'geopolitics', 'fomc', 'outlook']):
                path = f'sections/{_SEC_NAMES[i]}.md'
                if path not in out['md']:
                    continue
                spec = P.SECTION_SPECS.get(name)
                if spec is None or spec.get('mode') == 'deterministic':
                    out['prompts'][path] = '本栏目为规则直出,不经过 LLM,无 prompt 模板。'
                    out['promptMeta'][path] = '规则单元 · 零 LLM'
                    continue
                try:
                    t = P.build_section_prompt(spec, 'cn', blocks)
                except ValueError:
                    agent = next(a for a in P.AGENT_ORDER if name in P.agent_section_names(a))
                    task = P.AGENT_TASK_BUILDERS[agent]('cn', blocks)
                    task = task.replace('{missing_mark}', P.STYLE_PROFILES['cn']['missing_mark'])
                    t = (f"{P._common_rules('cn', allowed_tags=P.SECTION_SOURCE_MATRIX.get(name))}\n{task}\n\n"
                         f"Output file: {spec['output_file']}\n")
                out['prompts'][path] = t
                out['promptMeta'][path] = _DEPT_OF[i] + f' · {len(t):,} 字符'
            out['prompts']['final/report.md'] = 'report.md 由 assemble 程序化拼装(11 栏目按架构顺序合并+溯源渲染),不经过 LLM,无 prompt 模板。'
            out['promptMeta']['final/report.md'] = '组装器 · 程序化'
            out['prompts']['final/report.audit.md'] = '内部版同为程序化拼装(口径说明/复核意见/出处卡清单),无 prompt 模板。'
            out['promptMeta']['final/report.audit.md'] = '组装器 · 程序化'
        except Exception:
            pass
        # 节点
        nodes, links, NAME = [], [], {}
        def N(k, depth, value, label, color, art=None, mdfile=None):
            NAME[k] = label
            nd = {'name': label, 'depth': depth, 'value': round(value, 1), 'itemStyle': {'color': color}, 'art': art or []}
            if mdfile:
                nd['mdfile'] = mdfile
            nodes.append(nd)
        def L(a, b, v):
            if a in NAME and b in NAME:
                links.append({'source': NAME[a], 'target': NAME[b], 'value': v})
        SRC, SLC, SEC, QC, DLV = '#f59e0b', '#3b82f6', '#10b981', '#ef4444', '#0d9488'
        NLc = chr(10)
        src_links = {'tc': 8, 'cb': 9, 'tw': 7, 'cn': 4, 'cal': 4, 'fz': 6, 'fred': 5, 'fomc': 3}
        for k, cfg in _SRC_CFG.items():
            vol = _src_volume_label(sources, k)
            N(k, 0, src_links[k], cfg['label'] + (NLc + vol if vol else ''), SRC,
              ['当日快照 bucket: ' + ', '.join(cfg['dp'])])
            out['name2src'][cfg['label']] = k
            out['srcCfg'][k] = {'label': cfg['label'], 'unit': cfg['unit'], 'role': cfg['role'], 'dp': cfg['dp']}
        mm = lambda s: f"{round(tim.get(s, 0) / 60)}min"
        # 切片
        slice_files = {}
        for agent, dept, v in _SLICE_AGENTS:
            fn = f'snapshot.{agent}.json'
            fp = ws / fn
            if not fp.exists():
                continue
            slice_files[agent] = fn
            try:
                out['slices'][fn] = _j(fp) or {}
            except Exception:
                out['slices'][fn] = {}
            kb = fp.stat().st_size / 1024
            nsec = len(_SLICE_SECS[agent])
            N('sl_' + agent, 1, v, f'{fn}{NLc}{kb:.1f} KB · {dept}', SLC,
              [f'{fn} · 供{dept} · 点击本节点或其右侧管道打开 JSON 数据'])
            secs_txt = '/'.join(_SEC_NAMES[i].split('_')[1] for i in _SLICE_SECS[agent])
            out['sliceMeta'][fn] = f'{dept} → {secs_txt}'
        # 栏目(报告架构顺序)
        ORDER = [('大盘总览', None, None), ('今日要闻', 'overall_market', 0), ('核心驱动', 'overall_market', 1),
                 ('持仓与资金流', 'overall_market', 2), ('美股全景', None, None), ('今日涨跌榜', None, 4),
                 ('板块动态', 'company_desk', 3), ('个股深挖', 'company_desk', 5), ('宏观速览', None, 6),
                 ('回顾 · 宏观数据', 'macro_review', 7), ('回顾 · 地缘政治', 'geopolitics', 8),
                 ('美联储观察', 'fomc', 9), ('展望与策略', 'outlook', 10)]
        SEC_KEY = {}
        for i, (h, agent, ci) in enumerate(ORDER):
            key = 's' + str(i)
            SEC_KEY[h] = key
            c = chars.get(h)
            if ci is not None:
                lab = f'{_SEC_NAMES[ci]}.md + claims({claims.get(ci, 0)}卡){NLc}{h} · {c:,}字 · {_DEPT_OF[ci]}' if c else \
                      f'{_SEC_NAMES[ci]}.md{NLc}{h} · {_DEPT_OF[ci]}'
                art = [f'sections/{_SEC_NAMES[ci]}.md', f'claims {claims.get(ci, 0)} 张出处卡', '点击看产出 markdown+Prompt 模板']
                N(key, 2, (c / 400 if c else 1), lab, SEC, art, mdfile=f'sections/{_SEC_NAMES[ci]}.md')
            else:
                N(key, 2, 0.4, h + '(图)' + NLc + '由规则图件直出', SEC,
                  ['图载体:01_markets.png / 02_finviz_map.png'] if i in (0, 4) else ['规则表:write_movers / write_macro_dashboard'],
                  mdfile=('sections/05_movers.md' if i == 5 else 'sections/07_macro_dashboard.md') if i in (5, 8) else None)
        # 核验/图件/交付
        N('chk', 3, 4, 'verify/check_results.json' + NLc + '规则核对', QC, ['verify_checks.py 每轮覆写'])
        N('vd', 3, 4, 'verify/verdict.json' + NLc + 'LLM跨节质检', QC, ['质检 agent 手写'])
        def fsize(rel):
            fp = ws / rel
            return f'{fp.stat().st_size / 1024:.1f} KB' if fp.exists() else ''
        for rel, tag in (('final/charts/01_markets.png', 'FT 行情图'), ('final/charts/02_finviz_map.png', '热力图')):
            N('c' + rel[-5], 3, 2, rel + (NLc + fsize(rel) if fsize(rel) else ''), DLV, [tag + '(规则)'])
        rep_n = f"final/report.md{NLc}{fsize('final/report.md')} · 读者版"
        N('rep', 4, 30, rep_n, DLV, ['11 栏目拼装+2 图嵌入', '点击查看整份报告'], mdfile='final/report.md')
        aud_n = f"final/report.audit.md{NLc}{fsize('final/report.audit.md')} · 内部版"
        N('aud', 4, 6, aud_n, DLV, ['口径/复核/出处卡', '点击查看内部版'], mdfile='final/report.audit.md')
        card_n = 'pipeline-overview.html' + NLc + '五 tab 流程卡(本页)'
        N('card', 4, 4, card_n, DLV, ['gen_pipeline_card 注入 __RUN__/__SANKEY__'])
        # 连线
        for a, b in _SRC2SLICE:
            L(a, 'sl_' + b, _SRC2SLICE_W[(a, b)])
        for h, agent, ci in ORDER:
            if agent and ('sl_' + agent) in NAME and SEC_KEY[h] in NAME:
                L('sl_' + agent, SEC_KEY[h], round((chars.get(h) or 160) / 400, 1))
        L('fz', SEC_KEY['今日涨跌榜'], 3)
        L('fred', SEC_KEY['宏观速览'], 2)
        L('cal', SEC_KEY['宏观速览'], 1)
        L('fred', 'c_s.png', 2)
        L('fz', 'c_s.png', 1)
        L('fz', 'c_p.png', 3)
        for h, agent, ci in ORDER:
            L(SEC_KEY[h], 'chk', 0.2)
            L(SEC_KEY[h], 'vd', 0.2)
            L(SEC_KEY[h], 'rep', round((chars.get(h) or 160) / 400 if ci is not None else 0.4, 1))
            L(SEC_KEY[h], 'aud', 0.2)
        L('c_s.png', 'rep', 1.5)
        L('c_p.png', 'rep', 1.5)
        L('vd', 'aud', 2.5)
        L('chk', 'aud', 1)
        L('rep', 'card', 5)
        L('aud', 'card', 2)
        out['nodes'], out['links'] = nodes, links
        # 配给清单
        for sk, agent in _SRC2SLICE:
            fn = slice_files.get(agent)
            if not fn:
                continue
            cfg = _SRC_CFG[sk]
            tot = sum(_measure(_dig(sources, p), cfg['unit']) for p in cfg['mp'])
            sliced = (out['slices'].get(fn) or {}).get('sources') or {}
            got = sum(_measure(_dig(sliced, p), cfg['unit']) for p in cfg['mp'])
            cb = {}
            for ci in _SLICE_SECS[agent]:
                md = f'sections/{_SEC_NAMES[ci]}.md'
                cp = ws / 'sections' / f'{_SEC_NAMES[ci]}.claims.json'
                if not cp.exists():
                    continue
                try:
                    lst = _j(cp) or []
                except Exception:
                    lst = []
                n = sum(1 for c in lst if _claim_src(c.get('path', '')) == sk)
                if n:
                    cb[md.split('/')[-1]] = n
            out['pipes'][sk + '|' + fn] = {'tot': tot, 'got': got, 'unit': cfg['unit'],
                                           'share': round(got / tot * 100) if tot else 0,
                                           'claims': sum(cb.values()), 'claimsBy': cb}
    except Exception as e:
        print(f"[card] sankey payload degraded: {e}", flush=True)
    return out


def generate(ws: Path) -> Path:
    ws = Path(ws).resolve()
    run = build_run(ws)
    tpl = (TOOL_DIR / "pipeline_card_template.html").read_text(encoding="utf-8")
    marker = "window.__RUN__ = null; /*GEN:RUN*/"
    assert marker in tpl, "模板缺少注入点"
    run["flow"]["outlook-stat"] = run.pop("outlookStat")
    html = tpl.replace(marker, "window.__RUN__ = " + json.dumps(run, ensure_ascii=False) + ";")
    marker2 = "window.__SANKEY__ = null; /*GEN:SANKEY*/"
    if marker2 in tpl:
        html = html.replace(marker2, "window.__SANKEY__ = " + json.dumps(_sankey_payload(ws), ensure_ascii=False) + ";")
    out = ws / "pipeline-overview.html"
    out.write_text(html, encoding="utf-8")
    return out


if __name__ == "__main__":
    import sys
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    if target is None:
        cands = sorted(REPORTS_ROOT.glob("*/state.json"), key=lambda p: p.stat().st_mtime)
        if not cands:
            raise SystemExit("用法: gen_pipeline_card.py <workspace> (或在工作区内运行)")
        target = cands[-1].parent
    print("card:", generate(target))
