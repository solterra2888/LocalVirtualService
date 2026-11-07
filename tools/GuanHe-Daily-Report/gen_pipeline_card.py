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
REPORTS_ROOT = TOOL_DIR.parents[1] / "backend" / "reports" / "guanhe-daily-report"

AGENT_LABEL = {
    "overall_market": "叙事组(叙事/驱动/持仓)", "company_desk": "公司与板块部",
    "macro_review": "宏观数据部", "geopolitics": "地缘政治部", "fomc": "美联储观察部",
    "outlook": "展望与策略",
}

# 模板 SECTIONS(13) 与 sections/*.md(01-11) 的对应;None=非 md 栏目
SEC_MAP = [None, "01_narrative", "02_drivers", "03_positioning", None, "05_movers",
           "04_sector_action", "06_deep_dives", "07_macro_dashboard", "08_macro_review",
           "09_geopolitics", "10_fomc", "11_outlook"]
SEC_NAMES = ["大盘总览", "今日叙事", "核心驱动", "持仓与资金流", "美股全景", "今日涨跌榜",
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
        claims.append({"sec": {"01_narrative": "今日叙事", "02_drivers": "核心驱动",
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
        "header-time": _header_time(ws, total_s),
        "raw": f"今日实测:约 {raw_total:,} 条原料 + {(tc_len + cb_len) / 1e4:.1f} 万字节目文字稿",
        "plan": f"今日实测:一次成稿,{_fmt_k(tok('plan'))} tokens",
        "stage1": f"{round(stage1 / 60)} 分钟<br>(最慢编辑部)",
        "outlook": f"{round((outlook_s or 0) / 60)} 分钟",
        "verify": ("今日实测:首轮查出 %d 处不合格,%d 轮重写后%s" % (highs[0] if highs else 0, len(rounds),
                    "带清单发布" if degraded else "全部修复")) if highs else "今日实测:核验完成",
        "final": f"今日实测:11 个栏目 {chars_total:,} 字 · 全程零人工 · {round((total_s or 0) / 60)} 分钟",
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


def generate(ws: Path) -> Path:
    ws = Path(ws).resolve()
    run = build_run(ws)
    tpl = (TOOL_DIR / "pipeline_card_template.html").read_text(encoding="utf-8")
    marker = "window.__RUN__ = null; /*GEN:RUN*/"
    assert marker in tpl, "模板缺少注入点"
    run["flow"]["outlook-stat"] = run.pop("outlookStat")
    html = tpl.replace(marker, "window.__RUN__ = " + json.dumps(run, ensure_ascii=False) + ";")
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
