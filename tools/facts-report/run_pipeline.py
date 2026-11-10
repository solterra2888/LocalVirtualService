#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""facts-report:GuanHe-Daily-Report 的事实/观点分离变体(郑翔需求 痛点 2)。

与日报同一套编排,差别只在内容规则(见 README):01 节「今日要闻」只写事实;
各节嘉宾/分析师观点收进块末「观点：」行。快照由本管线 build_snapshot 自建。

以下为原编排说明(v4)。

流程(详见 tools/US-market-report/README.md):
    Stage 0   build_snapshot.py 产出的冻结快照(本脚本不重建,只校验存在)
    Stage 1-6 三组章节 Agent 并行(节目转录 + 新闻层为素材,无摘要):
              overall_market(The Close) / company_desk(Closing Bell 主段+OT)
               / macro;今日涨跌榜(美股/港股/A股成交额榜)由编排器按冻结
              数据确定性渲染;macro 块矩阵由
              编排器按美国一线白名单 / 极端行情阈值纯代码判定后注入任务书
    Stage 7a  确定性 verify_checks.py(数字/三元组/since/概率/矩阵/结构)
    Stage 7b  核验 Agent:叙事质量审查 → verify/verdict.json
    Gate      纯 Python 门控: pass → 拼接发布(审计注释+口径块进 sidecar);
              fail → 按问题清单 --resume 修订对应章节再核验(≤ N 轮);
              仍 fail → 退出码 2 转人工

设计规则: 文件即契约、步级幂等、门控不经过 LLM、全审计落盘。

用法:
    python tools/US-market-report/run_pipeline.py 2026-10-24 [--style en|cn]
    python tools/US-market-report/run_pipeline.py 2026-10-24 --fake-zcode

退出码: 0=已发布  1=运行错误  2=核验未通过,待人工
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess

# 2026-11-04:禁用 vfork(同 build_snapshot.py 注释;run_agent 的 zcode 调用
# 依赖 subprocess,同一风险面)
try:
    subprocess._USE_VFORK = False  # type: ignore[attr-defined]
except AttributeError:
    pass
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prompts import (  # noqa: E402
    AGENT_ORDER,
    AGENT_STAGES,
    SECTION_ORDER,
    SECTION_SPECS,
    PLAN_OWNER_ROLES,
    STYLE_PROFILES,
    agent_section_names,
    annotate_event_units,
    assign_item_ids,
    agent_snapshot_filter,
    build_agent_prompt,
    event_unit,
    flag_reported_items,
    fmt_event_value,
    build_light_verifier_prompt,
    build_plan_prompt,
    build_verifier_prompt,
    fake_section_text,
)
from verify_checks import claims_path, run_checks, verified_canonicals  # noqa: E402
from provenance import item_index, render_section  # noqa: E402

DEFAULT_ZCODE_BIN = "/root/.zcode/server/bin/zcode"
DEFAULT_TIMEOUT_SEC = 1800  # v7 任务书更重:company_desk 连续两次撞 1200s 线(1169/1200.1s),留余量
DEFAULT_MAX_VERIFY_RETRIES = 2
DEFAULT_STYLE = "cn"
DONE_MARKER = "<!-- SECTION DONE -->"
HEADLINE_RE = re.compile(
    r"<!--\s*HEADLINE_CANDIDATES\s*\n(.*?)-->", re.DOTALL)


# ── workspace & state ──────────────────────────────────────────────────

REPORTS_DIR = "facts-report"


def workspace(report_date: str) -> Path:
    return REPO_ROOT / "backend" / "reports" / REPORTS_DIR / report_date


def load_state(ws: Path) -> Dict[str, Any]:
    f = ws / "state.json"
    if f.exists():
        return json.loads(f.read_text(encoding="utf-8"))
    return {"sections": {}, "verify": {"attempts": 0, "status": "pending"},
            "usage_total": {}, "published": False}


def save_state(ws: Path, state: Dict[str, Any]) -> None:
    (ws / "state.json").write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _add_usage(state: Dict[str, Any], usage: Optional[Dict[str, Any]],
               phase: Optional[str] = None) -> None:
    """累计 token;phase(plan/write/verify/revise)另记分项,看核验+修订占比。"""
    if not usage:
        return
    buckets = [state.setdefault("usage_total", {})]
    if phase:
        buckets.append(state.setdefault("usage_by_phase", {}).setdefault(phase, {}))
    for tot in buckets:
        for k in ("inputTokens", "outputTokens", "cacheReadTokens"):
            tot[k] = tot.get(k, 0) + (usage.get(k) or 0)


def _record_timing(state: Dict[str, Any], key: str, seconds: float) -> None:
    """每步耗时落 state.json(秒;同名步骤多次运行追加)。"""
    state.setdefault("timings", {}).setdefault(key, []).append(round(seconds, 1))


# ── zcode headless invocation ──────────────────────────────────────────

def _parse_zcode_json(stdout: str) -> Dict[str, Any]:
    """zcode --json 输出单个 JSON 对象;容错地截取首个 { 到末个 }。"""
    stdout = stdout.strip()
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        pass
    start, end = stdout.find("{"), stdout.rfind("}")
    if start >= 0 and end > start:
        return json.loads(stdout[start : end + 1])
    raise RuntimeError(f"cannot parse zcode output: {stdout[:300]!r}")


def run_agent(
    zcode_bin: str,
    ws: Path,
    prompt: str,
    snapshot: Optional[Path],
    timeout: int,
    resume_session: Optional[str] = None,
    extra_attachments: Optional[List[Path]] = None,
) -> Dict[str, Any]:
    """一次无头 ZCode 调用。返回解析后的 JSON(sessionId/response/usage)。

    注意: 超时(TimeoutExpired)直接向上抛;调用方按各自的"文件契约"决定
    超时后是否仍可接受 —— Agent 写完产物文件后进程可能迟迟不退出,
    产物有效性以文件为准,不以进程退出为准。
    """
    cmd = [zcode_bin, "-p", prompt, "--json", "--mode", "yolo",
           "--surface", "terminal", "--cwd", str(ws)]
    if snapshot is not None:  # 轻量核验不挂快照
        cmd += ["--attach", str(snapshot)]
    for p in extra_attachments or []:
        if p.exists():
            cmd += ["--attach", str(p)]
    if resume_session:
        cmd += ["--resume", resume_session]
    # v8.5-fix(P8/P23):非零退出重试 1 次、20s 退避 —— provider/SQLite 锁类
    # 瞬时故障一天可放倒 2-3 次调用;TimeoutExpired 不重试(文件契约由调用方判定)
    last_err: Optional[Exception] = None
    for attempt in (1, 2):
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=timeout)
        if proc.returncode == 0:
            return _parse_zcode_json(proc.stdout)
        last_err = RuntimeError(
            f"zcode exited {proc.returncode}: {proc.stderr[-500:]}")
        if attempt == 1:
            print(f"[agent] transient failure, retrying once in 20s "
                  f"({str(last_err)[:120]})", flush=True)
            time.sleep(20)
    raise last_err  # type: ignore[misc]


# ── fake mode(无 ZCode 时测试状态机/门控/重试) ────────────────────────

def _fake_section(ws: Path, name: str, corrected: bool = False,
                  blocks: Optional[Dict[str, Any]] = None,
                  style: str = "en") -> None:
    """占位章节:故意让 drivers 含一个与快照矛盾的数字,供核验流程抓出来。"""
    f = ws / SECTION_SPECS[name]["output_file"]
    f.parent.mkdir(parents=True, exist_ok=True)
    text = fake_section_text(name, corrected=corrected)
    if name == "macro_review" and not (blocks or {}).get("review_releases"):
        text = f"## 回顾 · 宏观数据\n\n{STYLE_PROFILES[style]['missing_mark']}\n"
    if style == "cn":
        text = text.replace("Placeholder headline clause. Other clause.",
                            "占位标题一。占位标题二。")
    f.write_text(text + f"\n{DONE_MARKER}\n", encoding="utf-8")
    claims_path(ws, name).write_text("[]\n", encoding="utf-8")


# ── v8 Macros 块矩阵 + movers/dashboard 确定性渲染 ─────────────────────

from datetime import date as _date_cls, timedelta as _timedelta_cls

# 边际变化选题(v8):四经济体 T 日有 actual 的一线事件,按族优先级每国 ≤2。
# 事件名以 EODHD 日历为准;分项(Ex-*/Non-Defense)不作主行;yoy 优先于水平。
REVIEW_COUNTRIES = ("US", "EU", "CN", "JP")
REVIEW_FAMILY_ORDER = (
    ("rate", r"fed\s+interest\s+rate|deposit\s+facility|boj\s+interest|loan\s+prime"),
    ("jobs", r"non[\s\-]?farm\s+payroll|unemployment\s+rate"),
    ("cpi", r"inflation\s+rate|\bcpi\b|hicp|harmonised"),
    ("pce", r"\bpce\b"),
    ("gdp", r"gdp\s+growth"),
    ("jolts", r"job\s+openings"),
    ("pmi", r"\bpmi\b"),
    ("ppi", r"\bppi\b|producer\s+price"),
    ("retail", r"retail\s+sales"),
    ("ip", r"industrial\s+production"),
    ("trade", r"balance\s+of\s+trade"),
)
_REVIEW_FAMS = [(k, re.compile(m, re.I)) for k, m in REVIEW_FAMILY_ORDER]
# 分项/子口径不作主行(Durable Goods Ex Transp / Ex Defense 等)
_SUBCOMPONENT_RE = re.compile(r"\bex\b|excluding|non[\s\-]?defense|\bnsa\b", re.I)
# 同一事件多口径行时的取舍:yoy 优先(通胀/增长头条惯例),水平次之,mom 最后
_COMPARISON_RANK = {"yoy": 0, "": 1, None: 1, "index": 1, "qoq": 1, "mom": 2}


def _event_date(e: Dict[str, Any]) -> str:
    return str(e.get("date") or "")[:10]


def _has_actual(e: Dict[str, Any]) -> bool:
    return e.get("actual") not in (None, "")


def _cmp_rank(e: Dict[str, Any]) -> int:
    return _COMPARISON_RANK.get((e.get("comparison") or "").lower() or None, 1)


def select_country_releases(snapshot: Dict[str, Any],
                            per_country: int = 2) -> Dict[str, List[Dict[str, Any]]]:
    """边际变化选题:四经济体 T 日一线、含 actual 的发布,每国 ≤2 个事件族。

    按族优先级取(利率决议 > 就业 > CPI > GDP > PMI > PPI > 零售 > 工业 >
    贸易);每族只取一行主行(排除分项;yoy 优先)。返回 {country: [row…]},
    供 08 任务书点名注入。
    """
    report_date = snapshot.get("report_date") or ""
    recent = ((snapshot.get("sources") or {}).get("economic_events") or {}).get(
        "recent") or []
    tday = [e for e in recent
            if e.get("country") in REVIEW_COUNTRIES
            and _event_date(e) == report_date]
    out: Dict[str, List[Dict[str, Any]]] = {}
    for country in REVIEW_COUNTRIES:
        picked: List[Dict[str, Any]] = []
        for key, rx in _REVIEW_FAMS:
            cands = [e for e in tday
                     if e.get("country") == country
                     and rx.search(e.get("event") or "")
                     and not _SUBCOMPONENT_RE.search(e.get("event") or "")]
            mains = [e for e in cands if _has_actual(e)]
            if not mains:
                continue
            mains.sort(key=lambda e: (_cmp_rank(e), len(e.get("event") or "")))
            best = mains[0]
            row = {k: best[k] for k in
                   ("event", "comparison", "period", "date", "actual",
                    "forecast", "previous", "time_et")
                   if best.get(k) not in (None, "")}
            row["family"] = key
            picked.append(row)
            if len(picked) >= per_country:
                break
        if picked:
            out[country] = picked
    return out


# 地缘主题与焦点事件判定(素材 = 快照宏观地缘素材层 geo,中英关键词统计)
GEO_THEME_DEFS = (
    ("美伊局势", re.compile(
        r"伊朗|霍尔木兹|波斯湾|胡塞|也门|德黑兰"
        r"|Iran|Hormuz|Persian Gulf|Houthi|Yemen|Tehran", re.I)),
    ("俄乌冲突", re.compile(
        r"俄乌|乌克兰|普京|泽连斯基|克里姆林宫|停火谈判"
        r"|Ukraine|Putin|Zelensky|Kremlin", re.I)),
    ("中东局势", re.compile(
        r"加沙|以色列|内塔尼亚胡|黎巴嫩|真主党|哈马斯"
        r"|Gaza|Israel|Netanyahu|Lebanon|Hezbollah|Hamas", re.I)),
    ("贸易与制裁", re.compile(
        r"关税|制裁|出口管制|贸易战|tariff|sanction|export control", re.I)),
)
GEO_THEME_MIN_HITS = 3
FOCAL_EVENT_RE = re.compile(
    r"选举|大选|投票|公投|弹劾|组阁|就职|中期选举"
    r"|election|referendum|ballot|impeach", re.I)
FOCAL_MIN_HITS = 2


def _geo_stats(snapshot: Dict[str, Any]) -> tuple:
    """统计地缘主题与焦点事件命中(附 top 条目摘要供任务书选题参考)。

    v8.2:条目带 theme(triage 语义分拣)时按 theme 分组 —— 主题开放集,
    新事件天然可开块;无 theme 的条目(正则回退路径产物)才走 GEO_THEME_DEFS
    正则归类。focal(选举等一次性事件)仍按关键词在已选条目上做二次识别。
    """
    geo = (((snapshot.get("sources") or {}).get("news_macro") or {})
           .get("geo") or [])

    def _blob(g: Dict[str, Any]) -> str:
        return f"{g.get('title', '')} {g.get('text', '')}"

    def _top(rows: List[Dict[str, Any]], n: int = 3) -> List[str]:
        return [f"{g.get('time_cn', '')} {str(g.get('title', ''))[:40]}".strip()
                for g in rows[:n]]

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for g in geo:
        name = (g.get("theme") or "").strip()
        if not name:  # 正则回退路径:无主题标签 → 关键词归类
            for name, rx in GEO_THEME_DEFS:
                if rx.search(_blob(g)):
                    break
            else:
                name = ""
        if name:
            grouped.setdefault(name, []).append(g)
    opened = {name: rows for name, rows in grouped.items()
              if len(rows) >= GEO_THEME_MIN_HITS}
    themes = {name: {"hits": len(rows), "top": _top(rows)}
              for name, rows in opened.items()}
    focal = [g for g in geo if FOCAL_EVENT_RE.search(_blob(g))]
    # 焦点事件的条目多数已归入某个已开主题(triage 常把选举直接开成主题)时,
    # 再开「焦点事件」块只会把同一事件写两遍
    covered = any(
        sum(1 for g in focal if any(g is r for r in rows)) * 2 >= len(focal)
        for rows in opened.values()) if focal else False
    focal_events: List[Dict[str, Any]] = []
    if len(focal) >= FOCAL_MIN_HITS and not covered:
        focal_events.append({"name": "选举与权力更替", "hits": len(focal),
                             "top": _top(focal)})
    return themes, focal_events


def macro_blocks(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """v8 Macros 块矩阵 —— 纯代码判定,对齐 xiao 式参考稿的回顾/当下/展望。

    review_releases  四经济体 T 日边际变化选题(每国 ≤2 族)
    geo_themes       地缘主题(快照 geo 层命中 ≥3 开块,附 top 条目)
    focal_events     焦点事件(选举等一次性政治事件,命中 ≥2 开块)
    fomc_deep        决议日在 [T-2, T] → 10 节开「决议深读」
    outlook          恒 True(11 节第二段综合)
    """
    review = select_country_releases(snapshot)
    themes, focal = _geo_stats(snapshot)
    fomc_deep = False
    decisions = (((snapshot.get("sources") or {}).get("fomc") or {})
                 .get("decisions") or [])
    if decisions:
        try:
            dd = _date_cls.fromisoformat(str(decisions[0].get("decision_date"))[:10])
            t = _date_cls.fromisoformat(snapshot.get("report_date") or "")
            fomc_deep = _timedelta_cls(0) <= (t - dd) <= _timedelta_cls(days=2)
        except ValueError:
            fomc_deep = False
    return {
        "review_releases": review,
        "geo_themes": themes,
        "focal_events": focal,
        "fomc_deep": fomc_deep,
        "outlook": True,
    }


# 今日涨跌榜(v8.6):美股/港股/A股成交额榜。三榜在 Stage 0 由 build_snapshot
# 冻结进 market.volume_leaders(美股=finviz 成分股近似,港/A股=EODHD bulk),
# 编排器只渲染不取数,不经 LLM、无台账。
VL_MARKETS = (("us", "美股", "亿美元"), ("hk", "港股", "亿港元"),
              ("cn", "A股", "亿元"))


def _vl_code(row: Dict[str, Any]) -> str:
    """EODHD 内部后缀(SHG/SHE/HK)→ 中文行情惯例的展示码。"""
    code, ex = str(row.get("code", "")), row.get("exchange")
    if ex == "US":
        return code
    if ex == "HK":
        return f"{code.zfill(4)}.HK"
    if ex == "SHG":
        return f"{code}.SH"
    if ex == "SHE":
        return f"{code}.SZ"
    return code


def render_movers(snapshot: Dict[str, Any]) -> str:
    vl = ((snapshot.get("sources") or {}).get("market") or {}
          ).get("volume_leaders")
    if not vl:
        # 旧快照无 volume_leaders 块 —— 如实说明缺失,不编榜单(勿用禁词)
        return "## 今日涨跌榜\n\n当日成交额榜数据不可用,本表从略。\n"
    parts = ["## 今日涨跌榜", "",
             "美股 ≥10 亿美元、港股 ≥10 亿港元、A股 ≥20 亿元人民币,"
             "按当日成交额降序,各取前 8。", ""]
    for key, label, unit in VL_MARKETS:
        m = vl.get(key) or {}
        parts += [f"### {label}", ""]
        rows = m.get("rows") or []
        if not rows:
            parts += [m.get("note") or "当日无成交额达标个股。", ""]
            continue
        parts += ["| 代码 | 公司 | 收盘价 | 涨跌幅 | 成交额 |",
                  "|---|---|---|---|---|"]
        for r in rows:
            chg = r.get("change_percent")
            chg_s = f"{chg:+.2f}%" if isinstance(chg, (int, float)) else "—"
            price = r.get("price")
            price_s = f"{float(price):,.2f}" if price not in (None, "") else "—"
            name = r.get("name_cn") or r.get("name", "")
            parts.append(f"| {_vl_code(r)} | {name} | {price_s} "
                         f"| {chg_s} | {r.get('turnover', 0) / 1e8:.1f}{unit} |")
        parts += [""]
    return "\n".join(parts)


def write_movers(ws: Path, snapshot: Dict[str, Any]) -> None:
    f = ws / SECTION_SPECS["movers"]["output_file"]
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(render_movers(snapshot).rstrip() + f"\n{DONE_MARKER}\n",
                 encoding="utf-8")
    claims_path(ws, "movers").write_text("[]\n", encoding="utf-8")


# ── 07 宏观速览:确定性渲染(零 LLM,同 movers 模式;无台账) ────────────

DASH_COUNTRY_CN = {"US": "美国", "EU": "欧元区", "CN": "中国", "JP": "日本"}
DASH_COMMODITIES = (
    ("BZ1.COMMODITY", "Brent 原油(期货)"),
    ("CL1.COMMODITY", "WTI 原油(期货)"),
    ("GC1.COMMODITY", "COMEX 黄金(期货)"),
    ("SI1.COMMODITY", "COMEX 白银(期货)"),
    ("HG1.COMMODITY", "COMEX 铜(期货)"),
)
_CN_EVENT_SHORT = {
    "Inflation Rate": "CPI", "Harmonised Inflation Rate": "HICP",
    "Core Inflation Rate": "核心CPI", "Core PCE Price Index": "核心PCE",
    "Producer Price Index": "PPI", "Tokyo CPI": "东京CPI",
    "Tokyo Core CPI": "东京核心CPI", "CPI": "CPI",
}
_CN_WEEKDAY = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")
_CN_COUNTRY = {"US": "美", "EU": "欧", "CN": "中", "JP": "日", "DE": "德", "KR": "韩"}
FWD_KEY_RE = re.compile(
    r"interest\s+rate|fomc|payroll|cpi|inflation|hicp|gdp|pmi|unemployment"
    r"|pce|ppi|retail\s+sales|loan\s+prime|lpr", re.I)
FWD_PER_DAY = 6
_FWD_CMP_RANK = {"yoy": 0, "mom": 1, "qoq": 1}
_FWD_CMP_CN = {"yoy": "同比", "mom": "环比", "qoq": "环比"}


def _fmt_num(v: Any) -> str:
    try:
        f = float(str(v).replace(",", ""))
        return f"{f:.2f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return str(v)


def _dash_growth(rows: Optional[List[Dict[str, Any]]]) -> str:
    """增长格:同比与环比并列(口径不同都是头条惯例);无行则 – 。"""
    if not rows:
        return "–"
    parts = []
    for cmp_, lbl in (("yoy", "同比"), ("qoq", "环比")):
        r = next((x for x in rows if (x.get("comparison") or "") == cmp_), None)
        if r:
            parts.append(f"{lbl} {_fmt_num(r.get('actual'))}%")
    if not parts:
        r = rows[0]
        parts.append(f"{_fmt_num(r.get('actual'))}%")
    period = next((x.get("period") for x in rows if x.get("period")), "")
    return " / ".join(parts) + (f"({period})" if period else "")


def _dash_inflation(rows: Optional[List[Dict[str, Any]]]) -> str:
    """通胀格:两行 —— 首行 yoy 头条;次行优先不同事件(PPI/核心),否则环比。

    跳过无 comparison 的行(CPI s.a 指数水平,如 336.94,不是百分比)。
    """
    if not rows:
        return "–"
    usable = [r for r in rows if r.get("comparison") in ("yoy", "mom")]
    if not usable:
        return "–"

    def _cell(r: Dict[str, Any]) -> str:
        nm = _CN_EVENT_SHORT.get(r.get("event") or "", r.get("event") or "")
        cmp_ = "同比" if (r.get("comparison") or "") == "yoy" else "环比"
        return f"{nm} {cmp_} {_fmt_num(r.get('actual'))}%"

    first = usable[0]
    second = next((r for r in usable[1:]
                   if r.get("event") != first.get("event")), None)
    parts = [_cell(first)] + ([_cell(second)] if second else [])
    period = next((x.get("period") for x in usable if x.get("period")), "")
    return " / ".join(parts) + (f"({period})" if period else "")


def _rate_action(actual: Any, previous: Any) -> str:
    """利率动作:与 previous 相比为维持/加息Nbp/降息Nbp(数值单位是百分比)。"""
    try:
        a, p = float(actual), float(previous)
    except (TypeError, ValueError):
        return ""
    if abs(a - p) < 1e-9:
        return "维持"
    d = (a - p) * 100
    return f"{'加息' if d > 0 else '降息'}{abs(d):.0f}bp"


def _dash_policy(country: str, rows: Optional[List[Dict[str, Any]]],
                 fomc: Dict[str, Any]) -> str:
    """政策利率格:美国用 fomc.decisions(含投票比);欧中日用日历决议行。"""
    if country == "US":
        d = ((fomc.get("decisions") or [{}])[0])
        low, high = d.get("rate_low_bp"), d.get("rate_high_bp")
        if low is None or high is None:
            return "–"
        chg = d.get("change_bp") or 0
        act = "维持" if not chg else (
            f"{'加息' if chg > 0 else '降息'}{abs(chg):.0f}bp")
        vote = ""
        if d.get("vote_for") is not None and d.get("vote_against") is not None:
            vote = f",{int(d['vote_for'])}:{int(d['vote_against'])}"
        when = str(d.get("decision_date") or "")[5:].replace("-", "/")
        return f"{low / 100:.2f}%~{high / 100:.2f}%({when}{act}{vote})"
    if not rows:
        return "–"
    r = rows[0]
    act = _rate_action(r.get("actual"), r.get("previous"))
    when = str(r.get("date") or "")[5:].replace("-", "/")
    cell = f"{_fmt_num(r.get('actual'))}%"
    if act:
        cell += f"({when}{act})"
    return cell


_FWD_EVENT_CN = {
    "Initial Jobless Claims": "初请失业金人数", "Continuing Jobless Claims": "续请失业金人数",
    "Non Farm Payrolls": "非农就业", "ADP Employment Change": "ADP就业",
    "Unemployment Rate": "失业率", "Average Hourly Earnings": "平均时薪",
    "JOLTs Job Openings": "JOLTS职位空缺", "Inflation Rate": "CPI",
    "Core Inflation Rate": "核心CPI", "Harmonised Inflation Rate": "HICP",
    "Core Harmonised Inflation Rate": "核心HICP", "PCE Price Index": "PCE物价",
    "Core PCE Price Index": "核心PCE", "PPI": "PPI", "Producer Price Index": "PPI",
    "Core PPI": "核心PPI", "Retail Sales": "零售销售", "Retail Sales Ex Autos": "核心零售",
    "GDP Growth Rate": "GDP", "GDP Growth Rate Annualized": "GDP(年化)",
    "Industrial Production": "工业产出", "Balance of Trade": "贸易差额",
    "Exports": "出口", "Imports": "进口", "Current Account": "经常账户",
    "Foreign Exchange Reserves": "外汇储备", "Interest Rate Decision": "利率决议",
    "Fed Interest Rate Decision": "美联储利率决议", "ECB Interest Rate Decision": "欧央行利率决议",
    "BoJ Interest Rate Decision": "日央行利率决议", "Loan Prime Rate 1Y": "1年期LPR",
    "Loan Prime Rate 5Y": "5年期LPR", "Michigan Consumer Sentiment": "密歇根消费者信心",
    "CB Consumer Confidence": "谘商会消费者信心",
    "ISM Manufacturing PMI": "ISM制造业PMI", "ISM Services PMI": "ISM服务业PMI",
    "S&P Global Manufacturing PMI": "标普制造业PMI", "S&P Global Services PMI": "标普服务业PMI",
    "S&P Global Composite PMI": "标普综合PMI", "NBS Manufacturing PMI": "统计局制造业PMI",
    "NBS Non Manufacturing PMI": "统计局非制造业PMI", "HCOB Manufacturing PMI": "HCOB制造业PMI",
    "HCOB Services PMI": "HCOB服务业PMI", "Ifo Business Climate": "Ifo商业景气",
    "ZEW Economic Sentiment Index": "ZEW经济景气", "Tokyo CPI": "东京CPI",
    "Tokyo Core CPI": "东京核心CPI", "EIA Crude Oil Stocks Change": "EIA原油库存",
    "Baker Hughes Oil Rig Count": "贝克休斯石油钻井数",
    "2-Year Note Auction": "2年期美债拍卖", "5-Year Note Auction": "5年期美债拍卖",
    "10-Year Note Auction": "10年期美债拍卖", "30-Year Bond Auction": "30年期美债拍卖",
    "30-Year Bund Auction": "30年期德债拍卖", "FOMC Minutes": "FOMC会议纪要",
}
_FWD_REGION_CN = {"US": "美国", "EU": "欧元区", "CN": "中国", "JP": "日本",
                  "DE": "德国", "KR": "韩国"}
_MONTH_CN = {m: i + 1 for i, m in enumerate(
    ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"))}
_MONTH_EN_FULL = {m: i + 1 for i, m in enumerate(
    ("january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"))}
# 无预期/前值的讲话、会议、报告对读者没有可比数字,不进前瞻表
_FWD_NOISE_RE = re.compile(r"speech|speaks|testimony|meeting|summit|report|hearing", re.I)


def _fwd_period(period: Any) -> str:
    """Oct → 10月;Oct/31 → 10/31当周;Q3 原样。"""
    p = str(period or "").strip()
    m = re.fullmatch(r"([A-Za-z]{3})(?:/(\d{1,2}))?", p)
    if not m or m.group(1).lower() not in _MONTH_CN:
        return p
    mon = _MONTH_CN[m.group(1).lower()]
    return f"{mon}/{int(m.group(2))}当周" if m.group(2) else f"{mon}月"


def _fwd_amount(v: Any, unit: str) -> str:
    """日历数值转中文量级:K→万、M→万、B→亿、T→万亿;% 与无单位原样。"""
    if v in (None, ""):
        return "–"
    try:
        f = float(str(v).replace(",", ""))
    except ValueError:
        return str(v)
    if unit == "K":
        return f"{_fmt_num(f / 10)}万"
    if unit == "M":
        return f"{_fmt_num(f * 100)}万"
    if unit == "B":
        return f"{_fmt_num(f / 1000)}万亿" if abs(f) >= 1000 else f"{_fmt_num(f * 10)}亿"
    if unit == "T":
        return f"{_fmt_num(f)}万亿"
    return f"{_fmt_num(f)}{unit}"


def _fwd_event_name(e: Dict[str, Any]) -> str:
    ev = str(e.get("event") or "")
    name = _FWD_EVENT_CN.get(ev, ev) + _FWD_CMP_CN.get((e.get("comparison") or "").lower(), "")
    period = _fwd_period(e.get("period"))
    return f"{name}({period})" if period else name


def _fwd_fomc_line(text: str) -> str:
    m = re.match(r"([A-Za-z]+)\s+(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?,?\s*(\d{4})?", text.strip())
    if not m or m.group(1).lower() not in _MONTH_EN_FULL:
        return f"**下次 FOMC 议息**:{text}"
    mon = _MONTH_EN_FULL[m.group(1).lower()]
    days = f"{m.group(2)}–{m.group(3)}日" if m.group(3) else f"{m.group(2)}日"
    return f"**下次 FOMC 议息**:{mon}月{days}"


_FWD_BANK = {"US": "美联储", "EU": "欧央行", "DE": "欧央行", "JP": "日央行",
             "CN": "人民银行", "KR": "韩国央行"}
_FWD_FX = {"US": "美元", "EU": "欧元", "DE": "欧元", "JP": "日元", "CN": "人民币",
           "KR": "韩元"}
# 事件族 → 看点模板({bank}/{fx} 按国家填);先匹配先用
_FWD_WATCH = (
    (re.compile(r"payroll|unemployment|jobless|claims|adp|jolts|job openings|"
                r"hourly earnings|employment", re.I), "就业 → {bank}路径、{fx}"),
    (re.compile(r"inflation|cpi|hicp|pce|ppi|price index", re.I), "通胀 → {bank}路径、利率"),
    (re.compile(r"interest rate|rate decision|fomc|loan prime", re.I), "政策利率 → {fx}、利率"),
    (re.compile(r"auction", re.I), "国债供给 → 长端利率"),
    (re.compile(r"balance of trade|exports|imports|current account", re.I), "外需 → {fx}"),
    (re.compile(r"foreign exchange reserves", re.I), "外汇储备 → {fx}汇率"),
    (re.compile(r"crude|rig count|opec", re.I), "原油供需 → 油价"),
    (re.compile(r"sentiment|confidence|retail sales", re.I), "消费需求"),
    (re.compile(r"pmi|gdp|industrial production|ifo|zew", re.I), "增长动能"),
)
_SECTOR_CN = {"Technology": "科技", "Healthcare": "医疗", "Financial": "金融",
              "Consumer Cyclical": "可选消费", "Consumer Defensive": "必需消费",
              "Industrials": "工业", "Energy": "能源", "Basic Materials": "材料",
              "Communication Services": "通信服务", "Utilities": "公用事业",
              "Real Estate": "房地产"}
FWD_EARNINGS_MAX = 6
FWD_EARNINGS_PER_DAY = 2
FWD_EARNINGS_MIN_CAP_MN = 50_000   # 市值 ≥ $500 亿(finviz market_cap 以百万美元计)


def _fwd_watch(e: Dict[str, Any]) -> str:
    ev, c = e.get("event") or "", e.get("country") or ""
    for rx, tpl in _FWD_WATCH:
        if rx.search(ev):
            return tpl.format(bank=_FWD_BANK.get(c, "央行"), fx=_FWD_FX.get(c, "汇率"))
    return "–"


_COMPANY_SUFFIX_RE = re.compile(
    r"[,.]?\s+(?:(?:and|&)\s+Co(?:mpany)?|Inc|Corp(?:oration)?|Co|Company|Ltd|plc|"
    r"Holdings?|Group|N\.?V|S\.?A|Class\s+[A-C])\.?$", re.I)


def _short_company(name: str) -> str:
    out, prev = (name or "").strip(), None
    while out and out != prev:           # "KKR & Co. Inc" 要剥两层
        prev, out = out, _COMPANY_SUFFIX_RE.sub("", out)
    return out or name


def _fwd_earnings(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    """下周大市值财报(S&P 500 成分股,市值降序,每日≤2、合计≤6),转成前瞻表行。"""
    src = snapshot.get("sources") or {}
    cons = {r.get("ticker"): r for r in (src.get("market") or {}).get(
        "sp500_constituents") or [] if r.get("ticker")}
    rows = []
    for r in (src.get("earnings_calendar") or {}).get("upcoming") or []:
        c = cons.get(r.get("code"))
        cap = (c or {}).get("market_cap") or 0
        if not isinstance(r, dict) or not r.get("report_date") or cap < FWD_EARNINGS_MIN_CAP_MN:
            continue
        rows.append((cap, r, c))
    # 同一公司常有暂定/确认两个日期:有盘前/盘后时段的那条才是确认日
    rows.sort(key=lambda x: (-x[0], 0 if x[1].get("session") in ("before", "after") else 1))
    per_day: Dict[str, int] = {}
    seen: set = set()
    out = []
    for _, r, c in rows:
        if r.get("code") in seen:
            continue
        seen.add(r.get("code"))
        d = str(r["report_date"])[:10]
        if len(out) >= FWD_EARNINGS_MAX or per_day.get(d, 0) >= FWD_EARNINGS_PER_DAY:
            continue
        per_day[d] = per_day.get(d, 0) + 1
        sess = (r.get("session") or "").lower()
        est = r.get("eps_estimate")
        out.append({
            "date": d, "sort_time": {"before": "00:00", "after": "16:30"}.get(sess, "12:00"),
            "time": {"before": "盘前", "after": "盘后"}.get(sess, "–"),
            "region": "美国",
            "event": f"{_short_company(r.get('name') or r.get('code'))}({r.get('code')})财报",
            "forecast": f"EPS {_fmt_num(est)}" if isinstance(est, (int, float)) else "–",
            "previous": "–",
            "watch": f"{_SECTOR_CN.get(c.get('sector'), c.get('sector') or '')}板块风向",
        })
    return out


def _render_forward_calendar(snapshot: Dict[str, Any]) -> List[str]:
    """本周前瞻:economic_events.upcoming 渲染成表(按美东日期分日,关键事件优先,每日≤6)。

    同一 (国家,事件,周期) 的多口径/多时间戳行合并为一条:同比 > 环比 > 无口径
    (无口径的通胀行是指数水平),同口径内有预期的优先;既无预期也无前值的讲话/
    会议/报告不列;数值换成中文量级(19.6万、156亿)。
    """
    econ = ((snapshot.get("sources") or {}).get("economic_events") or {})
    by_date: Dict[str, List[Dict[str, Any]]] = {}
    for e in econ.get("upcoming") or []:
        no_numbers = (e.get("forecast") in (None, "") and e.get("previous") in (None, ""))
        if no_numbers and _FWD_NOISE_RE.search(e.get("event") or ""):
            continue
        d = str(e.get("date_et") or e.get("date") or "")[:10]
        if d:
            by_date.setdefault(d, []).append(e)
    table: Dict[str, List[Dict[str, Any]]] = {}
    seen_keys: set = set()          # 跨日去重:EODHD 同一发布常有两个时间戳的重复行
    for d in sorted(by_date):
        dedup: List[Dict[str, Any]] = []
        for e in sorted(by_date[d], key=lambda e: (
                0 if FWD_KEY_RE.search(e.get("event") or "") else 1,
                e.get("event") or "", _FWD_CMP_RANK.get(
                    (e.get("comparison") or "").lower(), 2),
                0 if e.get("forecast") not in (None, "") else 1)):
            key = (e.get("country"), e.get("event"), e.get("period"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            dedup.append(e)
        for e in dedup[:FWD_PER_DAY]:
            unit = e["unit"] if "unit" in e else event_unit(e)
            t = str(e.get("time_et") or "")[:5]
            table.setdefault(d, []).append({
                "sort_time": t or "99", "time": t or "–",
                "region": _FWD_REGION_CN.get(e.get("country"), e.get("country") or "–"),
                "event": _fwd_event_name(e),
                "forecast": _fwd_amount(e.get("forecast"), unit),
                "previous": _fwd_amount(e.get("previous"), unit),
                "watch": _fwd_watch(e)})
    for r in _fwd_earnings(snapshot):
        table.setdefault(r["date"], []).append(r)
    rows_md: List[str] = []
    for d in sorted(table):
        try:
            wd = _CN_WEEKDAY[_date_cls.fromisoformat(d).weekday()]
        except ValueError:
            wd = ""
        for i, r in enumerate(sorted(table[d], key=lambda r: r["sort_time"])):
            day = f"{int(d[5:7])}/{int(d[8:10])} {wd}" if i == 0 else ""
            rows_md.append(f"| {day} | {r['time']} | {r['region']} | {r['event']} | "
                           f"{r['forecast']} | {r['previous']} | {r['watch']} |")
    out: List[str] = []
    if rows_md:
        out += ["| 日期 | 时间(美东) | 地区 | 事件 | 预期 | 前值 | 看点 |",
                "|---|---|---|---|---|---|---|", *rows_md]
    cal = (((snapshot.get("sources") or {}).get("macro_pillars") or {})
           .get("fomc_calendar") or [])
    if cal and (cal[0].get("text") or ""):
        out += ["", _fwd_fomc_line(cal[0]["text"])]
    return out


DASH_FX = (("EURUSD.FOREX", "欧元/美元", 4), ("USDJPY.FOREX", "美元/日元", 2))


def _render_rates_fx(sources: Dict[str, Any], cross: Dict[str, Any]) -> List[str]:
    """美债(财政部 par curve,全报告唯一收益率口径)+ 2s10s 利差 + 主要汇率。"""
    curve = (sources.get("macro_pillars") or {}).get("par_curve") or {}
    lv, ch = curve.get("levels_pct") or {}, curve.get("change_bp") or {}
    rows = []

    def _bp(x: Any) -> str:
        return f"{x:+.0f}bp" if isinstance(x, (int, float)) else "–"

    for tenor, name in (("2y", "美债2年"), ("10y", "美债10年"), ("30y", "美债30年")):
        if isinstance(lv.get(tenor), (int, float)):
            rows.append(f"| {name} | {lv[tenor]:.2f}% | {_bp(ch.get(tenor))} |")
    if all(isinstance(lv.get(k), (int, float)) for k in ("2y", "10y")):
        d = (ch["10y"] - ch["2y"]) if all(
            isinstance(ch.get(k), (int, float)) for k in ("2y", "10y")) else None
        rows.append(f"| 2s10s 利差 | {(lv['10y'] - lv['2y']) * 100:+.0f}bp | {_bp(d)} |")
    for code, name, dec in DASH_FX:
        r = cross.get(code)
        if r and isinstance(r.get("close"), (int, float)):
            chg = r.get("pct_change_1d")
            chg_s = f"{chg:+.2f}%" if isinstance(chg, (int, float)) else "–"
            rows.append(f"| {name} | {r['close']:.{dec}f} | {chg_s} |")
    if not rows:
        return []
    return ["| 品种 | 收盘 | 1日变动 |", "|---|---|---|", *rows]


def render_macro_dashboard(snapshot: Dict[str, Any]) -> str:
    sources = snapshot.get("sources") or {}
    md = (sources.get("macro_dashboard") or {}).get("countries") or {}
    fomc = sources.get("fomc") or {}
    cross = {r.get("code"): r
             for r in (sources.get("market") or {}).get("cross_asset") or []}

    lines = ["## 宏观速览", "", "### 四大经济体", "",
             "| 经济体 | 增长 | 通胀 | 政策利率 |",
             "|---|---|---|---|"]
    for c in ("US", "EU", "CN", "JP"):
        block = md.get(c) or {}
        lines.append(
            f"| {DASH_COUNTRY_CN[c]} | {_dash_growth(block.get('growth'))} | "
            f"{_dash_inflation(block.get('inflation'))} | "
            f"{_dash_policy(c, block.get('policy_rate'), fomc)} |")
    lines.append("")

    comm_rows = ["| 品种 | 收盘 | 1日涨跌 |", "|---|---|---|"]
    have_comm = False
    for code, name in DASH_COMMODITIES:
        r = cross.get(code)
        if not r or r.get("close") is None:
            continue
        have_comm = True
        chg = r.get("pct_change_1d")
        chg_s = f"{chg:+.1f}%" if isinstance(chg, (int, float)) else "–"
        comm_rows.append(f"| {name} | {float(r['close']):,.2f} | {chg_s} |")
    if have_comm:
        lines += ["### 商品与贵金属", "", *comm_rows, ""]

    rates = _render_rates_fx(sources, cross)
    if rates:
        lines += ["### 利率与汇率", "", *rates, ""]

    fwd = _render_forward_calendar(snapshot)
    if fwd:
        lines += ["### 本周前瞻", "", *fwd, ""]
    return "\n".join(lines)


def write_macro_dashboard(ws: Path, snapshot: Dict[str, Any]) -> None:
    f = ws / SECTION_SPECS["macro_dashboard"]["output_file"]
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(render_macro_dashboard(snapshot).rstrip()
                 + f"\n{DONE_MARKER}\n", encoding="utf-8")
    claims_path(ws, "macro_dashboard").write_text("[]\n", encoding="utf-8")


# 确定性节的渲染器分派(生成与修订共用)
DETERMINISTIC_WRITERS = {
    "movers": write_movers,
    "macro_dashboard": write_macro_dashboard,
}


# ── 平静日渲染:08/10 在块矩阵为空时不启动 LLM ─────────────────────────
# 11-03 实测:08 只写一行占位却跑了 203s 写作 + 4 次修订;10 在非议息日
# 的内容全部来自 fomc.decisions / next_fomc_odds / 合约路径,LLM 无增量。

FED_SPEAK_RE = re.compile(
    r"\bFED(?:'S|’S)\s+[A-Z]{3,}"
    r"|\bFed(?:'s|’s)\s+[A-Z][a-z]+(?:\s+says|\s+said|:)"
    r"|\bFed\s+(?:Governor|Chair(?:man)?|Vice\s+Chair|President)\s+[A-Z][a-z]+")
_MONTH_NUM = {m: i + 1 for i, m in enumerate(
    ("january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"))}


def _cn_day(iso: str) -> str:
    try:
        d = _date_cls.fromisoformat(str(iso)[:10])
    except ValueError:
        return str(iso)
    return f"{d.month}月{d.day}日"


def _cn_meeting(meeting: str) -> str:
    """"December 8-9, 2026" → "12月8–9日"。"""
    m = re.match(r"([A-Za-z]+)\s+(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?", meeting or "")
    if not m or m.group(1).lower() not in _MONTH_NUM:
        return meeting
    days = m.group(2) + (f"–{m.group(3)}" if m.group(3) else "")
    return f"{_MONTH_NUM[m.group(1).lower()]}月{days}日"


def _cn_contract(month: str) -> str:
    y, _, mo = str(month).partition("-")
    return f"{y}年{int(mo)}月" if mo.isdigit() else str(month)


def _dissent_cn(reason: str, direction: str) -> str:
    size = {"1/4": 25, "1/2": 50, "3/4": 75}
    m = re.search(r"(1/4|1/2|3/4)\s+percentage\s+point|(\d+)\s+basis\s+points?",
                  reason or "", re.I)
    bp = (size[m.group(1)] if m and m.group(1) else int(m.group(2))) if m else None
    if re.search(r"\b(raise|increase|hike)", reason or "", re.I):
        act = f"加息{bp}bp" if bp else "加息"
    elif re.search(r"\b(lower|reduce|cut)", reason or "", re.I):
        act = f"降息{bp}bp" if bp else "降息"
    elif re.search(r"\b(maintain|keep|unchanged|no\s+change)", reason or "", re.I):
        act = "维持利率不变"
    else:
        act = ""
    tag = {"hawkish": "鹰派", "dovish": "鸽派"}.get((direction or "").lower(), "")
    out = f"主张{act}" if act else ""
    return out + (f"（{tag}异议）" if tag else "")


def _has_fed_speak(snapshot: Dict[str, Any]) -> bool:
    news = (snapshot.get("sources") or {}).get("news") or {}
    return any(FED_SPEAK_RE.search(f"{i.get('title') or ''} {i.get('text') or ''}")
               for b in ("flash", "wire") for i in news.get(b) or [])


def render_fomc_quiet(snapshot: Dict[str, Any], blocks: Dict[str, Any],
                      style: str) -> Optional[str]:
    """非议息日且无官员表态 → 确定性 10 节;否则 None(交给 LLM)。"""
    if blocks.get("fomc_deep") or _has_fed_speak(snapshot):
        return None
    src = snapshot.get("sources") or {}
    fomc = src.get("fomc") or {}
    decisions = fomc.get("decisions") or []
    pillars = src.get("macro_pillars") or {}
    nxt = pillars.get("next_fomc_odds") or {}
    if not decisions:
        return f"## 美联储观察\n\n{STYLE_PROFILES[style]['missing_mark']}\n"
    if nxt.get("odds_pct") is None:
        return None

    d = decisions[0]
    rng = f"{d['rate_low_bp'] / 100:.2f}%–{d['rate_high_bp'] / 100:.2f}%"
    chg = d.get("change_bp") or 0
    act = (f"维持联邦基金利率目标区间在 {rng} 不变" if not chg else
           f"{'加息' if chg > 0 else '降息'} {abs(chg):.0f}bp 至 {rng}")
    line = f"- 美联储在{_cn_day(d.get('decision_date'))}会议上{act}"
    if d.get("vote_for") is not None and d.get("vote_against") is not None:
        line += f"，表决结果 {int(d['vote_for'])}:{int(d['vote_against'])}"
    for x in d.get("dissenters") or []:
        names = str(x.get("name") or "").replace(" and ", "与")
        line += f"；{names}投下反对票" + (
            f"，{_dissent_cn(x.get('reason'), x.get('direction'))}"
            if _dissent_cn(x.get("reason"), x.get("direction")) else "")
    lines = ["## 美联储观察", "", "### 利率与决议", "", line + "。", "",
             "### 市场定价路径", ""]

    cum = nxt.get("cum_bp_priced") or 0
    verb = "加息" if cum >= 0 else "降息"
    asof = (fomc.get("path") or {}).get("asof") or ""
    odds = f"- {('截至' + _cn_day(asof) + '，') if asof else ''}期货对" \
           f"{_cn_meeting(nxt.get('meeting'))}会议的定价为{verb}概率 " \
           f"~{_fmt_num(nxt['odds_pct'])}%"
    prior = src.get("prior_odds") or {}
    if prior.get("odds_pct") is not None and prior.get("meeting") == nxt.get("meeting"):
        odds += f"（前值 ~{_fmt_num(prior['odds_pct'])}%）"
    lines.append(odds + "。")

    later = [c for c in (pillars.get("fed_odds") or {}).get("contracts") or []
             if str(c.get("month")) > str(nxt.get("contract_month") or "")
             and c.get("cum_bp_priced") is not None]
    if len(later) >= 2:
        mid = later[min(1, len(later) - 1)]
        peak = max(later, key=lambda c: abs(c["cum_bp_priced"]))
        last = later[-1]
        s = (f"- 更远端以合约定价计：{_cn_contract(mid['month'])}合约累计定价 "
             f"~{_fmt_num(mid['cum_bp_priced'])}bp")
        if peak is not mid:
            s += (f"，峰值落在{_cn_contract(peak['month'])}合约（累计 "
                  f"~{_fmt_num(peak['cum_bp_priced'])}bp）")
        if last is not peak and last is not mid:
            s += (f"，此后回落至{_cn_contract(last['month'])}合约的 "
                  f"~{_fmt_num(last['cum_bp_priced'])}bp")
        lines.append(s + "。")

    curve = pillars.get("par_curve") or {}
    lv, ch = curve.get("levels_pct") or {}, curve.get("change_bp") or {}
    if lv.get("2y") is not None and ch.get("2y") is not None:
        def _move(bp: float) -> str:
            return "持平" if not bp else f"{'上行' if bp > 0 else '下行'} {_fmt_num(abs(bp))}bp"
        s = f"- 2年期美债收益率收于 {float(lv['2y']):.2f}%，较前一交易日{_move(ch['2y'])}"
        if lv.get("10y") is not None and ch.get("10y") is not None:
            s += f"；10年期 {float(lv['10y']):.2f}%，{_move(ch['10y'])}"
        lines.append(s + "。")
    return "\n".join(lines) + "\n"


def render_macro_review_quiet(snapshot: Dict[str, Any], blocks: Dict[str, Any],
                              style: str) -> Optional[str]:
    if blocks.get("review_releases"):
        return None
    return f"## 回顾 · 宏观数据\n\n{STYLE_PROFILES[style]['missing_mark']}\n"


QUIET_RENDERERS = {
    "macro_review": render_macro_review_quiet,
    "fomc": render_fomc_quiet,
}


def _fake_verdict(ws: Path, attempt: int) -> None:
    """第 1 轮核验 fail(drivers 数字可疑),修订后第 2 轮 pass。"""
    vdir = ws / "verify"
    vdir.mkdir(parents=True, exist_ok=True)
    (vdir / "check.py").write_text(
        "# fake audit script (fake-zcode mode)\n", encoding="utf-8"
    )
    if attempt <= 1:
        verdict = {
            "verdict": "fail",
            "summary": "fake mode: suspicious untraceable number in drivers",
            "issues": [{
                "section": "drivers",
                "claim": "S&P 500 moved +9.99%",
                "snapshot_value": "absent",
                "severity": "high",
                "suggestion": "replace with snapshot-aligned placeholder",
            }],
            "quality_notes": ["fake mode"],
        }
    else:
        verdict = {"verdict": "pass", "summary": "fake mode: all clear",
                   "issues": [], "quality_notes": ["fake mode"]}
    (vdir / "verdict.json").write_text(
        json.dumps(verdict, ensure_ascii=False, indent=2), encoding="utf-8"
    )


# ── stages ─────────────────────────────────────────────────────────────

def section_done(ws: Path, name: str) -> bool:
    f = ws / SECTION_SPECS[name]["output_file"]
    return f.exists() and DONE_MARKER in f.read_text(encoding="utf-8")


def _sp_mover_view(cons: List[Dict[str, Any]], top_n: int = 15) -> List[Dict[str, Any]]:
    """S&P 异动视图:|涨跌| 前 top_n 只 ∪ 每板块 |涨跌| 最大 1 只(板块代表)。

    v8.8 company_desk 瘦身:替代 503 行成分全表进切片。同键名
    (sp500_constituents)写回,claims 的 [ticker=X] 选择器对全量快照
    依然可解析;榜单只含大盘股,与转录/flash 的叙事覆盖宇宙对齐。
    """
    def amp(r: Dict[str, Any]) -> float:
        try:
            return abs(float(r.get("change_percent") or 0))
        except (TypeError, ValueError):
            return 0.0

    top = sorted(cons, key=amp, reverse=True)[:top_n]
    picked = {id(r) for r in top}
    covered = {r.get("sector") for r in top}
    for r in sorted(cons, key=amp, reverse=True):
        s = r.get("sector")
        if s and s not in covered and id(r) not in picked:
            top.append(r)
            covered.add(s)
            picked.add(id(r))
    return top


def _sliced_snapshot(ws: Path, snapshot: Path, agent: str) -> Path:
    """按 agent 白名单物理切片快照(v8.3)。

    SOURCE RESTRICTION 此前只是 prompt 约定 + 台账审计;现在写作/修订
    agent 直接挂切片文件,白名单外素材(含其中的转录全文)根本不进入其
    上下文 —— 输入面与合同一致。核验器仍用全量快照(需要跨节视野),
    claims 台账也仍对全量快照校验路径(切片 ⊆ 全量,天然可解析)。
    子键级过滤(news.flash 等)之外保留各块的短标量元数据(标题/时间戳)。
    """
    snap = json.loads(snapshot.read_text(encoding="utf-8"))
    allow = agent_snapshot_filter(agent)
    sliced: Dict[str, Any] = {}
    for head, val in (snap.get("sources") or {}).items():
        subs = allow.get(head)
        if subs is None:
            continue                     # 整块不在白名单 → 丢弃
        if not subs or not isinstance(val, dict):
            sliced[head] = val
            continue
        keep = {k: v for k, v in val.items()
                if k in subs
                or (not isinstance(v, (dict, list))
                    and len(str(v)) < 300)}
        sliced[head] = keep or val       # 子键全未命中(路径命名漂移)→ 保守整块保留
    mkt = sliced.get("market")
    if isinstance(mkt, dict) and isinstance(mkt.get("cross_asset"), list):
        sliced["market"] = {**mkt, "cross_asset": [
            r for r in mkt["cross_asset"]
            if not UST_CODE_RE.match(str((r or {}).get("code") or ""))]}
    # v8.8 company_desk 瘦身(2026-11-06 实测:叙事源对 top_movers 覆盖率
    # 7/30、S&P 占比 3/30、agent 实际引用 1/144,该组却连续撞 1200/1800s
    # 超时线):constituents 全表 → S&P 异动视图;top_movers 移出;
    # earnings_calendar 只留与(视图 ∪ flash 涉及 ticker)相交条目,
    # 深挖的财报催化剂保留、无关小票噪音清掉。全量快照不动,核验/图表
    # /涨跌榜照旧读全量。
    if agent == "company_desk":
        full = snap.get("sources") or {}
        cons = (full.get("market") or {}).get("sp500_constituents") or []
        view = _sp_mover_view(cons) if cons else []
        if isinstance(sliced.get("market"), dict):
            if view:
                sliced["market"]["sp500_constituents"] = view
                sliced["market"]["_constituents_note"] = (
                    f"view: top-{len(view)} |move| + sector leaders "
                    f"(full {len(cons)}-stock table is not in your context)")
            sliced["market"].pop("top_movers", None)
        flash_tick = set()
        for f in ((full.get("news") or {}).get("flash") or []):
            for t in (f.get("tickers") or []):
                if isinstance(t, str):
                    flash_tick.add(t.split(":")[0].strip().upper())
        want = flash_tick | {str(r.get("ticker") or "").upper()
                             for r in view if r.get("ticker")}
        ec = sliced.get("earnings_calendar")
        if isinstance(ec, dict) and want:
            def _keep(e: Any) -> bool:
                if not isinstance(e, dict):
                    return True
                code = str(e.get("code") or e.get("ticker") or "").upper()
                return not code or code in want
            for k in ("prior_after_close", "today", "upcoming"):
                if isinstance(ec.get(k), list):
                    ec[k] = [e for e in ec[k] if _keep(e)]
    snap["sources"] = sliced
    snap["sliced_for"] = agent
    out = ws / f"snapshot.{agent}.json"
    out.write_text(json.dumps(snap, ensure_ascii=False, default=str),
                   encoding="utf-8")
    return out


def _run_one_agent(args: argparse.Namespace, ws: Path, snapshot: Path,
                   snapshot_data: Dict[str, Any], blocks: Dict[str, Any],
                   agent: str,
                   prior: Optional[Dict[str, Any]] = None,
                   plan: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """跑一个写作 agent(或确定性 movers)。返回要合并进 state 的片段。

    不写 state.json:并行时由调用方持锁合并,避免交错写入。
    """
    names = agent_section_names(agent)
    if all(section_done(ws, n) for n in names):
        # 返回形状须与正常路径一致(_apply 只认 "sections" 信封);保留原记录,
        # 否则 session_id/usage/mode 被幂等重跑清空(P16)
        return {"sections": {n: {**(prior or {}).get(n, {}), "status": "done"}
                             for n in names},
                "elapsed": 0.0, "names": names, "usage": None}
    print(f"[agent] {agent} ({', '.join(names)}) ...", flush=True)
    t0 = time.time()
    sections: Dict[str, Any] = {}
    usage = None
    quiet = QUIET_RENDERERS.get(agent)
    quiet_text = quiet(snapshot_data, blocks, args.style) if quiet else None
    if quiet_text is not None:
        f = ws / SECTION_SPECS[names[0]]["output_file"]
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(quiet_text.rstrip() + f"\n{DONE_MARKER}\n", encoding="utf-8")
        claims_path(ws, names[0]).write_text("[]\n", encoding="utf-8")
        sections = {names[0]: {"status": "done", "mode": "deterministic",
                               "note": "quiet day"}}
    elif all(SECTION_SPECS[n].get("mode") == "deterministic" for n in names):
        DETERMINISTIC_WRITERS[names[0]](ws, snapshot_data)
        sections = {n: {"status": "done", "mode": "deterministic"} for n in names}
    elif args.fake_zcode:
        for n in names:
            _fake_section(ws, n, blocks=blocks, style=args.style)
        sections = {n: {"status": "done"} for n in names}
    else:
        try:
            result = run_agent(
                args.zcode_bin, ws,
                build_agent_prompt(agent, style=args.style, blocks=blocks, plan=plan),
                _sliced_snapshot(ws, snapshot, agent), args.timeout)
            usage = result.get("usage")
            for n in names:
                sections[n] = {
                    "status": "done" if section_done(ws, n) else "missing_output",
                    "session_id": result.get("sessionId"),
                    "usage": usage if n == names[0] else None,
                }
        except subprocess.TimeoutExpired:
            if not all(section_done(ws, n) for n in names):
                raise
            sections = {n: {"status": "done",
                            "note": "agent timed out but output file complete"}
                        for n in names}
        for n in names:
            if sections[n]["status"] != "done":
                raise RuntimeError(
                    f"agent {agent}: finished but output file "
                    f"{SECTION_SPECS[n]['output_file']} missing/incomplete"
                )
    elapsed = round(time.time() - t0, 1)
    for n in names:
        print(f"[section] {n} done in {elapsed}s", flush=True)
    return {"sections": sections, "elapsed": elapsed, "names": names,
            "usage": usage}


# ── 编辑计划(写作前一次规划) ──────────────────────────────────────────

PLAN_TIMEOUT_SEC = 600  # v8.5-fix:单发模式预期 60-120s;600 为保险带
                          # (v8.4 实测 391/549s,300 硬杀低于经验分布 → P23)
PLAN_MAX_STORIES = 8
PLAN_MAX_CANONICALS = 12


# EODHD 的美债收益率(US10Y.INDX 等)与财政部 par curve 差 1bp 级,全报告只用后者
UST_CODE_RE = re.compile(r"US\d+Y\.INDX$", re.I)


def _rep(item: Dict[str, Any]) -> str:
    return " [reported]" if item.get("reported") else ""


def _clip(text: Any, n: int) -> str:
    t = re.sub(r"\s+", " ", str(text or "")).strip()
    return t if len(t) <= n else t[:n - 1] + "…"


def render_plan_input(snapshot: Dict[str, Any], blocks: Dict[str, Any]) -> str:
    """规划用摘要:结构化数字 + 带条目 id 的新闻标题,不含节目转录(太长)。"""
    src = snapshot.get("sources") or {}
    m = src.get("market") or {}
    out = [f"# Plan input — {snapshot.get('report_date') or ''}", "",
           "## Cross-asset (market.cross_asset[code=..])"]
    for r in m.get("cross_asset") or []:
        if UST_CODE_RE.match(str(r.get("code") or "")):
            continue
        out.append(f"- {r.get('code')} {r.get('name')}: close {_fmt_num(r.get('close'))}, "
                   f"{r.get('pct_change_1d')}%")
    b = m.get("breadth") or {}
    if b:
        out += ["", f"Breadth (market.breadth): advancers {b.get('advancers')}, "
                f"decliners {b.get('decliners')}, median {b.get('median_change_pct')}%"]
    sec = m.get("sp500_sector_avg") or []
    if sec:
        out += ["", "## S&P 500 sectors (market.sp500_sector_avg)"]
        out += [f"- {r.get('sector')}: {r.get('avg_change_pct')}%" for r in sec]
    cons = sorted((r for r in m.get("sp500_constituents") or []
                   if isinstance(r.get("change_percent"), (int, float))),
                  key=lambda r: r["change_percent"])
    if cons:
        out += ["", "## Largest S&P 500 moves "
                "(market.sp500_constituents[ticker=..].change_percent)"]
        for r in cons[-10:][::-1] + cons[:10]:
            out.append(f"- {r['ticker']} {r.get('company')}: {r['change_percent']}% "
                       f"({r.get('sector')})")
    caps = {r.get("ticker"): r.get("market_cap") or 0 for r in cons}
    cal = src.get("earnings_calendar") or {}
    for key, label in (("prior_after_close", "Reported after the prior close"),
                       ("today", "Reported today"), ("upcoming", "Upcoming")):
        rows = [r for r in cal.get(key) or [] if isinstance(r, dict)]
        rows.sort(key=lambda r: -(caps.get(r.get("code")) or 0))
        if not rows:
            continue
        out += ["", f"## {label} (earnings_calendar.{key}[code=..])"]
        for r in rows[:20]:
            eps = (f"EPS {r.get('eps_actual')} vs est {r.get('eps_estimate')}"
                   if r.get("eps_actual") is not None else f"est {r.get('eps_estimate')}")
            out.append(f"- {r.get('code')} {r.get('name')} ({r.get('session')}, "
                       f"{r.get('report_date')}): {eps}")
    out += ["", "## Macro (decided by the orchestrator)"]
    rel = blocks.get("review_releases") or {}
    if rel:
        for country, rows in rel.items():
            for r in rows:
                u = r["unit"] if "unit" in r else event_unit(r)
                out.append(f"- Release for 08 ({country}): {r.get('event')} "
                           f"({r.get('period')}): {fmt_event_value(r.get('actual'), u)} "
                           f"vs est {fmt_event_value(r.get('forecast'), u)}, "
                           f"prior {fmt_event_value(r.get('previous'), u)}")
    else:
        out.append("- No release selected for 08 today")
    themes = blocks.get("geo_themes") or {}
    out.append("- Geo themes opened for 09: " + (", ".join(
        f"{k} ({v.get('hits')} hits)" for k, v in themes.items()) or "none"))
    if blocks.get("focal_events"):
        out.append("- 09 also opens a focal-events block: "
                   + ", ".join(f.get("name", "") for f in blocks["focal_events"]))
    mp = src.get("macro_pillars") or {}
    odds = mp.get("next_fomc_odds") or {}
    if odds.get("odds_pct") is not None:
        out.append(f"- Fed odds (macro_pillars.next_fomc_odds): {odds.get('meeting')} "
                   f"{odds.get('direction') or 'move'} {odds.get('odds_pct')}%")
    curve = mp.get("par_curve") or {}
    if curve.get("levels_pct"):
        out.append("- Treasury curve — the ONLY source for US yields "
                   "(macro_pillars.par_curve.levels_pct.<tenor> / .change_bp.<tenor>): "
                   + ", ".join(
            f"{k} {float(v):.2f}% ({(curve.get('change_bp') or {}).get(k)}bp)"
            for k, v in curve["levels_pct"].items() if k in ("2y", "10y", "30y")))
    out.append(f"- FOMC decision within the last 3 sessions: "
               f"{'yes' if blocks.get('fomc_deep') else 'no'}")
    upcoming = list(dict.fromkeys(
        f"{str(e.get('date'))[:10]} {e.get('event')}"
        for e in (src.get("economic_events") or {}).get("upcoming") or []
        if e.get("country") == "US"))
    if upcoming:
        out += ["", "## US calendar ahead (economic_events.upcoming)"]
        out += [f"- {e}" for e in upcoming[:12]]
    news = src.get("news") or {}
    flash = [x for x in news.get("flash") or [] if x.get("info_level") == "L1"]
    for title, rows, fmt in (
            ("News flash — L1 (news.flash[id=..])", flash[:120],
             lambda x: f"- {x.get('id')}{_rep(x)} {x.get('time_et')} {_clip(x.get('text'), 220)}"),
            ("Newswire (news.wire[id=..])", news.get("wire") or [],
             lambda x: f"- {x.get('id')}{_rep(x)} {x.get('time_et')} {_clip(x.get('title'), 140)}"),
            ("Analyst calls (news.calls[id=..])", news.get("calls") or [],
             lambda x: f"- {x.get('id')} {_clip(x.get('title'), 160)}")):
        if rows:
            out += ["", f"## {title}"] + [fmt(x) for x in rows]
    nm = src.get("news_macro") or {}
    for bucket in ("digest", "research", "geo"):
        rows = nm.get(bucket) or []
        if rows:
            out += ["", f"## Macro {bucket} (news_macro.{bucket}[id=..])"]
            out += [f"- {x.get('id')}{_rep(x)} {x.get('time_cn')} [{x.get('theme') or '-'}] "
                    f"{_clip(x.get('title') or x.get('text'), 120)}" for x in rows]
    return "\n".join(out) + "\n"


def _plan_owners(snapshot: Dict[str, Any], blocks: Dict[str, Any],
                 style: str) -> List[str]:
    """今天能认领故事的节:平静日的 08/10、没开主题的 09 不接故事。"""
    owners = []
    for n in PLAN_OWNER_ROLES:
        if n == "macro_review" and not blocks.get("review_releases"):
            continue
        if n == "geopolitics" and not blocks.get("geo_themes"):
            continue
        quiet = QUIET_RENDERERS.get(SECTION_SPECS[n].get("agent", n))
        if quiet and quiet(snapshot, blocks, style) is not None:
            continue
        owners.append(n)
    return owners


def _load_plan(ws: Path, owners: List[str],
               sources: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    f = ws / "plan.json"
    if not f.exists():
        return None
    try:
        plan = json.loads(f.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    stories = [s for s in (plan.get("stories") if isinstance(plan, dict) else None) or []
               if isinstance(s, dict) and s.get("title")]
    if not stories:
        return None
    for s in stories:
        if s.get("owner") not in owners:
            s["owner"] = "drivers"
        s["may_mention"] = [n for n in s.get("may_mention") or []
                            if n in SECTION_ORDER and n != s["owner"]]
    return {"stories": stories[:PLAN_MAX_STORIES],
            "canonical": verified_canonicals(plan.get("canonical"),
                                             sources)[:PLAN_MAX_CANONICALS]}


def _plan_once(zcode_bin: str, prompt: str,
               timeout: int) -> Dict[str, Any]:
    """编辑计划单发调用(2026-11-05 P23 修复):完整复用 triage 的成熟模式
    (build_snapshot._triage_chunk)—— 不带 --cwd、不挂附件,工作区文件不可
    达,agent 无工具需求;失败重试 1 次、20s 退避。返回 zcode envelope
    (调用方从 response 提取 JSON、从 usage 记账)。"""
    cmd = [zcode_bin, "-p", prompt, "--json", "--mode", "yolo",
           "--surface", "terminal"]
    last_err: Optional[Exception] = None
    for attempt in (1, 2):
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=timeout)
            if proc.returncode != 0:
                raise RuntimeError(
                    f"zcode exited {proc.returncode}: {proc.stderr[-300:]}")
            return _parse_zcode_json(proc.stdout)
        except Exception as e:  # noqa: BLE001 — 与 _triage_chunk 同语义
            last_err = e
            if attempt == 1:
                print(f"[plan] call failed ({str(e)[:120]}); retry in 20s",
                      flush=True)
                time.sleep(20)
    raise last_err  # type: ignore[misc]


def _parse_plan_response(text: str) -> Optional[Dict[str, Any]]:
    """从单发回复中提取 plan JSON 对象:剥代码围栏,截首尾大括号,容忍前后杂文。"""
    text = (text or "").strip()
    for ln in ("```json", "```"):
        text = text.removeprefix(ln).removesuffix(ln).strip()
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        obj = json.loads(text[start:end + 1])
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def run_plan(args: argparse.Namespace, ws: Path, snapshot_data: Dict[str, Any],
             blocks: Dict[str, Any], state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """写作前一次 LLM 规划:故事归属 + 规范数值。已有有效 plan.json 就复用;
    失败不阻断(各 agent 按原任务书写,跨节一致性退回"多数/已核实"参照)。

    v8.5-fix:改为单发 stdout 模式(_plan_once)——prompt 内联 digest、输出
    由编排器解析并落盘 plan.json,agent 不写文件;两次失败走 unavailable。"""
    if args.fake_zcode or args.no_plan:
        return None
    owners = _plan_owners(snapshot_data, blocks, args.style)
    sources = snapshot_data.get("sources") or {}
    plan = _load_plan(ws, owners, sources)
    if plan is not None:
        return plan
    if all(section_done(ws, n) for n in SECTION_ORDER):
        return None
    inp = ws / "plan_input.md"
    digest = render_plan_input(snapshot_data, blocks)
    inp.write_text(digest, encoding="utf-8")  # 工件留档,便于排障与复现
    print("[plan] editorial plan (single-shot) ...", flush=True)
    t0 = time.time()
    try:
        envelope = _plan_once(
            args.zcode_bin, build_plan_prompt(owners, digest),
            min(args.timeout, PLAN_TIMEOUT_SEC))
        _add_usage(state, envelope.get("usage"), "plan")
        obj = _parse_plan_response(envelope.get("response") or "")
        if obj is not None and isinstance(obj.get("stories"), list) \
                and obj["stories"]:
            (ws / "plan.json").write_text(
                json.dumps(obj, ensure_ascii=False, indent=2),
                encoding="utf-8")
        else:
            print("[plan] response had no usable JSON object", flush=True)
    except (subprocess.TimeoutExpired, RuntimeError) as e:
        print(f"[plan] single-shot failed ({str(e)[:120]})", flush=True)
    _record_timing(state, "plan", time.time() - t0)
    plan = _load_plan(ws, owners, sources)
    state["plan"] = {"status": "done" if plan else "unavailable",
                     "stories": len(plan["stories"]) if plan else 0,
                     "canonical": len(plan["canonical"]) if plan else 0}
    print(f"[plan] {state['plan']}", flush=True)
    save_state(ws, state)
    return plan


def run_sections(args: argparse.Namespace, ws: Path, snapshot: Path,
                 state: Dict[str, Any]) -> None:
    """两段编排(AGENT_STAGES):第一段按信源并行(overall_market /
    company_desk / movers / macro_dashboard / macro_review / geopolitics /
    fomc,墙钟 ≈ 最慢一组);第二段 outlook 读第一段成品做综合。"""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    snapshot_data = json.loads(snapshot.read_text(encoding="utf-8"))
    blocks = macro_blocks(snapshot_data)
    state["macro_blocks"] = blocks
    print(f"[macro] blocks: { {k: v for k, v in blocks.items()} }", flush=True)

    def _apply(result: Dict[str, Any]) -> None:
        for n, rec in (result.get("sections") or {}).items():
            state["sections"][n] = rec
            _record_timing(state, f"section:{n}", result["elapsed"])
        _add_usage(state, result.get("usage"), "write")
        save_state(ws, state)

    prior = json.loads(json.dumps(state.get("sections") or {}))
    plan = run_plan(args, ws, snapshot_data, blocks, state)
    for stage in AGENT_STAGES:
        if args.fake_zcode:
            # fake:秒级,串行即可,避免线程里抢写同一份 fake 文件
            for agent in stage:
                _apply(_run_one_agent(args, ws, snapshot, snapshot_data,
                                      blocks, agent, prior, plan))
            continue
        # --max-parallel:限制同时运行的 agent 数(0=全并发,原行为)。
        # 7GB 机与后端/交互会话同驻时,6 路齐射两次触发全局 OOM(2026-11-06
        # 20:51/22:40 尸检:击杀 zcode server 与编排器);2 路峰值 ~1.7GB 安全。
        cap = args.max_parallel if args.max_parallel > 0 else len(stage)
        with ThreadPoolExecutor(max_workers=min(cap, len(stage))) as pool:
            # v8.5-fix:提交错峰 3s —— 削 N 个 CLI 同时起跑时对 db.sqlite 的
            # 启动争夺(2026-11-05 plan CLI 挂死 300s 零进展的疑似诱因)
            futs: Dict[Any, Any] = {}
            for i, a in enumerate(stage):
                if i:
                    time.sleep(3)
                futs[pool.submit(_run_one_agent, args, ws, snapshot,
                                 snapshot_data, blocks, a, prior, plan)] = a
            first_err: Optional[BaseException] = None
            for fut in as_completed(futs):
                try:
                    _apply(fut.result())
                except BaseException as e:  # noqa: BLE001
                    first_err = first_err or e
            if first_err:
                raise first_err
    missing = [n for n in SECTION_ORDER
               if state["sections"].get(n, {}).get("status") != "done"]
    if missing:
        raise RuntimeError(f"sections incomplete: {missing}")


def _high_issues(verdict: Dict[str, Any]) -> List[Dict[str, Any]]:
    """门控用的 high:annotate 模式下 LLM 意见带 advisory 标记,只进复核备注。"""
    return [i for i in verdict.get("issues") or []
            if str(i.get("severity") or "").lower() == "high"
            and not i.get("advisory")]


def _gate_llm_issues(ws: Path, verdict: Dict[str, Any], style: str,
                     state: Dict[str, Any]) -> None:
    """annotate 模式的 LLM 意见:能路由到可修订节的 high 驱动且只驱动一轮修订,
    其余(medium/low、路由不到的 high)只作复核备注。轻量核验看不到快照,但
    实测其 high 命中率高(2026-11-04 6/6),值得修一次;不值得反复复核。"""
    llm = [i for i in verdict.get("issues") or []
           if i.get("source") != "deterministic"]
    gating = 0
    for i in llm:
        routable = any(_revisable(ws, n, i, style, state)
                       for n in _issue_section_keys(str(i.get("section") or "")))
        if str(i.get("severity") or "").lower() == "high" and routable:
            gating += 1
        else:
            i["advisory"] = True
    if gating:
        state["verify"]["llm_fix_used"] = True
        state["verify"]["llm_issues"] = llm
        print(f"[verify] {gating} LLM high issue(s) get one revision round", flush=True)


def _read_verdict(path: Path) -> Dict[str, Any]:
    """读核验 agent 手写的 verdict.json;容忍 JSON 非法转义(如 \' )。"""
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return json.loads(re.sub(r"\\(['\"])", r"\1", text))


def run_verifier(args: argparse.Namespace, ws: Path, snapshot: Path,
                 state: Dict[str, Any]) -> Dict[str, Any]:
    verdict_file = ws / "verify" / "verdict.json"

    # 复用判定(verdict 有效性范围 = 章节内容): 若现存 verdict 比所有章节文件
    # 都新且可解析,说明它针对的就是当前章节内容 —— 直接复用,不重跑。
    # 典型场景: 核验 Agent 写完 verdict 后进程迟迟不退被超时杀掉,重跑管线时
    # 无需再付一整轮核验成本。用严格大于(>):同秒并发的 mtime 并列(如 fake
    # 模式)不构成"更新",避免误复用陈旧 fail 引发空转修订循环。
    section_files = [ws / SECTION_SPECS[n]["output_file"] for n in SECTION_ORDER]
    # P17 修复:复用判定同时看配套 claims 台账 —— 修订只动 .claims.json 不动
    # .md 时,旧 verdict 不应被当作"仍有效"(2026-11-04 实测空转两轮修订)
    claims_files = [claims_path(ws, n) for n in SECTION_ORDER]
    consecutive_reuses = state["verify"].get("consecutive_reuses", 0)
    if (consecutive_reuses < 2
            and verdict_file.exists() and all(f.exists() for f in section_files)):
        v_mtime = verdict_file.stat().st_mtime
        newest_section = max(
            f.stat().st_mtime for f in
            section_files + [c for c in claims_files if c.exists()])
        if v_mtime > newest_section:
            try:
                verdict = _read_verdict(verdict_file)
                if verdict.get("verdict") in ("pass", "fail"):
                    verdict["verdict"] = "fail" if _high_issues(verdict) else "pass"
                    print("[verify] reusing existing verdict (newer than all "
                          "sections)", flush=True)
                    state["verify"]["status"] = verdict["verdict"]
                    state["verify"]["reused"] = True
                    state["verify"]["consecutive_reuses"] = consecutive_reuses + 1
                    save_state(ws, state)
                    return verdict
            except json.JSONDecodeError:
                pass  # verdict 损坏 → 重新核验

    state["verify"]["attempts"] += 1
    state["verify"]["consecutive_reuses"] = 0
    attempt = state["verify"]["attempts"]
    print(f"[verify] attempt {attempt} ...", flush=True)
    t0 = time.time()

    # Layer 1 — deterministic audit (warehouse script, not LLM-authored)
    print("[verify] running verify_checks.py ...", flush=True)
    check_issues = run_checks(ws, style=args.style)
    check_path = ws / "verify" / "check_results.json"
    check_path.parent.mkdir(parents=True, exist_ok=True)
    check_path.write_text(
        json.dumps(check_issues, ensure_ascii=False, indent=2), encoding="utf-8")
    high_fails = [i for i in check_issues
                  if not i.get("match", True) and i.get("severity") == "high"]
    print(f"[verify] deterministic checks: {len(check_issues)} rows, "
          f"{len(high_fails)} high fails", flush=True)

    mtime_before = verdict_file.stat().st_mtime if verdict_file.exists() else 0
    mode = getattr(args, "llm_verify", "annotate")
    verdict_file.parent.mkdir(parents=True, exist_ok=True)
    if high_fails:
        # 脚本已定位到具体数字/结构错误:直接按清单修订,不再花一轮 LLM 核验
        verdict_file.write_text(json.dumps({
            "verdict": "fail", "summary": "deterministic checks failed",
            "issues": [], "quality_notes": [], "source": "deterministic",
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        state["verify"][f"llm_attempt_{attempt}"] = "skipped: deterministic high fails"
    elif args.fake_zcode:
        _fake_verdict(ws, attempt)
    elif mode == "off":
        verdict_file.write_text(json.dumps({
            "verdict": "pass", "summary": "deterministic checks passed; LLM review off",
            "issues": [], "quality_notes": [], "source": "deterministic",
        }, ensure_ascii=False, indent=2), encoding="utf-8")
    elif mode == "annotate" and state["verify"].get("llm_fix_used"):
        # LLM 的 high 已定向修订过一轮:之后只跑确定性核验,不再复核(避免 LLM
        # 每轮换说法导致修订空转);上一轮意见留作复核备注
        carried = [{**i, "advisory": True, "note": "revised once; not re-reviewed"}
                   for i in state["verify"].get("llm_issues") or []]
        verdict_file.write_text(json.dumps({
            "verdict": "pass", "summary": "deterministic checks passed; LLM issues "
            "were revised once and are not re-reviewed",
            "issues": carried, "quality_notes": [], "source": "deterministic",
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        state["verify"][f"llm_attempt_{attempt}"] = "skipped: LLM fix round used"
    else:
        light = mode in ("light", "annotate")
        try:
            result = run_agent(
                args.zcode_bin, ws,
                (build_light_verifier_prompt(style=args.style) if light
                 else build_verifier_prompt(style=args.style)),
                None if light else snapshot, args.timeout)
            _add_usage(state, result.get("usage"), "verify")
            state["verify"]["session_id"] = result.get("sessionId")
            state["verify"][f"usage_attempt_{attempt}"] = result.get("usage")
        except subprocess.TimeoutExpired:
            fresh = (verdict_file.exists()
                     and verdict_file.stat().st_mtime >= mtime_before)
            if not fresh:
                raise
            print("[verify] agent timed out but verdict.json was written — "
                  "accepting file contract", flush=True)
    _record_timing(state, f"verify:attempt_{attempt}", time.time() - t0)
    print(f"[verify] attempt {attempt} done in "
          f"{state['timings'][f'verify:attempt_{attempt}'][-1]}s", flush=True)
    if not verdict_file.exists():
        raise RuntimeError("verifier finished but verify/verdict.json missing")
    verdict = _read_verdict(verdict_file)
    if (mode == "annotate" and not high_fails and not args.fake_zcode
            and not state["verify"].get("llm_fix_used")):
        _gate_llm_issues(ws, verdict, args.style, state)

    # Merge deterministic high fails into verdict (orchestrator owns the gate)
    if high_fails:
        merged = list(verdict.get("issues") or [])
        existing = {(i.get("section"), i.get("claim")) for i in merged}
        for row in high_fails:
            key = (row.get("section"), row.get("claim"))
            if key in existing:
                continue
            merged.append({
                "section": row.get("section"),
                "claim": row.get("claim"),
                "snapshot_value": row.get("snapshot_value"),
                "severity": "high",
                "suggestion": row.get("suggestion")
                or "align with snapshot / source matrix / block matrix",
                "source": "deterministic",
            })
        verdict["issues"] = merged

    # 门控只看 high:medium/low 是复核备注(进 audit sidecar),不驱动修订,
    # 也不因 LLM 自报 "fail" 而拦下发布
    verdict["verdict"] = "fail" if _high_issues(verdict) else "pass"
    verdict_file.write_text(
        json.dumps(verdict, ensure_ascii=False, indent=2), encoding="utf-8")

    state["verify"]["status"] = verdict.get("verdict", "fail")
    state["verify"]["deterministic_high_fails"] = len(high_fails)
    save_state(ws, state)
    return verdict


_SEC_TOKEN_RE = re.compile(r"\d{2}_([a-z_]+)")


def _issue_section_keys(section_field: str) -> List[str]:
    """verdict 的 section 字段 → SECTION_SPECS 键列表。

    核验 agent 的写法不固定:裸键("macro")、文件名("06_deep_dives.md")、
    或带跨节装饰("02_drivers.md(与 04_sector_action.md 交叉)")。提取字段中
    出现的全部 NN_name token 映射回章节键;跨节问题映射到多组、各自修订 ——
    旧版只认裸键,跨节矛盾映射不到任何修订组,修订空转直至轮次耗尽。
    """
    keys: List[str] = []
    for m in _SEC_TOKEN_RE.finditer(section_field or ""):
        name = m.group(1)
        if name in SECTION_SPECS and name not in keys:
            keys.append(name)
    bare = (section_field or "").strip()
    if bare in SECTION_SPECS and bare not in keys:
        keys.append(bare)
    return keys


def _is_placeholder(ws: Path, name: str, style: str) -> bool:
    """节正文只有标题 + missing_mark(块矩阵为空时的合同输出)。"""
    f = ws / SECTION_SPECS[name]["output_file"]
    if not f.exists():
        return False
    text = re.sub(r"<!--.*?-->", "", f.read_text(encoding="utf-8"), flags=re.S)
    body = [ln.strip() for ln in text.splitlines()
            if ln.strip() and not ln.lstrip().startswith("#")]
    return body == [STYLE_PROFILES[style]["missing_mark"]]


def _revisable(ws: Path, name: str, issue: Dict[str, Any], style: str,
               state: Dict[str, Any]) -> bool:
    """确定性节(含平静日渲染的 08/10)重渲染结果不变,不修;占位节只接受
    确定性核验的问题 —— LLM 的「质量」意见曾把 08 占位改写成正文,违反
    块矩阵合同(P18)。"""
    if (SECTION_SPECS[name].get("mode") == "deterministic"
            or (state.get("sections") or {}).get(name, {}).get("mode")
            == "deterministic"):
        return False
    if issue.get("source") != "deterministic" and _is_placeholder(ws, name, style):
        return False
    return True


def revise_sections(args: argparse.Namespace, ws: Path, snapshot: Path,
                    state: Dict[str, Any], issues: List[Dict[str, Any]]) -> int:
    """按核验问题清单定向修订(--resume 续接原会话)。返回实际修订的组数。

    只修 high;medium/low 留作复核备注。问题按 agent 分组:overall_market
    组(narrative+drivers)的问题合并为一次修订调用 —— 两文件出自同一会话,
    拆开修会破坏"速览/深挖"分工。
    """
    by_agent: Dict[str, List[Dict[str, Any]]] = {}
    for issue in issues:
        if str(issue.get("severity") or "").lower() != "high":
            continue
        for name in _issue_section_keys(str(issue.get("section") or "")):
            if not _revisable(ws, name, issue, args.style, state):
                print(f"[revise] skip {name}: locked section "
                      f"({issue.get('claim', '')[:60]})", flush=True)
                continue
            agent = SECTION_SPECS[name].get("agent", name)
            if issue not in by_agent.setdefault(agent, []):
                by_agent[agent].append(issue)
    for agent, agent_issues in by_agent.items():
        names = agent_section_names(agent)
        print(f"[revise] {agent} ({', '.join(names)}; "
              f"{len(agent_issues)} issue(s)) ...", flush=True)
        t0 = time.time()
        if args.fake_zcode:
            for n in names:
                _fake_section(ws, n, corrected=True, style=args.style,
                              blocks=state.get("macro_blocks"))
            _record_timing(state, f"revise:{agent}", time.time() - t0)
            continue
        feedback = "\n".join(
            f"- [{i.get('severity')}] ({i.get('section')}) {i.get('claim')} "
            f"| snapshot={i.get('snapshot_value')} | fix: {i.get('suggestion')}"
            for i in agent_issues
        )
        files_note = " / ".join(SECTION_SPECS[n]["output_file"] for n in names)
        prompt = (
            "Verification failed. Fix ONLY the issues below in your section "
            f"file(s) ({files_note}); leave everything else unchanged. Rules "
            "from the original task still apply — including the division of "
            "labor between the files if you wrote more than one. Update the "
            "matching .claims.json ledger so it reflects the revised text.\n\n"
            f"Issues:\n{feedback}"
        )
        session_id = state["sections"].get(names[0], {}).get("session_id")
        try:
            result = run_agent(args.zcode_bin, ws, prompt,
                               _sliced_snapshot(ws, snapshot, agent), args.timeout,
                               resume_session=session_id)
            _add_usage(state, result.get("usage"), "revise")
        except subprocess.TimeoutExpired:
            if not all(section_done(ws, n) for n in names):
                raise
            print(f"[revise] {agent}: agent timed out but revised files complete",
                  flush=True)
        _record_timing(state, f"revise:{agent}", time.time() - t0)
        for n in names:
            if not section_done(ws, n):
                raise RuntimeError(f"revision of {n} did not produce a valid file")
    return len(by_agent)


def _caliber_header(snapshot: Dict[str, Any], style: str) -> List[str]:
    """口径/数据质量声明块 —— 确定性渲染自快照元数据,不经 LLM。

    参考 AI 投资周报的"时间与口径/数据质量"前置声明:数据边界、各源状态、
    缺口全部先于正文公开,缺失是一等信息,不被抹掉。节目摘要落后报告日时
    显式披露 lag_days(不再静默当作同日口径)。
    """
    src = snapshot.get("sources", {})
    errors = snapshot.get("errors", {})
    cn = style == "cn"

    def _show(key: str, label: str) -> str:
        v = src.get(key)
        if not (isinstance(v, dict)
                and (v.get("clean_transcript") or v.get("clean_transcript_ot"))):
            return f"{label}转录✗{'不可得' if cn else 'unavailable'}"
        s = v.get("session_date") or "?"
        if key == "cnbc_closing_bell":
            m = len(v.get("clean_transcript") or "")
            o = len(v.get("clean_transcript_ot") or "")
            slots = (f"主段{m // 1000}k" if cn else f"main {m // 1000}k") \
                + ("/" if cn else "/") \
                + (f"OT {o // 1000}k" if o else ("OT✗" if cn else "OT none"))
            return f"{label}转录✓{s}({slots})"
        t = len(v.get("clean_transcript") or "")
        return f"{label}转录✓{s}({t // 1000}k)"

    # 新闻事件层(v6):快讯/通讯社/电话会状态
    news = src.get("news") or {}
    n_flash = len(news.get("flash") or [])
    n_wire = len(news.get("wire") or [])
    n_calls = len(news.get("calls") or [])
    if cn:
        news_note = (f"新闻层✓快讯{n_flash}条/通讯社{n_wire}篇/电话会{n_calls}篇"
                     if (n_flash or n_wire) else "新闻层✗不可得")
    else:
        news_note = (f"news✓{n_flash} flash/{n_wire} wire/{n_calls} calls"
                     if (n_flash or n_wire) else "news✗unavailable")

    market = src.get("market") or {}
    cross = market.get("cross_asset") or []
    dates = sorted({a.get("date") for a in cross if a.get("date")})
    if len(dates) > 1:
        cross_span = f"{dates[0]}~{dates[-1]}"
    else:
        cross_span = dates[-1] if dates else ""
    econ = src.get("economic_events") or {}
    n_recent = len(econ.get("recent") or [])
    n_upcoming = len(econ.get("upcoming") or [])
    econ_note = f"({econ['filter']})" if econ.get("filter") else ""
    pillars = src.get("macro_pillars") or {}
    pillar_ok = [k for k in ("par_curve", "fred_ranks", "fred_extras",
                             "fed_odds", "fomc_calendar") if pillars.get(k)]

    # v8 Macros 新源:宏观地缘素材层(Admin 订阅,中英)/ FOMC 表 / 四经济体指标表
    news_macro = src.get("news_macro") or {}
    n_dig = len(news_macro.get("digest") or [])
    n_res = len(news_macro.get("research") or [])
    n_geo = len(news_macro.get("geo") or [])
    tri = news_macro.get("triage") or {}
    tri_note = (f"·triage{tri.get('selected')}/{tri.get('candidates')}"
                if tri else "·正则回退")
    fomc = src.get("fomc") or {}
    n_dec = len(fomc.get("decisions") or [])
    n_path = len((fomc.get("path") or {}).get("meetings") or [])
    has_stmt = bool(fomc.get("statement"))
    n_dash = len((src.get("macro_dashboard") or {}).get("countries") or {})
    if cn:
        cn_note = (f"宏观素材层✓日报{n_dig}/解读{n_res}/地缘{n_geo}{tri_note}"
                   if (n_dig or n_res or n_geo) else "宏观素材层✗不可得")
        fomc_note = (f"FOMC✓决议{n_dec}场/路径{n_path}场/声明{'✓' if has_stmt else '✗'}"
                     if fomc else "FOMC✗不可得")
        dash_note = (f"四经济体表✓{n_dash}/4" if n_dash else "四经济体表✗不可得")
    else:
        cn_note = (f"macro feed✓{n_dig} digest/{n_res} research/{n_geo} geo"
                   f"{tri_note}"
                   if (n_dig or n_res or n_geo) else "macro feed✗unavailable")
        fomc_note = (f"FOMC✓{n_dec} decisions/{n_path} path/"
                     f"stmt{'✓' if has_stmt else '✗'}"
                     if fomc else "FOMC✗unavailable")
        dash_note = (f"macro dashboard✓{n_dash}/4 economies"
                     if n_dash else "macro dashboard✗unavailable")
    if cn:
        pillar_note = (f"宏观底座✓{len(pillar_ok)}/5({','.join(pillar_ok)})"
                       if pillar_ok else "宏观底座✗不可得")
    else:
        pillar_note = (f"macro pillars✓{len(pillar_ok)}/5({','.join(pillar_ok)})"
                       if pillar_ok else "macro pillars✗unavailable")

    if cn:
        lines = [
            f"> **数据口径** 快照冻结于 {snapshot.get('generated_at', '?')};"
            f"正文所有数字以该快照为准,禁止替代指标。",
            "> **数据源** "
            + " · ".join(filter(None, [
                _show("the_close", "The Close"),
                _show("cnbc_closing_bell", "CNBC Closing Bell"),
                (f"跨资产行情✓{cross_span}({len(cross)}项)" if cross_span else "跨资产行情✗不可得"),
                (f"板块/个股(finviz✓{market.get('snapshot_date') or '?'})"
                 f"✓{len(market.get('sp500_sector_avg') or [])}板块"
                 f"/{len(market.get('top_movers') or [])}只异动" if market else "板块/个股✗不可得"),
                (f"经济日历✓近{n_recent}/前瞻{n_upcoming}条{econ_note}" if econ else "经济日历✗不可得"),
                news_note,
                pillar_note,
                cn_note,
                fomc_note,
                dash_note,
            ])),
        ]
    else:
        lines = [
            f"> **Data caliber** snapshot frozen at {snapshot.get('generated_at', '?')}; "
            f"all numbers in this brief refer to that snapshot only.",
            "> **Sources** "
            + " · ".join(filter(None, [
                _show("the_close", "The Close transcript"),
                _show("cnbc_closing_bell", "Closing Bell transcript"),
                (f"cross-asset✓{cross_span}({len(cross)} series)" if cross_span else "cross-asset✗unavailable"),
                (f"sectors/stocks(finviz✓{market.get('snapshot_date') or '?'})"
                 f"✓{len(market.get('sp500_sector_avg') or [])} sectors"
                 f"/{len(market.get('top_movers') or [])} movers" if market else "sectors/stocks✗unavailable"),
                (f"economic calendar✓{n_recent} recent/{n_upcoming} upcoming" if econ else "economic calendar✗unavailable"),
                news_note,
                pillar_note,
                cn_note,
                fomc_note,
                dash_note,
            ])),
        ]
    if errors:
        gaps = ";".join(f"{k}:{v}" for k, v in errors.items())
        lines.append(f"> **{'数据缺口' if cn else 'Data gaps'}** {gaps}"
                     f"({'相关内容已按「不可得」红线标注' if cn else 'marked per the missing-data rule'})")
    else:
        lines.append(f"> **{'数据缺口' if cn else 'Data gaps'}** "
                     f"{'无' if cn else 'none'}")
    return lines


def _extract_headline(narrative_text: str, report_date: str) -> str:
    """从 01_narrative.md 的 HEADLINE_CANDIDATES 注释块取首个编辑化标题。"""
    m = HEADLINE_RE.search(narrative_text)
    if m:
        for line in m.group(1).strip().splitlines():
            line = line.strip().strip('"').strip()
            if line:
                return line
    return f"GuanHe 每日晨报(事实版) — {report_date}"


AUDIT_SIDECAR_HEADER = (
    "# Audit trail — stripped from the published report\n\n"
    "Per-bullet audit comments (snapshot-key provenance), extracted at assemble "
    "time. The verifier ran against sections/*.md WITH these comments present.\n")


def _review_notes(ws: Path, verdict: Dict[str, Any]) -> List[str]:
    """复核备注:核验意见 + 确定性核验的非 high 残留。只进 sidecar,
    正文出现「05 节」「确定性核验」这类生产过程用语违反审计分层。"""
    lines: List[str] = []
    for i in verdict.get("issues") or []:
        tag = f"{i.get('severity')}, advisory" if i.get("advisory") else i.get("severity")
        lines.append(f"- [{tag}] ({i.get('section')}) "
                     f"{i.get('claim')} — {i.get('suggestion') or ''}".rstrip(" —"))
    for note in verdict.get("quality_notes") or []:
        lines.append(f"- [note] {note}")
    check_file = ws / "verify" / "check_results.json"
    if check_file.exists():
        try:
            rows = json.loads(check_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            rows = []
        for r in rows:
            if not r.get("match", True) and r.get("severity") != "high":
                lines.append(f"- [check:{r.get('severity')}] ({r.get('section')}) "
                             f"{r.get('claim')} | {r.get('snapshot_value')}")
    if not lines:
        return []
    return ["## Review notes (not published in body)", "", *lines, ""]


def assemble(ws: Path, report_date: str, verdict: Dict[str, Any],
             style: str, provenance: bool = True) -> Path:
    """拼接成品:正文仅标题+七节(GuanHe 表面文体);口径块与审计注释进 sidecar。

    provenance=True 时 audit 注释先反查成要点末尾的来源/L·E 标记与节末来源列表
    (provenance.py),原始注释仍完整保留在 sidecar。
    """
    snapshot = json.loads((ws / "snapshot.json").read_text(encoding="utf-8"))
    narrative_raw = (ws / SECTION_SPECS["narrative"]["output_file"]
                     ).read_text(encoding="utf-8")
    headline = _extract_headline(narrative_raw, report_date)
    parts = [
        f"# {headline}",
        "",
        f"*GuanHe 每日晨报(事实版) — {report_date}*",
        "",
    ]
    # v8.7:核验未通过时照常发布 —— 顶部一行指针 + 文末核验备注清单
    degraded = verdict.get("verdict") != "pass"
    open_issues = [i for i in verdict.get("issues") or [] if not i.get("advisory")]
    if degraded:
        n = len(open_issues)
        parts += [f"> ⚠️ 本期有 {n} 项未通过自动核验的问题,报告照常发布;"
                  "清单见文末「核验备注」,阅读时请对照。", ""]
    audit_parts = [
        AUDIT_SIDECAR_HEADER,
        "",
        "## Data caliber (not published in body)",
        "",
        *_caliber_header(snapshot, style),
        "",
        f"> Verification: **{verdict.get('verdict')}** "
        f"({len(verdict.get('issues', []))} issue(s))",
        "",
        *_review_notes(ws, verdict),
    ]
    # 审计注释剥离:只删注释本身、保留换行(吃掉换行会把相邻两行合并)
    audit_comment_re = re.compile(r"[ \t]*<!--\s*audit:.*?-->")
    headline_block_re = re.compile(r"<!--\s*HEADLINE_CANDIDATES.*?-->\s*",
                                   re.DOTALL)
    sources = snapshot.get("sources") or {}
    items = item_index(sources)
    for name in SECTION_ORDER:
        content = (ws / SECTION_SPECS[name]["output_file"]).read_text(encoding="utf-8")
        content = content.replace(DONE_MARKER, "").rstrip()
        audit_parts.append(f"\n## {name}\n")
        found = re.findall(r"<!--\s*audit:.*?-->", content) or []
        clean = headline_block_re.sub("", content)
        src_lines: List[str] = []
        if provenance:
            clean, src_lines = render_section(clean, sources, report_date, items)
        clean = audit_comment_re.sub("", clean)
        clean = re.sub(r"[ \t]+(\n|$)", r"\1", clean)
        clean = re.sub(r"\n{3,}", "\n\n", clean).rstrip()
        audit_parts.append("\n".join(found))
        parts += [clean, ""]
        if src_lines:
            parts += [*src_lines, ""]
        parts += ["---", ""]
    # 图表嵌入(与 GuanHe 版式一致): US close 总览在正文首个 "## 大盘总览"
    # 标题下(顶部); finviz 热力图放 "## 美股全景" 段、紧贴 今日涨跌榜
    # 上方(涨跌榜排在板块动态之前)。其余图表暂不嵌入(逐张优化中)。
    charts_dir = ws / "final" / "charts"

    def _chart_block(heading: str, fname: str) -> List[str]:
        return [f"## {heading}", "",
                f"![{fname.split('.')[0]}](charts/{fname})", ""]

    if (charts_dir / "01_markets.png").exists():
        block = _chart_block("大盘总览", "01_markets.png")
        # 插在正文首个 ## 标题之前(= 标题/日期/核验指针区之后;不再用固定
        # 下标 4,顶部指针行会移动插入点)
        idx0 = next((i for i, p in enumerate(parts) if p.startswith("## ")),
                    len(parts))
        for i, line in enumerate(block):
            parts.insert(idx0 + i, line)
    if (charts_dir / "02_finviz_map.png").exists():
        block = _chart_block("美股全景", "02_finviz_map.png")
        # 定位「## 今日涨跌榜」标题行,插到它正上方(各 section 内容以标题行开头)
        idx = next((i for i, p in enumerate(parts)
                    if p.startswith("## 今日涨跌榜")), len(parts))
        for i, line in enumerate(block):
            parts.insert(idx + i, line)
    if degraded:
        shown = open_issues or (verdict.get("issues") or [])
        # 末节收尾已带一条 ---,这里不重复加分隔线
        parts += [f"## ⚠️ 核验备注(自动标注,{len(shown)} 项未决)", ""]
        for i in shown:
            tag = str(i.get("severity") or "?")
            if i.get("advisory"):
                tag += ", advisory"
            parts.append(
                (f"- **[{tag}]** ({i.get('section')}) {i.get('claim')}"
                 f" — {i.get('suggestion') or ''}").rstrip(" —"))
        parts += ["",
                  "*以上由自动核验标注,供阅读时对照;口径与审计轨迹见同目录 "
                  "report.audit.md。*", ""]
    out = ws / "final" / "report.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(parts), encoding="utf-8")
    (ws / "final" / "report.audit.md").write_text(
        "\n".join(audit_parts), encoding="utf-8")
    return out


def _coerce_event_actuals(sources: Dict[str, Any]) -> int:
    """旧快照的日历 actual 字符串 → 数值(幂等;与 build_snapshot._clean 同规则)。
    返回转换行数,供补丁批次判断是否需要回写快照。"""
    def _num(s: str) -> Optional[float]:
        try:
            return float(s.replace(",", "").replace("−", "-"))
        except ValueError:
            return None

    n = 0
    econ = sources.get("economic_events") or {}
    for bucket in ("recent", "upcoming"):
        for row in econ.get(bucket) or []:
            if isinstance(row, dict) and isinstance(row.get("actual"), str):
                v = _num(row["actual"].replace("%", ""))
                if v is not None:
                    row["actual"] = int(v) if v == int(v) else v
                    n += 1
    return n


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report_date", help="YYYY-MM-DD,对应 build_snapshot 的日期")
    parser.add_argument("--zcode-bin", default=os.environ.get("ZCODE_BIN", DEFAULT_ZCODE_BIN))
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SEC)
    parser.add_argument("--max-verify-retries", type=int, default=DEFAULT_MAX_VERIFY_RETRIES)
    parser.add_argument("--style", choices=("en", "cn"), default=DEFAULT_STYLE,
                        help="风格档案: en=与 GuanHe 一致的英文正文(默认); cn=中文正文")
    parser.add_argument(
        "--llm-verify", choices=("off", "annotate", "light", "full"),
        default="annotate",
        help="确定性台账核验通过后的 LLM 叙事核验: off=不跑; annotate(默认)="
             "light 同款只读正文核验,意见一律作复核备注、不参与门控; light=同上但"
             "high 意见触发修订; full=旧版全量核验(挂快照)。"
             "确定性核验有 high 失败时一律跳过 LLM、直接修订")
    parser.add_argument("--no-plan", action="store_true",
                        help="跳过写作前的编辑计划(故事归属 + 规范数值)")

    parser.add_argument("--no-provenance", action="store_true",
                        help="成品不渲染要点来源标记与节末来源列表(审计注释仍进 sidecar)")
    parser.add_argument("--max-parallel", type=int, default=0,
                        help="限制 stage-1 同时运行的 agent 数(0=全并发,原行为)。"
                             "与后端/交互会话同机的低内存时段建议 2")
    parser.add_argument("--strict-verify", action="store_true",
                        help="核验超限时回到旧行为:rc=2 转人工、不发布"
                             "(v8.7 起默认照常发布,未决问题自动标注在报告"
                             "顶部与文末)")
    parser.add_argument("--fake-zcode", action="store_true",
                        help="不用 ZCode:写占位章节,核验先 fail 后 pass,验证门控逻辑")
    parser.add_argument("--publish-cmd",
                        help="发布钩子:发布成功后执行的 shell 命令,"
                             "环境变量 REPORT_FILE 为成品路径;缺省仅打印")
    args = parser.parse_args()

    ws = workspace(args.report_date)
    snapshot = ws / "snapshot.json"

    if not snapshot.exists():
        print(f"snapshot missing: {snapshot} — run build_snapshot.py first",
              file=sys.stderr)
        return 1
    ws.mkdir(parents=True, exist_ok=True)
    snap_data = json.loads(snapshot.read_text(encoding="utf-8"))
    snap_src = snap_data.get("sources") or {}
    if (assign_item_ids(snap_src) + annotate_event_units(snap_src)
            + flag_reported_items(snap_src) + _coerce_event_actuals(snap_src)):
        # 旧快照补条目 id、日历单位、传闻标记与 actual 数值化;只加字段/统一
        # 类型、不动数值含义,台账才能按条目引用、核验比较不踩类型坑
        snapshot.write_text(json.dumps(snap_data, ensure_ascii=False, default=str),
                            encoding="utf-8")
        print("[snapshot] backfilled item ids / event units / reported flags "
              "/ numeric actuals", flush=True)
    state = load_state(ws)
    t_start = time.time()

    try:
        run_sections(args, ws, snapshot, state)

        def _needs_human(verdict: Dict[str, Any], reason: str) -> int:
            (ws / "NEEDS_HUMAN_REVIEW.md").write_text(
                json.dumps(verdict, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"{reason} → {ws / 'NEEDS_HUMAN_REVIEW.md'}", file=sys.stderr)
            return 2

        while True:
            # 手工删 verdict 重跑(核验器修复后的标准动作,见 DEPLOYMENT §6.3)
            # 应重开核验计数 —— 否则上一段的确定性失败会吃掉 LLM 修订轮的
            # 预算(2026-11-05 实测:attempts=4>3 直接转人工,修订轮没跑)。
            # llm_fix_used 同理重置:上一轮若没真正发生修订,不该抑制本轮
            # 的 LLM 复审(P26:曾导致两个真问题未修就发布)。
            if not (ws / "verify" / "verdict.json").exists():
                if state["verify"].get("attempts") or state["verify"].get("llm_fix_used"):
                    print(f"[verify] verdict reset detected; attempts "
                          f"{state['verify'].get('attempts')} -> 0, "
                          f"llm_fix_used -> False", flush=True)
                state["verify"]["attempts"] = 0
                state["verify"]["llm_fix_used"] = False
            verdict = run_verifier(args, ws, snapshot, state)
            if verdict.get("verdict") == "pass":
                break
            attempts = state["verify"]["attempts"]
            reason = None
            if attempts > args.max_verify_retries:
                reason = f"verify failed after {attempts} attempts"
            elif not revise_sections(args, ws, snapshot, state,
                                     _high_issues(verdict)):
                reason = "high issues only touch locked sections"
            if reason:
                if args.strict_verify:
                    return _needs_human(verdict, reason)
                # v8.7:核验超限不再拦截 —— 照常发布,未决问题自动标注在
                # 报告(顶部指针 + 文末核验备注);旧 rc=2 转人工行为由
                # --strict-verify 恢复。轮次用尽/锁定节两类超限同此处理。
                print(f"[verify] {reason} — publishing with annotations",
                      file=sys.stderr, flush=True)
                state["verify"]["published_with_issues"] = True
                break

        # Charts before assemble so report.md can link them
        try:
            from render_charts import render_all
            render_all(ws)
        except Exception as e:  # noqa: BLE001 — charts are best-effort
            print(f"[charts] skipped: {e}", flush=True)

        report = assemble(ws, args.report_date, verdict, args.style,
                          provenance=not args.no_provenance)
        if args.publish_cmd:
            env = {**os.environ, "REPORT_FILE": str(report)}
            subprocess.run(shlex.split(args.publish_cmd), env=env, check=True)
        state["published"] = True
        state["final_report"] = str(report)
        _record_timing(state, "total", time.time() - t_start)
        save_state(ws, state)
        print(f"published: {report}")
        print(f"token usage: {state.get('usage_total')}")
        print(f"timings: {state.get('timings')}")
        # 每日流程卡(纯脚本,不调 LLM):把当日运行数据注入模板,产出
        # {ws}/pipeline-overview.html;失败只提示,绝不阻断发报。
        try:
            import gen_pipeline_card
            print(f"[card] {gen_pipeline_card.generate(ws)}", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"[card] skipped: {e}", flush=True)
        return 0
    except (subprocess.TimeoutExpired, RuntimeError) as e:
        print(f"pipeline error: {e}", file=sys.stderr)
        save_state(ws, state)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
