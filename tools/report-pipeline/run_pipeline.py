#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""CNT 风格 Daily Market Brief 生成管线编排器(v2)。

流程(详见 tools/report-pipeline/README.md):
    Stage 0   build_snapshot.py 产出的冻结快照(本脚本不重建,只校验存在)
    Stage 1-6 六个章节 Agent + movers 确定性渲染,每个只写各自文件;
              narrative 附带 HEADLINE_CANDIDATES(编辑化标题),macro 的块
              矩阵(Releases/拍卖/Fed/利率预期/Forward Catalysts)由编排器
              按快照纯代码判定后注入任务书
    Stage 7   核验 Agent:自写 check.py 对账快照数字(含三元组/since-claims/
              概率/归因映射/结构 lint)+ 叙事质量审查,输出 verify/verdict.json
    Gate      纯 Python 门控: pass → 拼接发布(审计注释剥离到 report.audit.md);
              fail → 按问题清单 --resume 修订对应章节再核验(≤ N 轮);
              仍 fail → 退出码 2 转人工

设计规则: 文件即契约(编排器只读 Agent 写的文件)、步级幂等(state.json
断点续跑)、门控不经过 LLM、全审计落盘、usage 记录进 state 以便成本核算。

用法:
    # 完整跑(先确保 snapshot.json 已由 build_snapshot.py 生成)
    python tools/report-pipeline/run_pipeline.py 2026-10-24 [--style en|cn]

    # 无 ZCode 环境下测试状态机/门控/重试逻辑(写占位章节,先 fail 后 pass)
    python tools/report-pipeline/run_pipeline.py 2026-10-24 --fake-zcode

退出码: 0=已发布  1=运行错误  2=核验未通过,待人工
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prompts import (  # noqa: E402
    AGENT_ORDER,
    SECTION_ORDER,
    SECTION_SPECS,
    agent_section_names,
    build_agent_prompt,
    build_verifier_prompt,
    fake_section_text,
)

DEFAULT_ZCODE_BIN = "/root/.zcode/server/bin/zcode"
DEFAULT_TIMEOUT_SEC = 900
DEFAULT_MAX_VERIFY_RETRIES = 2
DEFAULT_STYLE = "en"
DONE_MARKER = "<!-- SECTION DONE -->"
HEADLINE_RE = re.compile(
    r"<!--\s*HEADLINE_CANDIDATES\s*\n(.*?)-->", re.DOTALL)


# ── workspace & state ──────────────────────────────────────────────────

def workspace(report_date: str) -> Path:
    return REPO_ROOT / "backend" / "reports" / "pipeline" / report_date


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


def _add_usage(state: Dict[str, Any], usage: Optional[Dict[str, Any]]) -> None:
    if not usage:
        return
    tot = state.setdefault("usage_total", {})
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
    snapshot: Path,
    timeout: int,
    resume_session: Optional[str] = None,
) -> Dict[str, Any]:
    """一次无头 ZCode 调用。返回解析后的 JSON(sessionId/response/usage)。

    注意: 超时(TimeoutExpired)直接向上抛;调用方按各自的"文件契约"决定
    超时后是否仍可接受 —— Agent 写完产物文件后进程可能迟迟不退出,
    产物有效性以文件为准,不以进程退出为准。
    """
    cmd = [zcode_bin, "-p", prompt, "--json", "--mode", "yolo",
           "--surface", "terminal", "--cwd", str(ws),
           "--attach", str(snapshot)]
    if resume_session:
        cmd += ["--resume", resume_session]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(
            f"zcode exited {proc.returncode}: {proc.stderr[-500:]}"
        )
    return _parse_zcode_json(proc.stdout)


# ── fake mode(无 ZCode 时测试状态机/门控/重试) ────────────────────────

def _fake_section(ws: Path, name: str, corrected: bool = False) -> None:
    """占位章节:故意让 drivers 含一个与快照矛盾的数字,供核验流程抓出来。"""
    f = ws / SECTION_SPECS[name]["output_file"]
    f.parent.mkdir(parents=True, exist_ok=True)
    text = fake_section_text(name, corrected=corrected)
    f.write_text(text + f"\n{DONE_MARKER}\n", encoding="utf-8")


# ── movers 确定性渲染 + Macros 块矩阵 ─────────────────────────────────

FINVIZ_GAINERS_URL = "https://finviz.com/screener.ashx?v=111&s=ta_topgainers"
FINVIZ_LOSERS_URL = "https://finviz.com/screener.ashx?v=111&s=ta_toplosers"


def _fmt_volume(v) -> str:
    if v is None:
        return "—"
    try:
        n = float(v)
    except (TypeError, ValueError):
        return str(v)
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if n >= div:
            return f"{n / div:.1f}{suffix}"
    return str(int(n))


def render_movers(ws: Path, snapshot: Dict[str, Any], style: str = "en") -> None:
    """sections/05_movers.md —— 数字不经 LLM,直接自快照渲染(CNT 的 Movers 节
    本就是 Finviz 筛选器链接 + 榜单,无需叙事加工)。"""
    market = (snapshot.get("sources") or {}).get("market") or {}
    movers = market.get("top_movers") or []
    as_of = market.get("snapshot_date") or "?"
    cn = style == "cn"
    lines = ["## Today's Movers", ""]
    if not movers:
        missing = "「不可得」——finviz 异动数据缺失" if cn else \
            "(not in snapshot) — no finviz movers available"
        lines += [f"{missing}. <!-- audit: [data market.top_movers] -->"]
    else:
        lead = (f"全市场异动榜(按单日绝对涨跌幅排序,截至 {as_of},Finviz):"
                if cn else
                f"Top absolute daily moves across the screener universe, "
                f"as of {as_of} (Finviz):")
        lines += [f"{lead} <!-- audit: [data market.top_movers] -->", ""]
        if cn:
            lines += ["| 代码 | 公司 | 板块 | 价格 | 涨跌幅 | 成交量 |",
                      "|---|---|---|---:|---:|---:|"]
        else:
            lines += ["| Ticker | Company | Sector | Price | Chg % | Volume |",
                      "|---|---|---|---:|---:|---:|"]
        gainers = [m for m in movers if (m.get("change_percent") or 0) > 0]
        losers = [m for m in movers if (m.get("change_percent") or 0) < 0]
        for _, group in (("GAINERS", sorted(gainers, key=lambda m: -m["change_percent"])),
                         ("LOSERS", sorted(losers, key=lambda m: m["change_percent"]))):
            for m in group:
                price = m.get("price")
                lines.append(
                    f"| {m.get('ticker', '?')} | {m.get('company') or '—'} "
                    f"| {m.get('sector') or '—'} "
                    f"| {price if price is not None else '—'} "
                    f"| {m['change_percent']:+.2f}% "
                    f"| {_fmt_volume(m.get('volume'))} |")
    screeners = ("完整榜单:[涨幅榜]({g}) · [跌幅榜]({l}) (Finviz)" if cn else
                 "Full screeners: [top gainers]({g}) · [top losers]({l}) (Finviz)")
    lines += ["", screeners.format(g=FINVIZ_GAINERS_URL, l=FINVIZ_LOSERS_URL), ""]
    f = ws / SECTION_SPECS["movers"]["output_file"]
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text("\n".join(lines) + f"\n{DONE_MARKER}\n", encoding="utf-8")


def macro_blocks(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """纯代码判定 Macros 当日块矩阵(不经 LLM)—— 复刻 CNT '不是每期一致'。

    releases          economic_events.recent 存在含 actual 的事件
    auction           拍卖事件:今日有结果或未来有拍卖
    fomc              FOMC 决议/纪要在 recent 含 actual,或在 upcoming 窗口
    rate_expectations macro_pillars.fed_odds 存在
    forward           恒 True
    """
    sources = snapshot.get("sources") or {}
    econ = sources.get("economic_events") or {}
    pillars = sources.get("macro_pillars") or {}

    def _has_actual(e: Dict[str, Any]) -> bool:
        return e.get("actual") not in (None, "")

    recent = econ.get("recent") or []
    upcoming = econ.get("upcoming") or []
    releases = any(_has_actual(e) for e in recent)

    auctions = pillars.get("auction_events") or {}
    auction = bool(auctions.get("today")) or bool(auctions.get("upcoming"))

    def _is_fomc(e: Dict[str, Any]) -> bool:
        name = (e.get("event") or "").lower()
        return "fomc" in name or "fed interest rate" in name or "federal funds rate" in name

    fomc = (any(_is_fomc(e) and _has_actual(e) for e in recent)
            or any(_is_fomc(e) for e in upcoming)
            or bool(pillars.get("next_fomc_odds")))

    return {
        "releases": releases,
        "auction": auction,
        "fomc": fomc,
        "rate_expectations": bool(pillars.get("fed_odds")),
        "forward": True,
        "recent_with_actual": sum(1 for e in recent if _has_actual(e)),
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


def run_sections(args: argparse.Namespace, ws: Path, snapshot: Path,
                 state: Dict[str, Any]) -> None:
    """按 agent 分组执行:narrative+drivers = overall_market 一次调用产两文件
    (防重复 + 省一次调用);movers 确定性渲染;其余单章节一调用。"""
    snapshot_data = json.loads(snapshot.read_text(encoding="utf-8"))
    blocks = macro_blocks(snapshot_data)
    state["macro_blocks"] = blocks
    print(f"[macro] blocks: { {k: v for k, v in blocks.items()} }", flush=True)
    for agent in AGENT_ORDER:
        names = agent_section_names(agent)
        if all(section_done(ws, n) for n in names):
            for n in names:
                state["sections"].setdefault(n, {"status": "done"})
            continue
        print(f"[agent] {agent} ({', '.join(names)}) ...", flush=True)
        t0 = time.time()
        specs = [SECTION_SPECS[n] for n in names]
        if specs[0].get("mode") == "deterministic":
            render_movers(ws, snapshot_data, style=args.style)
            for n in names:
                state["sections"][n] = {"status": "done", "mode": "deterministic"}
        elif args.fake_zcode:
            for n in names:
                _fake_section(ws, n)
                state["sections"][n] = {"status": "done"}
        else:
            try:
                result = run_agent(
                    args.zcode_bin, ws,
                    build_agent_prompt(agent, style=args.style, blocks=blocks),
                    snapshot, args.timeout)
                _add_usage(state, result.get("usage"))
                for n in names:
                    state["sections"][n] = {
                        "status": "done" if section_done(ws, n) else "missing_output",
                        "session_id": result.get("sessionId"),
                        "usage": result.get("usage") if n == names[0] else None,
                    }
            except subprocess.TimeoutExpired:
                # 文件契约: 超时但产物文件完整 → 接受(usage 未知,不计)
                if not all(section_done(ws, n) for n in names):
                    raise
                for n in names:
                    state["sections"][n] = {
                        "status": "done",
                        "note": "agent timed out but output file complete",
                    }
            for n in names:
                if state["sections"][n]["status"] != "done":
                    raise RuntimeError(
                        f"agent {agent}: finished but output file "
                        f"{SECTION_SPECS[n]['output_file']} missing/incomplete"
                    )
        for n in names:
            _record_timing(state, f"section:{n}", time.time() - t0)
            print(f"[section] {n} done in "
                  f"{state['timings'][f'section:{n}'][-1]}s", flush=True)
        save_state(ws, state)
    missing = [n for n in SECTION_ORDER
               if state["sections"].get(n, {}).get("status") != "done"]
    if missing:
        raise RuntimeError(f"sections incomplete: {missing}")


def run_verifier(args: argparse.Namespace, ws: Path, snapshot: Path,
                 state: Dict[str, Any]) -> Dict[str, Any]:
    verdict_file = ws / "verify" / "verdict.json"

    # 复用判定(verdict 有效性范围 = 章节内容): 若现存 verdict 比所有章节文件
    # 都新且可解析,说明它针对的就是当前章节内容 —— 直接复用,不重跑。
    # 典型场景: 核验 Agent 写完 verdict 后进程迟迟不退被超时杀掉,重跑管线时
    # 无需再付一整轮核验成本。用严格大于(>):同秒并发的 mtime 并列(如 fake
    # 模式)不构成"更新",避免误复用陈旧 fail 引发空转修订循环。
    section_files = [ws / SECTION_SPECS[n]["output_file"] for n in SECTION_ORDER]
    consecutive_reuses = state["verify"].get("consecutive_reuses", 0)
    if (consecutive_reuses < 2
            and verdict_file.exists() and all(f.exists() for f in section_files)):
        v_mtime = verdict_file.stat().st_mtime
        newest_section = max(f.stat().st_mtime for f in section_files)
        if v_mtime > newest_section:
            try:
                verdict = json.loads(verdict_file.read_text(encoding="utf-8"))
                if verdict.get("verdict") in ("pass", "fail"):
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
    mtime_before = verdict_file.stat().st_mtime if verdict_file.exists() else 0
    if args.fake_zcode:
        _fake_verdict(ws, attempt)
    else:
        try:
            result = run_agent(args.zcode_bin, ws,
                               build_verifier_prompt(style=args.style),
                               snapshot, args.timeout)
            _add_usage(state, result.get("usage"))
            state["verify"]["session_id"] = result.get("sessionId")
            state["verify"][f"usage_attempt_{attempt}"] = result.get("usage")
        except subprocess.TimeoutExpired:
            # 文件契约: 超时但本轮内写出了有效 verdict → 接受
            fresh = (verdict_file.exists()
                     and verdict_file.stat().st_mtime >= mtime_before)
            if not fresh:
                raise
            print("[verify] agent timed out but verdict.json was written — "
                  "accepting file contract", flush=True)
    _record_timing(state, f"verify:attempt_{attempt}", time.time() - t0)
    print(f"[verify] attempt {attempt} done in {state['timings'][f'verify:attempt_{attempt}'][-1]}s", flush=True)
    if not verdict_file.exists():
        raise RuntimeError("verifier finished but verify/verdict.json missing")
    verdict = json.loads(verdict_file.read_text(encoding="utf-8"))
    state["verify"]["status"] = verdict.get("verdict", "fail")
    save_state(ws, state)
    return verdict


def revise_sections(args: argparse.Namespace, ws: Path, snapshot: Path,
                    state: Dict[str, Any], issues: List[Dict[str, Any]]) -> None:
    """按核验问题清单定向修订(--resume 续接原会话)。

    问题按 agent 分组:overall_market 组(narrative+drivers)的问题合并为一次
    修订调用 —— 两文件出自同一会话,拆开修会破坏"速览/深挖"分工。
    """
    by_agent: Dict[str, List[Dict[str, Any]]] = {}
    for issue in issues:
        sec = issue.get("section")
        if sec in SECTION_SPECS:
            agent = SECTION_SPECS[sec].get("agent", sec)
            by_agent.setdefault(agent, []).append(issue)
    for agent, agent_issues in by_agent.items():
        names = agent_section_names(agent)
        print(f"[revise] {agent} ({', '.join(names)}; "
              f"{len(agent_issues)} issue(s)) ...", flush=True)
        t0 = time.time()
        if args.fake_zcode:
            for n in names:
                _fake_section(ws, n, corrected=True)
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
            "labor between the files if you wrote more than one.\n\n"
            f"Issues:\n{feedback}"
        )
        session_id = state["sections"].get(names[0], {}).get("session_id")
        try:
            run_agent(args.zcode_bin, ws, prompt, snapshot, args.timeout,
                      resume_session=session_id)
        except subprocess.TimeoutExpired:
            if not all(section_done(ws, n) for n in names):
                raise
            print(f"[revise] {agent}: agent timed out but revised files complete",
                  flush=True)
        _record_timing(state, f"revise:{agent}", time.time() - t0)
        for n in names:
            if not section_done(ws, n):
                raise RuntimeError(f"revision of {n} did not produce a valid file")


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
        if isinstance(v, dict) and v.get("report_date"):
            lag = v.get("lag_days") or 0
            suffix = f"(滞后{lag}天)" if (cn and lag) else (f"(lag {lag}d)" if lag else "")
            return f"{label}✓{v['report_date']}{suffix}"
        return f"{label}✗{'不可得' if cn else 'unavailable'}"

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
                _show("the_close", "The Close 摘要"),
                _show("cnbc_closing_bell", "CNBC Closing Bell 摘要"),
                (f"跨资产行情✓{cross_span}({len(cross)}项)" if cross_span else "跨资产行情✗不可得"),
                (f"板块/个股(finviz✓{market.get('snapshot_date') or '?'})"
                 f"✓{len(market.get('sp500_sector_avg') or [])}板块"
                 f"/{len(market.get('top_movers') or [])}只异动" if market else "板块/个股✗不可得"),
                (f"经济日历✓近{n_recent}/前瞻{n_upcoming}条{econ_note}" if econ else "经济日历✗不可得"),
                pillar_note,
            ])),
        ]
    else:
        lines = [
            f"> **Data caliber** snapshot frozen at {snapshot.get('generated_at', '?')}; "
            f"all numbers in this brief refer to that snapshot only.",
            "> **Sources** "
            + " · ".join(filter(None, [
                _show("the_close", "The Close digest"),
                _show("cnbc_closing_bell", "Closing Bell digest"),
                (f"cross-asset✓{cross_span}({len(cross)} series)" if cross_span else "cross-asset✗unavailable"),
                (f"sectors/stocks(finviz✓{market.get('snapshot_date') or '?'})"
                 f"✓{len(market.get('sp500_sector_avg') or [])} sectors"
                 f"/{len(market.get('top_movers') or [])} movers" if market else "sectors/stocks✗unavailable"),
                (f"economic calendar✓{n_recent} recent/{n_upcoming} upcoming" if econ else "economic calendar✗unavailable"),
                pillar_note,
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
    return f"Daily market brief — {report_date}"


AUDIT_SIDECAR_HEADER = (
    "# Audit trail — stripped from the published report\n\n"
    "Per-bullet audit comments (snapshot-key provenance), extracted at assemble "
    "time. The verifier ran against sections/*.md WITH these comments present.\n")


def assemble(ws: Path, report_date: str, verdict: Dict[str, Any],
             style: str) -> Path:
    snapshot = json.loads((ws / "snapshot.json").read_text(encoding="utf-8"))
    narrative_raw = (ws / SECTION_SPECS["narrative"]["output_file"]
                     ).read_text(encoding="utf-8")
    headline = _extract_headline(narrative_raw, report_date)
    parts = [
        f"# {headline}",
        "",
        f"*Daily market brief — {report_date}*",
        "",
        *_caliber_header(snapshot, style),
        f"> Verification: **{verdict.get('verdict')}** "
        f"({len(verdict.get('issues', []))} issue(s) resolved)",
        "",
    ]
    audit_parts = [AUDIT_SIDECAR_HEADER]
    # 审计注释剥离:只删注释本身、保留换行(吃掉换行会把相邻两行合并)
    audit_comment_re = re.compile(r"[ \t]*<!--\s*audit:.*?-->")
    headline_block_re = re.compile(r"<!--\s*HEADLINE_CANDIDATES.*?-->\s*",
                                   re.DOTALL)
    for name in SECTION_ORDER:
        content = (ws / SECTION_SPECS[name]["output_file"]).read_text(encoding="utf-8")
        content = content.replace(DONE_MARKER, "").rstrip()
        audit_parts.append(f"\n## {name}\n")
        found = re.findall(r"<!--\s*audit:.*?-->", content) or []
        clean = headline_block_re.sub("", content)
        clean = audit_comment_re.sub("", clean)
        clean = re.sub(r"[ \t]+(\n|$)", r"\1", clean)
        clean = re.sub(r"\n{3,}", "\n\n", clean).rstrip()
        audit_parts.append("\n".join(found))
        parts += [clean, "", "---", ""]
    out = ws / "final" / "report.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(parts), encoding="utf-8")
    (ws / "final" / "report.audit.md").write_text(
        "\n".join(audit_parts), encoding="utf-8")
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report_date", help="YYYY-MM-DD,对应 build_snapshot 的日期")
    parser.add_argument("--zcode-bin", default=os.environ.get("ZCODE_BIN", DEFAULT_ZCODE_BIN))
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SEC)
    parser.add_argument("--max-verify-retries", type=int, default=DEFAULT_MAX_VERIFY_RETRIES)
    parser.add_argument("--style", choices=("en", "cn"), default=DEFAULT_STYLE,
                        help="风格档案: en=与 CNT 一致的英文正文(默认); cn=中文正文")
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
    state = load_state(ws)
    t_start = time.time()

    try:
        run_sections(args, ws, snapshot, state)

        while True:
            verdict = run_verifier(args, ws, snapshot, state)
            if verdict.get("verdict") == "pass":
                break
            attempts = state["verify"]["attempts"]
            if attempts > args.max_verify_retries:
                (ws / "NEEDS_HUMAN_REVIEW.md").write_text(
                    json.dumps(verdict, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                print(f"verify failed after {attempts} attempts → "
                      f"{ws / 'NEEDS_HUMAN_REVIEW.md'}", file=sys.stderr)
                return 2
            revise_sections(args, ws, snapshot, state, verdict.get("issues", []))

        report = assemble(ws, args.report_date, verdict, args.style)
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
        return 0
    except (subprocess.TimeoutExpired, RuntimeError) as e:
        print(f"pipeline error: {e}", file=sys.stderr)
        save_state(ws, state)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
