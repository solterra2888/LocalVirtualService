#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""CNT 风格 Daily Market Brief 生成管线编排器(v4)。

流程(详见 tools/report-pipeline/README.md):
    Stage 0   build_snapshot.py 产出的冻结快照(本脚本不重建,只校验存在)
    Stage 1-6 章节 Agent + movers 确定性渲染; macro 块矩阵由编排器按
              美国一线白名单 / 周五规则纯代码判定后注入任务书
    Stage 7a  确定性 verify_checks.py(数字/三元组/since/概率/矩阵/结构)
    Stage 7b  核验 Agent:叙事质量审查 → verify/verdict.json
    Gate      纯 Python 门控: pass → 拼接发布(审计注释+口径块进 sidecar);
              fail → 按问题清单 --resume 修订对应章节再核验(≤ N 轮);
              仍 fail → 退出码 2 转人工

设计规则: 文件即契约、步级幂等、门控不经过 LLM、全审计落盘。

用法:
    python tools/report-pipeline/run_pipeline.py 2026-10-24 [--style en|cn]
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
    build_light_verifier_prompt,
    build_verifier_prompt,
    fake_section_text,
)
from verify_checks import claims_path, run_checks  # noqa: E402

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
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(
            f"zcode exited {proc.returncode}: {proc.stderr[-500:]}"
        )
    return _parse_zcode_json(proc.stdout)


# ── fake mode(无 ZCode 时测试状态机/门控/重试) ────────────────────────

def _fake_section(ws: Path, name: str, corrected: bool = False,
                  blocks: Optional[Dict[str, Any]] = None,
                  style: str = "en") -> None:
    """占位章节:故意让 drivers 含一个与快照矛盾的数字,供核验流程抓出来。"""
    f = ws / SECTION_SPECS[name]["output_file"]
    f.parent.mkdir(parents=True, exist_ok=True)
    text = fake_section_text(name, corrected=corrected)
    if style == "cn":
        text = text.replace("Placeholder headline clause. Other clause.",
                            "占位标题一。占位标题二。")
    if name == "macro" and blocks:
        parts = ["## Macros\n"]
        if blocks.get("releases"):
            parts.append(
                "### Releases\n"
                "#### Durable Goods Orders\n"
                "- **+1.8%** (est: +1.7%, prior: +0.1%) "
                "<!-- audit: [data economic_events.recent] -->\n"
            )
        if blocks.get("rate_expectations"):
            parts.append(
                "### Rate Expectations\n"
                "- Hike Odds ~8% (prior: ~16%) "
                "<!-- audit: [data macro_pillars.fed_odds] -->\n"
            )
        if blocks.get("cross_asset_review"):
            parts.append(
                "### Treasuries and oil\n"
                "- Placeholder cross-asset review (fake mode). "
                "<!-- audit: [data market.cross_asset] -->\n"
            )
        parts.append(
            "### Forward Catalysts\n"
            "- Placeholder catalyst (fake mode). "
            "<!-- audit: [data economic_events.upcoming] -->\n"
        )
        text = "\n".join(parts)
    f.write_text(text + f"\n{DONE_MARKER}\n", encoding="utf-8")
    claims_path(ws, name).write_text("[]\n", encoding="utf-8")


# ── movers 确定性渲染 + Macros 块矩阵 ─────────────────────────────────

# CNT 美国一线 Releases 事件族(按优先级从高到低);每天最多 2 个族。
# main: 命中则该行可作为本族主行;also: 同日同族的辅助行(挂在 related 里,不单独占名额)。
# 事件名以 EODHD 日历为准(如 "Non Farm Payrolls" / "Fed Interest Rate Decision")。
US_TIER1_FAMILIES = (
    ("fomc", r"fed\s+interest\s+rate|fomc\s+(rate|decision)|federal\s+funds\s+rate",
     r"fomc\s+(statement|economic\s+projections|press)"),
    ("jobs", r"non[\s\-]?farm\s+payroll|employment\s+situation",
     r"unemployment\s+rate|average\s+hourly|participation"),
    ("cpi", r"\bcpi\b|consumer\s+price|^inflation\s+rate",
     r"core\s+(cpi|inflation)"),
    ("pce", r"core\s+pce\s+price|\bpce\s+price|\bpce\b",
     r"personal\s+(income|spending|outlay)"),
    ("gdp", r"\bgdp\s+growth|^gdp\b|gross\s+domestic", r"gdp\s+price|core\s+pce"),
    ("ppi", r"\bppi\b|producer\s+price", r"core\s+ppi"),
    ("retail", r"retail\s+sales", r"control\s+group"),
    ("ism", r"ism\s+(manufacturing|services)|ism\s+pmi", r"ism\s+.*(prices|employment|new\s+orders)"),
    ("jolts", r"jolts|job\s+openings", None),
    ("durable", r"durable\s+goods\s+orders", r"durable\s+goods|core\s+capital|capital\s+goods"),
    ("michigan", r"michigan\s+consumer\s+sentiment|u\.?\s*of\s*m\s+sentiment|consumer\s+sentiment",
     r"michigan"),
    ("housing", r"housing\s+starts|building\s+permits", r"building\s+permits|housing\s+starts"),
    ("existing", r"existing\s+home\s+sales", None),
    ("auction10", r"10[\-\s]?year\s+.*auction|auction.*10[\-\s]?year", None),
)
# 分项/子口径不作主行(Durable Goods Ex Transp / Ex Defense / Non Defense Goods Orders Ex Air 等)
_SUBCOMPONENT_RE = re.compile(r"\bex\b|excluding|non[\s\-]?defense|\bnsa\b", re.I)
_FAMILIES = [(k, re.compile(m, re.I), re.compile(a, re.I) if a else None)
             for k, m, a in US_TIER1_FAMILIES]
# 同一事件多口径行时的取舍:m/m 优先(CNT 以环比为题),其次指数/水平,最后 y/y
_COMPARISON_RANK = {"mom": 0, "": 1, None: 1, "index": 1, "yoy": 2}

# 极端行情阈值:加息概率变动(百分点)或 10Y 变动(bp)
EXTREME_ODDS_PT = 10.0
EXTREME_10Y_BP = 8.0


def _event_date(e: Dict[str, Any]) -> str:
    return str(e.get("date") or "")[:10]


def _is_us(e: Dict[str, Any]) -> bool:
    return (e.get("country") or "").upper() in ("US", "USA", "UNITED STATES")


def _has_actual(e: Dict[str, Any]) -> bool:
    return e.get("actual") not in (None, "")


def _cmp_rank(e: Dict[str, Any]) -> int:
    return _COMPARISON_RANK.get((e.get("comparison") or "").lower() or None, 1)


def _brief(e: Dict[str, Any]) -> Dict[str, Any]:
    keys = ("event", "comparison", "period", "time_et", "actual", "forecast", "previous")
    return {k: e[k] for k in keys if e.get(k) not in (None, "")}


def select_tier1_releases(snapshot: Dict[str, Any],
                          limit: int = 2) -> List[Dict[str, Any]]:
    """T 日美国一线、含 actual 的发布,最多 limit 个**事件族**。

    - 按 US_TIER1_FAMILIES 的优先级取族(FOMC > 非农 > CPI > PCE > GDP > ... > 耐用品 > 房屋);
    - 每族只取一行主行(排除 Ex-*/Non-Defense 分项;m/m 优先,其次水平,最后 y/y);
    - 同族其余行(其他口径、辅助事件)放进主行副本的 related,不占名额。
    """
    report_date = snapshot.get("report_date") or ""
    recent = ((snapshot.get("sources") or {}).get("economic_events") or {}).get(
        "recent") or []
    tday = [e for e in recent if _is_us(e) and _event_date(e) == report_date]
    picked: List[Dict[str, Any]] = []
    for key, main_rx, also_rx in _FAMILIES:
        cands = [e for e in tday
                 if main_rx.search(e.get("event") or "")
                 and not _SUBCOMPONENT_RE.search(e.get("event") or "")]
        mains = [e for e in cands if _has_actual(e)]
        if not mains:
            continue
        mains.sort(key=lambda e: (_cmp_rank(e), len(e.get("event") or "")))
        main = dict(mains[0])
        main["family"] = key
        related = [_brief(e) for e in cands if e is not mains[0]]
        if also_rx:
            related += [_brief(e) for e in tday
                        if e not in cands and also_rx.search(e.get("event") or "")
                        and not _SUBCOMPONENT_RE.search(e.get("event") or "")]
        if related:
            main["related"] = related[:6]
        picked.append(main)
        if len(picked) >= limit:
            break
    return picked


# Closing Bell 摘要里 Stock Movers 段的标题(中英都有:模板要求英文,实际输出常为中文)
_MOVERS_START_RE = re.compile(
    r"^##\s+(Stock Movers|股市异动股)\s*$", re.I | re.M)
_MOVERS_END_RE = re.compile(
    r"^##\s+(Company Deep Dives|公司深度|Sector Action|行业板块|交易策略)",
    re.I | re.M)
_SUBHEAD_MAP = (
    (re.compile(r"^###\s+(Winners|涨幅居前)\s*$", re.I | re.M), "### Winners"),
    (re.compile(r"^###\s+(Losers|跌幅居前)\s*$", re.I | re.M), "### Losers"),
    (re.compile(r"^###\s+(After[-\s]?Hours Movers|盘后异动)\s*$", re.I | re.M),
     "### After-Hours Movers"),
)


def extract_closing_bell_movers(digest: Optional[str]) -> Optional[str]:
    """从 Closing Bell 摘要抽出 Winners / Losers / After-Hours 表。

    这是 CNT Today's Movers 的真正来源(节目点名的异动股 + 催化剂),
    不是 Finviz 按绝对涨跌幅排的全市场榜。摘要没有该段时返回 None。
    """
    if not digest or not _MOVERS_START_RE.search(digest):
        return None
    start = _MOVERS_START_RE.search(digest)
    end = _MOVERS_END_RE.search(digest, start.end())
    body = digest[start.end(): end.start() if end else None].strip()
    if "|" not in body:  # 没有表格就不是可用的 movers 段
        return None
    for rx, repl in _SUBHEAD_MAP:
        body = rx.sub(repl, body)
    return body


def render_movers(ws: Path, snapshot: Dict[str, Any], style: str = "en") -> None:
    """sections/05_movers.md —— 从 Closing Bell 摘要的 Stock Movers 表抽出,不编造。"""
    sources = snapshot.get("sources") or {}
    digest = ((sources.get("cnbc_closing_bell") or {}).get("summary_markdown") or "")
    cn = style == "cn"
    extracted = extract_closing_bell_movers(digest)
    lines = ["## Today's Movers", ""]
    if extracted:
        lines += [
            ("来自 Closing Bell 摘要的节目点名异动(非 Finviz 全市场排名)。 "
             if cn else
             "Named movers as covered by Closing Bell (not a Finviz ranked tape). ")
            + "<!-- audit: [show cnbc_closing_bell] -->",
            "",
            extracted,
            "",
        ]
    else:
        missing = ("「不可得」——Closing Bell 摘要中没有 Stock Movers 表"
                   if cn else
                   "(not in snapshot) — Closing Bell digest has no Stock Movers table")
        lines += [f"{missing}. <!-- audit: [show cnbc_closing_bell] -->", ""]
    f = ws / SECTION_SPECS["movers"]["output_file"]
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text("\n".join(lines) + f"\n{DONE_MARKER}\n", encoding="utf-8")


def macro_blocks(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """纯代码判定 Macros 当日块矩阵 —— 对齐 CNT 2026-10-12 后现行版式。

    releases              T 日美国一线白名单 + actual,最多 2 个
    rate_expectations     周五,或加息概率/10Y 相对 prior 超阈值
    cross_asset_review    同 rate_expectations
    forward               恒 True
    selected_releases     供任务书点名的事件列表
    """
    from datetime import date as _date

    sources = snapshot.get("sources") or {}
    pillars = sources.get("macro_pillars") or {}
    selected = select_tier1_releases(snapshot, limit=2)

    report_date_s = snapshot.get("report_date") or ""
    try:
        rd = _date.fromisoformat(report_date_s)
        is_friday = rd.weekday() == 4
    except ValueError:
        is_friday = False

    extreme = False
    prior = sources.get("prior_odds") or {}
    odds = pillars.get("next_fomc_odds") or {}
    if prior.get("odds_pct") is not None and odds.get("odds_pct") is not None:
        try:
            if abs(float(odds["odds_pct"]) - float(prior["odds_pct"])) >= EXTREME_ODDS_PT:
                extreme = True
        except (TypeError, ValueError):
            pass
    curve = pillars.get("par_curve") or {}
    chg = (curve.get("change_bp") or {}).get("10y")
    if chg is not None:
        try:
            if abs(float(chg)) >= EXTREME_10Y_BP:
                extreme = True
        except (TypeError, ValueError):
            pass

    open_review = is_friday or extreme
    return {
        "releases": bool(selected),
        "selected_releases": selected,
        "rate_expectations": open_review and bool(pillars.get("fed_odds")
                                                   or pillars.get("next_fomc_odds")),
        "cross_asset_review": open_review,
        "forward": True,
        "is_friday": is_friday,
        "extreme_move": extreme,
        # legacy keys kept so old state files / docs don't break readers
        "auction": False,
        "fomc": False,
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


def _run_one_agent(args: argparse.Namespace, ws: Path, snapshot: Path,
                   snapshot_data: Dict[str, Any], blocks: Dict[str, Any],
                   agent: str) -> Dict[str, Any]:
    """跑一个写作 agent(或确定性 movers)。返回要合并进 state 的片段。

    不写 state.json:并行时由调用方持锁合并,避免交错写入。
    """
    names = agent_section_names(agent)
    if all(section_done(ws, n) for n in names):
        return {n: {"status": "done"} for n in names}
    print(f"[agent] {agent} ({', '.join(names)}) ...", flush=True)
    t0 = time.time()
    specs = [SECTION_SPECS[n] for n in names]
    sections: Dict[str, Any] = {}
    usage = None
    if specs[0].get("mode") == "deterministic":
        render_movers(ws, snapshot_data, style=args.style)
        sections = {n: {"status": "done", "mode": "deterministic"} for n in names}
    elif args.fake_zcode:
        for n in names:
            _fake_section(ws, n, blocks=blocks, style=args.style)
        sections = {n: {"status": "done"} for n in names}
    else:
        try:
            result = run_agent(
                args.zcode_bin, ws,
                build_agent_prompt(agent, style=args.style, blocks=blocks),
                snapshot, args.timeout)
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


def run_sections(args: argparse.Namespace, ws: Path, snapshot: Path,
                 state: Dict[str, Any]) -> None:
    """按信源三组并行:overall_market(The Close) / company_desk(Closing Bell) /
    macro;movers 确定性、立刻跑。互不依赖,墙钟 ≈ 最慢那一组。"""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    snapshot_data = json.loads(snapshot.read_text(encoding="utf-8"))
    blocks = macro_blocks(snapshot_data)
    state["macro_blocks"] = blocks
    print(f"[macro] blocks: { {k: v for k, v in blocks.items()} }", flush=True)

    def _apply(result: Dict[str, Any]) -> None:
        for n, rec in (result.get("sections") or {}).items():
            state["sections"][n] = rec
            _record_timing(state, f"section:{n}", result["elapsed"])
        _add_usage(state, result.get("usage"))
        save_state(ws, state)

    pending = []
    for agent in AGENT_ORDER:
        names = agent_section_names(agent)
        specs = [SECTION_SPECS[n] for n in names]
        if specs[0].get("mode") == "deterministic" or args.fake_zcode:
            # 确定性 / fake:秒级,串行即可,避免线程里抢写同一份 fake 文件
            _apply(_run_one_agent(args, ws, snapshot, snapshot_data, blocks, agent))
        else:
            pending.append(agent)
    if pending:
        with ThreadPoolExecutor(max_workers=len(pending)) as pool:
            futs = {pool.submit(_run_one_agent, args, ws, snapshot,
                                snapshot_data, blocks, a): a for a in pending}
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
    mode = getattr(args, "llm_verify", "light")
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
    else:
        light = mode == "light"
        try:
            result = run_agent(
                args.zcode_bin, ws,
                (build_light_verifier_prompt(style=args.style) if light
                 else build_verifier_prompt(style=args.style)),
                None if light else snapshot, args.timeout)
            _add_usage(state, result.get("usage"))
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
    verdict = json.loads(verdict_file.read_text(encoding="utf-8"))

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
            })
        verdict["issues"] = merged
        verdict["verdict"] = "fail"
        verdict_file.write_text(
            json.dumps(verdict, ensure_ascii=False, indent=2), encoding="utf-8")

    state["verify"]["status"] = verdict.get("verdict", "fail")
    state["verify"]["deterministic_high_fails"] = len(high_fails)
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
            # 展示美东场次;report_date 是北京生成日,会比报告日晚一天
            return f"{label}✓{v.get('session_date') or v['report_date']}{suffix}"
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
    """拼接成品:正文仅标题+七节(CNT 表面文体);口径块与审计注释进 sidecar。"""
    snapshot = json.loads((ws / "snapshot.json").read_text(encoding="utf-8"))
    narrative_raw = (ws / SECTION_SPECS["narrative"]["output_file"]
                     ).read_text(encoding="utf-8")
    headline = _extract_headline(narrative_raw, report_date)
    parts = [
        f"# {headline}",
        "",
        f"*Daily market brief — {report_date}*",
        "",
    ]
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
    ]
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
    # Optional charts (Phase 3) — insert markdown image links if present
    charts_dir = ws / "final" / "charts"
    if charts_dir.exists():
        imgs = sorted(charts_dir.glob("*.png"))
        if imgs:
            chart_block = ["## Charts", ""]
            for img in imgs:
                chart_block.append(f"![{img.stem}](charts/{img.name})")
                chart_block.append("")
            # Insert charts after narrative/drivers area: after first ---
            # Simpler: append before final empty — place after headline block
            # Insert after date line (index of first section already in parts)
            insert_at = 4  # after title blank date blank
            for i, line in enumerate(chart_block):
                parts.insert(insert_at + i, line)
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
    parser.add_argument(
        "--llm-verify", choices=("off", "light", "full"), default="light",
        help="确定性台账核验通过后的 LLM 叙事核验: off=不跑; light(默认)=只读七节"
             "正文、不挂快照与转录,查跨节矛盾与文风; full=旧版全量核验(挂快照)。"
             "确定性核验有 high 失败时一律跳过 LLM、直接修订")
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

        # Charts before assemble so report.md can link them
        try:
            from render_charts import render_all
            render_all(ws)
        except Exception as e:  # noqa: BLE001 — charts are best-effort
            print(f"[charts] skipped: {e}", flush=True)

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
