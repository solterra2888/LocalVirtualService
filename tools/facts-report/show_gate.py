#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""节目转录就绪门:卡在 build_snapshot 与 run_pipeline 之间,等两档节目入库。

背景(2026-11 实测, subscription_content):The Close 转录就绪带在北京
07:30-08:41、Closing Bell 约 07:00-08:30,而 facts-report cron 08:00 启动,
build_snapshot 里的 prep_workspace 只查一次 —— 一锤子买卖,缺了之后转录
到达也不会再补。本门把"一次查询"变成"轮询到截止":

  - 启动即查,缺 blocking 节目 → ⚠️ Lark 预警(此时人工还来得及处置,
    如唤醒家用 Worker / youtube_fetching 队列)
  - 每 --poll 秒重跑 transcript_prep.prep_workspace(幂等直查 DB,
    秒级、无 LLM;转录到了会原地补进 snapshot.json)
  - 全部就绪 → 放行(exit 0);到 --deadline 仍缺 → 🔴 Lark 告警后放行
    (报告如实缺源继续生成,流程卡上可见)
  - Closing Bell Overtime 段缺失不阻塞(与建快照口径一致)

CLI:
    python show_gate.py <workspace> [--deadline 09:40] [--poll 300] [--no-notify]

退出码恒 0:就绪门自身绝不阻断管线(真正的阻断决策交给 deadline)。
"""

from __future__ import annotations

import argparse
import functools
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
from zoneinfo import ZoneInfo

# cron 重定向下 stdout 为块缓冲,等待期的 [gate] 状态会滞留缓冲区 —— 强制行刷新
print = functools.partial(print, flush=True)

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lark_alert import notify  # noqa: E402

BJ = ZoneInfo("Asia/Shanghai")

# blocking: 整档缺失才等;overtime 单段缺失已在建快照口径内容忍
BLOCKING = (
    ("the_close", "The Close 转录(Bloomberg, youtube)"),
    ("cnbc_closing_bell", "Closing Bell 转录(CNBC, podcast 主段)"),
)


def _now_bj() -> datetime:
    return datetime.now(BJ)


def missing_shows(snap: Dict) -> List[str]:
    """blocking 节目中尚未入 snapshot.sources 的(以 sources 为准,非 errors)。"""
    src = snap.get("sources") or {}
    out = []
    if not src.get("the_close"):
        out.append("the_close")
    # cb 只要有 main 或 overtime 任一段即算在库(与 prep_workspace 写入口径一致)
    cb = src.get("cnbc_closing_bell") or {}
    if not (cb.get("main") or cb.get("overtime")):
        out.append("cnbc_closing_bell")
    return out


def _labels(missing: List[str]) -> List[str]:
    d = dict(BLOCKING)
    return [d.get(k, k) for k in missing]


def _deadline_today(hhmm: str) -> datetime:
    h, m = (int(x) for x in hhmm.split(":"))
    n = _now_bj()
    d = n.replace(hour=h, minute=m, second=0, microsecond=0)
    if d < n:  # 手动补跑已过点:顺延到明日语义(今日当次立即放行)
        d += timedelta(days=1)
    return d


def _alert(title: str, lines: List[str], enabled: bool) -> None:
    if enabled:
        ok = notify(title, lines)
        print(f"[gate] lark sent={ok}")
    else:
        print(f"[gate] lark suppressed({title})")


def _probe_stuck(snap: Dict, missing: List[str]) -> List[str]:
    """窗口内已有候选行但快照仍缺 → 匹配器缺口描述行;空列表=数据未到,正常等。

    2026-11-16 事故:Closing Bell 两集 06:37 已入库、因标题退化被过滤,
    门空等到 09:40 —— "到了但没匹配上"等下去不会自愈,应立即暴露。
    诊断自身失败不影响等待逻辑(按"未到"处理)。
    """
    if not missing:
        return []
    import transcript_prep
    from datetime import date as _date
    try:
        probe = transcript_prep.window_probe(
            _date.fromisoformat(snap["report_date"]))
    except Exception as e:  # noqa: BLE001
        print(f"[gate] window_probe error (treated as not-arrived): {e}")
        return []
    out: List[str] = []
    if "the_close" in missing:
        p = probe.get("the_close") or {}
        if p.get("candidate_rows"):
            out.append(
                f"The Close: 窗口内已有 {p['candidate_rows']} 条 ≥50k 转录但零自识别"
                f"({'; '.join(p.get('titles') or [])})")
    if "cnbc_closing_bell" in missing:
        p = probe.get("cnbc_closing_bell") or {}
        if p.get("cb_feed_rows"):
            out.append(
                f"Closing Bell: 官方 feed 已入库 {p['cb_feed_rows']} 行但零匹配"
                f"({'; '.join(p.get('titles') or [])})")
    return out


def run(ws: Path, deadline: datetime, poll: int, do_notify: bool) -> int:
    snap_path = ws / "snapshot.json"
    started = _now_bj()
    warned = False

    while True:
        snap = json.loads(snap_path.read_text(encoding="utf-8"))
        missing = missing_shows(snap)
        now = _now_bj()

        if not missing:
            waited_min = int((now - started).total_seconds() // 60)
            print(f"[gate] ready at {now:%H:%M:%S} — 两档节目转录均已在库"
                  + (f"(等待 {int((now - started).total_seconds())}s)"
                     if now > started else ""))
            # 🚀 生命周期通知:就绪门放行 = 管线真正开跑(等过 ⚠️ 的场景给出等待时长)
            _alert(
                f"【facts-report 🚀】{snap.get('report_date', '?')} 报告开始生成",
                [f"时间: 北京 {now:%m-%d %H:%M}",
                 "节目转录: 两档均已就绪"
                 + (f"(就绪门等待 {waited_min} 分钟后放行)" if waited_min else ""),
                 "管线预计 40-60 分钟,完成后另行通知"],
                do_notify)
            return 0

        # 首次发现缺失:⚠️ 预警(人工窗口尚在)
        if not warned:
            _alert(
                f"【facts-report ⚠️】{snap.get('report_date', '?')} 节目转录未就绪",
                [f"时间: 北京 {now:%m-%d %H:%M}",
                 "缺失: " + ";".join(_labels(missing)),
                 f"就绪门: 每 {poll}s 轮询, {deadline:%H:%M} 截止后缺源继续",
                 "处置: 转录通常 07:30-08:40 到库;持续缺失请检查",
                 "  · The Close → 家用 Worker(youtube_fetching 队列)是否在线",
                 "  · Closing Bell → podcast 抓取/ASR 是否积压"],
                do_notify)
            warned = True
        print(f"[gate] {now:%H:%M:%S} missing={missing}, "
              f"deadline={deadline:%H:%M}")

        # 已入库但零匹配 → 匹配器缺口:等下去不会自愈,立即 🔴 放行把缺口
        # 暴露在人工窗口(2026-11-17 加;此前会空等到截止)
        stuck = _probe_stuck(snap, missing)
        if stuck:
            _alert(
                f"【facts-report 🔴】{snap.get('report_date', '?')} "
                f"节目行已在库但零匹配(匹配器缺口)",
                [f"时间: 北京 {now:%m-%d %H:%M}"]
                + [f"缺口: {s}" for s in stuck]
                + ["处置: 数据已入库,是 transcript_prep 匹配逻辑没认出来;",
                   "修复后可手动 `python transcript_prep.py --ws " + str(ws)
                   + "` 补挂重跑;",
                   "报告现在放行,如实缺源生成"],
                do_notify)
            return 0  # 放行,不阻断

        if now >= deadline:
            _alert(
                f"【facts-report 🔴】{snap.get('report_date', '?')} 超截止仍缺节目",
                [f"时间: 北京 {now:%m-%d %H:%M}",
                 "仍缺: " + ";".join(_labels(missing)),
                 f"等待: {int((now - started).total_seconds() // 60)} 分钟后放行,"
                 "报告将如实缺源生成",
                 "事后: 转录到库后可手动 "
                 f"`python transcript_prep.py --ws {ws}` 补挂并重跑"],
                do_notify)
            return 0  # 放行,不阻断

        time.sleep(min(poll, max(1, int((deadline - now).total_seconds()))))
        # 幂等重查:转录到了原地补进快照(prep 成功时会清掉对应 error 键)
        import transcript_prep
        try:
            transcript_prep.prep_workspace(ws)
        except Exception as e:  # noqa: BLE001 — DB 抖动不终止等待
            print(f"[gate] prep_workspace retry error (ignored): {e}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("workspace", help="管线工作区目录(含 snapshot.json)")
    ap.add_argument("--deadline", default="08:25",
                    help="北京时刻 HH:MM,到点仍缺则告警放行(默认 08:25,8:30 开工 SLA)")
    ap.add_argument("--poll", type=int, default=300,
                    help="轮询间隔秒(默认 300)")
    ap.add_argument("--no-notify", action="store_true",
                    help="不发 Lark(本地联调用)")
    args = ap.parse_args()
    ws = Path(args.workspace).resolve()
    if not (ws / "snapshot.json").exists():
        print(f"[gate] snapshot not found: {ws}")
        return 0
    return run(ws, _deadline_today(args.deadline), args.poll,
               not args.no_notify)


if __name__ == "__main__":
    raise SystemExit(main())
