#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""今日涨跌榜按日重算(不重跑 LLM 管线)—— 港股/A股收盘补数 与 美股事后修正。

「当日」口径(2026-11-23,用户决策):当日 = 报告生成日 D(北京自然日,= T+1)。
  美股      北京 D 日凌晨收盘的那一场(美东 T 交易日)—— 生成时已是最终值,
            页面不再刷新;只有数据出错时(如拆股日)用 --markets us 重算。
  港股/A股  北京 D 日自己的交易时段 —— 08:00 生成时还没开盘,快照里先是上一
            交易日(T)的行;D 日收盘后由本脚本用 D 日 EODHD 文件重算并重渲染
            重注册,之后固定。此前只在生成当天/次日由前端临时刷新,过后回退。

流程: 重算指定市场的行(_volume_leaders_eodhd)→ 走势小图/周期涨跌/拆股修正
(_rank_sparks)→ 写回 snapshot.json(原文件先备份 .pre_rank_refresh)→
md2html_report.py → register_morning_brief.py --ok(share_token 不变,已分发
的分享链接不受影响)。行内容与快照现有值一致时不重渲染(定时任务的第二次
尝试据此空转退出)。

用法:
    python refresh_rank_rows.py 2026-11-20                 # 港股/A股,用 D=T+1 日文件
    python refresh_rank_rows.py 2026-11-20 --markets us    # 美股,用 T 日文件
    python refresh_rank_rows.py 2026-11-20 --dry-run       # 只打印,不落盘不发布
    python refresh_rank_rows.py 2026-11-20 --no-publish    # 只写快照,不渲染不注册

退出码: 0=已更新或无需更新  1=运行错误(数据源失败/工作区缺失/渲染注册失败)
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
PIPE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(PIPE_DIR))

import build_snapshot as bs  # noqa: E402

# 与 md2html_report.py / register_morning_brief.py 同一产物目录(渲染器不接受
# FACTS_REPORT_ROOT 重定向,这里也用正式目录)
REPORTS_ROOT = REPO_ROOT / "backend" / "reports" / "facts-report"
PIPELINE_LOCK = "/tmp/facts_report_cron.lock"
MARKETS = ("us", "hk", "cn")


def _pipeline_running() -> bool:
    """晨报管线(cron_daily.sh)是否在跑 —— 它会整份重写 snapshot.json。"""
    try:
        with open(PIPELINE_LOCK, "a") as fh:
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return True
            fcntl.flock(fh, fcntl.LOCK_UN)
    except OSError:
        return False
    return False


def _sig(rows: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    """行内容指纹(代码/收盘/当日/成交额):两次定时尝试之间据此判定有无变化。"""
    return [(r.get("code"), r.get("price"), r.get("change_percent"),
             r.get("turnover")) for r in rows]


def refresh(session: str, markets: Tuple[str, ...], dry_run: bool,
            publish: bool) -> int:
    t = date.fromisoformat(session)
    # 「当日」= 报告日 D = T+1:港股/A股取 D 日文件;美股行固定取 T 日
    day = t if "us" in markets else t + timedelta(days=1)
    snap_path = REPORTS_ROOT / session / "snapshot.json"
    if not snap_path.exists():
        print(f"[rank_refresh] 工作区快照不存在: {snap_path}")
        return 1
    snap = json.loads(snap_path.read_text(encoding="utf-8"))
    market = (snap.get("sources") or {}).get("market")
    if not isinstance(market, dict):
        print("[rank_refresh] 快照无 market 块,退出")
        return 1
    vl = market.setdefault("volume_leaders", {})

    diag: Dict[str, bool] = {}
    new = bs._volume_leaders_eodhd(day, markets=markets, diag=diag)
    live = tuple(m for m in markets
                 if any(diag.get(ex) for ex in bs.VL_MARKET_EXCHANGES[m]))
    if not live:
        print(f"[rank_refresh] {day.isoformat()} 无 {'/'.join(markets)} 日线数据"
              f"(休市或 EODHD 文件未出),不改动")
        return 0
    for m in markets:
        if m not in live:
            print(f"[rank_refresh] {m} {day.isoformat()} 无数据,保留原有行"
                  f"(as_of={(vl.get(m) or {}).get('as_of')})")
    bs._rank_sparks(new, day, markets=live)
    # 有当日文件却算不出行(无达标个股/全部被过滤):保留原有行,绝不拿空榜覆盖
    usable = tuple(m for m in live if (new.get(m) or {}).get("rows"))
    for m in live:
        if m not in usable:
            print(f"[rank_refresh] ⚠ {m} {day.isoformat()} 有当日文件但无达标"
                  f"个股,保留原有行,不用空榜覆盖")

    changed = [m for m in usable
               if (vl.get(m) or {}).get("as_of") != day.isoformat()
               or _sig((vl.get(m) or {}).get("rows") or []) != _sig(new[m]["rows"])]
    if not changed:
        print(f"[rank_refresh] {day.isoformat()} {'/'.join(usable or live)} 行与"
              f"快照一致,无需重渲染")
        return 0
    for m in changed:
        rows = new[m]["rows"]
        head = ", ".join(
            f"{r.get('code')} {r.get('price')} "
            f"{r['change_percent']:+.2f}%" for r in rows[:3]
            if isinstance(r.get("change_percent"), (int, float)))
        print(f"[rank_refresh] {m}: {len(rows)} 行, as_of {day.isoformat()}; "
              f"成交额前 3 → {head}")
    if dry_run:
        print("[rank_refresh] --dry-run: 未写快照、未重渲染、未注册")
        return 0

    for m in changed:
        vl[m] = new[m]
    notes = snap.setdefault("notes", {})
    hist = dict(notes.get("rank_refresh") or {})
    hist[day.isoformat()] = {
        "markets": changed,
        "at": datetime.now(timezone.utc).isoformat()}
    notes["rank_refresh"] = hist
    # 首次改动前留底(只留最早的一份,便于对照生成时的榜单)
    bak = snap_path.with_name(snap_path.name + ".pre_rank_refresh")
    if not bak.exists():
        shutil.copy2(snap_path, bak)
    tmp = snap_path.with_name(snap_path.name + ".tmp")
    tmp.write_text(
        json.dumps(snap, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8")
    os.replace(tmp, snap_path)
    print(f"[rank_refresh] 快照已更新: {snap_path} ({','.join(changed)})")
    if not publish:
        print("[rank_refresh] --no-publish: 仅写快照")
        return 0

    subprocess.run([sys.executable, str(PIPE_DIR / "report_html_tools" /
                                         "md2html_report.py"), session],
                   check=True, cwd=str(PIPE_DIR))
    subprocess.run([sys.executable, str(PIPE_DIR / "register_morning_brief.py"),
                    "--ok", session], check=True, cwd=str(PIPE_DIR))
    print(f"[rank_refresh] 已重渲染并重注册: 工作区 T={session} "
          f"(报告日 {(t + timedelta(days=1)).isoformat()})")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("session_date", help="美股交易日 T (YYYY-MM-DD,工作区目录名)")
    ap.add_argument("--markets", default="hk,cn",
                    help="重算哪些市场,逗号分隔(默认 hk,cn = 收盘补数;us = "
                         "用 T 日文件修正美股行)")
    ap.add_argument("--dry-run", action="store_true",
                    help="只打印重算结果,不落盘不发布")
    ap.add_argument("--no-publish", action="store_true",
                    help="只写回快照,不重渲染不注册")
    args = ap.parse_args()
    markets = tuple(m.strip() for m in args.markets.split(",") if m.strip())
    bad = [m for m in markets if m not in MARKETS]
    if bad or not markets:
        ap.error(f"--markets 只接受 {','.join(MARKETS)}")
    if "us" in markets and len(markets) > 1:
        ap.error("美股取 T 日、港股/A股取 D=T+1 日,不能混在一次重算里")
    if _pipeline_running():
        print("[rank_refresh] 晨报管线正在运行(持有 cron 锁),退出以免并发改写快照")
        return 1
    try:
        return refresh(args.session_date, markets, args.dry_run,
                       not args.no_publish)
    except subprocess.CalledProcessError as exc:
        print(f"[rank_refresh] 渲染/注册失败: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
