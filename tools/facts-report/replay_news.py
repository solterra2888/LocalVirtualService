"""A/B 复测:把 v9 文本选材层重放到正式快照的副本上。

    FACTS_REPORT_ROOT=/tmp/mb_ab python replay_news.py 2026-11-18 2026-11-20

读正式目录 {date}/snapshot.json,只重算 sources.news 与 sources.news_macro
(行情/日历/节目转录等原样保留,A/B 只差文本层),窗口上沿截到正式快照的
generated_at(复现 08:00 构建时刻的可见数据),写到 $FACTS_REPORT_ROOT/{date}/。
之后照常 run_pipeline.py {date}(同一 FACTS_REPORT_ROOT)即可;不登记、不发布。
FACTS_REPORT_ROOT 未设置或指向正式目录时拒绝运行。
"""
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOL_DIR))

import build_snapshot as bs  # noqa: E402
from prompts import assign_item_ids, flag_reported_items  # noqa: E402

PROD_ROOT = TOOL_DIR.parents[1] / "backend" / "reports" / "facts-report"


def replay(report_date: str, out_root: Path) -> None:
    src_path = PROD_ROOT / report_date / "snapshot.json"
    snap = json.loads(src_path.read_text(encoding="utf-8"))
    sources = snap["sources"]
    bs.REPLAY_AS_OF = datetime.fromisoformat(snap["generated_at"])
    t = date.fromisoformat(report_date)

    sources["news"] = bs._news_snapshot(t, sources.get("earnings_calendar"),
                                        sources.get("market"))
    sources["news_macro"] = bs._news_macro_regex(t)
    assign_item_ids(sources)
    flag_reported_items(sources)
    errors = snap.get("errors") or {}
    for k in ("news", "news_macro"):
        errors.pop(k, None)
        if sources[k].get("error"):
            errors[k] = sources[k]["error"]
    snap["errors"] = errors
    snap.setdefault("notes", {})["replay"] = (
        f"news/news_macro replayed as of {snap['generated_at']} from {src_path}")

    out = out_root / report_date / "snapshot.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(snap, ensure_ascii=False, default=str), encoding="utf-8")
    sel = sources["news"].get("selection") or {}
    wire, flash = sel.get("wire") or {}, sel.get("flash") or {}
    print(f"{report_date} as_of={snap['generated_at']} -> {out}\n"
          f"  wire {wire.get('selected')} (t1 {wire.get('t1_selected')}/"
          f"{wire.get('t1_available')}, t1 L1 {wire.get('t1_l1_selected')}/"
          f"{wire.get('t1_l1_available')}) dropped={wire.get('dropped')}\n"
          f"  flash {flash.get('selected')} geo {len(sources['news_macro'].get('geo') or [])}"
          f" research {len(sources['news_macro'].get('research') or [])}"
          f" digest {len(sources['news_macro'].get('digest') or [])}", flush=True)


def main() -> int:
    root = os.environ.get("FACTS_REPORT_ROOT")
    if not root or Path(root).resolve() == PROD_ROOT.resolve():
        print("set FACTS_REPORT_ROOT to a scratch dir (not the production reports dir)",
              file=sys.stderr)
        return 2
    dates = sys.argv[1:]
    if not dates:
        print(__doc__)
        return 1
    for d in dates:
        replay(d, Path(root))
    return 0


if __name__ == "__main__":
    sys.exit(main())
