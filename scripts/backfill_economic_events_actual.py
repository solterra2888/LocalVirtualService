#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回补 economic_events 表已发布事件的 actual 值。

背景: EODHD /economic-events 宽窗口会被静默截断到最新端（from 形同虚设，
offset 上限 1000），导致每日任务的 lookback 回填从未真正生效，
2026-08 下旬起已过期事件的 actual 覆盖率从 ~80% 跌到个位数。
修复后的 EconomicEventsService.get_economic_events_chunked 按 ≤7 天分片抓取，
本脚本复用它对指定日期范围做一次性回补（upsert 幂等，可重复执行）。

用法（在项目根目录，guanhe 环境）:
  python scripts/backfill_economic_events_actual.py --from-date 2026-08-01 --to-date 2026-10-22
  python scripts/backfill_economic_events_actual.py --from-date 2026-08-01 --to-date 2026-10-22 --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 允许从项目根目录直接运行
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backend.utils.env_loader import load_project_env  # noqa: E402

load_project_env()

from backend.services.economic_events_service import (  # noqa: E402
    EconomicEventsService,
    calculate_event_impact,
)
from backend.services.market_data_service import create_market_data_service  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="回补 economic_events 已发布事件的 actual 值")
    parser.add_argument("--from-date", required=True, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--country", default=None, help="可选，限定国家代码")
    parser.add_argument("--dry-run", action="store_true", help="只抓取并统计，不写库")
    args = parser.parse_args()

    api_client = EconomicEventsService()
    market_service = create_market_data_service()

    print(f"抓取 {args.from_date} ~ {args.to_date} (country={args.country or 'ALL'}) ...")
    events = api_client.get_economic_events_chunked(
        from_date=args.from_date,
        to_date=args.to_date,
        country=args.country,
    )
    print(f"API 返回 {len(events)} 条")

    formatted = []
    for event in events:
        row = api_client.format_event_data(event)
        row["impact"] = calculate_event_impact(row.get("event", ""))
        formatted.append(row)

    with_actual = sum(1 for r in formatted if r.get("actual") not in (None, ""))
    print(f"格式化 {len(formatted)} 条，其中带 actual {with_actual} 条")

    if args.dry_run:
        print("dry-run，不写库")
        return 0

    result = market_service.batch_insert_economic_events(formatted)
    print(
        f"入库完成: 新增 {result.get('inserted', 0)} 条，"
        f"更新 {result.get('updated', 0)} 条，"
        f"去重移除 {result.get('duplicates_removed', 0)} 条"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
