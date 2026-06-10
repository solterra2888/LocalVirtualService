#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量回填: 为过去 48 小时内有 tickers 的已标记内容生成主题匹配。

用法（在项目根目录执行）:
  python scripts/backfill_content_theme_matches.py
  python scripts/backfill_content_theme_matches.py --content-type file
  python scripts/backfill_content_theme_matches.py --force    # 重新匹配已有记录
  python scripts/backfill_content_theme_matches.py --dry-run  # 只统计，不派发

此脚本通过 Celery 任务异步执行匹配，不会阻塞。
默认仅处理过去 48 小时内的数据。
"""

import os
import sys
import argparse
from datetime import datetime, timezone, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)
except ImportError:
    pass

from backend.utils.database_config import get_psycopg2_config
from backend.utils.logger import get_logger

logger = get_logger("backfill_theme_matches")

# 默认回填窗口：过去 48 小时
DEFAULT_LOOKBACK_HOURS = 48


def count_items(cursor, content_type: str, force: bool, since: datetime) -> int:
    """统计待处理的条目数（仅过去 N 小时内）"""
    if content_type == "file":
        if force:
            cursor.execute(
                """
                SELECT COUNT(*) FROM file_tags ft
                JOIN file_processing_details fpd ON fpd.id = ft.file_id
                WHERE ft.ticker IS NOT NULL
                  AND fpd.created_at >= %s
                """,
                (since,),
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*) FROM file_tags ft
                JOIN file_processing_details fpd ON fpd.id = ft.file_id
                WHERE ft.ticker IS NOT NULL
                  AND fpd.created_at >= %s
                  AND NOT EXISTS (
                    SELECT 1 FROM content_theme_matches ctm
                    WHERE ctm.content_type='file' AND ctm.content_id=ft.file_id
                      AND ctm.match_mode='ticker'
                  )
                """,
                (since,),
            )
    elif content_type == "feed":
        if force:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM subscription_content_tags sct
                JOIN subscription_content sc ON sc.id = sct.subscription_content_id
                WHERE sct.ticker IS NOT NULL
                  AND sc.published_at >= %s
                """,
                (since,),
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM subscription_content_tags sct
                JOIN subscription_content sc ON sc.id = sct.subscription_content_id
                WHERE sct.ticker IS NOT NULL
                  AND sc.published_at >= %s
                  AND NOT EXISTS (
                    SELECT 1 FROM content_theme_matches ctm
                    WHERE ctm.content_type='feed'
                      AND ctm.content_id=sct.subscription_content_id
                      AND ctm.match_mode='ticker'
                  )
                """,
                (since,),
            )
    else:
        return 0

    return cursor.fetchone()[0]


def main():
    parser = argparse.ArgumentParser(description="批量回填内容主题匹配（默认过去 48 小时）")
    parser.add_argument(
        "--content-type",
        choices=["file", "feed"],
        default=None,
        help="仅处理指定类型（默认两者都处理）",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=DEFAULT_LOOKBACK_HOURS,
        help=f"回填时间窗口（小时），默认 {DEFAULT_LOOKBACK_HOURS}",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="重新匹配已有记录",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只统计待处理数量，不实际派发任务",
    )
    args = parser.parse_args()

    since = datetime.now(timezone.utc) - timedelta(hours=args.hours)

    import psycopg2

    conn = psycopg2.connect(**get_psycopg2_config())
    cursor = conn.cursor()

    types_to_process = [args.content_type] if args.content_type else ["file", "feed"]

    print(f"回填窗口: {since.isoformat()} ~ now ({args.hours}h)")
    print()

    for ct in types_to_process:
        count = count_items(cursor, ct, args.force, since)
        label = "Files" if ct == "file" else "Feeds"
        print(f"[{label}] 待处理: {count} 条" + (" (force)" if args.force else ""))

    if args.dry_run:
        print("\n[dry-run] 未派发任何任务。")
        conn.close()
        return

    # 派发 Celery 回填任务
    from backend.celery_app import app as celery_app

    # 确保 Celery 可以发现任务
    from backend.tasks.theme_matching_tasks import backfill_content_theme_matches_task  # noqa: F401

    if args.content_type:
        task = backfill_content_theme_matches_task.apply_async(
            kwargs={"content_type": args.content_type, "force": args.force, "hours": args.hours},
        )
        print(f"\n已派发回填任务: task_id={task.id} content_type={args.content_type}")
    else:
        # 分别派发 file 和 feed
        for ct in types_to_process:
            task = backfill_content_theme_matches_task.apply_async(
                kwargs={"content_type": ct, "force": args.force, "hours": args.hours},
            )
            label = "Files" if ct == "file" else "Feeds"
            print(f"已派发 [{label}] 回填任务: task_id={task.id}")

    conn.close()
    print("\n完成。可通过 Celery Flower 或日志监控进度。")


if __name__ == "__main__":
    main()
