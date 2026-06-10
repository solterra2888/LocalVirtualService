#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同步回填脚本: 使用向量匹配关联内容与投资主题，不依赖 Celery Worker。

用法（在项目根目录执行）:
  python scripts/backfill_content_theme_matches_sync.py
  python scripts/backfill_content_theme_matches_sync.py --hours 168   # 回填 7 天
  python scripts/backfill_content_theme_matches_sync.py --content-type feed
  python scripts/backfill_content_theme_matches_sync.py --dry-run

默认回填过去 48 小时。直接调用向量匹配服务层，不需要 Celery Worker 运行。
每条内容生成一次 Qwen embedding，成本极低（约 0.0003 元/条）。
"""

import asyncio
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

from backend.utils.logger import get_logger

logger = get_logger("backfill_theme_matches_sync")

DEFAULT_LOOKBACK_HOURS = 48


def main():
    parser = argparse.ArgumentParser(description="同步回填：向量匹配内容与投资主题")
    parser.add_argument("--content-type", choices=["file", "feed"], default=None)
    parser.add_argument("--hours", type=int, default=DEFAULT_LOOKBACK_HOURS)
    parser.add_argument("--force", action="store_true", help="重新匹配已有记录")
    parser.add_argument("--dry-run", action="store_true", help="只统计不执行")
    args = parser.parse_args()

    from backend.database.db_manager import DatabaseManager
    from backend.utils.database_config import get_database_config
    from backend.services.content_theme_matching_service import ContentThemeMatchingService

    since = datetime.now(timezone.utc) - timedelta(hours=args.hours)
    types_to_process = [args.content_type] if args.content_type else ["file", "feed"]

    db = DatabaseManager(get_database_config())
    conn = db.get_connection()
    cursor = conn.cursor()

    print(f"回填窗口: {since.isoformat()} ~ now ({args.hours}h)")
    print(f"模式: vector matching {'(force)' if args.force else '(skip existing)'}")
    print()

    service = ContentThemeMatchingService()
    loop = asyncio.new_event_loop()

    total_matched = 0
    total_skipped = 0
    total_failed = 0

    try:
        for ct in types_to_process:
            label = "Files" if ct == "file" else "Feeds"

            if ct == "file":
                # 向量匹配需要 summary 或 content 文本，不依赖 ticker
                if args.force:
                    cursor.execute(
                        """
                        SELECT ft.file_id FROM file_tags ft
                        JOIN file_processing_details fpd ON fpd.id = ft.file_id
                        WHERE (ft.summary IS NOT NULL AND LENGTH(ft.summary) > 10
                               OR fpd.content IS NOT NULL AND LENGTH(fpd.content) > 50)
                          AND fpd.created_at >= %s
                        ORDER BY fpd.created_at DESC
                        """,
                        (since,),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT ft.file_id FROM file_tags ft
                        JOIN file_processing_details fpd ON fpd.id = ft.file_id
                        WHERE (ft.summary IS NOT NULL AND LENGTH(ft.summary) > 10
                               OR fpd.content IS NOT NULL AND LENGTH(fpd.content) > 50)
                          AND fpd.created_at >= %s
                          AND NOT EXISTS (
                            SELECT 1 FROM content_theme_matches ctm
                            WHERE ctm.content_type='file' AND ctm.content_id=ft.file_id
                          )
                        ORDER BY fpd.created_at DESC
                        """,
                        (since,),
                    )
            else:
                # 向量匹配需要 title+summary 文本，不依赖 ticker
                if args.force:
                    cursor.execute(
                        """
                        SELECT sc.id
                        FROM subscription_content sc
                        LEFT JOIN subscription_content_tags sct ON sct.subscription_content_id = sc.id
                        WHERE (sct.summary IS NOT NULL AND LENGTH(sct.summary) > 10
                               OR sc.content IS NOT NULL AND LENGTH(sc.content) > 50)
                          AND sc.published_at >= %s
                        ORDER BY sc.published_at DESC
                        """,
                        (since,),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT sc.id
                        FROM subscription_content sc
                        LEFT JOIN subscription_content_tags sct ON sct.subscription_content_id = sc.id
                        WHERE (sct.summary IS NOT NULL AND LENGTH(sct.summary) > 10
                               OR sc.content IS NOT NULL AND LENGTH(sc.content) > 50)
                          AND sc.published_at >= %s
                          AND NOT EXISTS (
                            SELECT 1 FROM content_theme_matches ctm
                            WHERE ctm.content_type='feed'
                              AND ctm.content_id=sc.id
                          )
                        ORDER BY sc.published_at DESC
                        """,
                        (since,),
                    )

            ids = [row[0] for row in cursor.fetchall()]
            print(f"[{label}] 待处理: {len(ids)} 条")

            if args.dry_run:
                continue

            for i, cid in enumerate(ids):
                try:
                    result = loop.run_until_complete(
                        service.match_by_vector(ct, cid)
                    )
                    status = result.get("status")
                    count = result.get("matches_count", 0)

                    if status == "completed":
                        total_matched += 1
                        if count > 0:
                            print(f"  ✅ {ct}/{cid}: {count} themes")
                    elif status == "skipped":
                        total_skipped += 1
                    else:
                        total_failed += 1
                        reason = result.get("reason", "unknown")
                        if i < 20:  # 只打印前 20 个错误
                            print(f"  ❌ {ct}/{cid}: {reason}")
                except Exception as e:
                    total_failed += 1
                    if i < 20:
                        print(f"  ❌ {ct}/{cid}: {e}")

                # 每 100 条打印进度
                if (i + 1) % 100 == 0:
                    print(f"  ... 进度: {i + 1}/{len(ids)} "
                          f"(matched={total_matched} skipped={total_skipped} failed={total_failed})")

    finally:
        loop.close()
        conn.close()

    print(f"\n{'[dry-run] 未执行任何匹配。' if args.dry_run else '完成。'}")
    if not args.dry_run:
        print(f"  匹配成功: {total_matched}")
        print(f"  无匹配/跳过: {total_skipped}")
        print(f"  失败: {total_failed}")


if __name__ == "__main__":
    main()
