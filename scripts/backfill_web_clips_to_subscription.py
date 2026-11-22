# -*- coding: utf-8 -*-
"""存量 Web Clip → subscription_content 回填脚本 (2026-11-22)。

把 file_processing_details 里 task='Web Clip' 的 Bloomberg/Reuters 等白名单
新闻源全文, 经 web_clip_feed_service 清洗后转投 subscription_content('news'),
让晨报等订阅生态立刻可用 (含 2026-11-20/21 的 242 条)。

用法:
  python -m scripts.backfill_web_clips_to_subscription --dry-run   # 只统计
  python -m scripts.backfill_web_clips_to_subscription            # 执行回填
  python -m scripts.backfill_web_clips_to_subscription --limit 10 # 冒烟

幂等: 转投走 (channel_type, content_id) upsert, 重复执行安全;
      is_new=False 不重复触发打标/向量化。
"""
import argparse
import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.services.web_clip_feed_service import (  # noqa: E402
    clean_clip_text, forward_clip_to_feed, match_feed_domain, recover_title,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('backfill_web_clips')


def fetch_clips(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT fpd.id, fpd.content, fpd.created_at, ps."user",
               COALESCE((ps.upload_params::jsonb)->>'source_url', '') AS source_url,
               COALESCE((ps.upload_params::jsonb)->>'title', '') AS title
        FROM file_processing_details fpd
        JOIN processing_stats ps ON ps.id = fpd.processing_stats_id
        WHERE ps.task_name = 'Web Clip'
          AND fpd.status = 'completed'
          AND fpd.content IS NOT NULL
        ORDER BY fpd.created_at
    """)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def user_id_map(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id, username FROM users")
    m = {name: uid for uid, name in cursor.fetchall()}
    cursor.close()
    return m


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--dry-run', action='store_true', help='只统计不落库')
    ap.add_argument('--limit', type=int, default=0, help='最多处理 N 条 (0=不限)')
    ap.add_argument('--no-trigger', action='store_true', help='不派发 celery 自动处理 (向量化等)')
    args = ap.parse_args()

    from backend.database.db_manager import DatabaseManager
    from backend.utils.database_config import get_database_config
    db = DatabaseManager(get_database_config())
    conn = db.get_connection()
    if not conn:
        logger.error("无法连接数据库")
        sys.exit(1)

    clips = fetch_clips(conn)
    uids = user_id_map(conn)
    conn.close()

    total = eligible = skipped_short = inserted = updated = failed = 0
    by_source = {}
    for clip_id, content, created_at, username, source_url, title in clips:
        total += 1
        if not match_feed_domain(source_url):
            continue  # 非白名单域: 保持原行为, 不计也不警告
        eligible += 1
        if args.limit and (inserted + updated + skipped_short) >= args.limit:
            continue
        clean = clean_clip_text(content or '')
        if len(clean) < 100:
            skipped_short += 1
            continue
        uid = uids.get(username)
        if uid is None:
            logger.warning("clip %s: 用户 %r 不存在, 跳过", clip_id, username)
            failed += 1
            continue

        if args.dry_run:
            rt = recover_title(title, clean, source_url)
            logger.info("[dry] clip=%s user=%s len=%d title=%r", clip_id, username, len(clean), rt[:70])
            inserted += 1
            continue

        try:
            result = forward_clip_to_feed(
                source_url=source_url,
                title=title,
                plain_text=content,
                user_ids=[uid],
                clip_time=created_at if isinstance(created_at, datetime) else None,
                trigger_processing=not args.no_trigger,
            )
            if result is None:
                skipped_short += 1
            elif result['is_new']:
                inserted += 1
            else:
                updated += 1
            by_source[result['source'] if result else '?'] = by_source.get(result['source'] if result else '?', 0) + 1
        except Exception as e:
            failed += 1
            logger.error("clip %s 转投失败: %s (url=%s)", clip_id, e, source_url[:90])

    logger.info(
        "回填完成: 扫描 %d 条 / 白名单 %d / 新增 %d / 更新 %d / 过短跳过 %d / 失败 %d %s %s",
        total, eligible, inserted, updated, skipped_short, failed,
        f"/ 来源 {by_source}" if by_source else "",
        "(DRY-RUN)" if args.dry_run else "",
    )
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()
