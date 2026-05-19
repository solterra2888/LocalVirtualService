#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回填 Twitter 未分类短推 — 使用 tweet_signal_v1 重新打标

仅处理:
  - channel_type = twitter
  - sector = '未分类'
  - 组装后文本长度 < TWEET_SIGNAL_HEAVY_THRESHOLD_CHARS (默认 250)
  - 不含 $cashtag (含 cashtag 的应走重型模板, 本脚本跳过)

灰度前请设置: TWEET_SIGNAL_V1_ENABLED=true

用法:
    # 预览候选
    python scripts/maintenance/backfill_tweet_signal_unclassified.py --dry-run

    # 同步跑 10 条测试
    TWEET_SIGNAL_V1_ENABLED=true python scripts/maintenance/backfill_tweet_signal_unclassified.py --sync --limit 10

    # 派发 Celery 批量
    TWEET_SIGNAL_V1_ENABLED=true python scripts/maintenance/backfill_tweet_signal_unclassified.py --limit 500
"""
import argparse
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.database.db_manager import DatabaseManager  # noqa: E402
from backend.utils.database_config import get_database_config  # noqa: E402
from backend.utils.logger import get_logger  # noqa: E402

logger = get_logger("backfill_tweet_signal_unclassified")

_CASHTAG_RE = re.compile(r"\$[A-Z]{1,5}\b")


def _effective_len(row) -> int:
    parts = [(row[2] or "").strip()]
    if row[4] and row[3]:
        parts.append(str(row[4]))
    if row[7] and row[6]:
        parts.append(str(row[7]))
    return len("\n".join(parts))


def fetch_candidates(db: DatabaseManager, limit: int | None) -> list:
    threshold = int(os.getenv("TWEET_SIGNAL_HEAVY_THRESHOLD_CHARS", "250"))
    conn = db.get_connection()
    if not conn:
        raise RuntimeError("无法连接数据库")
    try:
        cur = conn.cursor()
        sql = """
            SELECT sc.id, sc.author_name, sc.content,
                   sc.is_retweet, sc.retweeted_content,
                   sc.is_quote, sc.quoted_content
            FROM subscription_content sc
            JOIN subscription_content_tags sct
              ON sct.subscription_content_id = sc.id
            WHERE sct.channel_type = 'twitter'
              AND sct.sector = '未分类'
            ORDER BY sc.published_at DESC
        """
        if limit:
            sql += f" LIMIT {int(limit) * 3}"
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        conn.close()

    out = []
    for r in rows:
        elen = _effective_len(r)
        text = (r[2] or "")
        if elen >= threshold or _CASHTAG_RE.search(text):
            continue
        out.append({"id": r[0], "author_name": r[1], "effective_len": elen})
        if limit and len(out) >= limit:
            break
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sync", action="store_true", help="同步调用 service (不写 Celery)")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--rate", type=float, default=2.0, help="sync 模式每秒条数上限")
    args = parser.parse_args()

    if os.getenv("TWEET_SIGNAL_V1_ENABLED", "false").lower() != "true":
        logger.warning(
            "TWEET_SIGNAL_V1_ENABLED 未开启; 回填仍将走路由, "
            "短推会走 tweet_signal_v1 仅当 env=true"
        )

    db = DatabaseManager(get_database_config())
    candidates = fetch_candidates(db, args.limit)
    print(f"候选 (短推无 cashtag): {len(candidates)} 条")

    if args.dry_run:
        for c in candidates[:20]:
            print(f"  id={c['id']} author={c['author_name']} len={c['effective_len']}")
        if len(candidates) > 20:
            print(f"  ... 还有 {len(candidates) - 20} 条")
        return

    if args.sync:
        import asyncio
        from backend.services.feed_content_tagging_service import FeedContentTaggingService

        svc = FeedContentTaggingService(db)
        interval = 1.0 / max(args.rate, 0.1)
        ok, fail = 0, 0
        for c in candidates:
            try:
                asyncio.run(
                    svc.tag_content(c["id"], force=True)
                )
                ok += 1
                print(f"OK id={c['id']}")
            except Exception as e:
                fail += 1
                print(f"FAIL id={c['id']}: {e}")
            time.sleep(interval)
        print(f"完成: ok={ok} fail={fail}")
        return

    from backend.tasks.feed_tagging_tasks import tag_feed_content_task

    dispatched = 0
    for c in candidates:
        tag_feed_content_task.apply_async(
            args=[c["id"]],
            kwargs={"force": True},
            countdown=2,
        )
        dispatched += 1
    print(f"已派发 {dispatched} 个 tag_feed_content_task (force=True)")


if __name__ == "__main__":
    main()
