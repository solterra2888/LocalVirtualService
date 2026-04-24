#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回填 Feeds 内容的 AI 标签 + 摘要 (Phase 4)

对在 `--since-hours` 窗口内新入库的 Twitter / YouTube 订阅内容, 调用
`tag_feed_content_task` 生成 AI 标签 + 摘要, 写入 subscription_content_tags.

核心能力:
    - 只跑过去 24 小时 (默认) 或自定义窗口内的未打标内容
    - 复用 FeedContentTaggingService 的长度门槛 (Twitter ≥50, YouTube transcript ≥200)
    - --dry-run:  仅盘点候选数据, 不派发
    - --sync:     同步直接调 service (性能测试友好, 逐条 tokens/cost 可见)
    - 默认 async: 派发到 ai-worker (file_processing 队列), 脚本 poll 所有 task_id
                 直到完成, 打印全量性能汇总

使用示例:
    # 1. 预估 24h 内候选数
    python scripts/maintenance/backfill_feed_tags.py --dry-run

    # 2. 先跑 10 条 sample 做性能测试 (sync, 立即看每条耗时)
    python scripts/maintenance/backfill_feed_tags.py --sync --limit 10

    # 3. 全量派发到 Celery, 脚本监控任务完成
    python scripts/maintenance/backfill_feed_tags.py

    # 4. 只回填 Twitter
    python scripts/maintenance/backfill_feed_tags.py --channel twitter

    # 5. 自定义时间窗口 (过去 2 天)
    python scripts/maintenance/backfill_feed_tags.py --since-hours 48

设计文档: docs/instructions/FEEDS_AUTO_TAGGING_PLAN.md §9
"""
import argparse
import asyncio
import os
import sys
import time
from typing import Any, Dict, List, Tuple

# 将仓库根加入 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.database.db_manager import DatabaseManager  # noqa: E402
from backend.utils.database_config import get_database_config  # noqa: E402
from backend.utils.logger import get_logger  # noqa: E402

logger = get_logger("backfill_feed_tags")

SUPPORTED_CHANNELS = {"twitter", "youtube"}


def _fetch_candidates(
    db: DatabaseManager,
    since_hours: float,
    channels: List[str],
    min_twitter_len: int,
    min_youtube_len: int,
    limit: int = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    扫描 subscription_content LEFT JOIN subscription_content_tags,
    返回候选 (id, channel_type, effective_len, title_preview) 列表 + 计数统计.

    `effective_len` 的计算方式:
      - twitter: LEN(content) + LEN(quoted_content?) + LEN(retweeted_content?)
      - youtube: LEN(transcript)
    """
    conn = db.get_connection()
    if not conn:
        raise RuntimeError("无法连接数据库")
    try:
        cur = conn.cursor()

        channel_list_sql = ",".join(f"'{c}'" for c in channels)
        sql = f"""
            SELECT
                sc.id,
                sc.channel_type,
                LEFT(COALESCE(sc.title, sc.content, ''), 60) AS preview,
                CASE
                    WHEN sc.channel_type = 'twitter' THEN
                        LENGTH(COALESCE(sc.content,''))
                        + COALESCE(LENGTH(sc.quoted_content),0)
                        + COALESCE(LENGTH(sc.retweeted_content),0)
                    WHEN sc.channel_type = 'youtube' THEN
                        LENGTH(COALESCE(sc.transcript,''))
                    ELSE 0
                END AS effective_len
            FROM subscription_content sc
            LEFT JOIN subscription_content_tags sct
                ON sct.subscription_content_id = sc.id
            WHERE sc.channel_type IN ({channel_list_sql})
              AND sc.fetched_at >= NOW() - INTERVAL '{since_hours} hours'
              AND sct.id IS NULL
            ORDER BY sc.fetched_at DESC, sc.id DESC
        """
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        conn.close()

    all_candidates: List[Dict[str, Any]] = []
    stats = {"twitter_total": 0, "youtube_total": 0,
             "twitter_short_skip": 0, "youtube_short_skip": 0,
             "twitter_taggable": 0, "youtube_taggable": 0}

    for row in rows:
        cid, ch, preview, elen = row
        if ch == "twitter":
            stats["twitter_total"] += 1
            if elen < min_twitter_len:
                stats["twitter_short_skip"] += 1
                continue
            stats["twitter_taggable"] += 1
        elif ch == "youtube":
            stats["youtube_total"] += 1
            if elen < min_youtube_len:
                stats["youtube_short_skip"] += 1
                continue
            stats["youtube_taggable"] += 1
        else:
            continue

        all_candidates.append({
            "id": cid,
            "channel_type": ch,
            "preview": preview or "",
            "effective_len": int(elen or 0),
        })

        if limit and len(all_candidates) >= limit:
            break

    return all_candidates, stats


# ---------------------------------------------------------------
# 同步模式: 直接调 FeedContentTaggingService (适合少量样本性能测试)
# ---------------------------------------------------------------

async def _run_sync(
    db: DatabaseManager,
    candidates: List[Dict[str, Any]],
) -> Dict[str, Any]:
    from backend.services.feed_content_tagging_service import (
        FeedContentTaggingService,
    )

    service = FeedContentTaggingService(db_manager=db)

    results = {
        "total": len(candidates),
        "completed": 0,
        "skipped": 0,
        "failed": 0,
        "total_tokens": 0,
        "total_cost": 0.0,
        "total_duration": 0.0,
        "per_channel": {
            "twitter": {"completed": 0, "tokens": 0, "cost": 0.0, "duration": 0.0},
            "youtube": {"completed": 0, "tokens": 0, "cost": 0.0, "duration": 0.0},
        },
        "failures": [],
    }

    start_all = time.time()
    for idx, c in enumerate(candidates, 1):
        t0 = time.time()
        cid = c["id"]
        ch = c["channel_type"]
        try:
            # force=True 确保幂等地覆盖, 但 service 内部若已有行会直接 return skipped
            r = await service.tag_content(content_id=cid, force=False)
        except Exception as e:
            results["failed"] += 1
            results["failures"].append({"id": cid, "error": str(e)})
            logger.error(f"[{idx}/{results['total']}] cid={cid} FAILED: {e}")
            continue

        dur = time.time() - t0

        if r.get("skipped"):
            results["skipped"] += 1
            logger.info(
                f"[{idx}/{results['total']}] cid={cid} ch={ch} SKIPPED "
                f"({r.get('message')})"
            )
            continue

        tokens = (r.get("ai_metadata") or {}).get("total_tokens", 0) or 0
        cost = (r.get("ai_metadata") or {}).get("total_price", 0) or 0
        tags = r.get("tags") or {}

        results["completed"] += 1
        results["total_tokens"] += tokens
        results["total_cost"] += cost
        results["total_duration"] += dur
        pc = results["per_channel"][ch]
        pc["completed"] += 1
        pc["tokens"] += tokens
        pc["cost"] += cost
        pc["duration"] += dur

        logger.info(
            f"[{idx}/{results['total']}] cid={cid} ch={ch} len={c['effective_len']} "
            f"sector={tags.get('sector')} tickers={len(tags.get('tickers') or [])} "
            f"tokens={tokens} cost={cost:.4f} dur={dur:.2f}s"
        )

    results["wall_time"] = time.time() - start_all
    return results


# ---------------------------------------------------------------
# 异步模式: 派发到 Celery + poll AsyncResult
# ---------------------------------------------------------------

def _run_async(
    candidates: List[Dict[str, Any]],
    per_item_countdown: int,
    poll_interval: int,
    poll_timeout: int,
) -> Dict[str, Any]:
    from backend.celery_app import app as celery_app
    from backend.tasks.feed_tagging_tasks import tag_feed_content_task

    dispatched: List[Tuple[str, int, str]] = []  # (task_id, content_id, channel_type)

    logger.info(
        f"派发 {len(candidates)} 条到 tag_feed_content_task "
        f"(countdown 梯度 {per_item_countdown}s)"
    )
    start_dispatch = time.time()
    for idx, c in enumerate(candidates):
        try:
            task = tag_feed_content_task.apply_async(
                args=(c["id"], "backfill", "", False),
                countdown=per_item_countdown * idx,
            )
            dispatched.append((task.id, c["id"], c["channel_type"]))
        except Exception as e:
            logger.error(f"派发失败 cid={c['id']}: {e}")
    logger.info(f"派发完成: {len(dispatched)} 条, 耗时 {time.time()-start_dispatch:.2f}s")

    # Poll
    from celery.result import AsyncResult
    pending = {tid: (cid, ch) for tid, cid, ch in dispatched}
    results = {
        "total": len(dispatched),
        "completed": 0,
        "skipped": 0,
        "failed": 0,
        "total_tokens": 0,
        "total_cost": 0.0,
        "total_duration": 0.0,
        "per_channel": {
            "twitter": {"completed": 0, "tokens": 0, "cost": 0.0, "duration": 0.0},
            "youtube": {"completed": 0, "tokens": 0, "cost": 0.0, "duration": 0.0},
        },
        "failures": [],
    }

    start_poll = time.time()
    poll_rounds = 0
    while pending and (time.time() - start_poll) < poll_timeout:
        poll_rounds += 1
        newly_done = []
        for tid, (cid, ch) in list(pending.items()):
            ar = AsyncResult(tid, app=celery_app)
            if not ar.ready():
                continue
            newly_done.append(tid)
            if not ar.successful():
                results["failed"] += 1
                results["failures"].append({
                    "id": cid, "task_id": tid, "error": str(ar.info)[:200]
                })
                logger.warning(f"FAILED cid={cid}: {ar.info}")
                continue
            result = ar.result or {}
            status = result.get("status", "completed")
            if status == "skipped":
                results["skipped"] += 1
            elif status == "completed":
                tokens = (result.get("ai_metadata") or {}).get("total_tokens", 0) or 0
                cost = (result.get("ai_metadata") or {}).get("total_price", 0) or 0
                dur = result.get("processing_time", 0) or 0
                results["completed"] += 1
                results["total_tokens"] += tokens
                results["total_cost"] += cost
                results["total_duration"] += dur
                pc = results["per_channel"][ch]
                pc["completed"] += 1
                pc["tokens"] += tokens
                pc["cost"] += cost
                pc["duration"] += dur
            else:
                results["failed"] += 1
                results["failures"].append({
                    "id": cid, "task_id": tid, "error": str(result)[:200]
                })

        for tid in newly_done:
            pending.pop(tid, None)

        done = results["completed"] + results["skipped"] + results["failed"]
        elapsed = time.time() - start_poll
        rate = done / elapsed if elapsed > 0 else 0
        logger.info(
            f"[poll #{poll_rounds}] done={done}/{results['total']} "
            f"(completed={results['completed']} skipped={results['skipped']} failed={results['failed']}) "
            f"elapsed={elapsed:.0f}s rate={rate:.2f}/s remaining={len(pending)}"
        )

        if not pending:
            break
        time.sleep(poll_interval)

    if pending:
        logger.warning(
            f"poll 超时, 仍有 {len(pending)} 个任务未完成 (可能 worker 慢或死锁)"
        )
        for tid, (cid, ch) in pending.items():
            results["failures"].append({
                "id": cid, "task_id": tid, "error": "poll timeout (task still pending)"
            })
            results["failed"] += 1

    results["wall_time"] = time.time() - start_poll
    return results


# ---------------------------------------------------------------
# 报告
# ---------------------------------------------------------------

def _print_report(args, stats: Dict[str, int], results: Dict[str, Any]):
    print()
    print("=" * 78)
    print(" Backfill Feed Tags — 性能报告")
    print("=" * 78)
    print(f" 窗口: 过去 {args.since_hours} 小时")
    print(f" 模式: {'SYNC (直接调 service)' if args.sync else 'ASYNC (派发 Celery)'}")
    print(f" 渠道: {','.join(args.channels)}")
    print()
    print(" [候选盘点]")
    for ch in ("twitter", "youtube"):
        total = stats.get(f"{ch}_total", 0)
        skip = stats.get(f"{ch}_short_skip", 0)
        tag = stats.get(f"{ch}_taggable", 0)
        print(f"   {ch:<10s}  total={total:<5d} 过短跳过={skip:<5d} 待打标={tag:<5d}")
    print()

    if not results:
        return

    print(" [执行结果]")
    print(f"   total={results['total']} "
          f"completed={results['completed']} "
          f"skipped={results['skipped']} "
          f"failed={results['failed']}")
    wt = results.get("wall_time", 0)
    print(f"   wall_time={wt:.2f}s ({wt/60:.1f} min)")
    if results["completed"] > 0:
        avg_dur = results["total_duration"] / results["completed"]
        print(f"   avg LLM duration={avg_dur:.2f}s/item "
              f"throughput={results['completed']/wt:.2f}/s (wall)")
    print(f"   total_tokens={results['total_tokens']}")
    print(f"   total_cost={results['total_cost']:.4f} RMB")
    print()
    print(" [按渠道]")
    for ch, pc in results["per_channel"].items():
        if pc["completed"] == 0:
            continue
        avg_dur = pc["duration"] / pc["completed"]
        avg_tokens = pc["tokens"] / pc["completed"]
        avg_cost = pc["cost"] / pc["completed"]
        print(f"   {ch:<10s} completed={pc['completed']:<4d} "
              f"avg_tokens={avg_tokens:.0f} avg_dur={avg_dur:.2f}s "
              f"avg_cost={avg_cost:.4f} total_cost={pc['cost']:.4f}")
    print()
    if results["failures"]:
        print(f" [失败前 10 条]")
        for f in results["failures"][:10]:
            print(f"   cid={f['id']} error={f.get('error','-')[:120]}")
        if len(results["failures"]) > 10:
            print(f"   ... 还有 {len(results['failures']) - 10} 条失败")
        print()
    print("=" * 78)


# ---------------------------------------------------------------
# CLI
# ---------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="回填 Feeds 内容的 AI 标签 + 摘要 (Phase 4)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--since-hours", type=float, default=24.0,
                        help="时间窗口 (小时), 默认 24")
    parser.add_argument("--channel", choices=["twitter", "youtube", "all"], default="all",
                        help="限定渠道, 默认 all")
    parser.add_argument("--limit", type=int, default=None,
                        help="最多处理多少条 (调试用)")
    parser.add_argument("--dry-run", action="store_true",
                        help="只盘点候选, 不派发 / 不调 LLM")
    parser.add_argument("--sync", action="store_true",
                        help="同步模式: 直接调 FeedContentTaggingService (性能测试友好). "
                             "默认异步: 派发 Celery")
    parser.add_argument("--per-item-countdown", type=int, default=1,
                        help="Celery 相邻任务的 countdown (秒), 默认 1, 避免 LLM API 瞬时 QPS 峰值")
    parser.add_argument("--poll-interval", type=int, default=5,
                        help="async 模式下 poll 间隔 (秒), 默认 5")
    parser.add_argument("--poll-timeout", type=int, default=1800,
                        help="async 模式下最长等待 (秒), 默认 1800 即 30min")
    parser.add_argument("--min-twitter-len", type=int,
                        default=int(os.getenv("AUTO_FEED_TAG_MIN_LENGTH_TWITTER", "50")),
                        help="Twitter 最小有效长度 (默认读 AUTO_FEED_TAG_MIN_LENGTH_TWITTER 或 50)")
    parser.add_argument("--min-youtube-len", type=int,
                        default=int(os.getenv("AUTO_FEED_TAG_MIN_LENGTH_YOUTUBE", "200")),
                        help="YouTube transcript 最小长度 (默认读 AUTO_FEED_TAG_MIN_LENGTH_YOUTUBE 或 200)")
    args = parser.parse_args()
    args.channels = list(SUPPORTED_CHANNELS) if args.channel == "all" else [args.channel]

    db = DatabaseManager(get_database_config())

    logger.info("=" * 78)
    logger.info(f"backfill_feed_tags 开始 since={args.since_hours}h channels={args.channels} "
                f"dry_run={args.dry_run} sync={args.sync} limit={args.limit}")
    logger.info("=" * 78)

    candidates, stats = _fetch_candidates(
        db=db,
        since_hours=args.since_hours,
        channels=args.channels,
        min_twitter_len=args.min_twitter_len,
        min_youtube_len=args.min_youtube_len,
        limit=args.limit,
    )
    logger.info(
        f"候选盘点: {stats}  实际派发={len(candidates)}"
        + (f" (受 --limit {args.limit} 限制)" if args.limit else "")
    )

    if args.dry_run:
        logger.info("--dry-run 模式, 不派发任务, 直接打印报告")
        _print_report(args, stats, None)
        if candidates:
            logger.info("前 5 条 preview:")
            for c in candidates[:5]:
                logger.info(f"  id={c['id']} ch={c['channel_type']} len={c['effective_len']} "
                            f"preview={c['preview']!r}")
        return

    if not candidates:
        logger.info("没有待打标候选, 直接退出")
        _print_report(args, stats, None)
        return

    if args.sync:
        results = asyncio.run(_run_sync(db, candidates))
    else:
        results = _run_async(
            candidates,
            per_item_countdown=args.per_item_countdown,
            poll_interval=args.poll_interval,
            poll_timeout=args.poll_timeout,
        )

    _print_report(args, stats, results)


if __name__ == "__main__":
    main()
