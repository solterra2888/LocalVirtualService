#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""测试：能否只订阅 Bloomberg Television 频道内的 The Close 节目（节目级过滤）。

当前 Feeds 模块按整频道订阅入库，节目识别延迟到摘要生成阶段。本脚本验证
"抓取时过滤"是否可行：只拉 The Close、不整频道入库。

验证链路（全部复用生产代码，只读不写库）:
  1. YouTubeSubscriptionService.fetch_channel_videos() 拉频道最新视频
     （与生产一致：RSS 优先 ~15 条，YouTube Data API v3 兜底最多 50 条）
  2. YouTubeAdapter.normalize() 标准化 → 取 title / description / show_name
     （与生产入库时同一套 show_name 提取逻辑）
  3. is_bloomberg_the_close_video() 判断是否 The Close
     （标题 / show_name / 简介层；转录口播层用库里已入库的转录补验）
  4. 审计两级过滤的真实覆盖：
     a. 近漏——标题含 "close" 但未命中；
     b. 疑似整期——时长 ≥1h、发布在 20:00–24:00 UTC 晚间窗口、非其他栏目，
        但标题不带 The Close（2026-09 起常见命名）。对已入库的疑似整期跑
        口播正则，直接演示"仅标题过滤会漏多少期"。
  5. 只读查 subscription_content：已入库 / 转录状态

用法（在仓库根目录运行）:
  python tools/test_the_close_subscription.py                # 默认拉 50 条、近 7 天
  python tools/test_the_close_subscription.py --days 3       # 只看近 3 天
  python tools/test_the_close_subscription.py --max-items 15 # 只走 RSS
  python tools/test_the_close_subscription.py --no-db        # 跳过数据库交叉验证
"""

import argparse
import os
import re
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.services.youtube_subscription_service import YouTubeSubscriptionService
from backend.services.the_close_summary_service import (
    BLOOMBERG_TV_CHANNEL_ID,
    THE_CLOSE_SHOW_NAME,
    _has_the_close_bumper,
    _title_is_other_show,
    is_bloomberg_the_close_video,
)
from backend.services.content_adapters import get_adapter

CST = timezone(timedelta(hours=8))

# 疑似整期判定：正片时长与发布时段（UTC）
SUSPECT_MIN_DURATION_SEC = 3600
SUSPECT_HOURS_UTC = range(20, 24)

# ISO 8601 duration（PT1H32M9S / PT47M51S）→ 秒
_DURATION_RE = re.compile(
    r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:([\d.]+)S)?$", re.IGNORECASE
)


def bj(dt: datetime) -> str:
    return dt.astimezone(CST).strftime("%m-%d %H:%M")


def parse_published(raw) -> datetime:
    """解析 adapter 产出的 published_at（datetime 或 ISO8601 字符串）；失败返回 epoch。"""
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    text = (str(raw or "")).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def parse_duration_sec(raw: str):
    """ISO 8601 duration → 秒；无法解析返回 None。"""
    m = _DURATION_RE.match((raw or "").strip())
    if not m:
        return None
    hours = int(m.group(1) or 0)
    minutes = int(m.group(2) or 0)
    seconds = float(m.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def is_suspect_full_episode(item) -> bool:
    """疑似 The Close 整期：够长、晚间窗口、非其他栏目、标题又不含节目名。"""
    duration = parse_duration_sec((item.get("metadata") or {}).get("duration"))
    if duration is None or duration < SUSPECT_MIN_DURATION_SEC:
        return False
    if item["_published"].hour not in SUSPECT_HOURS_UTC:
        return False
    if _title_is_other_show(item.get("title")):
        return False
    return True


def load_db_status(video_ids):
    """只读查询 subscription_content：video_id → (已入库?, 转录?, 转录长度)。"""
    if not video_ids:
        return {}
    from backend.utils.database_config import get_database_config
    from backend.database.db_manager import DatabaseManager

    db = DatabaseManager(db_conn_params=get_database_config())
    conn = db.get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT content_id,
                   coalesce(transcript_cleaned, transcript) IS NOT NULL,
                   length(coalesce(transcript_cleaned, transcript))
            FROM subscription_content
            WHERE channel_type = 'youtube' AND content_id = ANY(%s)
            """,
            (video_ids,),
        )
        return {row[0]: (True, row[1], row[2]) for row in cur.fetchall()}
    finally:
        conn.close()


def load_db_suspects(cutoff):
    """只读查询窗口内的疑似整期：≥1h、20-24 点 UTC、非其他栏目、标题不带节目名。

    返回 [{title, published_at, transcript}]，按发布时间倒序。
    """
    from backend.utils.database_config import get_database_config
    from backend.database.db_manager import DatabaseManager

    db = DatabaseManager(db_conn_params=get_database_config())
    conn = db.get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT title, published_at, coalesce(transcript_cleaned, transcript)
            FROM subscription_content
            WHERE channel_type = 'youtube'
              AND author_identifier = %s
              AND published_at >= %s
              AND extract(hour from published_at) >= 20
              AND metadata->>'duration' ~ '^PT[0-9]+H'
            ORDER BY published_at DESC
            """,
            (BLOOMBERG_TV_CHANNEL_ID, cutoff),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    suspects = []
    for title, published_at, transcript in rows:
        if _title_is_other_show(title):
            continue
        # 标题层已能命中的不算"疑似"（它们不需要口播兜底）
        if is_bloomberg_the_close_video(
            title, show_name=None, transcript=None, description=None,
        ):
            continue
        suspects.append({
            "title": title,
            "published_at": published_at,
            "transcript": transcript or "",
        })
    return suspects


def main():
    parser = argparse.ArgumentParser(description="The Close 节目级订阅过滤测试")
    parser.add_argument("--max-items", type=int, default=50,
                        help="拉取条数（RSS 上限 15，API 兜底上限 50，默认 50）")
    parser.add_argument("--days", type=int, default=7,
                        help="只看近 N 天发布的视频（默认 7）")
    parser.add_argument("--no-db", action="store_true",
                        help="跳过数据库交叉验证（不连库）")
    args = parser.parse_args()

    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(days=args.days)

    print("=" * 88)
    print("步骤 1: 拉取 Bloomberg Television 频道最新视频（复用生产 fetch_channel_videos）")
    print("=" * 88)
    raw_videos = YouTubeSubscriptionService.fetch_channel_videos(
        channel_identifier=BLOOMBERG_TV_CHANNEL_ID,
        max_items=args.max_items,
    )
    if not raw_videos:
        print("[FAIL] 未获取到任何视频（RSS 与 API 均失败），检查网络/代理/API key")
        sys.exit(1)
    print(f"获取 {len(raw_videos)} 条（RSS 优先，失败走 YouTube Data API v3）")

    # ── 标准化 + 时间窗过滤 ──────────────────────────────────────────
    adapter = get_adapter("youtube")
    in_window, dropped_old = [], 0
    for raw in raw_videos:
        item = adapter.normalize(raw)
        published = parse_published(item.get("published_at"))
        if published < cutoff:
            dropped_old += 1
            continue
        item["_published"] = published
        in_window.append(item)
    print(f"近 {args.days} 天: {len(in_window)} 条（窗口外 {dropped_old} 条忽略）")

    print()
    print("=" * 88)
    print("步骤 2: 节目级过滤（复用 is_bloomberg_the_close_video + adapter show_name）")
    print("=" * 88)
    matched, near_miss = [], []
    for item in in_window:
        metadata = item.get("metadata") or {}
        title = item.get("title")
        show_name = metadata.get("show_name")
        # 抓取阶段无转录，口播层传 None（该层正是 2026-09 无后缀时期的兜底，见近漏审计）
        if is_bloomberg_the_close_video(
            title,
            show_name=show_name,
            transcript=None,
            description=item.get("content"),
        ):
            matched.append(item)
        elif "close" in (title or "").lower():
            near_miss.append(item)

    print(f"\n命中 The Close: {len(matched)} 条")
    for item in matched:
        metadata = item.get("metadata") or {}
        mark = "show_name" if metadata.get("show_name") == THE_CLOSE_SHOW_NAME else "标题/简介"
        print(f"  [{mark}] {bj(item['_published'])}  {item['title']}  {item['url']}")

    print(f"\n近漏审计（标题含 'close' 但未命中）: {len(near_miss)} 条")
    for item in near_miss:
        print(f"  [SKIP] {bj(item['_published'])}  {item['title']}")

    suspects = [item for item in in_window
                if item not in matched and is_suspect_full_episode(item)]
    if suspects:
        print(f"\n实时拉取中的疑似整期（≥1h、20-24点UTC、非其他栏目、标题不带节目名）: {len(suspects)} 条")
        for item in suspects:
            print(f"  [SUSPECT] {bj(item['_published'])}  {item['title']}")

    # ── 数据库审计（只读）────────────────────────────────────────────
    # 实时拉取受 RSS 只回 15 条所限，覆盖不了整个时间窗；整频道订阅已把
    # 全部视频入库，直接查库才是完整事实。逐条演示仅标题过滤 vs 口播兜底。
    db_status = {}
    db_suspects = []
    bumper_confirmed = 0
    if not args.no_db:
        print()
        print("=" * 88)
        print("步骤 3: 数据库审计（只读查 subscription_content，完整时间窗）")
        print("=" * 88)
        all_ids = [item["content_id"] for item in in_window]
        try:
            db_status = load_db_status(all_ids)
            ingested = sum(1 for v in db_status.values() if v[0])
            print(f"实时拉取 {len(in_window)} 条中已入库: {ingested} 条")
        except Exception as exc:
            print(f"[warn] 数据库查询失败，跳过审计: {exc}")

        try:
            db_suspects = load_db_suspects(cutoff)
        except Exception as exc:
            db_suspects = []
            print(f"[warn] 疑似整期查询失败: {exc}")

        if db_suspects:
            print(f"\n窗口内疑似整期（≥1h、20-24点UTC、非其他栏目、标题不带节目名）: "
                  f"{len(db_suspects)} 条")
            print("口播验证（生产摘要管线靠这一层兜底）:")
            for row in db_suspects:
                title, published, transcript = row["title"], row["published_at"], row["transcript"]
                if _has_the_close_bumper(transcript):
                    verdict = "★ 口播命中 → 实为 The Close（仅标题过滤会漏掉）"
                    bumper_confirmed += 1
                else:
                    verdict = "口播未命中 → 非 The Close"
                print(f"  {bj(published)}  {title[:48]:<48}  {verdict}")
        else:
            print("窗口内无疑似整期")

    # ── 结论 ─────────────────────────────────────────────────────────
    print()
    print("=" * 88)
    print("结论")
    print("=" * 88)
    title_only_total = len(matched) + bumper_confirmed
    print(f"  近 {args.days} 天：仅标题过滤命中 {len(matched)} 期，"
          f"疑似整期 {len(db_suspects)} 条（口播确认 {bumper_confirmed} 期为 The Close）")
    if title_only_total:
        print(f"  → 实际 The Close 共 {title_only_total} 期，标题过滤覆盖率 "
              f"{len(matched)}/{title_only_total}")
    if bumper_confirmed:
        print("  ⚠ Bloomberg 命名在 'The Close M/D/YYYY' 与纯主题标题之间反复切换，")
        print("    仅靠标题/简介在抓取时过滤会漏正片")
        print("  → 节目级订阅可行方案：标题过滤先行 + 疑似整期（长视频/晚间窗口）")
        print("    拉转录跑口播正则确认；转录仍需抓取，省的只是入库量")
    else:
        print("  ✓ 本窗口标题过滤未见漏报；但 Bloomberg 命名习惯会变，")
        print("    生产摘要管线的口播兜底层（the_close_summary_service）请保留")


if __name__ == "__main__":
    main()
