#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 YouTube Feed 数据拉取（RSS + YouTube Data API v3 fallback）

用法（在项目根目录执行）:
  python scripts/test_youtube_feed.py
  python scripts/test_youtube_feed.py UCz6RzD6KG_hH_oHb2kyW5jQ
  python scripts/test_youtube_feed.py --max-items 5

会先尝试 RSS，若为空且配置了 YOUTUBE_DATA_API_KEY 则尝试 API v3。
"""

import os
import sys
import argparse

# 项目根目录加入 path，并加载 .env
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=False)
except ImportError:
    pass

from backend.services.youtube_subscription_service import (
    YouTubeSubscriptionService,
    _get_youtube_api_key,
)


# 默认测试频道（Morgan Stanley 官方）
DEFAULT_CHANNEL_ID = "UCz6RzD6KG_hH_oHb2kyW5jQ"


def main():
    parser = argparse.ArgumentParser(description="测试 YouTube Feed 拉取（RSS / API v3）")
    parser.add_argument(
        "channel_id",
        nargs="?",
        default=DEFAULT_CHANNEL_ID,
        help=f"频道 ID（默认: {DEFAULT_CHANNEL_ID}）",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=5,
        help="最多拉取几条视频（默认 5）",
    )
    parser.add_argument(
        "--force-api",
        action="store_true",
        help="跳过 RSS，仅用 YouTube Data API v3 测试（需配置 YOUTUBE_DATA_API_KEY）",
    )
    args = parser.parse_args()

    channel_id = args.channel_id.strip()
    max_items = max(1, min(args.max_items, 50))

    print("=" * 60)
    print("YouTube Feed 拉取测试")
    print("=" * 60)
    print(f"频道 ID: {channel_id}")
    print(f"最大条数: {max_items}")
    print(f"YOUTUBE_DATA_API_KEY: {'已配置' if _get_youtube_api_key() else '未配置'}")
    if os.getenv("YOUTUBE_PROXY") or os.getenv("HTTPS_PROXY"):
        print("代理: 已配置")
    else:
        print("代理: 未配置")
    print()

    if args.force_api:
        api_key = _get_youtube_api_key()
        if not api_key:
            print("错误: --force-api 需要配置 YOUTUBE_DATA_API_KEY（在 .env 或 deployment/.env.secrets）")
            sys.exit(1)
        print(">>> 仅使用 YouTube Data API v3 拉取...")
        videos = YouTubeSubscriptionService._fetch_channel_videos_via_api(
            channel_id=channel_id, max_items=max_items, api_key=api_key
        )
        source = "API v3"
    else:
        print(">>> 先尝试 RSS，失败则自动回退 API v3...")
        videos = YouTubeSubscriptionService.fetch_channel_videos(
            channel_identifier=channel_id, max_items=max_items
        )
        # 通过是否为空无法区分来源，这里只报结果；具体 RSS/API v3 见上方日志
        source = "RSS 或 API v3（见上方日志）"

    print()
    if not videos:
        print("结果: 未获取到任何视频")
        print("  - 若未配置代理且服务器在国内，请配置 YOUTUBE_PROXY")
        print("  - 若 RSS 常 404，请在 .env 中配置 YOUTUBE_DATA_API_KEY 以启用 API v3 回退")
        sys.exit(1)

    print(f"结果: 成功获取 {len(videos)} 条视频（来源: {source}）")
    print()
    for i, v in enumerate(videos[: max_items], 1):
        title = (v.get("snippet") or {}).get("title", "")[: 50]
        vid = v.get("id", "")
        print(f"  {i}. {title}...")
        print(f"     https://www.youtube.com/watch?v={vid}")
    print("=" * 60)


if __name__ == "__main__":
    main()
