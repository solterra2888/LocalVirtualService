#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
临时脚本：按用户名拉取最近 N 小时的推文（默认 @realDonaldTrump，近 48 小时）。

使用与 backend/services/twitter_subscription_service.py 相同的 twitterapi.io Advanced Search。

用法（在项目根目录）:
  export TWITTER_API_KEY=...   # 与后端一致，见 deployment/.env.secrets.example
  python scripts/fetch_twitter_user_recent.py
  python scripts/fetch_twitter_user_recent.py --handle SomeUser --hours 72
  python scripts/fetch_twitter_user_recent.py --raw   # 打印 API 原始 JSON

可选：在项目根放置 .env，脚本会尝试加载（python-dotenv）。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass


API_BASE = "https://api.twitterapi.io/twitter/tweet/advanced_search"


def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def fetch_tweets(
    author: str,
    since_hours: int,
    api_key: str,
    max_pages: int = 10,
) -> List[Dict[str, Any]]:
    author = author.lstrip("@").lower()
    until_time = datetime.now(timezone.utc)
    since_time = until_time - timedelta(hours=since_hours)
    since_str = since_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")
    until_str = until_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")
    query = f"from:{author} since:{since_str} until:{until_str} include:nativeretweets"

    params: Dict[str, Any] = {"query": query, "queryType": "Latest"}
    headers = {"X-API-Key": api_key}
    session = _session()
    all_tweets: List[Dict[str, Any]] = []
    next_cursor: Optional[str] = None
    page = 0

    while page < max_pages:
        if next_cursor:
            params["cursor"] = next_cursor
        r = session.get(API_BASE, headers=headers, params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:500]}")
        data = r.json()
        tweets: List[Dict[str, Any]] = []
        if isinstance(data, list):
            tweets = data
        elif isinstance(data, dict):
            tweets = data.get("tweets") or data.get("data") or []
        if tweets:
            all_tweets.extend(tweets)
        has_next = False
        cursor_val = ""
        if isinstance(data, dict):
            has_next = bool(data.get("has_next_page") or data.get("hasNextPage"))
            cursor_val = (data.get("next_cursor") or data.get("nextCursor") or "") or ""
        if has_next and cursor_val:
            next_cursor = cursor_val
            page += 1
            time.sleep(1)
            continue
        break

    return all_tweets


def main() -> None:
    p = argparse.ArgumentParser(description="Fetch recent tweets for one user via twitterapi.io")
    p.add_argument("--handle", default="@worldlibertyfi", help="Twitter username without @")
    p.add_argument("--hours", type=int, default=48, help="Look back this many hours (default: 48 ≈ 2 days)")
    p.add_argument("--max-pages", type=int, default=10, help="Pagination cap")
    p.add_argument("--raw", action="store_true", help="Print full JSON array instead of summary lines")
    args = p.parse_args()

    api_key = os.getenv("TWITTER_API_KEY", "").strip()
    if not api_key:
        print("缺少环境变量 TWITTER_API_KEY（与后端相同）", file=sys.stderr)
        sys.exit(1)

    try:
        tweets = fetch_tweets(
            args.handle,
            since_hours=args.hours,
            api_key=api_key,
            max_pages=args.max_pages,
        )
    except RuntimeError as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    if args.raw:
        print(json.dumps(tweets, ensure_ascii=False, indent=2))
        print(f"\n# total: {len(tweets)}", file=sys.stderr)
        return

    print(f"# {args.handle} | last {args.hours}h | count={len(tweets)}\n", file=sys.stderr)
    for t in tweets:
        tid = t.get("id", "")
        text = (t.get("text") or "").replace("\n", " ")
        created = t.get("createdAt") or t.get("created_at") or ""
        url = t.get("url") or ""
        au = (t.get("author") or {}) if isinstance(t.get("author"), dict) else {}
        uname = au.get("username") or au.get("screen_name") or ""
        line = f"{created}\t@{uname}\t{id}\t{url}\n{text}\n---"
        print(line)


if __name__ == "__main__":
    main()
