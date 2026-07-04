#!/usr/bin/env python
"""最小化验证：拉取指定博主最新微博。"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from crawl4weibo import WeiboClient


def load_config(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def build_client(config: dict, *, log_level: str = "INFO") -> WeiboClient:
    import os

    cookie_path = config.get("cookie_storage_path")
    cookie_str = os.environ.get("WEIBO_COOKIE", "").strip()
    if cookie_str:
        return WeiboClient(
            cookies=cookie_str,
            use_browser_cookies=False,
            auto_fetch_cookies=False,
            log_level=log_level,
        )

    cookies = _fetch_cookies_with_system_chrome()
    if cookies:
        return WeiboClient(
            cookies=cookies,
            use_browser_cookies=False,
            auto_fetch_cookies=False,
            log_level=log_level,
        )

    return WeiboClient(
        cookie_storage_path=cookie_path,
        browser_headless=True,
        log_level=log_level,
    )


def _fetch_cookies_with_system_chrome() -> dict[str, str]:
    """Playwright Chromium 未安装时，尝试用本机 Chrome 获取访客 Cookie。"""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {}

    user_agent = (
        "Mozilla/5.0 (Linux; Android 13; SM-G9980) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/112.0.5615.135 Mobile Safari/537.36"
    )
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                channel="chrome",
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = browser.new_context(
                user_agent=user_agent,
                viewport={"width": 393, "height": 851},
                locale="zh-CN",
                is_mobile=True,
                has_touch=True,
            )
            page = context.new_page()
            page.goto("https://m.weibo.cn/", wait_until="networkidle", timeout=60000)
            time.sleep(2)
            cookies = {c["name"]: c["value"] for c in context.cookies()}
            browser.close()
            return cookies
    except Exception:
        return {}


def fetch_latest(client: WeiboClient, uid: str, limit: int) -> list[dict]:
    user = client.get_user_by_uid(uid)
    posts = client.get_user_posts(uid, page=1, expand=True)
    rows = []
    for post in posts[:limit]:
        rows.append(
            {
                "uid": uid,
                "screen_name": user.screen_name,
                "post_id": post.id,
                "bid": post.bid,
                "created_at": post.created_at.isoformat() if post.created_at else None,
                "text": post.text,
                "reposts": post.reposts_count,
                "comments": post.comments_count,
                "likes": post.attitudes_count,
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="验证 crawl4weibo 能否拉取博主微博")
    parser.add_argument(
        "--config",
        default="config.json",
        help="配置文件路径（可复制 config.example.json）",
    )
    parser.add_argument(
        "--uid",
        help="仅测试单个博主 ID，覆盖配置文件中的列表",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"缺少配置文件: {config_path}")
        print("请复制 config.example.json 为 config.json 后重试。")
        return 1

    config = load_config(config_path)
    user_ids = [args.uid] if args.uid else config.get("user_ids", [])
    limit = int(config.get("posts_per_user", 5))

    if not user_ids:
        print("user_ids 为空，请在 config.json 中填写博主 ID。")
        return 1

    client = build_client(config)
    all_rows: list[dict] = []

    for uid in user_ids:
        print(f"\n=== 博主 {uid} ===")
        try:
            rows = fetch_latest(client, uid, limit)
        except Exception as exc:
            print(f"失败: {exc}")
            continue

        if not rows:
            print("未获取到微博（可能是私密账号或需要登录态）。")
            continue

        for row in rows:
            preview = (row["text"] or "").replace("\n", " ")[:80]
            print(
                f"[{row['created_at']}] {row['screen_name']}: {preview} "
                f"(转{row['reposts']} 评{row['comments']} 赞{row['likes']})"
            )
        all_rows.extend(rows)

    print(f"\n共获取 {len(all_rows)} 条微博。")
    return 0 if all_rows else 2


if __name__ == "__main__":
    sys.exit(main())
