#!/usr/bin/env python3
"""
简单脚本：拉取 @elonmusk 最新推文并保存为 JSON 文件。
使用 twitterapi.io 的 advanced_search 接口。
"""
import json
import os
import sys

# 支持从项目根目录或 X-test 目录运行
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
for env_path in [os.path.join(PROJECT_ROOT, ".env"), os.path.join(SCRIPT_DIR, ".env")]:
    if os.path.isfile(env_path):
        try:
            from dotenv import load_dotenv
            load_dotenv(env_path)
            break
        except ImportError:
            pass

import requests

# 配置：可从环境变量读取，否则用默认（请勿提交真实 key 到仓库）
X_API_KEY = os.environ.get("TWITTER_API_KEY", "")
USERNAME = "elonmusk"
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "elonmusk_latest_tweets.json")
API_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"


def fetch_latest_tweets(username: str, api_key: str, max_pages: int = 1):
    """拉取指定用户最新推文（可多页）。"""
    if not api_key:
        print("错误：未设置 TWITTER_API_KEY，请在 .env 中配置或导出环境变量。")
        sys.exit(1)

    query = f"from:{username} include:nativeretweets"
    headers = {"X-API-Key": api_key}
    params = {"query": query, "queryType": "Latest"}
    all_tweets = []
    next_cursor = None
    page = 0

    while page < max_pages:
        if next_cursor:
            params["cursor"] = next_cursor
        try:
            resp = requests.get(API_URL, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"请求失败: {e}")
            if hasattr(e, "response") and e.response is not None:
                print(f"响应: {e.response.text[:500]}")
            sys.exit(1)

        # 兼容多种返回结构
        tweets = []
        if isinstance(data, list):
            tweets = data
        elif isinstance(data, dict):
            tweets = data.get("tweets") or data.get("data") or []

        if tweets:
            all_tweets.extend(tweets)
            print(f"  第 {page + 1} 页获取 {len(tweets)} 条推文，累计 {len(all_tweets)} 条")
        else:
            print(f"  第 {page + 1} 页无更多推文")
            break

        has_next = data.get("has_next_page") or data.get("hasNextPage")
        next_cursor = data.get("next_cursor") or data.get("nextCursor")
        if not has_next or not next_cursor:
            break
        page += 1

    return all_tweets


def main():
    print(f"正在拉取 @{USERNAME} 最新推文...")
    tweets = fetch_latest_tweets(USERNAME, X_API_KEY, max_pages=1)
    print(f"共获取 {len(tweets)} 条推文")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(tweets, f, ensure_ascii=False, indent=2)

    print(f"已保存至: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
