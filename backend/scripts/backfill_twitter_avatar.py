#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
临时脚本：填充 subscription_content 表中 Twitter 内容的 author_avatar 字段
从 twitterapi.io user/info API 获取作者头像 URL

独立脚本，不依赖 backend 模块

使用方法：
    python scripts/backfill_twitter_avatar.py --dry-run
    python scripts/backfill_twitter_avatar.py --limit 5
"""

import os
import sys
import time
import argparse
import requests
import psycopg2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 数据库配置 - 从环境变量读取或使用默认值
DB_HOST = os.getenv('DB_HOST', '43.99.37.76')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'guanhe_database')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '1q23l,Sc45+@!')

# Twitter API Key
API_KEY = os.getenv('TWITTER_API_KEY', 'new1_958a42497f844e1e9dd1b866cb54a9d9')


def get_db_connection():
    """获取数据库连接"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def get_unique_twitter_authors(limit: int = None) -> list:
    """获取数据库中当前订阅的 Twitter 作者（仅处理 enabled=true 的订阅）"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # 只处理当前订阅的作者，不处理历史已取消订阅的内容
    query = """
        SELECT DISTINCT sc.author_identifier, sc.author_name
        FROM subscription_content sc
        INNER JOIN subscription_list sl
            ON sl.channel_type = 'twitter'
            AND LOWER(sl.channel_identifier) = LOWER(sc.author_identifier)
            AND sl.enabled = true
        WHERE sc.channel_type = 'twitter'
        AND sc.author_identifier IS NOT NULL
        AND sc.author_identifier != ''
        AND (sc.author_avatar IS NULL OR sc.author_avatar = '')
        ORDER BY sc.author_identifier
    """

    if limit:
        query += f" LIMIT {limit}"

    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return [(row[0], row[1]) for row in results]


def get_record_count_for_author(author_identifier: str) -> int:
    """获取指定作者的记录数量"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM subscription_content
        WHERE channel_type = 'twitter'
        AND author_identifier = %s
        AND (author_avatar IS NULL OR author_avatar = '')
        """,
        (author_identifier,)
    )
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count


def fetch_avatar_from_api(author_identifier: str, session: requests.Session) -> str:
    """从 twitterapi.io 获取作者头像 URL"""
    try:
        url = f"https://api.twitterapi.io/twitter/user/info?userName={author_identifier}"
        headers = {"X-API-Key": API_KEY}

        response = session.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        data = response.json()

        if data.get('status') == 'error':
            print(f"  ⚠️  API error: {data.get('msg')}")
            return None

        user_info = data.get('data', {})
        profile_picture = user_info.get('profilePicture')

        if profile_picture:
            return profile_picture
        else:
            print(f"  ⚠️  No profilePicture in response")
            return None

    except requests.exceptions.Timeout:
        print(f"  ❌ Timeout")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Request error: {str(e)}")
        return None
    except Exception as e:
        print(f"  ❌ Unexpected error: {str(e)}")
        return None


def update_avatar_in_db(author_identifier: str, avatar_url: str) -> int:
    """更新数据库中指定作者的所有内容的头像"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            UPDATE subscription_content
            SET author_avatar = %s
            WHERE channel_type = 'twitter'
            AND author_identifier = %s
            AND (author_avatar IS NULL OR author_avatar = '')
            """,
            (avatar_url, author_identifier)
        )

        updated_count = cursor.rowcount
        conn.commit()

        return updated_count

    except Exception as e:
        conn.rollback()
        print(f"  ❌ DB update error: {str(e)}")
        return 0
    finally:
        cursor.close()
        conn.close()


def create_session_with_retries() -> requests.Session:
    """创建带重试机制的 session"""
    session = requests.Session()

    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def main():
    parser = argparse.ArgumentParser(description='Backfill Twitter author avatars')
    parser.add_argument('--dry-run', action='store_true', help='Preview only, no updates')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of authors to process')
    args = parser.parse_args()

    print("=" * 60)
    print("Twitter Author Avatar Backfill Script")
    print("=" * 60)
    print(f"Database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print()

    # 获取需要更新的作者列表
    print("📊 Fetching unique Twitter authors from database...")
    authors = get_unique_twitter_authors(args.limit)

    if not authors:
        print("✅ No Twitter authors need avatar update")
        return

    print(f"📋 Found {len(authors)} unique authors to process")
    print()

    if args.dry_run:
        print("=== 🔍 DRY RUN - Preview only ===")
        print()
        for i, (author_identifier, author_name) in enumerate(authors, 1):
            record_count = get_record_count_for_author(author_identifier)
            print(f"  {i}. @{author_identifier} ({author_name or 'N/A'}) - {record_count} records")
        print()
        print(f"Total records to update: {sum(get_record_count_for_author(a[0]) for a in authors)}")
        return

    # 创建 session
    session = create_session_with_retries()

    # 统计
    total_updated = 0
    failed_authors = []

    # 处理每个作者
    print("🚀 Starting update process...")
    print()

    for i, (author_identifier, author_name) in enumerate(authors, 1):
        record_count = get_record_count_for_author(author_identifier)
        print(f"[{i}/{len(authors)}] @{author_identifier} ({author_name or 'N/A'}) - {record_count} records")

        avatar_url = fetch_avatar_from_api(author_identifier, session)

        if avatar_url:
            print(f"  🖼️  Avatar: {avatar_url}")
            updated_count = update_avatar_in_db(author_identifier, avatar_url)
            print(f"  ✅ Updated {updated_count} records")
            total_updated += updated_count
        else:
            failed_authors.append(author_identifier)
            print(f"  ⏭️  Skipped")

        # 避免 API 限速
        time.sleep(0.5)
        print()

    # 输出总结
    print("=" * 60)
    print("📊 Summary")
    print("=" * 60)
    print(f"  Authors processed: {len(authors)}")
    print(f"  Records updated:   {total_updated}")
    print(f"  Failed authors:    {len(failed_authors)}")

    if failed_authors:
        print()
        print(f"❌ Failed authors: {', '.join(failed_authors)}")

    print()
    print("✅ Done!")


if __name__ == '__main__':
    main()