#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
临时脚本：为 subscription_list 填充 channel_avatar
从 subscription_content 表获取已有的 author_avatar

使用方法：
    python backend/scripts/backfill_subscription_avatar.py --dry-run
    python backend/scripts/backfill_subscription_avatar.py
"""

import os
import sys
import argparse
import psycopg2

# 数据库配置
DB_HOST = os.getenv('DB_HOST', '43.99.37.76')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'guanhe_database')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '1q23l,Sc45+@!')


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def get_subscriptions_needing_avatar() -> list:
    """获取需要更新头像的订阅"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # 从 subscription_content 获取最新的 author_avatar
    # 并与 subscription_list 关联更新
    query = """
        SELECT DISTINCT
            sl.id,
            sl.channel_type,
            sl.channel_identifier,
            sl.channel_name,
            sc.author_avatar
        FROM subscription_list sl
        INNER JOIN subscription_content sc
            ON sc.channel_type = sl.channel_type
            AND LOWER(sc.author_identifier) = LOWER(sl.channel_identifier)
        WHERE sl.channel_type = 'twitter'
        AND sc.author_avatar IS NOT NULL
        AND sc.author_avatar != ''
        AND (sl.channel_avatar IS NULL OR sl.channel_avatar = '')
        ORDER BY sl.id
    """

    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return results


def update_subscription_avatar(subscription_id: int, avatar_url: str) -> bool:
    """更新订阅头像"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            UPDATE subscription_list
            SET channel_avatar = %s
            WHERE id = %s
            """,
            (avatar_url, subscription_id)
        )
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"  ❌ 更新失败: {str(e)}")
        return False
    finally:
        cursor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Backfill subscription avatar from content')
    parser.add_argument('--dry-run', action='store_true', help='Preview only')
    args = parser.parse_args()

    print("=" * 60)
    print("Subscription Avatar Backfill Script")
    print("=" * 60)
    print(f"Database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print()

    # 获取需要更新的订阅
    print("📊 获取需要更新头像的订阅...")
    subscriptions = get_subscriptions_needing_avatar()

    if not subscriptions:
        print("✅ 所有订阅已有头像或无可用头像")
        return

    print(f"📋 找到 {len(subscriptions)} 个订阅需要更新")
    print()

    if args.dry_run:
        print("=== 🔍 DRY RUN - Preview only ===")
        print()
        for row in subscriptions[:10]:
            sub_id, channel_type, channel_identifier, channel_name, avatar_url = row
            print(f"  [{sub_id}] @{channel_identifier} ({channel_name or 'N/A'})")
            print(f"      Avatar: {avatar_url}")
        if len(subscriptions) > 10:
            print(f"  ... 还有 {len(subscriptions) - 10} 个订阅")
        return

    # 执行更新
    print("🚀 开始更新...")
    print()

    updated_count = 0
    failed_count = 0

    for row in subscriptions:
        sub_id, channel_type, channel_identifier, channel_name, avatar_url = row
        print(f"[{sub_id}] @{channel_identifier} ({channel_name or 'N/A'})")
        print(f"  Avatar: {avatar_url}")

        if update_subscription_avatar(sub_id, avatar_url):
            print(f"  ✅ 更新成功")
            updated_count += 1
        else:
            failed_count += 1

        print()

    # 总结
    print("=" * 60)
    print("📊 Summary")
    print("=" * 60)
    print(f"  处理订阅: {len(subscriptions)}")
    print(f"  更新成功: {updated_count}")
    print(f"  更新失败: {failed_count}")
    print()
    print("✅ Done!")


if __name__ == '__main__':
    main()