#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清理 Twitter 订阅数据脚本
用途：测试时清除 subscription_content 和 subscription_list 表的数据，并重置 ID 序列

⚠️  警告：此脚本会删除所有 Twitter 订阅和相关内容数据！
"""

import os
import sys
import psycopg2
from datetime import datetime

# 数据库配置（从环境变量读取）
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '111.230.48.119'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'rc_database'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '1q23l,Sc45+@!')
}


def connect_db():
    """连接数据库"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        sys.exit(1)


def get_stats(conn):
    """获取当前数据统计"""
    cursor = conn.cursor()
    
    # subscription_list 统计
    cursor.execute("""
        SELECT 
            channel_type,
            COUNT(*) as count
        FROM subscription_list
        GROUP BY channel_type
        ORDER BY channel_type
    """)
    subscription_stats = cursor.fetchall()
    
    # subscription_content 统计
    cursor.execute("""
        SELECT 
            channel_type,
            COUNT(*) as count,
            COUNT(CASE WHEN is_retweet THEN 1 END) as reposts,
            COUNT(CASE WHEN is_quote THEN 1 END) as quotes,
            COUNT(CASE WHEN is_reply THEN 1 END) as replies
        FROM subscription_content
        GROUP BY channel_type
        ORDER BY channel_type
    """)
    content_stats = cursor.fetchall()
    
    cursor.close()
    
    return subscription_stats, content_stats


def print_stats(subscription_stats, content_stats, label="当前"):
    """打印统计信息"""
    print(f"\n{'='*60}")
    print(f"📊 {label}数据统计")
    print(f"{'='*60}")
    
    print(f"\n📋 subscription_list (订阅列表):")
    if subscription_stats:
        for channel_type, count in subscription_stats:
            print(f"  - {channel_type}: {count} 个订阅")
    else:
        print("  (空)")
    
    print(f"\n📰 subscription_content (订阅内容):")
    if content_stats:
        for row in content_stats:
            channel_type, total, reposts, quotes, replies = row
            print(f"  - {channel_type}: {total} 条内容")
            print(f"      Reposts: {reposts}, Quotes: {quotes}, Replies: {replies}")
    else:
        print("  (空)")


def clean_twitter_data(conn, channel_type='twitter'):
    """
    清理指定渠道的订阅数据
    
    Args:
        conn: 数据库连接
        channel_type: 渠道类型（默认 'twitter'）
    """
    cursor = conn.cursor()
    
    try:
        print(f"\n🗑️  开始清理 {channel_type} 数据...")
        
        # 1. 删除 subscription_content 中的内容（有外键约束，先删除）
        print(f"\n1️⃣  删除 subscription_content 中的 {channel_type} 内容...")
        cursor.execute("""
            DELETE FROM subscription_content
            WHERE channel_type = %s
        """, (channel_type,))
        deleted_content = cursor.rowcount
        print(f"   ✓ 已删除 {deleted_content} 条内容")
        
        # 2. 删除 subscription_list 中的订阅
        print(f"\n2️⃣  删除 subscription_list 中的 {channel_type} 订阅...")
        cursor.execute("""
            DELETE FROM subscription_list
            WHERE channel_type = %s
        """, (channel_type,))
        deleted_subscriptions = cursor.rowcount
        print(f"   ✓ 已删除 {deleted_subscriptions} 个订阅")
        
        # 3. 重置 subscription_content 的 ID 序列
        print(f"\n3️⃣  重置 subscription_content ID 序列...")
        cursor.execute("""
            SELECT COALESCE(MAX(id), 0) + 1 FROM subscription_content
        """)
        next_content_id = cursor.fetchone()[0]
        cursor.execute("""
            SELECT setval('subscription_content_id_seq', %s, false)
        """, (next_content_id,))
        print(f"   ✓ 序列已重置为: {next_content_id}")
        
        # 4. 重置 subscription_list 的 ID 序列
        print(f"\n4️⃣  重置 subscription_list ID 序列...")
        cursor.execute("""
            SELECT COALESCE(MAX(id), 0) + 1 FROM subscription_list
        """)
        next_subscription_id = cursor.fetchone()[0]
        cursor.execute("""
            SELECT setval('subscription_list_id_seq', %s, false)
        """, (next_subscription_id,))
        print(f"   ✓ 序列已重置为: {next_subscription_id}")
        
        # 5. 提交事务
        conn.commit()
        cursor.close()
        
        print(f"\n✅ 清理完成！")
        return deleted_content, deleted_subscriptions
        
    except Exception as e:
        conn.rollback()
        cursor.close()
        print(f"\n❌ 清理失败: {e}")
        raise


def clean_all_data(conn):
    """
    清理所有渠道的订阅数据（危险操作）
    """
    cursor = conn.cursor()
    
    try:
        print(f"\n🗑️  清理所有订阅数据...")
        
        # 1. 删除所有 subscription_content
        cursor.execute("DELETE FROM subscription_content")
        deleted_content = cursor.rowcount
        print(f"   ✓ 已删除 {deleted_content} 条内容")
        
        # 2. 删除所有 subscription_list
        cursor.execute("DELETE FROM subscription_list")
        deleted_subscriptions = cursor.rowcount
        print(f"   ✓ 已删除 {deleted_subscriptions} 个订阅")
        
        # 3. 重置序列
        cursor.execute("SELECT setval('subscription_content_id_seq', 1, false)")
        cursor.execute("SELECT setval('subscription_list_id_seq', 1, false)")
        print(f"   ✓ 序列已重置为 1")
        
        # 4. 删除 subscription_fetch_history
        cursor.execute("DELETE FROM subscription_fetch_history")
        deleted_history = cursor.rowcount
        print(f"   ✓ 已删除 {deleted_history} 条获取历史")
        
        cursor.execute("SELECT setval('subscription_fetch_history_id_seq', 1, false)")
        
        conn.commit()
        cursor.close()
        
        print(f"\n✅ 全部清理完成！")
        return deleted_content, deleted_subscriptions, deleted_history
        
    except Exception as e:
        conn.rollback()
        cursor.close()
        print(f"\n❌ 清理失败: {e}")
        raise


def main():
    """主函数"""
    print("="*60)
    print("🧹 Twitter 订阅数据清理脚本")
    print("="*60)
    
    # 连接数据库
    print("\n📡 连接数据库...")
    conn = connect_db()
    print("   ✓ 数据库连接成功")
    
    # 显示清理前的统计
    subscription_stats, content_stats = get_stats(conn)
    print_stats(subscription_stats, content_stats, "清理前")
    
    # 确认操作
    print(f"\n{'='*60}")
    print("⚠️  警告：此操作将删除数据，无法恢复！")
    print(f"{'='*60}")
    print("\n请选择清理模式：")
    print("  1. 仅清理 Twitter 数据（推荐）")
    print("  2. 清理所有渠道数据（危险）")
    print("  0. 取消")
    
    try:
        choice = input("\n请输入选项 [0/1/2]: ").strip()
    except KeyboardInterrupt:
        print("\n\n❌ 操作已取消")
        conn.close()
        sys.exit(0)
    
    if choice == '0':
        print("\n❌ 操作已取消")
        conn.close()
        sys.exit(0)
    elif choice == '1':
        # 清理 Twitter 数据
        print("\n⚠️  即将清理 Twitter 数据，确认继续？ [y/N]: ", end='')
        confirm = input().strip().lower()
        if confirm != 'y':
            print("\n❌ 操作已取消")
            conn.close()
            sys.exit(0)
        
        clean_twitter_data(conn, channel_type='twitter')
    elif choice == '2':
        # 清理所有数据
        print("\n⚠️⚠️⚠️  即将清理所有订阅数据！输入 'DELETE ALL' 确认: ", end='')
        confirm = input().strip()
        if confirm != 'DELETE ALL':
            print("\n❌ 操作已取消")
            conn.close()
            sys.exit(0)
        
        clean_all_data(conn)
    else:
        print("\n❌ 无效选项，操作已取消")
        conn.close()
        sys.exit(0)
    
    # 显示清理后的统计
    subscription_stats, content_stats = get_stats(conn)
    print_stats(subscription_stats, content_stats, "清理后")
    
    # 关闭连接
    conn.close()
    
    print(f"\n{'='*60}")
    print("✅ 脚本执行完成")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
