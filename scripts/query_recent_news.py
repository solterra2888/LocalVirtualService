#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速查询最近的新闻ID
用于测试项目新闻添加功能
"""

import sys
sys.path.append('/Users/kevinliu/rcproject')

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config

def main():
    try:
        db_config = get_database_config()
        db = DatabaseManager(db_config)
        
        # 查询最近的20条新闻
        query = """
        SELECT id, title, date, source, ticker
        FROM news 
        ORDER BY date DESC, id DESC 
        LIMIT 20
        """
        
        results = db.fetch_all(query)
        
        print("\n" + "="*100)
        print("📰 最近的20条新闻（可用于项目关联）")
        print("="*100)
        
        for row in results:
            news_id = row[0]
            title = row[1][:60] if row[1] else 'N/A'
            date = row[2]
            source = row[3] if row[3] else 'N/A'
            ticker = row[4] if row[4] else 'N/A'
            
            print(f"\n新闻ID: {news_id}")
            print(f"标题: {title}...")
            print(f"日期: {date} | 来源: {source} | 股票: {ticker}")
            print("-"*100)
        
        # 查询新闻总数
        count_query = "SELECT COUNT(*) FROM news"
        total = db.fetch_one(count_query)[0]
        
        print(f"\n✅ 数据库中共有 {total} 条新闻")
        print(f"\n💡 使用方法：在项目详情页的「新闻」Tab中，点击「添加新闻」，输入上述任意新闻ID\n")
        
    except Exception as e:
        print(f"\n❌ 查询失败: {str(e)}")
        print("请确保数据库连接正常\n")

if __name__ == '__main__':
    main()

