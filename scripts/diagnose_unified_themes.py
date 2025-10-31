#!/usr/bin/env python
"""
统一主题系统诊断脚本
快速诊断数据同步状态和问题
"""
import asyncio
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backend.database.async_db_manager import AsyncDatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("diagnose_themes")


async def check_original_data(db: AsyncDatabaseManager):
    """检查原始数据源"""
    print("\n" + "="*60)
    print("📊 检查原始数据源")
    print("="*60)
    
    # 检查新闻数据
    query1 = """
    SELECT 
        COUNT(*) as total_news,
        COUNT(*) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '30 days') as news_30d,
        MIN(date) as earliest,
        MAX(date) as latest
    FROM news
    """
    result1 = await db.fetch_one(query1)
    
    print(f"\n📰 新闻数据 (news表):")
    if not result1:
        print("  ⚠️  无法获取数据（数据库连接失败）")
        return 0
    
    print(f"  总数: {result1['total_news']}")
    print(f"  最近30天: {result1['news_30d']}")
    print(f"  最早: {result1['earliest']}")
    print(f"  最新: {result1['latest']}")
    
    # 检查EODHD标签
    query2 = """
    SELECT COUNT(DISTINCT natr.news_id) as news_count
    FROM news_auto_tags_relations natr
    INNER JOIN news n ON natr.news_id = n.id
    WHERE n.date >= CURRENT_DATE - INTERVAL '30 days'
    """
    result2 = await db.fetch_one(query2)
    print(f"\n🏷️  EODHD标签:")
    print(f"  有标签的新闻(30天): {result2['news_count']}")
    
    # 检查Vector匹配
    query3 = """
    SELECT COUNT(DISTINCT ntm.news_id) as news_count
    FROM news_theme_matches ntm
    INNER JOIN news n ON ntm.news_id = n.id
    WHERE n.date >= CURRENT_DATE - INTERVAL '30 days'
    """
    result3 = await db.fetch_one(query3)
    print(f"\n🎯 Vector匹配:")
    print(f"  有匹配的新闻(30天): {result3['news_count']}")
    
    # 检查LLM分析
    query4 = """
    SELECT COUNT(*) as count
    FROM news
    WHERE analysis_result IS NOT NULL
      AND analysis_status = 'completed'
      AND date >= CURRENT_DATE - INTERVAL '30 days'
    """
    result4 = await db.fetch_one(query4)
    print(f"\n🤖 LLM分析:")
    print(f"  已分析的新闻(30天): {result4['count']}")
    
    return result1['news_30d']


async def check_unified_data(db: AsyncDatabaseManager):
    """检查统一表数据"""
    print("\n" + "="*60)
    print("📊 检查统一表数据")
    print("="*60)
    
    # 统一表统计
    query1 = """
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT news_id) as news_count,
        COUNT(DISTINCT theme_name) as theme_count
    FROM news_themes_unified
    WHERE is_active = true
    """
    result1 = await db.fetch_one(query1)
    
    print(f"\n📋 news_themes_unified表:")
    print(f"  总记录数: {result1['total']}")
    print(f"  覆盖新闻数: {result1['news_count']}")
    print(f"  不同主题数: {result1['theme_count']}")
    
    # 按来源分布
    query2 = """
    SELECT 
        source_type,
        COUNT(*) as count,
        COUNT(DISTINCT news_id) as news_count,
        AVG(confidence_score)::numeric(5,2) as avg_confidence
    FROM news_themes_unified
    WHERE is_active = true
    GROUP BY source_type
    ORDER BY count DESC
    """
    results2 = await db.fetch_all(query2)
    
    if results2:
        print(f"\n📊 按来源分布:")
        for row in results2:
            print(f"  {row['source_type']:20s}: {row['count']:5d} 记录, "
                  f"{row['news_count']:4d} 新闻, "
                  f"平均置信度: {row['avg_confidence']}")
    
    # 主题主数据
    query3 = "SELECT COUNT(*) as count FROM theme_master_data WHERE is_active = true"
    result3 = await db.fetch_one(query3)
    print(f"\n📚 theme_master_data表:")
    print(f"  主题数: {result3['count']}")
    
    return result1['total']


async def check_sync_issues(db: AsyncDatabaseManager):
    """检查同步问题"""
    print("\n" + "="*60)
    print("🔍 检查潜在问题")
    print("="*60)
    
    # 查找有原始数据但未同步的新闻
    query = """
    SELECT 
        n.id,
        n.title,
        n.date,
        EXISTS(SELECT 1 FROM news_auto_tags_relations WHERE news_id = n.id) as has_tags,
        EXISTS(SELECT 1 FROM news_theme_matches WHERE news_id = n.id) as has_matches,
        EXISTS(SELECT 1 FROM news_themes_unified WHERE news_id = n.id AND is_active = true) as is_synced
    FROM news n
    WHERE n.date >= CURRENT_DATE - INTERVAL '7 days'
    ORDER BY n.date DESC
    LIMIT 10
    """
    
    results = await db.fetch_all(query)
    
    unsynced = [r for r in results if (r['has_tags'] or r['has_matches']) and not r['is_synced']]
    
    if unsynced:
        print(f"\n⚠️  发现 {len(unsynced)} 条有原始数据但未同步的新闻:")
        for row in unsynced[:5]:
            print(f"  ID={row['id']}, 日期={row['date']}, "
                  f"tags={row['has_tags']}, matches={row['has_matches']}")
    else:
        print(f"\n✅ 最近7天的新闻同步状态正常")
    
    # 检查视图是否可用
    try:
        query_view = "SELECT COUNT(*) as count FROM v_news_best_themes"
        result_view = await db.fetch_one(query_view)
        print(f"\n✅ 视图 v_news_best_themes 可用 (记录数: {result_view['count']})")
    except Exception as e:
        print(f"\n❌ 视图不可用: {e}")


async def suggest_actions(has_news: int, unified_count: int):
    """建议后续操作"""
    print("\n" + "="*60)
    print("💡 建议后续操作")
    print("="*60)
    
    if has_news == 0:
        print("\n❌ 没有最近30天的新闻数据")
        print("  建议: 确保新闻数据已导入")
    elif unified_count == 0:
        print("\n⚠️  有新闻但统一表为空")
        print("  建议执行:")
        print("  1. 手动同步: python scripts/sync_themes_manual.py")
        print("  2. 或通过API: POST /api/themes-unified/sync/recent?days=30")
    elif unified_count < has_news * 0.5:
        print("\n⚠️  同步覆盖率低")
        print(f"  统一表: {unified_count} 条记录")
        print(f"  新闻数: {has_news} 条")
        print("  建议: 检查日志并重新同步")
    else:
        print("\n✅ 数据同步正常")
        print(f"  统一表: {unified_count} 条记录")
        print(f"  新闻数: {has_news} 条")
        print("\n可以开始使用:")
        print("  - GET /api/themes-unified/trending")
        print("  - GET /api/themes-unified/news/{news_id}/themes")


async def main():
    print("\n" + "="*60)
    print("  统一主题系统诊断工具")
    print("="*60)
    
    try:
        config = get_database_config()
        db = AsyncDatabaseManager(config)
        
        # 检查原始数据
        has_news = await check_original_data(db)
        
        # 检查统一数据
        unified_count = await check_unified_data(db)
        
        # 检查同步问题
        await check_sync_issues(db)
        
        # 建议操作
        await suggest_actions(has_news, unified_count)
        
        print("\n" + "="*60)
        print("✅ 诊断完成")
        print("="*60 + "\n")
        
    except Exception as e:
        logger.error(f"诊断失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())

