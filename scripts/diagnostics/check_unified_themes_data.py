#!/usr/bin/env python3
"""
检查统一主题系统的数据状态
"""
import asyncio
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger(__name__)


async def check_unified_themes_data():
    """检查news_themes_unified表的数据"""
    config = get_database_config()
    db = DatabaseManager(config)
    
    print("=" * 60)
    print("🔍 检查统一主题系统数据状态")
    print("=" * 60)
    
    # 1. 检查news_themes_unified总记录数
    query1 = 'SELECT COUNT(*) as count FROM news_themes_unified'
    result1 = await db.fetch_one(query1)
    total_count = result1['count']
    print(f"\n📊 news_themes_unified 总记录数: {total_count}")
    
    if total_count == 0:
        print("\n⚠️  警告：news_themes_unified表为空！")
        print("可能原因：")
        print("  1. 还未触发同步任务")
        print("  2. 新闻表中没有数据")
        print("  3. 同步过程出错")
        
        # 检查新闻总数
        news_query = 'SELECT COUNT(*) as count FROM news'
        news_result = await db.fetch_one(news_query)
        print(f"\n📰 news表总记录数: {news_result['count']}")
        
        if news_result['count'] > 0:
            print("\n💡 建议：运行同步任务")
            print("   curl -X POST 'http://localhost:8000/api/themes-unified/sync/recent?days=30' \\")
            print("        -H 'Authorization: Bearer YOUR_TOKEN'")
        
        return
    
    # 2. 按来源统计
    query2 = '''
    SELECT source_type, COUNT(*) as count 
    FROM news_themes_unified 
    WHERE is_active = true
    GROUP BY source_type
    ORDER BY count DESC
    '''
    result2 = await db.fetch_all(query2)
    print(f"\n📈 按来源类型分布（活跃记录）:")
    for row in result2:
        print(f"   {row['source_type']}: {row['count']} 条")
    
    # 3. 主题统计
    query3 = '''
    SELECT 
        COUNT(DISTINCT theme_name) as unique_themes,
        COUNT(*) FILTER (WHERE theme_name IS NULL) as null_count
    FROM news_themes_unified
    WHERE is_active = true
    '''
    result3 = await db.fetch_one(query3)
    print(f"\n🏷️  主题状态:")
    print(f"   唯一主题数: {result3['unique_themes']}")
    print(f"   空主题记录数: {result3['null_count']}")
    
    if result3['null_count'] > 0:
        print(f"   ⚠️  有 {result3['null_count']} 条记录主题为空")
    
    # 4. 质量分数统计
    query4 = '''
    SELECT 
        COUNT(*) FILTER (WHERE quality_score IS NULL) as no_quality,
        AVG(quality_score) as avg_quality,
        MIN(quality_score) as min_quality,
        MAX(quality_score) as max_quality
    FROM news_themes_unified
    WHERE is_active = true
    '''
    result4 = await db.fetch_one(query4)
    print(f"\n⭐ 质量分数统计:")
    if result4['no_quality'] > 0:
        print(f"   无质量分数记录: {result4['no_quality']}")
    if result4['avg_quality']:
        print(f"   平均质量分数: {float(result4['avg_quality']):.4f}")
        print(f"   最低质量分数: {float(result4['min_quality']):.4f}")
        print(f"   最高质量分数: {float(result4['max_quality']):.4f}")
    
    # 5. 最近的记录
    query5 = '''
    SELECT 
        ntu.news_id, 
        ntu.theme_name,
        ntu.source_type, 
        ntu.confidence_score,
        ntu.quality_score,
        ntu.created_at
    FROM news_themes_unified ntu
    WHERE ntu.is_active = true
    ORDER BY ntu.created_at DESC
    LIMIT 10
    '''
    result5 = await db.fetch_all(query5)
    print(f"\n🕐 最近10条记录:")
    for row in result5:
        print(f"   新闻{row['news_id']}: {row['theme_name']}")
        print(f"      来源: {row['source_type']}, 置信度: {row['confidence_score']:.4f}, 质量: {row['quality_score'] or 'N/A'}")
    
    # 6. Top主题
    query6 = '''
    SELECT 
        theme_name,
        COUNT(DISTINCT news_id) as news_count,
        AVG(confidence_score) as avg_confidence
    FROM news_themes_unified
    WHERE is_active = true 
      AND theme_name IS NOT NULL
    GROUP BY theme_name
    ORDER BY news_count DESC
    LIMIT 10
    '''
    result6 = await db.fetch_all(query6)
    print(f"\n🔥 Top 10 热门主题:")
    for i, row in enumerate(result6, 1):
        print(f"   {i}. {row['theme_name']}: {row['news_count']} 条新闻, "
              f"平均置信度 {float(row['avg_confidence']):.4f}")
    
    # 7. 检查theme_master_data
    query7 = 'SELECT COUNT(*) as count FROM theme_master_data WHERE is_active = true'
    result7 = await db.fetch_one(query7)
    print(f"\n📚 theme_master_data 主题词典数: {result7['count']}")
    if result7['count'] == 0:
        print("   ⚠️  警告：主题主数据表为空，标准化功能可能无法正常工作")
    
    # 8. 检查theme_aggregation_stats
    query8 = 'SELECT COUNT(*) as count FROM theme_aggregation_stats'
    result8 = await db.fetch_one(query8)
    print(f"\n📊 theme_aggregation_stats 统计记录数: {result8['count']}")
    if result8['count'] == 0:
        print("   ℹ️  提示：这个表用于性能优化，为空是正常的（需要定期统计任务）")
    
    print("\n" + "=" * 60)
    print("✅ 检查完成")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(check_unified_themes_data())





