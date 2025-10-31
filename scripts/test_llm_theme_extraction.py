#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 LLM 主题提取增强功能
验证 primary_theme 和 related_sectors 的提取
"""
import asyncio
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database.async_db_manager import AsyncDatabaseManager
from backend.services.theme_unification_service import ThemeUnificationService
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("test_theme_extraction")


async def test_single_news(news_id: int):
    """测试单条新闻的主题提取"""
    logger.info("=" * 80)
    logger.info(f"测试新闻 ID: {news_id}")
    logger.info("=" * 80)
    
    db_config = get_database_config()
    async_db = AsyncDatabaseManager(db_config)
    service = ThemeUnificationService(async_db)
    
    try:
        # 1. 查看原始分析结果
        query = """
        SELECT 
            id,
            title,
            analysis_result,
            analysis_status
        FROM news
        WHERE id = %s
        """
        
        result = await async_db.fetch_one(query, (news_id,))
        
        if not result:
            logger.error(f"新闻 {news_id} 不存在")
            return
        
        logger.info(f"标题: {result['title']}")
        logger.info(f"分析状态: {result['analysis_status']}")
        
        # 检查是否有 news_classification
        if result['analysis_result'] and 'news_classification' in result['analysis_result']:
            logger.info("✅ 发现 news_classification 数据")
            
            # 显示原始数据
            classification = result['analysis_result']['news_classification']
            content = classification.get('content', {})
            logger.info(f"原始 content 类型: {type(content)}")
            if isinstance(content, str):
                logger.info(f"原始 content 前200字符:\n{content[:200]}...")
        else:
            logger.warning("⚠️  没有 news_classification 数据，跳过测试")
            return
        
        # 2. 执行同步（强制重新同步）
        logger.info("\n" + "-" * 80)
        logger.info("执行主题同步...")
        logger.info("-" * 80)
        
        count = await service.sync_llm_classifications(news_id, force_resync=True)
        
        logger.info(f"\n✅ 同步完成，共同步 {count} 个主题")
        
        # 3. 查询同步结果
        logger.info("\n" + "-" * 80)
        logger.info("查询同步结果...")
        logger.info("-" * 80)
        
        query = """
        SELECT 
            theme_name,
            confidence_score,
            source_type,
            rank_in_news,
            metadata
        FROM news_themes_unified
        WHERE news_id = %s
          AND source_type = 'llm_classification'
          AND is_active = true
        ORDER BY confidence_score DESC, rank_in_news
        """
        
        themes = await async_db.fetch_all(query, (news_id,))
        
        if not themes:
            logger.warning("⚠️  未找到同步的主题")
            return
        
        # 分类统计
        primary_themes = []
        sectors = []
        event_types = []
        others = []
        
        for theme in themes:
            theme_type = theme['metadata'].get('theme_type', 'unknown')
            
            theme_info = {
                'name': theme['theme_name'],
                'confidence': theme['confidence_score'],
                'type': theme_type,
                'source_detail': theme['metadata'].get('source_detail', 'unknown')
            }
            
            if theme_type == 'primary_theme':
                primary_themes.append(theme_info)
            elif theme_type == 'sector':
                sectors.append(theme_info)
            elif theme_type == 'event_type':
                event_types.append(theme_info)
            else:
                others.append(theme_info)
        
        # 输出结果
        logger.info(f"\n📊 提取结果统计:")
        logger.info(f"  - 总计: {len(themes)} 个主题")
        logger.info(f"  - Primary Theme: {len(primary_themes)} 个")
        logger.info(f"  - Sectors: {len(sectors)} 个")
        logger.info(f"  - Event Types: {len(event_types)} 个")
        logger.info(f"  - Others: {len(others)} 个")
        
        # 详细信息
        if primary_themes:
            logger.info(f"\n🎯 Primary Theme:")
            for t in primary_themes:
                logger.info(f"  - {t['name']}: {t['confidence']:.3f} (来源: {t['source_detail']})")
        
        if sectors:
            logger.info(f"\n🏭 Related Sectors:")
            for t in sectors:
                logger.info(f"  - {t['name']}: {t['confidence']:.3f} (来源: {t['source_detail']})")
        
        if event_types:
            logger.info(f"\n📅 Event Types:")
            for t in event_types:
                logger.info(f"  - {t['name']}: {t['confidence']:.3f} (来源: {t['source_detail']})")
        
        if others:
            logger.info(f"\n📌 Other Themes:")
            for t in others:
                logger.info(f"  - {t['name']}: {t['confidence']:.3f}")
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ 测试完成")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"测试失败: {e}", exc_info=True)


async def test_batch_news():
    """测试批量新闻的主题提取"""
    logger.info("=" * 80)
    logger.info("批量测试最近10条有 news_classification 的新闻")
    logger.info("=" * 80)
    
    db_config = get_database_config()
    async_db = AsyncDatabaseManager(db_config)
    
    # 查找最近有 news_classification 的新闻
    query = """
    SELECT id, title
    FROM news
    WHERE analysis_result ? 'news_classification'
      AND analysis_status = 'completed'
    ORDER BY date DESC
    LIMIT 10
    """
    
    news_list = await async_db.fetch_all(query)
    
    if not news_list:
        logger.warning("未找到有 news_classification 的新闻")
        return
    
    logger.info(f"找到 {len(news_list)} 条新闻\n")
    
    total_themes = 0
    total_primary = 0
    total_sectors = 0
    
    for news in news_list:
        logger.info(f"处理新闻 {news['id']}: {news['title'][:50]}...")
        
        service = ThemeUnificationService(async_db)
        count = await service.sync_llm_classifications(news['id'], force_resync=True)
        
        # 统计结果
        query = """
        SELECT metadata
        FROM news_themes_unified
        WHERE news_id = %s
          AND source_type = 'llm_classification'
          AND is_active = true
        """
        
        themes = await async_db.fetch_all(query, (news['id'],))
        
        primary_count = sum(1 for t in themes if t['metadata'].get('theme_type') == 'primary_theme')
        sector_count = sum(1 for t in themes if t['metadata'].get('theme_type') == 'sector')
        
        total_themes += len(themes)
        total_primary += primary_count
        total_sectors += sector_count
        
        logger.info(f"  ✅ 提取 {len(themes)} 个主题 (primary: {primary_count}, sectors: {sector_count})\n")
    
    logger.info("=" * 80)
    logger.info("📊 批量测试总结:")
    logger.info(f"  - 处理新闻数: {len(news_list)}")
    logger.info(f"  - 总主题数: {total_themes}")
    logger.info(f"  - Primary Themes: {total_primary}")
    logger.info(f"  - Sectors: {total_sectors}")
    logger.info(f"  - 平均每条新闻: {total_themes / len(news_list):.1f} 个主题")
    logger.info("=" * 80)


async def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='测试 LLM 主题提取功能')
    parser.add_argument('--news-id', type=int, help='测试指定新闻ID')
    parser.add_argument('--batch', action='store_true', help='批量测试最近10条新闻')
    
    args = parser.parse_args()
    
    if args.news_id:
        await test_single_news(args.news_id)
    elif args.batch:
        await test_batch_news()
    else:
        # 默认：查找一条示例新闻进行测试
        db_config = get_database_config()
        async_db = AsyncDatabaseManager(db_config)
        
        query = """
        SELECT id
        FROM news
        WHERE analysis_result ? 'news_classification'
          AND analysis_status = 'completed'
        ORDER BY date DESC
        LIMIT 1
        """
        
        result = await async_db.fetch_one(query)
        
        if result:
            logger.info("未指定参数，使用最新的一条有 news_classification 的新闻进行测试\n")
            await test_single_news(result['id'])
        else:
            logger.error("未找到有 news_classification 的新闻")
            logger.info("\n使用方法:")
            logger.info("  python scripts/test_llm_theme_extraction.py --news-id 3601")
            logger.info("  python scripts/test_llm_theme_extraction.py --batch")


if __name__ == '__main__':
    asyncio.run(main())

