#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回填历史新闻的 LLM 分类主题
重新同步已有 news_classification 的新闻，提取 primary_theme 和 related_sectors
"""
import asyncio
import sys
import os
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database.async_db_manager import AsyncDatabaseManager
from backend.services.theme_unification_service import ThemeUnificationService
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("backfill_themes")


async def backfill_llm_themes(
    limit: int = None,
    days: int = None,
    dry_run: bool = False
):
    """
    回填历史新闻的 LLM 分类主题
    
    Args:
        limit: 限制处理数量
        days: 限制最近多少天的新闻
        dry_run: 仅预览，不实际执行
    """
    logger.info("=" * 80)
    logger.info("开始回填 LLM 分类主题（提取 primary_theme 和 related_sectors）")
    logger.info("=" * 80)
    
    db_config = get_database_config()
    async_db = AsyncDatabaseManager(db_config)
    service = ThemeUnificationService(async_db)
    
    # 构建查询条件
    conditions = [
        "n.analysis_result ? 'news_classification'",
        "n.analysis_status = 'completed'"
    ]
    
    params = []
    
    if days:
        conditions.append("n.date >= CURRENT_TIMESTAMP - INTERVAL '%s days'")
        params.append(days)
        logger.info(f"限制范围: 最近 {days} 天")
    
    where_clause = " AND ".join(conditions)
    
    # 查找需要回填的新闻
    query = f"""
    SELECT n.id, n.title, n.date
    FROM news n
    WHERE {where_clause}
    ORDER BY n.date DESC
    """
    
    if limit:
        query += f" LIMIT {limit}"
        logger.info(f"限制数量: {limit} 条")
    
    if params:
        news_list = await async_db.fetch_all(query, tuple(params))
    else:
        news_list = await async_db.fetch_all(query)
    
    if not news_list:
        logger.info("未找到需要回填的新闻")
        return
    
    logger.info(f"找到 {len(news_list)} 条新闻需要回填")
    
    if dry_run:
        logger.info("\n🔍 预览模式（不会实际执行）:")
        for i, news in enumerate(news_list[:10], 1):
            logger.info(f"  {i}. [{news['id']}] {news['title'][:60]} ({news['date']})")
        if len(news_list) > 10:
            logger.info(f"  ... 还有 {len(news_list) - 10} 条")
        logger.info("\n使用 --execute 参数执行实际回填")
        return
    
    # 执行回填
    logger.info(f"\n开始回填...\n")
    
    success_count = 0
    failed_count = 0
    total_themes = 0
    total_primary = 0
    total_sectors = 0
    
    start_time = datetime.now()
    
    for i, news in enumerate(news_list, 1):
        try:
            # 显示进度
            if i % 10 == 0 or i == 1:
                progress = (i / len(news_list)) * 100
                logger.info(f"进度: {i}/{len(news_list)} ({progress:.1f}%)")
            
            # 同步主题（强制重新同步）
            count = await service.sync_llm_classifications(
                news['id'], 
                force_resync=True
            )
            
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
            
            success_count += 1
            
            # 详细日志（每50条显示一次）
            if i % 50 == 0:
                logger.info(
                    f"  [{news['id']}] {news['title'][:40]}: "
                    f"{len(themes)} 个主题 (primary: {primary_count}, sectors: {sector_count})"
                )
            
            # 避免过载，稍微延迟
            if i % 100 == 0:
                await asyncio.sleep(0.1)
                
        except Exception as e:
            failed_count += 1
            logger.error(f"回填失败 [{news['id']}]: {e}")
    
    elapsed_time = (datetime.now() - start_time).total_seconds()
    
    # 输出总结
    logger.info("\n" + "=" * 80)
    logger.info("📊 回填完成")
    logger.info("=" * 80)
    logger.info(f"处理时间: {elapsed_time:.1f} 秒")
    logger.info(f"处理新闻数: {len(news_list)}")
    logger.info(f"  - 成功: {success_count}")
    logger.info(f"  - 失败: {failed_count}")
    logger.info(f"  - 成功率: {(success_count / len(news_list) * 100):.1f}%")
    logger.info(f"\n提取主题统计:")
    logger.info(f"  - 总主题数: {total_themes}")
    logger.info(f"  - Primary Themes: {total_primary}")
    logger.info(f"  - Sectors: {total_sectors}")
    logger.info(f"  - Others: {total_themes - total_primary - total_sectors}")
    
    if success_count > 0:
        logger.info(f"\n平均每条新闻:")
        logger.info(f"  - 主题数: {total_themes / success_count:.1f}")
        logger.info(f"  - Primary: {total_primary / success_count:.1f}")
        logger.info(f"  - Sectors: {total_sectors / success_count:.1f}")
    
    logger.info("=" * 80)


async def check_status():
    """检查当前状态"""
    logger.info("=" * 80)
    logger.info("检查 LLM 分类主题状态")
    logger.info("=" * 80)
    
    db_config = get_database_config()
    async_db = AsyncDatabaseManager(db_config)
    
    # 统计有 news_classification 的新闻
    query = """
    SELECT COUNT(*) as total
    FROM news
    WHERE analysis_result ? 'news_classification'
      AND analysis_status = 'completed'
    """
    result = await async_db.fetch_one(query)
    total_with_classification = result['total']
    
    # 统计已同步的新闻（有 primary_theme 或 sector）
    query = """
    SELECT COUNT(DISTINCT news_id) as synced
    FROM news_themes_unified
    WHERE source_type = 'llm_classification'
      AND is_active = true
      AND (
          metadata->>'theme_type' = 'primary_theme'
          OR metadata->>'theme_type' = 'sector'
      )
    """
    result = await async_db.fetch_one(query)
    synced_count = result['synced']
    
    # 统计主题类型
    query = """
    SELECT 
        metadata->>'theme_type' as theme_type,
        COUNT(*) as count
    FROM news_themes_unified
    WHERE source_type = 'llm_classification'
      AND is_active = true
    GROUP BY metadata->>'theme_type'
    ORDER BY count DESC
    """
    type_stats = await async_db.fetch_all(query)
    
    logger.info(f"\n📊 当前状态:")
    logger.info(f"  - 有 news_classification 的新闻: {total_with_classification}")
    logger.info(f"  - 已同步新版主题的新闻: {synced_count}")
    logger.info(f"  - 需要回填的新闻: {total_with_classification - synced_count}")
    
    if type_stats:
        logger.info(f"\n主题类型分布:")
        for stat in type_stats:
            theme_type = stat['theme_type'] or 'unknown'
            count = stat['count']
            logger.info(f"  - {theme_type}: {count}")
    
    logger.info("=" * 80)


async def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='回填历史新闻的 LLM 分类主题',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 检查当前状态
  python scripts/backfill_llm_classification_themes.py --status
  
  # 预览前100条需要回填的新闻
  python scripts/backfill_llm_classification_themes.py --limit 100 --dry-run
  
  # 回填最近30天的所有新闻
  python scripts/backfill_llm_classification_themes.py --days 30 --execute
  
  # 回填所有历史新闻（前1000条）
  python scripts/backfill_llm_classification_themes.py --limit 1000 --execute
        """
    )
    
    parser.add_argument('--status', action='store_true', help='检查当前状态')
    parser.add_argument('--limit', type=int, help='限制处理数量')
    parser.add_argument('--days', type=int, help='限制最近多少天的新闻')
    parser.add_argument('--dry-run', action='store_true', help='预览模式，不实际执行')
    parser.add_argument('--execute', action='store_true', help='执行实际回填')
    
    args = parser.parse_args()
    
    if args.status:
        await check_status()
    elif args.execute:
        await backfill_llm_themes(
            limit=args.limit,
            days=args.days,
            dry_run=False
        )
    elif args.dry_run:
        await backfill_llm_themes(
            limit=args.limit,
            days=args.days,
            dry_run=True
        )
    else:
        # 默认：显示状态
        await check_status()
        logger.info("\n使用 --help 查看更多选项")


if __name__ == '__main__':
    asyncio.run(main())

