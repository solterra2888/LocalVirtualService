#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速测试不同相似度阈值的匹配效果
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger
from backend.utils.env_loader import load_project_env
from backend.database.db_manager import DatabaseManager

logger = get_logger("test_similarity_threshold")


async def test_threshold(threshold: float, num_news: int = 10):
    """
    测试指定阈值的匹配效果
    
    Args:
        threshold: 相似度阈值
        num_news: 测试新闻数量
    """
    try:
        load_project_env()
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 获取最近分析的新闻
        cursor.execute("""
            SELECT id, title, content
            FROM news
            WHERE is_analyzed = TRUE
            AND content IS NOT NULL
            ORDER BY analyzed_at DESC
            LIMIT %s
        """, (num_news,))
        
        news_items = cursor.fetchall()
        
        if not news_items:
            logger.warning("未找到已分析的新闻")
            return None
        
        # 统计数据
        total_matches = 0
        news_with_matches = 0
        similarities = []
        
        for news in news_items:
            news_id = news[0]
            title = news[1]
            content = news[2] or ''
            
            # 构建新闻文本
            news_text = title
            if content:
                content_preview = content[:500]
                news_text = f"{title}. {content_preview}"
            
            # 执行匹配
            matches = await service.match_news_to_themes(
                news_text=news_text,
                news_id=None,
                market=None,
                top_k=10,
                min_similarity=threshold,
                save_to_db=False
            )
            
            if matches:
                news_with_matches += 1
                total_matches += len(matches)
                # 记录Top1相似度
                similarities.append(matches[0]['similarity'])
        
        cursor.close()
        conn.close()
        
        # 计算统计数据
        match_rate = (news_with_matches / len(news_items)) * 100
        avg_matches = total_matches / len(news_items) if len(news_items) > 0 else 0
        avg_similarity = sum(similarities) / len(similarities) if similarities else 0
        max_similarity = max(similarities) if similarities else 0
        
        return {
            'threshold': threshold,
            'total_news': len(news_items),
            'news_with_matches': news_with_matches,
            'match_rate': match_rate,
            'total_matches': total_matches,
            'avg_matches_per_news': avg_matches,
            'avg_top1_similarity': avg_similarity,
            'max_similarity': max_similarity
        }
        
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None


async def main():
    """主函数"""
    logger.info("=" * 80)
    logger.info("🧪 相似度阈值快速测试工具")
    logger.info("=" * 80)
    
    # 要测试的阈值列表
    thresholds = [0.35, 0.40, 0.45, 0.50, 0.55]
    num_news = 20  # 测试20条新闻
    
    logger.info(f"\n📊 测试配置:")
    logger.info(f"  - 测试阈值: {thresholds}")
    logger.info(f"  - 测试新闻数: {num_news}")
    logger.info("")
    
    results = []
    
    for threshold in thresholds:
        logger.info(f"⏳ 测试阈值 {threshold}...")
        result = await test_threshold(threshold, num_news)
        
        if result:
            results.append(result)
            logger.info(f"  ✅ 匹配率: {result['match_rate']:.1f}% | "
                       f"平均匹配数: {result['avg_matches_per_news']:.1f} | "
                       f"Top1相似度: {result['avg_top1_similarity']:.3f}")
        else:
            logger.error(f"  ❌ 测试失败")
    
    # 输出汇总表格
    logger.info("\n" + "=" * 80)
    logger.info("📊 测试结果汇总")
    logger.info("=" * 80)
    logger.info(f"{'阈值':<10}{'匹配率':<12}{'平均匹配数':<15}{'Top1相似度':<15}{'最高相似度':<15}")
    logger.info("-" * 80)
    
    for r in results:
        logger.info(
            f"{r['threshold']:<10.2f}"
            f"{r['match_rate']:<12.1f}%"
            f"{r['avg_matches_per_news']:<15.2f}"
            f"{r['avg_top1_similarity']:<15.3f}"
            f"{r['max_similarity']:<15.3f}"
        )
    
    # 推荐最佳阈值
    logger.info("\n" + "=" * 80)
    logger.info("💡 推荐阈值")
    logger.info("=" * 80)
    
    # 找到匹配率在60-80%的最高阈值
    best_threshold = None
    for r in sorted(results, key=lambda x: x['threshold'], reverse=True):
        if 60 <= r['match_rate'] <= 85:
            best_threshold = r
            break
    
    if best_threshold:
        logger.info(f"✅ 推荐阈值: {best_threshold['threshold']}")
        logger.info(f"   - 匹配率: {best_threshold['match_rate']:.1f}%")
        logger.info(f"   - 平均匹配数: {best_threshold['avg_matches_per_news']:.1f}个/新闻")
        logger.info(f"   - Top1相似度: {best_threshold['avg_top1_similarity']:.3f}")
    else:
        logger.warning("⚠️ 未找到理想阈值，请考虑：")
        logger.warning("  1. 扩充主题库（增加更多投资主题）")
        logger.warning("  2. 降低阈值到0.35-0.40")
        logger.warning("  3. 优化主题向量质量")
    
    logger.info("\n" + "=" * 80)
    logger.info("测试完成！")
    logger.info("=" * 80)
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

