#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
投资主题匹配诊断脚本
分析向量匹配效果，找出匹配率低的原因
"""

import asyncio
import sys
import os
import json
import numpy as np

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger
from backend.utils.env_loader import load_project_env
from backend.database.db_manager import DatabaseManager

logger = get_logger("diagnose_theme_matching")


async def analyze_similarity_distribution(service: InvestmentThemeEmbeddingService, db_config):
    """分析相似度分布"""
    logger.info("=" * 80)
    logger.info("分析1: 相似度分布")
    logger.info("=" * 80)
    
    db_manager = DatabaseManager(db_config)
    conn = db_manager.get_connection()
    
    if not conn:
        logger.error("数据库连接失败")
        return
    
    try:
        cursor = conn.cursor()
        
        # 获取最近10条已分析的新闻
        cursor.execute("""
            SELECT id, title, content
            FROM news
            WHERE is_analyzed = TRUE
            AND content IS NOT NULL
            ORDER BY analyzed_at DESC
            LIMIT 10
        """)
        
        news_items = cursor.fetchall()
        
        if not news_items:
            logger.warning("未找到已分析的新闻")
            return
        
        logger.info(f"找到 {len(news_items)} 条已分析的新闻用于测试")
        
        # 对每条新闻进行详细的相似度分析
        for news in news_items:
            news_id = news[0]
            title = news[1]
            content = news[2] or ''
            
            # 构建新闻文本
            news_text = title
            if content:
                content_preview = content[:500]
                news_text = f"{title}. {content_preview}"
            
            logger.info(f"\n📰 新闻 #{news_id}: {title[:60]}...")
            
            # 生成新闻向量
            from backend.core.llm_framework import llm_service
            
            if 'qwen' not in llm_service.providers:
                logger.error("Qwen提供商未配置")
                continue
            
            embeddings = await llm_service.providers['qwen'].generate_embeddings(
                texts=[news_text],
                model=service.embedding_model,
                text_type="query"
            )
            
            if not embeddings:
                logger.error("生成向量失败")
                continue
            
            news_vector = embeddings[0]
            
            # 获取所有主题向量并计算相似度
            cursor.execute("""
                SELECT theme_id, theme_name, market, embedding_data
                FROM investment_theme_embeddings
                LIMIT 100
            """)
            
            theme_rows = cursor.fetchall()
            similarities = []
            
            for row in theme_rows:
                theme_id = row[0]
                theme_name = row[1]
                market = row[2]
                embedding_data = row[3]
                
                # 解析向量
                if isinstance(embedding_data, str):
                    theme_vector = json.loads(embedding_data)
                else:
                    theme_vector = embedding_data
                
                # 计算余弦相似度
                similarity = service._cosine_similarity(news_vector, theme_vector)
                
                similarities.append({
                    'theme_id': theme_id,
                    'theme_name': theme_name,
                    'market': market,
                    'similarity': similarity
                })
            
            # 排序
            similarities.sort(key=lambda x: x['similarity'], reverse=True)
            
            # 显示Top 5
            logger.info("  🏆 Top 5 匹配主题:")
            for i, match in enumerate(similarities[:5]):
                emoji = "✅" if match['similarity'] >= 0.6 else "⚠️" if match['similarity'] >= 0.5 else "❌"
                logger.info(f"    {i+1}. {emoji} {match['theme_name'][:40]:40} | 相似度: {match['similarity']:.4f} | 市场: {match['market']}")
            
            # 统计相似度分布
            high_sim = sum(1 for s in similarities if s['similarity'] >= 0.6)
            medium_sim = sum(1 for s in similarities if 0.5 <= s['similarity'] < 0.6)
            low_sim = sum(1 for s in similarities if 0.4 <= s['similarity'] < 0.5)
            very_low_sim = sum(1 for s in similarities if s['similarity'] < 0.4)
            
            logger.info(f"  📊 相似度分布:")
            logger.info(f"    >= 0.6 (高): {high_sim} 个")
            logger.info(f"    0.5-0.6 (中): {medium_sim} 个")
            logger.info(f"    0.4-0.5 (低): {low_sim} 个")
            logger.info(f"    < 0.4 (极低): {very_low_sim} 个")
            
            if high_sim == 0:
                logger.warning(f"  ⚠️ 该新闻没有任何高相似度(>=0.6)的主题匹配！")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"分析失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())


async def check_theme_embeddings(db_config):
    """检查主题向量数据"""
    logger.info("\n" + "=" * 80)
    logger.info("分析2: 主题向量数据检查")
    logger.info("=" * 80)
    
    db_manager = DatabaseManager(db_config)
    conn = db_manager.get_connection()
    
    if not conn:
        logger.error("数据库连接失败")
        return
    
    try:
        cursor = conn.cursor()
        
        # 检查主题向量总数
        cursor.execute("SELECT COUNT(*) FROM investment_theme_embeddings")
        total_themes = cursor.fetchone()[0]
        
        logger.info(f"📊 主题向量总数: {total_themes}")
        
        # 按市场统计
        cursor.execute("""
            SELECT market, COUNT(*) 
            FROM investment_theme_embeddings
            GROUP BY market
            ORDER BY COUNT(*) DESC
        """)
        
        logger.info("📈 市场分布:")
        for row in cursor.fetchall():
            logger.info(f"  {row[0]}: {row[1]} 个主题")
        
        # 检查向量数据质量
        cursor.execute("""
            SELECT theme_id, theme_name, market, embedding_data
            FROM investment_theme_embeddings
            LIMIT 5
        """)
        
        logger.info("\n🔍 样本主题向量:")
        for row in cursor.fetchall():
            theme_id = row[0]
            theme_name = row[1]
            market = row[2]
            embedding_data = row[3]
            
            if isinstance(embedding_data, str):
                vector = json.loads(embedding_data)
            else:
                vector = embedding_data
            
            # 计算向量统计信息
            vector_array = np.array(vector)
            vec_mean = np.mean(vector_array)
            vec_std = np.std(vector_array)
            vec_norm = np.linalg.norm(vector_array)
            
            logger.info(f"  主题: {theme_name[:40]:40} | 市场: {market}")
            logger.info(f"    向量维度: {len(vector)} | 均值: {vec_mean:.6f} | 标准差: {vec_std:.6f} | 范数: {vec_norm:.6f}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"检查失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())


async def test_different_thresholds(service: InvestmentThemeEmbeddingService, db_config):
    """测试不同阈值的匹配效果"""
    logger.info("\n" + "=" * 80)
    logger.info("分析3: 不同相似度阈值的匹配效果")
    logger.info("=" * 80)
    
    db_manager = DatabaseManager(db_config)
    conn = db_manager.get_connection()
    
    if not conn:
        logger.error("数据库连接失败")
        return
    
    try:
        cursor = conn.cursor()
        
        # 获取5条新闻
        cursor.execute("""
            SELECT id, title, content
            FROM news
            WHERE is_analyzed = TRUE
            AND content IS NOT NULL
            ORDER BY analyzed_at DESC
            LIMIT 5
        """)
        
        news_items = cursor.fetchall()
        
        thresholds = [0.3, 0.4, 0.5, 0.6, 0.7]
        
        logger.info(f"测试阈值: {thresholds}")
        logger.info(f"使用 {len(news_items)} 条新闻进行测试\n")
        
        results_by_threshold = {t: 0 for t in thresholds}
        
        for news in news_items:
            news_id = news[0]
            title = news[1]
            content = news[2] or ''
            
            # 构建新闻文本
            news_text = title
            if content:
                content_preview = content[:500]
                news_text = f"{title}. {content_preview}"
            
            logger.info(f"📰 新闻 #{news_id}: {title[:50]}...")
            
            # 测试每个阈值
            for threshold in thresholds:
                matches = await service.match_news_to_themes(
                    news_text=news_text,
                    news_id=None,
                    market=None,
                    top_k=5,
                    min_similarity=threshold,
                    save_to_db=False
                )
                
                match_count = len(matches)
                results_by_threshold[threshold] += match_count
                
                logger.info(f"  阈值 {threshold}: {match_count} 个匹配")
        
        # 汇总结果
        logger.info("\n📊 汇总统计 (总共5条新闻):")
        for threshold in thresholds:
            total_matches = results_by_threshold[threshold]
            avg_matches = total_matches / len(news_items)
            logger.info(f"  阈值 {threshold}: 平均 {avg_matches:.1f} 个匹配/新闻, 总计 {total_matches} 个匹配")
        
        # 推荐阈值
        logger.info("\n💡 推荐:")
        if results_by_threshold[0.6] < 1:
            logger.warning(f"  ⚠️ 当前阈值 0.6 过高，平均只有 {results_by_threshold[0.6]/len(news_items):.1f} 个匹配")
            
            if results_by_threshold[0.5] >= 5:
                logger.info(f"  ✅ 建议降低阈值到 0.5，可获得平均 {results_by_threshold[0.5]/len(news_items):.1f} 个匹配")
            elif results_by_threshold[0.4] >= 5:
                logger.info(f"  ✅ 建议降低阈值到 0.4，可获得平均 {results_by_threshold[0.4]/len(news_items):.1f} 个匹配")
            else:
                logger.warning(f"  ⚠️ 即使降低到0.3，匹配率仍然很低，可能需要优化向量生成策略")
        else:
            logger.info(f"  ✅ 当前阈值 0.6 效果良好")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())


async def analyze_theme_text_quality(db_config):
    """分析主题文本质量"""
    logger.info("\n" + "=" * 80)
    logger.info("分析4: 主题文本质量检查")
    logger.info("=" * 80)
    
    db_manager = DatabaseManager(db_config)
    conn = db_manager.get_connection()
    
    if not conn:
        logger.error("数据库连接失败")
        return
    
    try:
        cursor = conn.cursor()
        
        # 获取主题数据
        cursor.execute("""
            SELECT theme_id, theme_name, market
            FROM investment_theme_embeddings
            LIMIT 20
        """)
        
        logger.info("🔍 主题文本示例 (用于生成向量的文本):")
        for row in cursor.fetchall():
            theme_id = row[0]
            theme_name = row[1]
            market = row[2]
            
            # 这是当前生成向量时使用的文本格式
            text = f"{theme_name} ({market} market)"
            
            logger.info(f"  '{text}'")
        
        logger.info("\n💡 观察:")
        logger.info("  - 当前主题文本非常简短，只有'主题名称 (市场)'")
        logger.info("  - 缺少主题的详细描述和上下文信息")
        logger.info("  - 这可能导致向量表示不够丰富，降低匹配准确性")
        
        logger.info("\n✅ 建议优化方向:")
        logger.info("  1. 从 futu_investment_themes 表获取更多主题信息（如股票列表）")
        logger.info("  2. 构建更丰富的主题描述文本，例如：")
        logger.info("     '主题名称: Electric Vehicles, 市场: US, 相关股票: TSLA, NIO, RIVN'")
        logger.info("  3. 或者使用主题相关的新闻标题作为扩展")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"分析失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())


async def main():
    """主函数"""
    logger.info("=" * 80)
    logger.info("投资主题向量匹配诊断工具")
    logger.info("=" * 80)
    
    try:
        # 1. 加载环境变量
        logger.info("加载环境配置...")
        load_project_env()
        
        # 2. 获取数据库配置
        logger.info("连接数据库...")
        db_config = get_database_config()
        
        # 3. 创建服务实例
        logger.info("初始化向量化服务...\n")
        service = InvestmentThemeEmbeddingService(db_config)
        
        # 4. 执行诊断分析
        await check_theme_embeddings(db_config)
        await analyze_theme_text_quality(db_config)
        await test_different_thresholds(service, db_config)
        await analyze_similarity_distribution(service, db_config)
        
        # 5. 总结
        logger.info("\n" + "=" * 80)
        logger.info("诊断完成！")
        logger.info("=" * 80)
        
        return 0
    
    except KeyboardInterrupt:
        logger.warning("\n⚠️ 用户中断执行")
        return 130
    
    except Exception as e:
        logger.error(f"\n❌ 发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

