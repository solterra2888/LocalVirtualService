#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化投资主题向量脚本
从 futu_investment_themes 表读取所有主题，生成并存储向量到 investment_theme_embeddings 表
"""

import asyncio
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger
from backend.utils.env_loader import load_project_env

logger = get_logger("initialize_theme_embeddings")


async def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("开始初始化投资主题向量")
    logger.info("=" * 60)
    
    try:
        # 1. 加载环境变量
        logger.info("步骤 1/4: 加载环境配置...")
        load_project_env()
        
        # 2. 获取数据库配置
        logger.info("步骤 2/4: 连接数据库...")
        db_config = get_database_config()
        
        # 3. 创建服务实例
        logger.info("步骤 3/4: 初始化向量化服务...")
        service = InvestmentThemeEmbeddingService(db_config)
        
        # 4. 执行初始化
        logger.info("步骤 4/4: 生成主题向量...")
        logger.info("这可能需要几分钟时间，请耐心等待...")
        
        result = await service.initialize_all_theme_embeddings(force_update=False)
        
        # 5. 输出结果
        logger.info("=" * 60)
        logger.info("✅ 初始化完成！")
        logger.info("=" * 60)
        logger.info(f"📊 处理统计:")
        logger.info(f"   - 主题总数: {result.get('total', 0)}")
        logger.info(f"   - 已处理: {result.get('processed', 0)}")
        logger.info(f"   - 跳过: {result.get('skipped', 0)}")
        logger.info(f"   - 失败: {result.get('failed', 0)}")
        
        if result.get('success'):
            logger.info("\n✅ 所有主题向量已准备就绪，可以开始使用向量检索功能！")
            
            # 显示统计信息
            stats = service.get_theme_statistics()
            logger.info("\n📈 主题统计:")
            logger.info(f"   - Embedding模型: {stats.get('embedding_model', 'N/A')}")
            logger.info(f"   - 向量维度: {stats.get('embedding_dimension', 0)}")
            logger.info(f"   - 市场分布:")
            
            for market_stat in stats.get('market_breakdown', []):
                logger.info(f"      • {market_stat['market']}: {market_stat['count']} 个主题")
            
            logger.info("\n🔍 测试查询示例:")
            logger.info("   SELECT theme_name, market FROM investment_theme_embeddings;")
            logger.info("\n🚀 下一步:")
            logger.info("   1. 在新闻分析中启用主题匹配: enable_theme_matching=True")
            logger.info("   2. 查看匹配结果: SELECT * FROM news_theme_matches LIMIT 10;")
            
            return 0
        else:
            logger.error(f"\n❌ 初始化失败: {result.get('error', 'Unknown error')}")
            return 1
    
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

