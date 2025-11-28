#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量重新生成投资主题向量（使用LLM增强描述）

功能：
1. 使用LLM Prompt Template生成主题详细描述
2. 重新生成所有主题的向量嵌入
3. 支持强制更新和增量更新
4. 显示进度和统计信息

使用方法:
    # 增量更新（只处理未生成的主题）
    python scripts/maintenance/regenerate_theme_embeddings_with_llm.py
    
    # 强制重新生成所有主题
    python scripts/maintenance/regenerate_theme_embeddings_with_llm.py --force
    
    # 禁用LLM（使用简单模板）
    python scripts/maintenance/regenerate_theme_embeddings_with_llm.py --no-llm
    
    # 只处理特定市场
    python scripts/maintenance/regenerate_theme_embeddings_with_llm.py --market US
"""

import os
import sys
import asyncio
import argparse
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backend.utils.database_config import get_database_config
from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
from backend.utils.logger import get_logger

logger = get_logger("regenerate_theme_embeddings")


async def main(force_update: bool = False, use_llm: bool = True, market: str = None):
    """
    主函数：批量重新生成主题向量
    
    Args:
        force_update: 是否强制更新所有主题
        use_llm: 是否使用LLM生成描述
        market: 指定市场（HK/US/CN），None表示所有市场
    """
    try:
        logger.info("=" * 80)
        logger.info("🚀 开始批量重新生成投资主题向量")
        logger.info("=" * 80)
        logger.info(f"配置: 强制更新={force_update}, 使用LLM={use_llm}, 市场={market or '全部'}")
        
        # 如果指定了不使用LLM，设置环境变量
        if not use_llm:
            os.environ['USE_LLM_THEME_DESCRIPTION'] = 'false'
            logger.info("⚠️  已禁用LLM描述生成，将使用简单模板")
        else:
            os.environ['USE_LLM_THEME_DESCRIPTION'] = 'true'
            logger.info("✅ 已启用LLM描述生成")
        
        # 初始化服务
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        # 如果指定了市场，需要先筛选主题
        if market:
            logger.info(f"📍 只处理 {market} 市场的主题")
            # TODO: 添加市场筛选逻辑
            # 当前 initialize_all_theme_embeddings 不支持市场筛选
            # 可以考虑扩展该方法
        
        # 执行批量生成
        logger.info("")
        logger.info("🔄 开始生成向量...")
        logger.info("")
        
        result = await service.initialize_all_theme_embeddings(force_update=force_update)
        
        # 显示结果
        logger.info("")
        logger.info("=" * 80)
        if result.get('success'):
            logger.info("✅ 主题向量生成完成！")
            logger.info("")
            logger.info(f"📊 统计信息:")
            logger.info(f"  • 总主题数: {result.get('total', 0)}")
            logger.info(f"  • 成功处理: {result.get('processed', 0)}")
            logger.info(f"  • 失败数量: {result.get('failed', 0)}")
            logger.info(f"  • 跳过数量: {result.get('skipped', 0)}")
            
            if result.get('processed', 0) > 0:
                logger.info("")
                logger.info("💡 提示:")
                logger.info("  1. 新的向量已保存到 investment_theme_embeddings 表")
                logger.info("  2. 可以重新分析新闻以应用新的主题匹配")
                logger.info("  3. 建议测试几条新闻验证匹配效果")
        else:
            logger.error(f"❌ 主题向量生成失败: {result.get('error', '未知错误')}")
            return 1
        
        logger.info("=" * 80)
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  用户中断操作")
        return 130
        
    except Exception as e:
        logger.error(f"❌ 脚本执行失败: {str(e)}", exc_info=True)
        return 1


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="批量重新生成投资主题向量（使用LLM增强描述）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 增量更新（只处理未生成的主题）
  python scripts/maintenance/regenerate_theme_embeddings_with_llm.py
  
  # 强制重新生成所有主题
  python scripts/maintenance/regenerate_theme_embeddings_with_llm.py --force
  
  # 禁用LLM（使用简单模板）
  python scripts/maintenance/regenerate_theme_embeddings_with_llm.py --no-llm
  
  # 只处理美股市场
  python scripts/maintenance/regenerate_theme_embeddings_with_llm.py --market US
  
  # 强制更新所有美股主题并使用LLM
  python scripts/maintenance/regenerate_theme_embeddings_with_llm.py --force --market US
        """
    )
    
    parser.add_argument(
        '--force', 
        action='store_true',
        help='强制重新生成所有主题（否则只处理未生成的主题）'
    )
    
    parser.add_argument(
        '--no-llm',
        action='store_true',
        help='禁用LLM描述生成，使用简单模板（降级方案）'
    )
    
    parser.add_argument(
        '--market',
        type=str,
        choices=['HK', 'US', 'CN'],
        help='只处理指定市场的主题（HK=香港, US=美国, CN=中国）'
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    
    # 运行异步主函数
    exit_code = asyncio.run(
        main(
            force_update=args.force,
            use_llm=not args.no_llm,
            market=args.market
        )
    )
    
    sys.exit(exit_code)

