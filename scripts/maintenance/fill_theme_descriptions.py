#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量填充投资主题 description 字段

功能：
1. 扫描 futu_investment_themes 表中 description 为空的主题
2. 使用 LLM 生成详细描述
3. 保存到数据库缓存
4. 显示进度和统计

使用方法:
    # 只填充缺失的描述
    python scripts/maintenance/fill_theme_descriptions.py
    
    # 强制重新生成所有描述
    python scripts/maintenance/fill_theme_descriptions.py --force
    
    # 只处理特定市场
    python scripts/maintenance/fill_theme_descriptions.py --market US
    
    # 禁用 LLM（使用简单模板）
    python scripts/maintenance/fill_theme_descriptions.py --no-llm
"""

import os
import sys
import asyncio
import argparse
from pathlib import Path
from datetime import datetime

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backend.utils.database_config import get_database_config
from backend.database.db_manager import DatabaseManager
from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
from backend.utils.logger import get_logger

logger = get_logger("fill_theme_descriptions")


async def fill_descriptions(
    force_update: bool = False,
    use_llm: bool = True,
    market: str = None,
    limit: int = None
):
    """
    批量填充主题描述
    
    Args:
        force_update: 是否强制更新已有描述
        use_llm: 是否使用 LLM 生成
        market: 指定市场（HK/US/CN）
        limit: 限制处理数量
    """
    try:
        logger.info("=" * 80)
        logger.info("🚀 开始批量填充投资主题描述")
        logger.info("=" * 80)
        logger.info(f"配置:")
        logger.info(f"  • 强制更新: {force_update}")
        logger.info(f"  • 使用LLM: {use_llm}")
        logger.info(f"  • 市场筛选: {market or '全部'}")
        logger.info(f"  • 处理限制: {limit or '无限制'}")
        logger.info("")
        
        # 设置环境变量
        if not use_llm:
            os.environ['USE_LLM_THEME_DESCRIPTION'] = 'false'
            logger.info("⚠️  已禁用LLM，将使用简单模板")
        else:
            os.environ['USE_LLM_THEME_DESCRIPTION'] = 'true'
            logger.info("✅ 已启用LLM描述生成")
        
        # 初始化数据库
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        service = InvestmentThemeEmbeddingService(db_config)
        
        conn = db_manager.get_connection()
        if not conn:
            logger.error("❌ 数据库连接失败")
            return 1
        
        cursor = conn.cursor()
        
        # 步骤1: 查询需要填充描述的主题
        logger.info("📊 查询需要处理的主题...")
        logger.info("")
        
        market_filter = ""
        params = []
        
        if force_update:
            # 强制更新所有主题
            if market:
                market_filter = "WHERE market = %s"
                params.append(market)
        else:
            # 只更新缺失描述的主题
            if market:
                market_filter = "WHERE description IS NULL AND market = %s"
                params.append(market)
            else:
                market_filter = "WHERE description IS NULL"
        
        query = f"""
            SELECT id, theme_id, theme_name, market, description, description_source
            FROM futu_investment_themes
            {market_filter}
            ORDER BY market, theme_name
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query, params)
        themes_to_process = cursor.fetchall()
        
        total_themes = len(themes_to_process)
        
        if total_themes == 0:
            logger.info("✅ 所有主题的描述都已填充，无需处理")
            logger.info("")
            cursor.close()
            conn.close()
            return 0
        
        logger.info(f"找到 {total_themes} 个需要处理的主题")
        logger.info("")
        
        # 步骤2: 批量处理
        logger.info("🔄 开始填充描述...")
        logger.info("")
        
        success_count = 0
        llm_generated_count = 0
        simple_template_count = 0
        failed_count = 0
        
        start_time = datetime.now()
        
        for idx, theme_row in enumerate(themes_to_process, 1):
            db_id, theme_id, theme_name, theme_market, existing_desc, existing_source = theme_row
            
            try:
                # 调用服务生成描述（会自动保存到数据库）
                description = await service._generate_enriched_theme_text(
                    cursor=cursor,
                    theme_id=theme_id,
                    theme_name=theme_name,
                    market=theme_market,
                    db_theme_id=db_id,
                    force_regenerate=force_update
                )
                
                # 查询保存后的 description_source
                cursor.execute("""
                    SELECT description_source 
                    FROM futu_investment_themes 
                    WHERE id = %s
                """, (db_id,))
                
                result = cursor.fetchone()
                source = result[0] if result else 'unknown'
                
                if description:
                    success_count += 1
                    if source == 'llm_generated':
                        llm_generated_count += 1
                    elif source == 'simple_template':
                        simple_template_count += 1
                    
                    # 显示进度
                    if idx % 10 == 0 or idx == total_themes:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        avg_time = elapsed / idx if idx > 0 else 0
                        eta = avg_time * (total_themes - idx)
                        
                        logger.info(f"进度: {idx}/{total_themes} ({idx*100//total_themes}%) | "
                                  f"成功: {success_count} | "
                                  f"失败: {failed_count} | "
                                  f"预计剩余: {int(eta)}秒")
                else:
                    failed_count += 1
                    logger.warning(f"  ⚠️  生成失败: {theme_name}")
                
            except Exception as e:
                failed_count += 1
                logger.error(f"  ❌ 处理失败: {theme_name} - {str(e)}")
        
        cursor.close()
        conn.close()
        
        # 步骤3: 显示统计
        elapsed_total = (datetime.now() - start_time).total_seconds()
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ 描述填充完成！")
        logger.info("=" * 80)
        logger.info(f"📊 统计信息:")
        logger.info(f"  • 总处理数: {total_themes}")
        logger.info(f"  • 成功填充: {success_count}")
        logger.info(f"  • LLM生成: {llm_generated_count}")
        logger.info(f"  • 简单模板: {simple_template_count}")
        logger.info(f"  • 失败数量: {failed_count}")
        logger.info(f"  • 总耗时: {elapsed_total:.1f}秒")
        logger.info(f"  • 平均耗时: {elapsed_total/total_themes:.2f}秒/主题")
        logger.info("")
        
        if success_count > 0:
            logger.info("💡 下一步:")
            logger.info("  1. 运行: python scripts/maintenance/regenerate_theme_embeddings_with_llm.py")
            logger.info("  2. 重新生成主题向量（使用新的描述）")
            logger.info("  3. 测试新闻主题匹配效果")
        
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
        description="批量填充投资主题 description 字段",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 只填充缺失的描述（增量更新）
  python scripts/maintenance/fill_theme_descriptions.py
  
  # 强制重新生成所有描述
  python scripts/maintenance/fill_theme_descriptions.py --force
  
  # 只处理美股市场
  python scripts/maintenance/fill_theme_descriptions.py --market US
  
  # 禁用LLM（使用简单模板）
  python scripts/maintenance/fill_theme_descriptions.py --no-llm
  
  # 限制处理数量（用于测试）
  python scripts/maintenance/fill_theme_descriptions.py --limit 10
        """
    )
    
    parser.add_argument(
        '--force', 
        action='store_true',
        help='强制重新生成所有描述（否则只处理缺失的）'
    )
    
    parser.add_argument(
        '--no-llm',
        action='store_true',
        help='禁用LLM，使用简单模板'
    )
    
    parser.add_argument(
        '--market',
        type=str,
        choices=['HK', 'US', 'CN'],
        help='只处理指定市场的主题'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='限制处理数量（用于测试）'
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    
    exit_code = asyncio.run(
        fill_descriptions(
            force_update=args.force,
            use_llm=not args.no_llm,
            market=args.market,
            limit=args.limit
        )
    )
    
    sys.exit(exit_code)

