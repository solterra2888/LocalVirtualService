#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试投资主题描述生成功能

功能：
1. 测试 LLM Prompt Template 是否正确配置
2. 测试生成单个主题的描述
3. 对比 LLM 模式和简单模式的输出

使用方法:
    python scripts/test_theme_description_generation.py
"""

import os
import sys
import asyncio
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backend.utils.database_config import get_database_config
from backend.database.db_manager import DatabaseManager
from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
from backend.core.llm_framework import llm_service
from backend.utils.logger import get_logger

logger = get_logger("test_theme_description")


async def test_prompt_template():
    """测试1: 验证 Prompt Template 是否存在"""
    logger.info("=" * 80)
    logger.info("测试1: 验证 Prompt Template 配置")
    logger.info("=" * 80)
    
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 检查分类是否存在
        cursor.execute("""
            SELECT id, category_code, category_name, description
            FROM llm_prompt_categories
            WHERE category_code = 'theme_analysis'
        """)
        
        category = cursor.fetchone()
        if category:
            logger.info(f"✅ 找到主题分析分类:")
            logger.info(f"  • ID: {category[0]}")
            logger.info(f"  • Code: {category[1]}")
            logger.info(f"  • Name: {category[2]}")
            logger.info(f"  • Description: {category[3]}")
        else:
            logger.error("❌ 未找到主题分析分类，请先运行数据库迁移脚本")
            cursor.close()
            conn.close()
            return False
        
        # 检查模板是否存在
        cursor.execute("""
            SELECT 
                id, template_code, template_name, 
                recommended_provider, recommended_model,
                temperature, max_tokens, is_active
            FROM llm_prompt_templates
            WHERE template_code = 'theme_description_generator_v1'
        """)
        
        template = cursor.fetchone()
        if template:
            logger.info(f"\n✅ 找到主题描述生成模板:")
            logger.info(f"  • ID: {template[0]}")
            logger.info(f"  • Code: {template[1]}")
            logger.info(f"  • Name: {template[2]}")
            logger.info(f"  • Provider: {template[3]}")
            logger.info(f"  • Model: {template[4]}")
            logger.info(f"  • Temperature: {template[5]}")
            logger.info(f"  • Max Tokens: {template[6]}")
            logger.info(f"  • Active: {template[7]}")
        else:
            logger.error("❌ 未找到主题描述生成模板，请先运行数据库迁移脚本")
            cursor.close()
            conn.close()
            return False
        
        cursor.close()
        conn.close()
        logger.info("\n✅ Prompt Template 配置验证通过\n")
        return True
        
    except Exception as e:
        logger.error(f"❌ 验证失败: {str(e)}", exc_info=True)
        return False


async def test_llm_generation():
    """测试2: 测试直接调用 LLM 生成描述"""
    logger.info("=" * 80)
    logger.info("测试2: 测试 LLM 生成主题描述")
    logger.info("=" * 80)
    
    try:
        # 测试示例
        test_case = {
            'theme_name': 'Electric Vehicles',
            'market': 'US',
            'stock_symbols': 'TSLA, NIO, RIVN, LCID, F, GM',
            'company_names': 'Tesla, NIO, Rivian, Lucid Motors, Ford, General Motors'
        }
        
        logger.info(f"📝 测试用例:")
        logger.info(f"  • 主题名: {test_case['theme_name']}")
        logger.info(f"  • 市场: {test_case['market']}")
        logger.info(f"  • 股票: {test_case['stock_symbols']}")
        logger.info(f"  • 公司: {test_case['company_names']}")
        logger.info("")
        
        logger.info("🤖 调用 LLM...")
        
        response = await llm_service.chat_completion_with_template(
            template_code='theme_description_generator_v1',
            variables=test_case
        )
        
        description = response.content.strip()
        
        logger.info("\n✅ LLM 生成成功!")
        logger.info(f"\n📄 生成的描述 ({len(description)} 字符):")
        logger.info("-" * 80)
        logger.info(description)
        logger.info("-" * 80)
        
        # 显示使用统计
        if hasattr(response, 'usage') and response.usage:
            logger.info(f"\n📊 使用统计:")
            logger.info(f"  • Provider: {response.provider if hasattr(response, 'provider') else 'N/A'}")
            logger.info(f"  • Model: {response.model if hasattr(response, 'model') else 'N/A'}")
            logger.info(f"  • Tokens: {response.usage.get('total_tokens', 'N/A')}")
            logger.info(f"  • Cost: ~${response.usage.get('total_tokens', 0) * 0.000001:.6f}")
        
        logger.info("\n✅ LLM 生成测试通过\n")
        return True
        
    except Exception as e:
        logger.error(f"❌ LLM 生成失败: {str(e)}", exc_info=True)
        return False


async def test_service_integration():
    """测试3: 测试服务集成（对比 LLM 模式和简单模式）"""
    logger.info("=" * 80)
    logger.info("测试3: 测试服务集成（对比两种模式）")
    logger.info("=" * 80)
    
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 获取一个真实的主题进行测试
        cursor.execute("""
            SELECT DISTINCT theme_id, theme_name, market, id
            FROM futu_investment_themes
            WHERE market = 'US'
            LIMIT 1
        """)
        
        theme = cursor.fetchone()
        if not theme:
            logger.warning("⚠️  未找到测试主题，跳过此测试")
            cursor.close()
            conn.close()
            return True
        
        theme_id, theme_name, market, db_theme_id = theme
        
        logger.info(f"📝 使用真实主题进行测试:")
        logger.info(f"  • Theme ID: {theme_id}")
        logger.info(f"  • Theme Name: {theme_name}")
        logger.info(f"  • Market: {market}")
        logger.info("")
        
        # 测试 LLM 模式
        logger.info("🤖 模式1: LLM 增强描述")
        logger.info("-" * 80)
        os.environ['USE_LLM_THEME_DESCRIPTION'] = 'true'
        service_llm = InvestmentThemeEmbeddingService(db_config)
        
        description_llm = await service_llm._generate_enriched_theme_text(
            cursor, theme_id, theme_name, market, db_theme_id
        )
        
        logger.info(f"LLM 模式输出 ({len(description_llm)} 字符):")
        logger.info(description_llm)
        logger.info("")
        
        # 测试简单模式
        logger.info("🔧 模式2: 简单模板")
        logger.info("-" * 80)
        os.environ['USE_LLM_THEME_DESCRIPTION'] = 'false'
        service_simple = InvestmentThemeEmbeddingService(db_config)
        
        description_simple = await service_simple._generate_enriched_theme_text(
            cursor, theme_id, theme_name, market, db_theme_id
        )
        
        logger.info(f"简单模式输出 ({len(description_simple)} 字符):")
        logger.info(description_simple)
        logger.info("")
        
        # 对比
        logger.info("📊 对比分析:")
        logger.info(f"  • LLM 模式字符数: {len(description_llm)}")
        logger.info(f"  • 简单模式字符数: {len(description_simple)}")
        logger.info(f"  • 详细度提升: {len(description_llm) / len(description_simple):.1f}x")
        
        cursor.close()
        conn.close()
        
        logger.info("\n✅ 服务集成测试通过\n")
        return True
        
    except Exception as e:
        logger.error(f"❌ 服务集成测试失败: {str(e)}", exc_info=True)
        return False


async def main():
    """主函数：运行所有测试"""
    logger.info("\n" + "=" * 80)
    logger.info("🧪 投资主题描述生成功能测试")
    logger.info("=" * 80 + "\n")
    
    results = []
    
    # 运行测试
    results.append(("Prompt Template 配置", await test_prompt_template()))
    results.append(("LLM 生成测试", await test_llm_generation()))
    results.append(("服务集成测试", await test_service_integration()))
    
    # 汇总结果
    logger.info("=" * 80)
    logger.info("📋 测试结果汇总")
    logger.info("=" * 80)
    
    all_passed = True
    for test_name, passed in results:
        status = "✅ 通过" if passed else "❌ 失败"
        logger.info(f"{status} - {test_name}")
        if not passed:
            all_passed = False
    
    logger.info("=" * 80)
    
    if all_passed:
        logger.info("\n🎉 所有测试通过！\n")
        logger.info("💡 下一步:")
        logger.info("  1. 运行: python scripts/regenerate_theme_embeddings_with_llm.py --force")
        logger.info("  2. 重新生成所有主题的向量嵌入")
        logger.info("  3. 测试新闻主题匹配效果\n")
        return 0
    else:
        logger.error("\n❌ 部分测试失败，请检查日志\n")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

