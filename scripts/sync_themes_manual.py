#!/usr/bin/env python
"""
手动同步主题数据脚本
从各个源（EODHD、Vector、LLM）同步主题数据到统一表
"""
import sys
import asyncio
from pathlib import Path
from datetime import datetime, timedelta

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backend.database.async_db_manager import AsyncDatabaseManager
from backend.services.theme_unification_service import ThemeUnificationService
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("sync_themes_manual")


async def main():
    """主函数"""
    print("\n" + "="*60)
    print("  手动同步主题数据")
    print("="*60)
    
    try:
        # 初始化数据库和服务
        config = get_database_config()
        db = AsyncDatabaseManager(config)
        service = ThemeUnificationService(db)
        
        # 确定同步时间范围（默认30天）
        days = int(sys.argv[1]) if len(sys.argv) > 1 else 30
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        print(f"\n📅 同步时间范围: {start_date} 至 {end_date} ({days}天)")
        print("="*60)
        
        # 使用通用同步方法
        print("\n🔄 开始同步所有源...")
        result = await service.sync_all_sources(start_date, end_date)
        
        # 从details中获取详细信息
        eodhd_count = result.details.get('eodhd_auto_tag', 0)
        vector_count = result.details.get('vector_retrieval', 0)
        llm_count = result.details.get('llm_classification', 0)
        
        print(f"\n📊 同步结果:")
        print(f"  🏷️  EODHD标签: {eodhd_count} 条")
        print(f"  🎯 Vector匹配: {vector_count} 条")
        print(f"  🤖 LLM分类: {llm_count} 条")
        print(f"  总计: {result.synced_count} 条成功")
        
        if result.failed_count > 0:
            print(f"  ⚠️  失败: {result.failed_count} 条")
        
        if result.errors:
            print("\n⚠️  错误列表:")
            for error in result.errors[:5]:  # 最多显示5个错误
                print(f"    - {error}")
        
        # 验证同步结果
        print("\n🔍 验证同步结果...")
        verify_query = """
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT news_id) as unique_news,
            COUNT(DISTINCT theme_name) as unique_themes
        FROM news_themes_unified
        """
        verify_result = await db.fetch_one(verify_query)
        
        if verify_result:
            print(f"  统一表记录数: {verify_result['total']}")
            print(f"  覆盖新闻数: {verify_result['unique_news']}")
            print(f"  不同主题数: {verify_result['unique_themes']}")
        
        print("\n" + "="*60)
        print("✅ 同步完成！")
        print("="*60)
        print("\n💡 后续操作:")
        print("  1. 检查trending主题: curl 'http://localhost:8000/api/themes-unified/trending'")
        print("  2. 查看特定新闻的主题: curl 'http://localhost:8000/api/themes-unified/news/<news_id>/fused-themes'")
        print("  3. 运行诊断: python scripts/diagnose_unified_themes.py")
        
    except Exception as e:
        logger.error(f"同步失败: {e}", exc_info=True)
        print(f"\n❌ 同步失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

