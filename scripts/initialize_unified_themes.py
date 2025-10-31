#!/usr/bin/env python
"""
初始化统一主题系统
1. 检查并运行必要的数据库迁移
2. 初始化主题主数据表
3. 同步已存在的新闻主题
"""
import sys
import asyncio
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backend.database.async_db_manager import AsyncDatabaseManager
from backend.database.db_manager import DatabaseManager
from backend.services.theme_unification_service import ThemeUnificationService
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("initialize_unified_themes")


async def check_tables_exist(db: AsyncDatabaseManager) -> dict:
    """检查统一主题系统的表是否存在"""
    tables_to_check = [
        'theme_master_data',
        'news_themes_unified',
        'theme_aggregation_stats'
    ]
    
    results = {}
    for table_name in tables_to_check:
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        )
        """
        result = await db.fetch_one(query, (table_name,))
        results[table_name] = result['exists'] if result else False
    
    return results


async def get_table_counts(db: AsyncDatabaseManager) -> dict:
    """获取各表的记录数"""
    counts = {}
    
    tables = ['theme_master_data', 'news_themes_unified', 'theme_aggregation_stats']
    for table in tables:
        try:
            query = f"SELECT COUNT(*) as count FROM {table}"
            result = await db.fetch_one(query)
            counts[table] = result['count'] if result else 0
        except Exception as e:
            counts[table] = f"Error: {str(e)}"
    
    return counts


async def run_migration(db_manager: DatabaseManager) -> bool:
    """运行统一主题系统的迁移脚本"""
    try:
        migration_file = project_root / 'backend' / 'database' / 'migrations' / 'create_unified_theme_system.sql'
        
        if not migration_file.exists():
            logger.error(f"迁移文件不存在: {migration_file}")
            return False
        
        logger.info(f"读取迁移脚本: {migration_file}")
        with open(migration_file, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        logger.info("执行迁移脚本...")
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 执行SQL脚本
        cursor.execute(sql_script)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        logger.info("✅ 迁移脚本执行成功")
        return True
        
    except Exception as e:
        logger.error(f"迁移脚本执行失败: {e}", exc_info=True)
        return False


async def sync_existing_news(service: ThemeUnificationService, days: int = 30) -> dict:
    """同步最近N天已分析的新闻"""
    try:
        logger.info(f"开始同步最近 {days} 天的新闻主题...")
        result = await service.sync_recent_news(days=days, force_resync=False)
        return {
            'success': result.success,
            'synced_count': result.synced_count,
            'failed_count': result.failed_count,
            'details': result.details
        }
    except Exception as e:
        logger.error(f"同步失败: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}


async def main():
    """主函数"""
    print("\n" + "="*70)
    print("  📦 初始化统一主题系统")
    print("="*70)
    
    config = get_database_config()
    
    # 步骤1: 检查表是否存在
    print("\n🔍 步骤1: 检查数据库表...")
    async_db = AsyncDatabaseManager(config)
    table_status = await check_tables_exist(async_db)
    
    all_tables_exist = all(table_status.values())
    
    for table_name, exists in table_status.items():
        status = "✅" if exists else "❌"
        print(f"  {status} {table_name}: {'存在' if exists else '不存在'}")
    
    # 步骤2: 如果表不存在，运行迁移
    if not all_tables_exist:
        print("\n⚠️  检测到缺少表，开始运行迁移脚本...")
        sync_db = DatabaseManager(config)
        
        if run_migration(sync_db):
            print("✅ 迁移脚本执行成功")
            
            # 重新检查
            table_status = await check_tables_exist(async_db)
            all_tables_exist = all(table_status.values())
            
            if all_tables_exist:
                print("✅ 所有表已创建")
            else:
                print("❌ 部分表创建失败")
                for table_name, exists in table_status.items():
                    if not exists:
                        print(f"  ❌ {table_name} 仍不存在")
                sys.exit(1)
        else:
            print("❌ 迁移脚本执行失败")
            sys.exit(1)
    else:
        print("✅ 所有表已存在")
    
    # 步骤3: 检查表中的数据
    print("\n📊 步骤2: 检查表中的数据...")
    counts = await get_table_counts(async_db)
    
    for table_name, count in counts.items():
        if isinstance(count, int):
            status = "✅" if count > 0 else "⚠️ "
            print(f"  {status} {table_name}: {count} 条记录")
        else:
            print(f"  ❌ {table_name}: {count}")
    
    # 步骤4: 检查theme_master_data是否有预定义主题
    master_count = counts.get('theme_master_data', 0)
    if isinstance(master_count, int) and master_count == 0:
        print("\n⚠️  theme_master_data 表为空")
        print("💡 提示: 迁移脚本应该会插入预定义主题，请检查迁移脚本是否完整执行")
    
    # 步骤5: 同步已存在的新闻主题
    unified_count = counts.get('news_themes_unified', 0)
    
    if isinstance(unified_count, int) and unified_count == 0:
        print("\n🔄 步骤3: 同步已存在的新闻主题...")
        
        # 询问用户是否要同步
        response = input("\n是否要同步最近30天的新闻主题? (y/n): ")
        
        if response.lower() == 'y':
            service = ThemeUnificationService(async_db)
            sync_result = await sync_existing_news(service, days=30)
            
            if sync_result.get('success'):
                print(f"\n✅ 同步完成!")
                print(f"  📊 成功: {sync_result.get('synced_count', 0)} 条新闻")
                print(f"  ❌ 失败: {sync_result.get('failed_count', 0)} 条新闻")
                
                details = sync_result.get('details', {})
                if details:
                    print(f"\n  详细统计:")
                    print(f"    🏷️  EODHD标签: {details.get('eodhd_auto_tag', 0)} 条")
                    print(f"    🎯 Vector匹配: {details.get('vector_retrieval', 0)} 条")
                    print(f"    🤖 LLM分类: {details.get('llm_classification', 0)} 条")
            else:
                print(f"\n❌ 同步失败: {sync_result.get('error', '未知错误')}")
        else:
            print("跳过同步")
    else:
        print("\n✅ news_themes_unified 表已有数据，跳过自动同步")
        print("💡 如需重新同步，请运行: python scripts/sync_themes_manual.py")
    
    # 步骤6: 显示最终状态
    print("\n📈 步骤4: 验证最终状态...")
    final_counts = await get_table_counts(async_db)
    
    print("\n最终统计:")
    for table_name, count in final_counts.items():
        if isinstance(count, int):
            print(f"  📊 {table_name}: {count} 条记录")
    
    print("\n" + "="*70)
    print("✅ 初始化完成!")
    print("="*70)
    
    print("\n💡 后续操作:")
    print("  1. 测试API: curl 'http://localhost:8000/api/themes-unified/trending'")
    print("  2. 查看特定新闻: curl 'http://localhost:8000/api/themes-unified/news/<news_id>/fused-themes'")
    print("  3. 运行诊断: python scripts/diagnose_unified_themes.py")
    print("  4. 查看流程图: cat UNIFIED_THEME_SYSTEM_FLOW.md")
    print()


if __name__ == "__main__":
    asyncio.run(main())

