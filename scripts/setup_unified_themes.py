#!/usr/bin/env python
"""
统一主题系统初始化脚本
快速部署和测试统一主题系统
"""
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("setup_unified_themes")


def check_tables_exist(db: DatabaseManager) -> bool:
    """检查必要的表是否存在"""
    try:
        tables_to_check = [
            'theme_master_data',
            'news_themes_unified',
            'theme_aggregation_stats'
        ]
        
        with db.get_connection_context() as conn:
            cursor = conn.cursor()
            
            for table in tables_to_check:
                query = f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                      AND table_name = '{table}'
                ) as exists
                """
                cursor.execute(query)
                result = cursor.fetchone()
                
                if not result or not result[0]:
                    logger.error(f"❌ 表 '{table}' 不存在")
                    return False
                else:
                    logger.info(f"✅ 表 '{table}' 存在")
            
            return True
        
    except Exception as e:
        logger.error(f"检查表失败: {e}")
        return False


def check_views_exist(db: DatabaseManager) -> bool:
    """检查视图是否存在"""
    try:
        views_to_check = [
            'v_news_best_themes',
            'v_news_themes_full',
            'v_theme_trending'
        ]
        
        with db.get_connection_context() as conn:
            cursor = conn.cursor()
            
            for view in views_to_check:
                query = f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.views 
                    WHERE table_schema = 'public' 
                      AND table_name = '{view}'
                ) as exists
                """
                cursor.execute(query)
                result = cursor.fetchone()
                
                if not result or not result[0]:
                    logger.warning(f"⚠️  视图 '{view}' 不存在")
                    return False
                else:
                    logger.info(f"✅ 视图 '{view}' 存在")
            
            return True
        
    except Exception as e:
        logger.error(f"检查视图失败: {e}")
        return False


def check_system_themes(db: DatabaseManager) -> int:
    """检查系统预定义主题数量"""
    try:
        with db.get_connection_context() as conn:
            cursor = conn.cursor()
            query = "SELECT COUNT(*) as count FROM theme_master_data WHERE is_system = true"
            cursor.execute(query)
            result = cursor.fetchone()
            count = result[0] if result else 0
            
            if count > 0:
                logger.info(f"✅ 系统预定义主题: {count} 个")
            else:
                logger.warning("⚠️  没有系统预定义主题")
            
            return count
        
    except Exception as e:
        logger.error(f"检查系统主题失败: {e}")
        return 0


def test_database_connection(db: DatabaseManager):
    """测试数据库连接和基本查询"""
    logger.info("\n🔍 测试数据库连接和统一主题系统...")
    
    try:
        with db.get_connection_context() as conn:
            cursor = conn.cursor()
            
            # 测试统一主题表
            cursor.execute("SELECT COUNT(*) FROM news_themes_unified WHERE is_active = true")
            unified_count = cursor.fetchone()[0]
            logger.info(f"  统一主题记录: {unified_count}")
            
            # 测试新闻覆盖
            cursor.execute("SELECT COUNT(DISTINCT news_id) FROM news_themes_unified WHERE is_active = true")
            news_count = cursor.fetchone()[0]
            logger.info(f"  覆盖新闻数: {news_count}")
            
            # 测试来源分布
            cursor.execute("""
                SELECT 
                    source_type,
                    COUNT(*) as count
                FROM news_themes_unified
                WHERE is_active = true
                GROUP BY source_type
                ORDER BY count DESC
            """)
            logger.info(f"  来源分布:")
            for row in cursor.fetchall():
                logger.info(f"    - {row[0]}: {row[1]}")
                
    except Exception as e:
        logger.warning(f"⚠️  测试查询失败: {e}")


def main():
    """主函数"""
    print("\n" + "="*60)
    print("     统一主题系统 - 初始化和测试")
    print("="*60 + "\n")
    
    try:
        # 初始化数据库连接
        logger.info("🔌 连接数据库...")
        config = get_database_config()
        db = DatabaseManager(config)
        
        # 检查表结构
        logger.info("\n📋 检查数据库结构...")
        tables_ok = check_tables_exist(db)
        
        if not tables_ok:
            logger.error("\n❌ 数据库表不存在，请先执行迁移SQL:")
            logger.error("   psql -U postgres -d rc_database -h 111.230.48.119")
            logger.error("   \\i backend/database/migrations/create_unified_theme_system.sql")
            return
        
        views_ok = check_views_exist(db)
        theme_count = check_system_themes(db)
        
        # 测试数据库连接和查询
        test_database_connection(db)
        
        print("\n" + "="*60)
        print("✅ 初始化和测试完成！")
        print("="*60)
        print("\n📚 下一步:")
        print("  1. 查看文档: docs/UNIFIED_THEME_SYSTEM_GUIDE.md")
        print("  2. 访问API: http://localhost:8000/api/docs")
        print("  3. 同步数据: POST /api/themes-unified/sync/recent?days=30")
        print("  4. 查看热度: GET /api/themes-unified/trending")
        print()
        print("💡 提示:")
        print("  - 数据库表和视图已创建")
        print("  - 可以通过API进行数据同步和查询")
        print("  - 详细使用方法请参考文档")
        print()
        
    except Exception as e:
        logger.error(f"\n❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()


