#!/bin/bash
# 执行用户新闻筛选预设的数据库迁移

echo "🚀 开始执行用户新闻筛选预设数据库迁移..."

# 数据库连接信息
DB_HOST="111.230.48.119"
DB_PORT="5432"
DB_NAME="rc_database"
DB_USER="postgres"
DB_PASSWORD="1q23l,Sc45+@!"

# 迁移文件路径
MIGRATION_FILE="../backend/database/migrations/create_user_news_filter_presets.sql"

# 检查迁移文件是否存在
if [ ! -f "$MIGRATION_FILE" ]; then
    echo "❌ 错误: 迁移文件不存在: $MIGRATION_FILE"
    exit 1
fi

echo "📄 迁移文件: $MIGRATION_FILE"
echo "🗄️  数据库: $DB_NAME @ $DB_HOST:$DB_PORT"
echo ""

# 执行迁移
PGPASSWORD="$DB_PASSWORD" psql \
    -h "$DB_HOST" \
    -p "$DB_PORT" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -f "$MIGRATION_FILE"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ 迁移执行成功！"
    echo ""
    echo "📊 验证表结构:"
    PGPASSWORD="$DB_PASSWORD" psql \
        -h "$DB_HOST" \
        -p "$DB_PORT" \
        -U "$DB_USER" \
        -d "$DB_NAME" \
        -c "\d user_news_filter_presets"
    
    echo ""
    echo "📈 查看表统计:"
    PGPASSWORD="$DB_PASSWORD" psql \
        -h "$DB_HOST" \
        -p "$DB_PORT" \
        -U "$DB_USER" \
        -d "$DB_NAME" \
        -c "SELECT COUNT(*) as total_presets FROM user_news_filter_presets;"
else
    echo ""
    echo "❌ 迁移执行失败，请检查错误信息"
    exit 1
fi

echo ""
echo "🎉 用户新闻筛选预设功能已就绪！"
echo ""
echo "📖 下一步:"
echo "   1. 重启后端服务以加载新的 API"
echo "   2. 在前端 FinvizDashboard 中集成 NewsFilterPresets 组件"
echo "   3. 参考 IMPLEMENTATION_SUMMARY_NEWS_PRESETS.md 文档"


