#!/bin/bash

# ===================================================================
# 执行性能优化索引迁移
# ===================================================================

# 数据库连接参数
DB_HOST="${DB_HOST:-111.230.48.119}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-rc_database}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-1q23l,Sc45+@!}"

# 脚本路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="$SCRIPT_DIR/add_performance_indexes.sql"

echo "=============================================="
echo "  Performance Indexes Migration"
echo "=============================================="
echo ""
echo "Database: $DB_HOST:$DB_PORT/$DB_NAME"
echo "User: $DB_USER"
echo "SQL File: $SQL_FILE"
echo ""
echo "This will create performance indexes on:"
echo "  - subscription_content table"
echo "  - subscription_list table"
echo "  - subscription_fetch_history table"
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Migration cancelled."
    exit 1
fi

echo ""
echo "Executing migration..."
echo ""

# 执行 SQL 迁移
PGPASSWORD="$DB_PASSWORD" psql \
    -h "$DB_HOST" \
    -p "$DB_PORT" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -f "$SQL_FILE"

# 检查执行结果
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Migration completed successfully!"
    echo ""
    echo "Created indexes:"
    PGPASSWORD="$DB_PASSWORD" psql \
        -h "$DB_HOST" \
        -p "$DB_PORT" \
        -U "$DB_USER" \
        -d "$DB_NAME" \
        -t -A -c "
            SELECT indexname, tablename 
            FROM pg_indexes 
            WHERE tablename IN ('subscription_content', 'subscription_list', 'subscription_fetch_history')
              AND indexname LIKE 'idx_%'
            ORDER BY tablename, indexname;
        "
    
    echo ""
    echo "📊 Index sizes:"
    PGPASSWORD="$DB_PASSWORD" psql \
        -h "$DB_HOST" \
        -p "$DB_PORT" \
        -U "$DB_USER" \
        -d "$DB_NAME" \
        -c "
            SELECT 
                schemaname,
                tablename,
                indexname,
                pg_size_pretty(pg_relation_size(indexrelid)) as index_size
            FROM pg_stat_user_indexes
            WHERE schemaname = 'public'
              AND tablename IN ('subscription_content', 'subscription_list', 'subscription_fetch_history')
            ORDER BY tablename, indexname;
        "
else
    echo ""
    echo "❌ Migration failed!"
    exit 1
fi

echo ""
echo "=============================================="
echo "  Migration Complete"
echo "=============================================="
