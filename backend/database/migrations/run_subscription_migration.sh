#!/bin/bash
# =====================================================
# Twitter Subscription 数据库迁移执行脚本
# =====================================================

set -e  # 遇到错误立即退出

echo "=========================================="
echo "Twitter Subscription 数据库迁移"
echo "=========================================="
echo ""

# 数据库连接配置
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="5432"
DB_NAME="${DB_NAME:-rc_database}"
DB_USER="postgres"
DB_PASSWORD="1q23l,Sc45+@!"

# SQL文件路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/create_subscription_tables.sql"

# 检查SQL文件是否存在
if [ ! -f "$SQL_FILE" ]; then
    echo "❌ 错误: SQL文件不存在: $SQL_FILE"
    exit 1
fi

echo "📋 配置信息:"
echo "  - 数据库主机: $DB_HOST:$DB_PORT"
echo "  - 数据库名称: $DB_NAME"
echo "  - SQL文件: $SQL_FILE"
echo ""

# 提示用户确认
read -p "是否继续执行迁移? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "❌ 已取消迁移"
    exit 0
fi

echo ""
echo "🚀 开始执行数据库迁移..."
echo ""

# 执行SQL迁移
PGPASSWORD="$DB_PASSWORD" psql \
    -h "$DB_HOST" \
    -p "$DB_PORT" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -f "$SQL_FILE"

# 检查执行结果
if [ $? -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "✅ 数据库迁移成功完成!"
    echo "=========================================="
    echo ""
    
    # 显示表结构
    echo "📊 查看新创建的表:"
    echo ""
    
    PGPASSWORD="$DB_PASSWORD" psql \
        -h "$DB_HOST" \
        -p "$DB_PORT" \
        -U "$DB_USER" \
        -d "$DB_NAME" \
        -c "\d subscription_list"
    
    echo ""
    
    PGPASSWORD="$DB_PASSWORD" psql \
        -h "$DB_HOST" \
        -p "$DB_PORT" \
        -U "$DB_USER" \
        -d "$DB_NAME" \
        -c "\d subscription_fetch_history"
    
    echo ""
    echo "✅ 迁移完成！可以开始使用订阅系统了。"
    echo ""
else
    echo ""
    echo "=========================================="
    echo "❌ 数据库迁移失败!"
    echo "=========================================="
    echo ""
    exit 1
fi
