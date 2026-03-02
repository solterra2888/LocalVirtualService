#!/bin/bash
# 更新数据库中现有内容的热度等级

set -e  # 遇到错误立即退出

# 数据库连接参数
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="5432"
DB_USER="postgres"
DB_PASSWORD="1q23l,Sc45+@!"
DB_NAME="${DB_NAME:-rc_database}"

echo "=========================================="
echo "更新 subscription_content 表的 heat_level"
echo "=========================================="
echo ""
echo "数据库: $DB_NAME"
echo "主机: $DB_HOST"
echo ""

# 执行SQL脚本
PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$(dirname "$0")/update_heat_level.sql"

echo ""
echo "=========================================="
echo "热度等级更新完成！"
echo "=========================================="
