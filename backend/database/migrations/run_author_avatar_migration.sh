#!/bin/bash
# 安全执行 add_author_avatar_to_subscription_content.sql
# - 从项目根 .env 读取数据库连接
# - lock_timeout 内拿不到锁则重试
# - 多次失败后可选短暂停写服务再执行

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
SQL_FILE="${SCRIPT_DIR}/add_author_avatar_to_subscription_content.sql"
MAX_RETRIES="${MAX_RETRIES:-5}"
RETRY_SLEEP="${RETRY_SLEEP:-10}"

if [ ! -f "$SQL_FILE" ]; then
    echo "SQL file not found: $SQL_FILE" >&2
    exit 1
fi

if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$PROJECT_ROOT/.env"
    set +a
fi

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-guanhe_database}"
DB_USER="${DB_USER:-postgres}"

if [ -z "${DB_PASSWORD:-}" ]; then
    echo "DB_PASSWORD is not set (check $PROJECT_ROOT/.env)" >&2
    exit 1
fi

run_migration() {
    PGPASSWORD="$DB_PASSWORD" psql \
        -v ON_ERROR_STOP=1 \
        -h "$DB_HOST" \
        -p "$DB_PORT" \
        -U "$DB_USER" \
        -d "$DB_NAME" \
        -f "$SQL_FILE"
}

echo "Target: ${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo "SQL:    $SQL_FILE"
echo ""

attempt=1
while [ "$attempt" -le "$MAX_RETRIES" ]; do
    echo "Attempt ${attempt}/${MAX_RETRIES}..."
    if run_migration; then
        echo ""
        echo "Migration completed successfully."
        exit 0
    fi
    if [ "$attempt" -eq "$MAX_RETRIES" ]; then
        break
    fi
    echo "Lock contention or transient error; retrying in ${RETRY_SLEEP}s..."
    sleep "$RETRY_SLEEP"
    attempt=$((attempt + 1))
done

echo ""
echo "Migration failed after ${MAX_RETRIES} attempts."
echo "Tip: run during off-peak hours, or briefly stop writers:"
echo "  sudo ./stop_services.sh && $0 && sudo ./start_services.sh"
exit 1
