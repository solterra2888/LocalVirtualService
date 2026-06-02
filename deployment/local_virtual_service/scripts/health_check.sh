#!/bin/bash
# ============================================================
# YouTube Transcription Worker — 健康检查脚本
#
# 由 yt-worker-health.timer 每 10 分钟触发（oneshot）。
# 也可手动运行调试：bash /opt/local_virtual_service/scripts/health_check.sh
#
# 检查逻辑（按顺序）：
#   1. Redis/Broker 可达性预检（不可达则只告警，跳过 worker 重启）
#   2. 4 个 worker 逐一 inspect ping（失败则重启对应 unit + 告警）
#   3. 关键队列积压检测（持续超阈值则告警，按配置决定是否重启）
#   4. 状态去重：同一故障每 HEALTH_ALERT_REPEAT_MINUTES 最多重发一次
#   5. 故障消失时发送「✓ 已恢复」通知
#
# 状态持久化：logs/.health_state（每行 KEY=value，记录上次告警时间/次数）
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"  # /opt/local_virtual_service

# ── 加载 .env ──────────────────────────────────────────────
if [ -f "$BASE_DIR/.env" ]; then
    set -a
    source "$BASE_DIR/.env"
    set +a
else
    echo "[health_check] ❌ 找不到 .env: $BASE_DIR/.env" >&2
    exit 1
fi

# ── 配置变量（可在 .env 里覆盖） ──────────────────────────────
: "${LARK_WEBHOOK_URL:=}"
: "${HEALTH_PING_TIMEOUT_SECONDS:=10}"
: "${HEALTH_LONG_BACKLOG_THRESHOLD:=3}"
: "${HEALTH_PRIORITY_BACKLOG_THRESHOLD:=5}"
: "${HEALTH_RESTART_ON_BACKLOG:=false}"
: "${HEALTH_ALERT_REPEAT_MINUTES:=60}"
: "${HEALTH_ALERT_PREFIX:=[HK-YT-Worker]}"

# ── 路径 ──────────────────────────────────────────────────
CONDA_BASE=$(conda info --base 2>/dev/null || echo "$HOME/miniconda3")
CONDA_ENV_NAME="yt_service"
CELERY="$CONDA_BASE/envs/$CONDA_ENV_NAME/bin/celery"
PYTHON="$CONDA_BASE/envs/$CONDA_ENV_NAME/bin/python"
LOGS_DIR="$BASE_DIR/logs"
STATE_FILE="$LOGS_DIR/.health_state"
HOSTNAME_SUFFIX="@$(hostname)"

mkdir -p "$LOGS_DIR"
touch "$STATE_FILE"

NOW_EPOCH=$(date +%s)

# ── 工具函数 ──────────────────────────────────────────────

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [health_check] $*" | tee -a "$LOGS_DIR/worker.log"
}

# 发送飞书/Lark 告警（LARK_WEBHOOK_URL 为空则静默）
lark_notify() {
    local msg="$1"
    if [[ -z "$LARK_WEBHOOK_URL" ]]; then
        return 0
    fi
    curl -s -o /dev/null -X POST "$LARK_WEBHOOK_URL" \
        -H "Content-Type: application/json" \
        -d "{\"msg_type\":\"text\",\"content\":{\"text\":\"${HEALTH_ALERT_PREFIX} ${msg}\"}}" \
        || log "⚠️  Lark 通知发送失败（网络问题），消息: ${msg}"
}

# 读取状态文件中某个 key 的值，未找到返回空串
state_get() {
    local key="$1"
    grep -E "^${key}=" "$STATE_FILE" 2>/dev/null | tail -1 | cut -d= -f2- || true
}

# 写入/更新状态文件中某个 key 的值
state_set() {
    local key="$1"
    local val="$2"
    # 先删后追，保持文件整洁
    local tmp
    tmp=$(mktemp)
    grep -v "^${key}=" "$STATE_FILE" > "$tmp" 2>/dev/null || true
    echo "${key}=${val}" >> "$tmp"
    mv "$tmp" "$STATE_FILE"
}

# 判断是否超过去重间隔，可以重新告警（返回 0 = 可以发；返回 1 = 抑制）
should_alert() {
    local key="$1"
    local last_alert
    last_alert=$(state_get "LAST_ALERT_${key}")
    if [[ -z "$last_alert" ]]; then
        return 0
    fi
    local elapsed=$(( NOW_EPOCH - last_alert ))
    local repeat_secs=$(( HEALTH_ALERT_REPEAT_MINUTES * 60 ))
    if (( elapsed >= repeat_secs )); then
        return 0
    fi
    return 1
}

# 记录告警时间戳
mark_alerted() {
    local key="$1"
    state_set "LAST_ALERT_${key}" "$NOW_EPOCH"
}

# 清除告警记录（故障消失时调用）
clear_alert() {
    local key="$1"
    local tmp
    tmp=$(mktemp)
    grep -v "^LAST_ALERT_${key}=" "$STATE_FILE" > "$tmp" 2>/dev/null || true
    mv "$tmp" "$STATE_FILE"
}

# ── 1. Redis/Broker 可达性预检 ────────────────────────────
log "检查 Broker 可达性..."

# 从 REDIS_URL 提取 host:port（兼容 redis://:pass@host:port/db 格式）
BROKER_HOST=$(echo "$REDIS_URL" | sed -E 's|redis://([^:@]*:[^@]*@)?([^:/]+):([0-9]+).*|\2|')
BROKER_PORT=$(echo "$REDIS_URL" | sed -E 's|redis://([^:@]*:[^@]*@)?([^:/]+):([0-9]+).*|\3|')
BROKER_PORT="${BROKER_PORT:-6379}"

BROKER_OK=true
if ! nc -z -w 5 "$BROKER_HOST" "$BROKER_PORT" 2>/dev/null; then
    BROKER_OK=false
    log "❌ Broker ${BROKER_HOST}:${BROKER_PORT} 不可达"
    ALERT_KEY="broker_down"
    if should_alert "$ALERT_KEY"; then
        lark_notify "❌ Redis Broker ${BROKER_HOST}:${BROKER_PORT} 不可达。HK Worker 无法消费任务，等待 Broker 恢复后将自动重连。"
        mark_alerted "$ALERT_KEY"
    fi
    # Broker 不可达时不对 worker 做任何重启（避免重启风暴），直接退出
    exit 0
else
    log "✅ Broker ${BROKER_HOST}:${BROKER_PORT} 可达"
    # Broker 若曾告警过，现已恢复则发恢复通知
    if [[ -n "$(state_get 'LAST_ALERT_broker_down')" ]]; then
        lark_notify "✅ Redis Broker ${BROKER_HOST}:${BROKER_PORT} 已恢复，Worker 将继续消费任务。"
        clear_alert "broker_down"
    fi
fi

export PYTHONPATH="$BASE_DIR"

# ── 2. Per-worker inspect ping ─────────────────────────────
declare -A WORKER_ROLES=(
    ["youtube-transcription-worker${HOSTNAME_SUFFIX}"]="main"
    ["youtube-long-worker${HOSTNAME_SUFFIX}"]="long"
    ["youtube-priority-worker${HOSTNAME_SUFFIX}"]="priority-transcript"
    ["youtube-priority-asr-worker${HOSTNAME_SUFFIX}"]="priority-asr"
)

declare -A UNIT_NAMES=(
    ["main"]="yt-worker-main"
    ["long"]="yt-worker-long"
    ["priority-transcript"]="yt-worker-priority-transcript"
    ["priority-asr"]="yt-worker-priority-asr"
)

PING_FAILED=()

for WORKER_HOST in "${!WORKER_ROLES[@]}"; do
    ROLE="${WORKER_ROLES[$WORKER_HOST]}"
    UNIT="${UNIT_NAMES[$ROLE]}"
    log "  ping ${WORKER_HOST} (role=${ROLE})..."
    if "$CELERY" -A worker.celery_app inspect ping \
            -d "$WORKER_HOST" \
            --timeout "${HEALTH_PING_TIMEOUT_SECONDS}" \
            2>/dev/null | grep -q "pong"; then
        log "  ✅ ${ROLE} 响应正常"
        # 若曾记录该 worker ping 失败，现已恢复
        ALERT_KEY="ping_fail_${ROLE//[-.]/_}"
        if [[ -n "$(state_get "LAST_ALERT_${ALERT_KEY}")" ]]; then
            lark_notify "✅ Worker [${ROLE}] (${WORKER_HOST}) 已恢复正常。"
            clear_alert "$ALERT_KEY"
        fi
    else
        log "  ❌ ${ROLE} ping 超时/失败 → 重启 ${UNIT}.service"
        PING_FAILED+=("$ROLE")
        # 重启对应 unit
        systemctl restart "${UNIT}.service" 2>&1 | tee -a "$LOGS_DIR/worker.log" || true
        # 告警（去重）
        ALERT_KEY="ping_fail_${ROLE//[-.]/_}"
        if should_alert "$ALERT_KEY"; then
            lark_notify "❌ Worker [${ROLE}] (${WORKER_HOST}) ping 超时/失败，已自动重启 ${UNIT}.service。队列可能有积压，请关注。"
            mark_alerted "$ALERT_KEY"
        fi
    fi
done

# ── 3. 关键队列积压检测 ───────────────────────────────────
log "检查队列积压..."

# 用 conda env python 执行 redis LLEN 查询（复用 REDIS_URL，无需额外工具）
get_queue_len() {
    local queue_name="$1"
    "$PYTHON" - <<PYEOF 2>/dev/null || echo "0"
import os, re
try:
    import redis
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(url, socket_connect_timeout=5, socket_timeout=5)
    print(r.llen("${queue_name}"))
except Exception:
    print(0)
PYEOF
}

check_queue_backlog() {
    local queue="$1"
    local threshold="$2"
    local role="$3"
    local unit="${UNIT_NAMES[$role]}"

    local cur_len
    cur_len=$(get_queue_len "$queue")
    local state_key="backlog_${queue//[-_.]/_}"
    local prev_count
    prev_count=$(state_get "BACKLOG_COUNT_${state_key}")
    prev_count="${prev_count:-0}"
    local prev_len
    prev_len=$(state_get "BACKLOG_LEN_${state_key}")
    prev_len="${prev_len:-0}"

    if (( cur_len > threshold )); then
        local new_count=$(( prev_count + 1 ))
        state_set "BACKLOG_COUNT_${state_key}" "$new_count"
        state_set "BACKLOG_LEN_${state_key}" "$cur_len"
        log "  ⚠️  队列 ${queue} 积压 ${cur_len} 条（阈值 ${threshold}，连续 ${new_count} 次）"

        # 连续 ≥2 次且当前积压未下降 → 告警/重启
        if (( new_count >= 2 && cur_len >= prev_len )); then
            ALERT_KEY="backlog_${state_key}"
            if should_alert "$ALERT_KEY"; then
                local msg="⚠️ 队列 [${queue}] 持续积压 ${cur_len} 条（阈值 ${threshold}，连续 ${new_count} 次未下降）"
                if [[ "$HEALTH_RESTART_ON_BACKLOG" == "true" ]]; then
                    msg="${msg}，已自动重启 ${unit}.service。"
                    log "  → HEALTH_RESTART_ON_BACKLOG=true，重启 ${unit}.service"
                    systemctl restart "${unit}.service" 2>&1 | tee -a "$LOGS_DIR/worker.log" || true
                fi
                lark_notify "$msg"
                mark_alerted "$ALERT_KEY"
            fi
        fi
    else
        log "  ✅ 队列 ${queue} 积压 ${cur_len} 条（阈值 ${threshold}）"
        # 积压消退时清除状态和告警记录
        if (( prev_count > 0 )); then
            ALERT_KEY="backlog_${state_key}"
            if [[ -n "$(state_get "LAST_ALERT_${ALERT_KEY}")" ]]; then
                lark_notify "✅ 队列 [${queue}] 积压已消退（当前 ${cur_len} 条）。"
                clear_alert "$ALERT_KEY"
            fi
            state_set "BACKLOG_COUNT_${state_key}" "0"
            state_set "BACKLOG_LEN_${state_key}" "0"
        fi
    fi
}

check_queue_backlog "youtube_transcription_long"     "$HEALTH_LONG_BACKLOG_THRESHOLD"     "long"
check_queue_backlog "youtube_transcription_priority" "$HEALTH_PRIORITY_BACKLOG_THRESHOLD" "priority-asr"

# ── 汇总 ──────────────────────────────────────────────────
if [[ ${#PING_FAILED[@]} -gt 0 ]]; then
    log "健康检查完成，重启了以下 worker: ${PING_FAILED[*]}"
else
    log "健康检查完成，所有 worker 正常"
fi
