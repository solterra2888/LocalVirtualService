#!/bin/bash
# facts-report 每日定时入口(GuanHe-Daily-Report 的事实/观点分离变体)。crontab 条目
# 建议排在日报之后(日报 10:00 起跑,快照约 10 分钟内写出):
#   20 10 * * 2-6 /bin/bash /opt/guanhetech/tools/facts-report/cron_daily.sh \
#       >> /opt/guanhetech/backend/reports/facts-report/cron.log 2>&1
#
# 流程: 借用日报 6 小时内写出的最新快照(run_pipeline 自动复制 snapshot.json +
#   transcripts/);没有则自行 build_snapshot → run_pipeline。
#
# cron 的极简 env 没有 zcode provider 配置 —— 交互 shell 由 harness 注入以下变量,
# cron 里缺了会报 "无法定位 CLI ZCode Built-in Provider Config",必须显式导出:
set -uo pipefail

REPO=/opt/guanhetech
PIPE=$REPO/tools/facts-report
OUT=$REPO/backend/reports/facts-report
SEED=$REPO/backend/reports/guanhe-daily-report
PY_BIN_DIR=/root/miniconda3/envs/guanhe/bin
PY=$PY_BIN_DIR/python
STYLE=cn

export HOME=/root
export ZCODE_DATA_BASE_DIR=/root
export ZCODE_PERSONAL_PROVIDER_CONFIG_FILE=/root/.zcode/v2/provider_config.json
BUILTIN=$(ls -1 /root/.zcode/v2/runtime/provider/linux-x86_64/*/endpoint-*/zcode-builtin.json \
          2>/dev/null | sort -V | tail -1)
if [ -z "$BUILTIN" ]; then
    echo "[cron] zcode builtin provider config not found under ~/.zcode/v2/runtime"
    exit 1
fi
export ZCODE_BUILTIN_PROVIDER_CONFIG_FILE=$BUILTIN
export PATH="$PY_BIN_DIR:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

echo "==== $(date '+%F %T %Z') facts-report cron start ===="

# 锁与日报不同 —— 两条管线独立,互不阻塞
exec 9>/tmp/facts_report_cron.lock
flock -n 9 || { echo "[cron] previous run still active, skip"; exit 0; }

cd "$PIPE" || exit 1
mkdir -p "$OUT"

SNAP=$(find "$SEED" -mindepth 2 -maxdepth 2 -name snapshot.json -mmin -360 2>/dev/null \
       | xargs -r ls -1t | head -1)
if [ -n "$SNAP" ]; then
    DATE=$(basename "$(dirname "$SNAP")")
    echo "[cron] reusing guanhe-daily-report snapshot for $DATE"
else
    echo "[cron] no fresh guanhe-daily-report snapshot; building our own"
    if ! "$PY" build_snapshot.py --no-triage; then
        echo "==== $(date '+%F %T %Z') facts-report cron end (rc=1, build_snapshot failed) ===="
        exit 1
    fi
    SNAP=$(ls -1t "$OUT"/*/snapshot.json | head -1)
    DATE=$(basename "$(dirname "$SNAP")")
fi
echo "[cron] running pipeline for $DATE (style=$STYLE)"

"$PY" run_pipeline.py "$DATE" --style "$STYLE"
rc=$?

echo "==== $(date '+%F %T %Z') facts-report cron end (rc=$rc) ===="
exit $rc
