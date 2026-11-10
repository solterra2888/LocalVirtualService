#!/bin/bash
# facts-report 每日定时入口(GuanHe-Daily-Report 的事实/观点分离变体)。2026-11-10 起
# 提前到 08:00(日报 08:31 之前):此时无当日日报快照可借,走自建 build_snapshot 路径;
# 测试期(2026-11-10 起)日报 cron 暂停、本管线不借快照每天自建;并发见文末 run_pipeline 行注。
#   0 8 * * 2-6 /bin/bash /opt/guanhetech/tools/facts-report/cron_daily.sh \
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

# 两条管线独立(2026-11-10 定):facts-report 不借用日报快照,每天自建。
# 测试期(2026-11-10 起)日报 cron 暂停,本管线独占早晨窗口,放 3 路并发。
if ! "$PY" build_snapshot.py --no-triage; then
    echo "==== $(date '+%F %T %Z') facts-report cron end (rc=1, build_snapshot failed) ===="
    exit 1
fi
SNAP=$(ls -1t "$OUT"/*/snapshot.json | head -1)
DATE=$(basename "$(dirname "$SNAP")")
echo "[cron] self-built snapshot for $DATE"

echo "[cron] running pipeline for $DATE (style=$STYLE)"

# 限 3 路(日报暂停期独占窗口;恢复日报并行时建议降回 2)。
"$PY" run_pipeline.py "$DATE" --style "$STYLE" --max-parallel 3
rc=$?

echo "==== $(date '+%F %T %Z') facts-report cron end (rc=$rc) ===="
exit $rc
