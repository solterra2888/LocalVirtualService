#!/bin/bash
# report-pipeline 每日定时入口。crontab 条目(北京时间周二~周六 09:20,美股交易日次晨):
#   20 9 * * 2-6 /bin/bash /opt/guanhetech/tools/report-pipeline/cron_daily.sh \
#       >> /opt/guanhetech/backend/reports/pipeline/cron.log 2>&1
#
# 流程: build_snapshot(缺省自动解析最新 The Close 交易日,含数据层/新闻层/转录内联)
#   → run_pipeline(章节 done 即秒级复用;全新交易日则完整生成 ~23 分钟)。
#
# cron 的极简 env 没有 zcode provider 配置 —— 交互 shell 由 harness 注入以下变量,
# cron 里缺了会报 "无法定位 CLI ZCode Built-in Provider Config"(2026-10-29 09:20
# 实跑失败原因),必须显式导出:
set -uo pipefail

REPO=/opt/guanhetech
PIPE=$REPO/tools/report-pipeline
PY_BIN_DIR=/root/miniconda3/envs/guanhe/bin
PY=$PY_BIN_DIR/python
STYLE=cn   # en=GuanHe 风格英文正文(调优基准); cn=中文正文

export HOME=/root
export ZCODE_DATA_BASE_DIR=/root
export ZCODE_PERSONAL_PROVIDER_CONFIG_FILE=/root/.zcode/v2/provider_config.json
# builtin 配置路径含版本号与 endpoint hash,zcode 升级后自动取最新一份
BUILTIN=$(ls -1 /root/.zcode/v2/runtime/provider/linux-x86_64/*/endpoint-*/zcode-builtin.json \
          2>/dev/null | sort -V | tail -1)
if [ -z "$BUILTIN" ]; then
    echo "[cron] zcode builtin provider config not found under ~/.zcode/v2/runtime"
    exit 1
fi
export ZCODE_BUILTIN_PROVIDER_CONFIG_FILE=$BUILTIN
export PATH="$PY_BIN_DIR:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

echo "==== $(date '+%F %T %Z') report-pipeline cron start ===="

# 防重叠:上一轮未结束则放弃本次(正常 23 分钟 << 24 小时,仅防御)
exec 9>/tmp/report_pipeline_cron.lock
flock -n 9 || { echo "[cron] previous run still active, skip"; exit 0; }

cd "$PIPE" || exit 1

if ! "$PY" build_snapshot.py; then
    echo "==== $(date '+%F %T %Z') report-pipeline cron end (rc=1, build_snapshot failed) ===="
    exit 1
fi

# 刚写出的快照即最新 → 其目录名就是报告日期
SNAP=$(ls -1t "$REPO"/backend/reports/pipeline/*/snapshot.json | head -1)
DATE=$(basename "$(dirname "$SNAP")")
echo "[cron] running pipeline for $DATE (style=$STYLE)"

"$PY" run_pipeline.py "$DATE" --style "$STYLE"
rc=$?

echo "==== $(date '+%F %T %Z') report-pipeline cron end (rc=$rc) ===="
exit $rc
