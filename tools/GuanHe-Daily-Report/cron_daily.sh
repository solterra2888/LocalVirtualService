#!/bin/bash
# GuanHe-Daily-Report 每日定时入口。crontab 条目(建议北京时间周二~周六 10:00,美股交易日次晨;
# 10:00 而非 09:20:中文晨报素材约 08:51-09:22 发布、节目转录 10 点后才稳定):
#   0 10 * * 2-6 /bin/bash /opt/guanhetech/tools/GuanHe-Daily-Report/cron_daily.sh \
#       >> /opt/guanhetech/backend/reports/guanhe-daily-report/cron.log 2>&1
#
# 流程: build_snapshot(缺省自动解析最新 The Close 交易日,含数据层/新闻层/中文层/转录内联)
#   → run_pipeline(章节 done 即秒级复用;全新交易日则完整生成)。
#
# cron 的极简 env 没有 zcode provider 配置 —— 交互 shell 由 harness 注入以下变量,
# cron 里缺了会报 "无法定位 CLI ZCode Built-in Provider Config"(2026-10-29 09:20
# 实跑失败原因),必须显式导出:
set -uo pipefail

REPO=/opt/guanhetech
PIPE=$REPO/tools/GuanHe-Daily-Report
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

echo "==== $(date '+%F %T %Z') guanhe-daily-report cron start ===="

# 防重叠:上一轮未结束则放弃本次(正常 23 分钟 << 24 小时,仅防御)。
# 锁名与稳定版管线的锁不同 —— 两条管线独立,互不阻塞。
exec 9>/tmp/guanhe_daily_report_cron.lock
flock -n 9 || { echo "[cron] previous run still active, skip"; exit 0; }

cd "$PIPE" || exit 1

# --no-triage(2026-11-07 起暂用):triage 4 路齐射连续撞 provider 限速
# (Turn execution failed,生产 0/3,详见 2026-11-06 会话分析),退正则路径
# P28 已带 L/E 等级;恢复语义分拣时改回默认并配 TRIAGE_WORKERS=1 串行。
if ! "$PY" build_snapshot.py --no-triage; then
    echo "==== $(date '+%F %T %Z') guanhe-daily-report cron end (rc=1, build_snapshot failed) ===="
    exit 1
fi

# 刚写出的快照即最新 → 其目录名就是报告日期
SNAP=$(ls -1t "$REPO"/backend/reports/guanhe-daily-report/*/snapshot.json | head -1)
DATE=$(basename "$(dirname "$SNAP")")
echo "[cron] running pipeline for $DATE (style=$STYLE)"

"$PY" run_pipeline.py "$DATE" --style "$STYLE"
rc=$?

echo "==== $(date '+%F %T %Z') guanhe-daily-report cron end (rc=$rc) ===="
exit $rc
