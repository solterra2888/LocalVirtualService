#!/bin/bash
# facts-report 每日定时入口(GuanHe-Daily-Report 的事实/观点分离变体)。2026-11-10 起
# 定档 08:00(周二至周六)。与日报管线完全独立:每天自建 build_snapshot,不借快照。
# 测试期(2026-11-10 起)日报 cron 暂停,本管线独占早晨窗口;并发见文末 run_pipeline 行注。
#   0 8 * * 2-6 /bin/bash /opt/guanhetech/tools/facts-report/cron_daily.sh \
#       >> /opt/guanhetech/backend/reports/facts-report/cron.log 2>&1
#
# 流程: build_snapshot(--no-triage) → show_gate 就绪门(节目转录缺失时轮询至
#   09:40,期间/截止 Lark 告警) → run_pipeline(--max-parallel 3)。
# 失败告警: build_snapshot / run_pipeline 非 0 → lark_alert.py 推服务状态监测群。
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
    "$PY" lark_alert.py --title "【facts-report 🔴】build_snapshot 失败" \
        --line "时间: $(date '+%F %T %Z')" \
        --line "处置: 查看 cron.log 定位(DB/网络/代码)" || true
    echo "==== $(date '+%F %T %Z') facts-report cron end (rc=1, build_snapshot failed) ===="
    exit 1
fi
SNAP=$(ls -1t "$OUT"/*/snapshot.json | head -1)
DATE=$(basename "$(dirname "$SNAP")")
echo "[cron] self-built snapshot for $DATE"

# 就绪门(2026-11-10):08:00 启动时 The Close/Closing Bell 转录常未入库
# (实测就绪带北京 07:30-08:41)。缺失时每 5min 轮询补查,期间 Lark ⚠️ 预警;
# 09:40 仍缺则 🔴 告警后放行(报告如实缺源)。Overtime 段不阻塞。
"$PY" show_gate.py "$OUT/$DATE" --deadline 09:40 --poll 300 || true

echo "[cron] running pipeline for $DATE (style=$STYLE)"

# 限 3 路(日报暂停期独占窗口;恢复日报并行时建议降回 2)。
"$PY" run_pipeline.py "$DATE" --style "$STYLE" --max-parallel 3
rc=$?
if [ $rc -ne 0 ]; then
    "$PY" lark_alert.py --title "【facts-report 🔴】run_pipeline 失败" \
        --line "日期: $DATE" --line "退出码: $rc" \
        --line "时间: $(date '+%F %T %Z')" \
        --line "处置: 查看 cron.log / 工作区 run.log 定位" || true
    # Morning Brief 登记失败态(reports 模块;注册失败不改写本脚本 rc)
    "$PY" register_morning_brief.py --fail "$DATE" --rc $rc || true
else
    # Morning Brief 发布注册(2026-11-11): HTML 落库 morning_brief_reports +
    # 上传 OSS;注册器失败自行 Lark 告警,不影响管线 rc。
    "$PY" register_morning_brief.py --ok "$DATE" || true
fi

echo "==== $(date '+%F %T %Z') facts-report cron end (rc=$rc) ===="
exit $rc
