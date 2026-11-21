#!/bin/bash
# facts-report 行情补齐(2026-11-20):10:30 定时对最新工作区做"临门二拉"。
#
# 背景:FT 表冻结(~09:18)早于部分上游发布 —— 次主流加密币的 EODHD 日 bar
# 北京 ~09:20-10:00 才出、DB 摄入遇单代码 402 时 FX 要等 14:20/19:20 的
# scheduler 才补。冻结时缺的行进 stale 台账(报告横幅)。本脚本在 10:30
# (实测此时 EODHD 已全齐)对最新工作区重跑图表取数 + 重渲染 + 重注册,
# 把 T 日收盘补进已发布报告;补不齐则如实保留横幅(不再重注册)。
#
# 触发条件:ft_sections.json 的 stale 非空(空 = 早晨已齐,无事可做)。
# 幂等:us_close_chart/--workspace 与 register --ok 均按 report_date upsert,
# share_token 在 UPDATE 时保持不变(同日重发不打断已分发链接)。
#
# 调度入口:task_scheduling(type=morning_brief_ft_refresh, cron 30 10 * * 2-6,
# Asia/Shanghai,scheduler_service 直接 detached 子进程,不经 celery)。
set -uo pipefail

REPO=/opt/guanhetech
PIPE=$REPO/tools/facts-report
OUT=$REPO/backend/reports/facts-report
PY_BIN_DIR=/root/miniconda3/envs/guanhe/bin
PY=$PY_BIN_DIR/python

export HOME=/root
export PATH="$PY_BIN_DIR:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

echo "==== $(date '+%F %T %Z') facts-report ft_refresh start ===="

exec 9>/tmp/facts_report_ft_refresh.lock
flock -n 9 || { echo "[ft_refresh] previous run still active, skip"; exit 0; }

cd "$PIPE" || exit 1

SNAP=$(ls -1t "$OUT"/*/snapshot.json 2>/dev/null | head -1)
[ -n "$SNAP" ] || { echo "[ft_refresh] no workspace found, exit"; exit 0; }
WS=$(dirname "$SNAP")
DATE=$(basename "$WS")
FT="$WS/final/charts/ft_sections.json"

# 只处理"活报告":报告 T 应为北京昨天(今晨生成,T+1=今天)。更老的工作区
# 已归档不动;周一 10:30 时最近工作区是周五 T,同样属于"已隔周末的旧报告",
# 天然被 RECENT 判定挡下(周一不补,周五报告如实保留)。
RECENT=$(date -d "-1 days" +%F)
[ "$DATE" == "$RECENT" ] || { echo "[ft_refresh] latest workspace $DATE not T+1-live ($RECENT), exit"; exit 0; }

[ -f "$FT" ] || { echo "[ft_refresh] no ft_sections.json in $DATE, exit"; exit 0; }

STALE_N=$("$PY" - "$FT" <<'PYEOF'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    print(len(d.get("stale") or []))
except Exception:
    print(0)
PYEOF
)
if [ "$STALE_N" = "0" ]; then
    echo "[ft_refresh] $DATE stale empty, nothing to do"
    echo "==== $(date '+%F %T %Z') facts-report ft_refresh end (no-op) ===="
    exit 0
fi
echo "[ft_refresh] $DATE stale=$STALE_N — refetching FT rows"

# 重跑取数(EODHD 已发布,含 _rescue_stale_rows;stale 归零与否以下一步实测为准)
if ! "$PY" us_close_chart.py --workspace "$WS"; then
    echo "[ft_refresh] us_close_chart failed"
    exit 1
fi

NEW_N=$("$PY" - "$FT" <<'PYEOF'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    print(len(d.get("stale") or []))
except Exception:
    print(-1)
PYEOF
)

if [ "$NEW_N" = "0" ]; then
    # 重渲染 + 重注册(share_token 保持不变)
    if ! "$PY" report_html_tools/md2html_report.py "$DATE"; then
        echo "[ft_refresh] md2html_report failed"
        exit 1
    fi
    if ! "$PY" register_morning_brief.py --ok "$DATE"; then
        echo "[ft_refresh] register_morning_brief failed"
        exit 1
    fi
    "$PY" lark_alert.py --title "【facts-report ✅】FT 行情已补齐" \
        --line "日期: $DATE (原缺 $STALE_N 行)" \
        --line "时间: $(date '+%F %T %Z')" \
        --line "报告已重渲染重发布,横幅消除" || true
    echo "==== $(date '+%F %T %Z') facts-report ft_refresh end (fixed) ===="
    exit 0
else
    # 仍缺:不动已发布报告(横幅是诚实标注),等 14:20 scheduler 补 DB 后明早自愈
    "$PY" lark_alert.py --title "【facts-report ⚠️】FT 行情补齐后仍缺 ${NEW_N} 行" \
        --line "日期: $DATE" \
        --line "时间: $(date '+%F %T %Z')" \
        --line "处置: 查 cron.log [us_close] 输出;横幅如实保留" || true
    echo "==== $(date '+%F %T %Z') facts-report ft_refresh end (still stale=$NEW_N) ===="
    exit 0
fi
