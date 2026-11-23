#!/bin/bash
# facts-report 港股/A股收盘补数(2026-11-23):把今日涨跌榜的港股/A股行从
# "上一交易日"更新为报告日 D(北京,= T+1)当日收盘,并重渲染重注册。
#
# 「当日」= 报告生成日 D(北京)。港股/A股在 08:00 生成时还没开盘,快照里先是
# 上一交易日(T)的行;16:00 收盘后 EODHD 当日 bulk 文件才出(11-23 实测 19:32
# 已可得),由 refresh_rank_rows.py 用 D 日文件重算 → 写回 snapshot.json →
# md2html → register --ok(share_token 保持不变,已分发的分享链接不受影响)。
# 美股行不在此处理:取北京 D 日凌晨收盘的那一场,生成时已是最终值。
#
# 触发条件:工作区 T = 北京昨天(今晨生成,T+1=今天)。周六 D 日港股/A股休市、
# 平日休市或 EODHD 文件未出时,脚本判定"无数据"空转退出,榜单保留上一交易日
# 的行(页面标注"暂为 … 收盘")。
# 幂等:两次尝试(19:30 / 22:30)按"行内容与快照是否一致"判定,首次成功后第二次
# 空转;文件较晚才出齐时第二次会把行补全并再发布一次。
#
# 调度入口:task_scheduling(type=morning_brief_asia_close, cron 30 19,22 * * 2-6,
# Asia/Shanghai,scheduler_service 直接 detached 子进程,不经 celery;stdout 续接
# facts-report/cron.log)。失败告警: lark_alert.py 推服务状态监测群。
set -uo pipefail

REPO=/opt/guanhetech
PIPE=$REPO/tools/facts-report
OUT=$REPO/backend/reports/facts-report
PY_BIN_DIR=/root/miniconda3/envs/guanhe/bin
PY=$PY_BIN_DIR/python

export HOME=/root
export PATH="$PY_BIN_DIR:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

echo "==== $(date '+%F %T %Z') facts-report asia_close_refresh start ===="

exec 9>/tmp/facts_report_asia_close.lock
flock -n 9 || { echo "[asia_close] previous run still active, skip"; exit 0; }

cd "$PIPE" || exit 1

SNAP=$(ls -1t "$OUT"/*/snapshot.json 2>/dev/null | head -1)
[ -n "$SNAP" ] || { echo "[asia_close] no workspace found, exit"; exit 0; }
WS=$(dirname "$SNAP")
DATE=$(basename "$WS")

# 只处理"活报告":报告 T 应为北京昨天(今晨生成,T+1=今天)。更老的工作区已
# 归档不动;周一 19:30 时最近工作区是周五 T,同样被挡下(不补,周五报告如实
# 保留上一交易日 = 周四的港股/A股行)。
RECENT=$(date -d "-1 days" +%F)
[ "$DATE" == "$RECENT" ] || { echo "[asia_close] latest workspace $DATE not T+1-live ($RECENT), exit"; exit 0; }

"$PY" refresh_rank_rows.py "$DATE"
rc=$?
if [ $rc -ne 0 ]; then
    "$PY" lark_alert.py --title "【facts-report 🔴】港股/A股收盘补数失败" \
        --line "工作区 T: $DATE(报告日 D: $(date -d "$DATE +1 day" +%F))" \
        --line "退出码: $rc" \
        --line "时间: $(date '+%F %T %Z')" \
        --line "处置: 榜单港股/A股行仍为上一交易日(页面已标注);查看 cron.log [rank_refresh] 输出" || true
fi

echo "==== $(date '+%F %T %Z') facts-report asia_close_refresh end (rc=$rc) ===="
exit $rc
