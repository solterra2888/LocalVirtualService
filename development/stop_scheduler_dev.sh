#!/bin/bash
# 调度器本地开发停止脚本（与 start_scheduler_dev.sh 配套）

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

echo -e "${RED}🛑 停止调度器本地开发服务${NC}"
echo "=============================================="
echo ""

# ── 按端口停止 ────────────────────────────────────────────────
echo -e "${BLUE}🔍 检查端口 8001 上的进程...${NC}"
dev_stop_by_port 8001 "调度器"

# ── 按进程模式兜底清理（port 释放失败时） ─────────────────────
echo ""
echo -e "${BLUE}🧹 按进程模式兜底清理...${NC}"
dev_stop_by_pattern "uvicorn.*scheduler_main" "调度器 (uvicorn)"

# ── 最终检查 ─────────────────────────────────────────────────
echo ""
echo -e "${BLUE}🔍 最终端口检查...${NC}"
if lsof -Pi :8001 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo -e "${RED}❌ 端口 8001 (调度器) 仍有进程${NC}"
else
    echo -e "${GREEN}✅ 端口 8001 (调度器) 已释放${NC}"
fi

echo ""
echo -e "${GREEN}🎉 调度器已停止!${NC}"
echo "=============================================="
echo -e "${CYAN}💡 重新启动: ./development/start_scheduler_dev.sh${NC}"
echo "=============================================="
