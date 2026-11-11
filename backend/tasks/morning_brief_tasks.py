# -*- coding: utf-8 -*-
"""
Morning Brief (facts-report) Celery Tasks

事实/观点分离版晨报：生成体是外部 LLM agent 管线（tools/facts-report，
40-60 分钟、单次 ~12M tokens），不能在 celery 任务体内运行 —— 本任务秒级
派发即返回：

  1. 防误触：当日已有 completed 行且非 force → 跳过（force 仅服务器 CLI
     直调可达，不经任何 HTTP 端点）
  2. 孤儿回收 + mark_generating（morning_brief_reports 状态机）
  3. detached 启动 cron_daily.sh（自带 ZCODE env + flock 防并发 + show_gate
     就绪门 + Lark 告警），stdout/stderr 续接 facts-report/cron.log
  4. 完成态由管线尾部 register_morning_brief.py 回写 —— 本任务不等待、
     不轮询

调度入口：task_scheduling(type=morning_brief, cron 0 8 * * 2-6, Asia/Shanghai)。
"""

import subprocess
from datetime import date as _date
from pathlib import Path

from backend.celery_app import app
from backend.utils.datetime_utils import window_label_date
from backend.utils.logger import get_logger

logger = get_logger("morning_brief_tasks")

REPO_ROOT = Path("/opt/guanhetech")
CRON_SCRIPT = REPO_ROOT / "tools" / "facts-report" / "cron_daily.sh"
PIPE_DIR = REPO_ROOT / "tools" / "facts-report"
CRON_LOG = REPO_ROOT / "backend" / "reports" / "facts-report" / "cron.log"


@app.task(
    bind=True,
    name="generate_morning_brief",
    queue="data_fetching",
    max_retries=0,
)
def generate_morning_brief_task(
    self,
    report_date: str = None,
    force: bool = False,
):
    """派发 Morning Brief 管线（detached，秒级返回，不等待结果）。

    Args:
        report_date: 可选，北京自然日 ISO 日期（默认今天）。
        force: 强制重派（跳过 completed 挡板）。仅服务器 CLI 直调可达。
    """
    from backend.services.morning_brief_report_service import (
        MorningBriefReportService,
    )

    try:
        svc = MorningBriefReportService()

        from backend.utils.datetime_utils import utcnow

        as_of = _date.fromisoformat(report_date) if report_date else \
            window_label_date(utcnow(), "Asia/Shanghai")

        # 防误触挡板：当日已完成且非 force → 跳过（管线极重，绝不重复跑）
        existing = svc.get_report_by_date(as_of)
        if not force and existing and existing["status"] == "completed":
            msg = (f"Morning Brief {as_of} 已是 completed，跳过派发"
                   f"（force=True 可强制，仅 CLI）")
            logger.info(msg)
            return {"success": True, "skipped": True, "message": msg}

        svc.reset_orphan_generating()
        svc.mark_generating(as_of)

        CRON_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(CRON_LOG, "ab") as log_fh:
            proc = subprocess.Popen(
                ["/bin/bash", str(CRON_SCRIPT)],
                stdout=log_fh,
                stderr=log_fh,
                cwd=str(PIPE_DIR),
                start_new_session=True,  # detached：不随 worker 重启被杀
            )
        msg = (f"Morning Brief 管线已 detached 启动: date={as_of}, "
               f"pid={proc.pid}, log={CRON_LOG}")
        logger.info(msg)
        return {
            "success": True,
            "skipped": False,
            "message": msg,
            "pid": proc.pid,
        }

    except Exception as exc:
        logger.error(f"Morning Brief 派发异常: {exc}", exc_info=True)
        try:
            svc.fail(as_of, f"dispatch failed: {exc}")
        except Exception:  # noqa: BLE001
            pass
        raise
