#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""facts-report Lark 群告警薄封装(复用 backend.utils.lark_notifier.send_text)。

发送失败只记日志不抛错,CLI 退出码恒 0 —— 告警是旁路,不得影响管线主流程。

CLI(供 cron_daily.sh / show_gate.py 任意阶段调用):
    python lark_alert.py --title "【facts-report 🔴】run_pipeline 失败" \
        --line "日期: 2026-11-11" --line "退出码: 1"

目标群: FACTS_LARK_WEBHOOK_URL 覆盖;默认服务状态监测群
(2026-11-10 用户指定,与 .env LARK_WEBHOOK_URL 同址)。
FACTS_LARK_NOTIFY_ENABLED=false 可整体关闭。
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List, Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

# 先触发 .env 加载(绝对路径),cron 裸 env 下 FACTS_* 覆盖项也可用
import config.settings  # noqa: F401,E402

from backend.utils.lark_notifier import (  # noqa: E402
    LARK_WEBHOOK_SERVICE_STATUS,
    send_card,
    send_text,
)

ENABLED_ENV = "FACTS_LARK_NOTIFY_ENABLED"
WEBHOOK_ENV = "FACTS_LARK_WEBHOOK_URL"


def notify(title: str, lines: Optional[Sequence[str]] = None) -> bool:
    """推送一条文本告警;开关关闭/发送失败均返回 False,不抛出。"""
    if os.getenv(ENABLED_ENV, "true").strip().lower() != "true":
        return False
    url = os.getenv(WEBHOOK_ENV, "").strip() or LARK_WEBHOOK_SERVICE_STATUS
    return send_text(title, lines, webhook_url=url)


def notify_card(card: dict, extra_webhooks: Optional[str] = None) -> bool:
    """推送一张交互卡片(webhook 文本消息不做链接 unfurl,观感走卡片)。

    extra_webhooks: 逗号分隔的额外 webhook(如 MB_LINK_LARK_WEBHOOK 指定的
    其它群);默认服务状态监测群始终发,额外目标逐一发送,任一失败只影响
    该目标的返回位。
    """
    if os.getenv(ENABLED_ENV, "true").strip().lower() != "true":
        return False
    urls = [os.getenv(WEBHOOK_ENV, "").strip() or LARK_WEBHOOK_SERVICE_STATUS]
    urls += [u for u in (x.strip() for x in (extra_webhooks or "").split(","))
             if u]
    ok = True
    for u in urls:
        ok = send_card(card, webhook_url=u) and ok
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--title", required=True, help="消息首行(含【facts-report】前缀)")
    ap.add_argument("--line", action="append", default=[],
                    help="正文行,可重复")
    args = ap.parse_args()
    lines: List[str] = list(args.line)
    ok = notify(args.title, lines)
    print(f"[lark_alert] sent={ok} title={args.title!r}")
    return 0  # 恒 0:告警失败不影响调用方退出码


if __name__ == "__main__":
    raise SystemExit(main())
