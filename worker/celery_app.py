# -*- coding: utf-8 -*-
"""
独立 Celery 应用 — 仅包含 YouTube 订阅获取 + 转录/ASR 任务。
与主项目共享 Redis broker 和 PostgreSQL，但不依赖主项目代码。
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from celery import Celery

# 加载 .env（优先项目根目录，其次当前目录）
_here = Path(__file__).resolve().parent
for candidate in [_here.parent / ".env", _here / ".env", Path.cwd() / ".env"]:
    if candidate.exists():
        load_dotenv(candidate, override=True)
        break

broker_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

app = Celery("universal_glance")
app.conf.update(
    broker_url=broker_url,
    result_backend=broker_url,
    broker_connection_retry_on_startup=True,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_ignore_result=False,
    result_expires=3600,
    timezone="Asia/Shanghai",
    enable_utc=True,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_max_tasks_per_child=50,
    task_time_limit=7200,
    task_soft_time_limit=6600,
)

# 自动发现当前包里的 tasks 模块
app.autodiscover_tasks(["worker"])
