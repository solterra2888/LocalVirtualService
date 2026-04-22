# -*- coding: utf-8 -*-
"""
独立 Celery 应用 — 仅包含 YouTube 订阅获取 + 转录/ASR 任务。
与主项目共享 Redis broker 和 PostgreSQL，但不依赖主项目代码。
"""

import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from celery import Celery
from celery.signals import after_setup_logger

# 加载 .env（优先项目根目录，其次当前目录）
_here = Path(__file__).resolve().parent
for candidate in [_here.parent / ".env", _here / ".env", Path.cwd() / ".env"]:
    if candidate.exists():
        load_dotenv(candidate, override=True)
        break

broker_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# 可调超时 / 结果保留（短队列/默认；长队列在 tasks.py 里按任务覆盖）
_task_time_limit = int(os.getenv("TASK_TIME_LIMIT_SECONDS", "7200"))
_task_soft_time_limit = int(os.getenv("TASK_SOFT_TIME_LIMIT_SECONDS", "6600"))
_result_expires = int(os.getenv("RESULT_EXPIRES_SECONDS", "3600"))

app = Celery("universal_glance")
app.conf.update(
    broker_url=broker_url,
    result_backend=broker_url,
    broker_connection_retry_on_startup=True,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_ignore_result=False,
    result_expires=_result_expires,
    timezone="Asia/Shanghai",
    enable_utc=True,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_max_tasks_per_child=50,
    task_time_limit=_task_time_limit,
    task_soft_time_limit=_task_soft_time_limit,
)

# 自动发现当前包里的 tasks 模块
app.autodiscover_tasks(["worker"])


@after_setup_logger.connect
def _suppress_gossip_noise(logger, **kwargs):
    # 本地 VM 与远程服务器共享同一 Redis Broker，两端时钟存在约 17s 偏差，
    # 导致 gossip 模块对每个远程 worker 的心跳几乎都判定为 "missed heartbeat"
    # 并以 INFO 级别刷屏，对实际运维无参考价值。将其提升到 WARNING 以静默该噪音，
    # 同时保留 WARNING+ 级别的真实告警（如 Substantial drift）。
    logging.getLogger("celery.worker.consumer.gossip").setLevel(logging.WARNING)
