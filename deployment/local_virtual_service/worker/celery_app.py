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
from celery.signals import after_setup_logger, worker_process_init

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


@worker_process_init.connect
def _eager_init_webshare(sender=None, **kwargs):
    """
    每个 prefork 子进程刚 fork 出来时跑一次 Webshare 初始化 + 自检。

    为什么要 eager init（而不是惰性初始化）:
      - 之前 _get_webshare_proxy_config 是"首次 fetch_transcript 调用"才初始化。
        如果一个批量任务的前几个频道都"无新视频"，就走不到 fetch_transcript，
        Webshare 会一直未启用，启动日志看不到 [via=webshare] 标记，运维很难判断
        到底配没配上。
      - 实测日志里出现过 [via=direct] 命中 IpBlocked 的情况（明明 .env 配了
        Webshare），疑似 prefork 子进程的初始化时机和某些 fetch_transcript 路径
        存在竞争。eager 初始化在 fork 后立即跑，彻底消除竞态。
      - Webshare 自检（打 ipify 拿 exit_ip）失败也只是 warning，不会影响 worker
        启动；所以这里完全是 net positive。

    每个子进程会跑一次（prefork 池大小 = 各 worker 的 concurrency 之和）。
    自检本身只发 1 个 HTTPS 请求 + 拿一个字符串响应，开销可忽略。
    """
    try:
        # 延迟 import 避免循环依赖（celery_app -> tasks -> services -> celery_app）
        from worker.services import YouTubeCaptionService
        YouTubeCaptionService._get_webshare_proxy_config()
    except Exception as e:
        logging.getLogger(__name__).warning(
            "Webshare eager init 失败（不影响 worker 启动）: %s: %s",
            type(e).__name__, e,
        )
