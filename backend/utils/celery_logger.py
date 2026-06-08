# -*- coding: utf-8 -*-
"""
Celery专用日志配置模块

- celery logger -> celery_worker.log（worker 生命周期）
- celery.task logger -> celery_tasks.log（任务审计，propagate=False 避免重复）
- 文件输出使用 WatchedFileHandler，轮转由 logrotate 负责
"""

import logging
import logging.handlers
from pathlib import Path
from typing import Dict, Optional

WORKER_LOGGER_NAME = "celery"
TASK_LOGGER_NAME = "celery.task"

# 高频任务：跳过常规开始/成功 INFO，失败与重试仍记录
QUIET_TASK_NAMES = frozenset({
    "tag_feed_content",
})

_celery_logging_initialized = False
_file_handlers: Dict[str, logging.Handler] = {}


class CeleryLogger:
    """Celery专用日志管理器"""

    def __init__(self):
        self.worker_logger = logging.getLogger(WORKER_LOGGER_NAME)
        self.task_logger = logging.getLogger(TASK_LOGGER_NAME)
        self._initialized = False

    def setup_celery_logging(
        self,
        log_dir: str = "logs",
        worker_log_file: str = "celery_worker.log",
        task_log_file: str = "celery_tasks.log",
        log_level: str = "INFO",
    ):
        """
        设置 Celery 日志配置。

        max_bytes / backup_count 参数已移除；轮转由外部 logrotate 负责。
        """
        global _celery_logging_initialized, _file_handlers

        if self._initialized:
            return

        log_path = Path(log_dir)
        log_path.mkdir(exist_ok=True)

        level = getattr(logging, log_level.upper(), logging.INFO)

        worker_formatter = logging.Formatter(
            "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        task_formatter = logging.Formatter(
            "[%(asctime)s: %(levelname)s/%(processName)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        self.worker_logger.handlers.clear()
        self.worker_logger.setLevel(level)
        self.worker_logger.propagate = False

        worker_handler = logging.handlers.WatchedFileHandler(
            log_path / worker_log_file,
            encoding="utf-8",
        )
        worker_handler.setFormatter(worker_formatter)
        worker_handler.setLevel(level)
        self.worker_logger.addHandler(worker_handler)
        _file_handlers[worker_log_file] = worker_handler

        self.task_logger.handlers.clear()
        self.task_logger.setLevel(level)
        self.task_logger.propagate = False

        task_handler = logging.handlers.WatchedFileHandler(
            log_path / task_log_file,
            encoding="utf-8",
        )
        task_handler.setFormatter(task_formatter)
        task_handler.setLevel(level)
        self.task_logger.addHandler(task_handler)
        _file_handlers[task_log_file] = task_handler

        self._initialized = True
        _celery_logging_initialized = True
        self.worker_logger.info(f"Celery日志系统初始化完成 - 级别: {log_level}")

    def log_worker_start(self, worker_name: str, queues: list, concurrency: int):
        self.worker_logger.info(
            f"Celery Worker启动 - 名称: {worker_name}, 队列: {queues}, 并发数: {concurrency}"
        )

    def log_worker_shutdown(self, worker_name: str):
        self.worker_logger.info(f"Celery Worker关闭 - 名称: {worker_name}")

    def log_task_received(
        self, task_id: str, task_name: str, args: tuple = (), kwargs: dict = None
    ):
        kwargs = kwargs or {}
        self.task_logger.info(
            f"任务接收 - ID: {task_id}, 名称: {task_name}, 参数: {args}, 关键字参数: {kwargs}"
        )

    def _is_quiet_task(self, task_name: str) -> bool:
        return task_name in QUIET_TASK_NAMES

    def log_task_started(self, task_id: str, task_name: str):
        if self._is_quiet_task(task_name):
            return
        self.task_logger.info(f"任务开始 - ID: {task_id}, 名称: {task_name}")

    def log_task_succeeded(
        self, task_id: str, task_name: str, result=None, runtime: float = None
    ):
        if self._is_quiet_task(task_name):
            return
        runtime_info = f", 运行时间: {runtime:.2f}秒" if runtime else ""

        if task_name in ["analyze_unanalyzed_news", "analyze_news_batch"] and isinstance(
            result, dict
        ):
            total = result.get("total", result.get("total_news", 0))
            success = result.get("success", result.get("news_analyzed", 0))
            failed = result.get("failed", result.get("news_failed", 0))
            template = result.get("template_code", "unknown")

            if total > 0:
                success_rate = success / total * 100
                self.task_logger.info(
                    f"任务成功 - ID: {task_id}, 名称: {task_name}{runtime_info}"
                )
                self.task_logger.info(
                    f"   处理摘要: 总计 {total} 条, 成功 {success} 条 ({success_rate:.1f}%), 失败 {failed} 条"
                )
                self.task_logger.info(f"   使用模板: {template}")
            else:
                self.task_logger.info(
                    f"任务成功 - ID: {task_id}, 名称: {task_name}{runtime_info}"
                )
                self.task_logger.info("   没有需要处理的新闻")
        else:
            self.task_logger.info(
                f"任务成功 - ID: {task_id}, 名称: {task_name}{runtime_info}"
            )

    def log_task_failed(
        self, task_id: str, task_name: str, exception: str, traceback: str = None
    ):
        self.task_logger.error(
            f"任务失败 - ID: {task_id}, 名称: {task_name}, 异常: {exception}"
        )
        if traceback:
            self.task_logger.error(f"任务失败堆栈 - ID: {task_id}:\n{traceback}")

    def log_task_retry(
        self, task_id: str, task_name: str, reason: str, countdown: float = None
    ):
        countdown_info = f", 倒计时: {countdown}秒" if countdown else ""
        self.task_logger.warning(
            f"任务重试 - ID: {task_id}, 名称: {task_name}, 原因: {reason}{countdown_info}"
        )

    def log_queue_info(self, queue_name: str, message_count: int):
        self.worker_logger.debug(
            f"队列状态 - 名称: {queue_name}, 消息数量: {message_count}"
        )

    def log_connection_info(self, broker_url: str, result_backend: str):
        self.worker_logger.info(
            f"Celery连接配置 - Broker: {broker_url}, Result Backend: {result_backend}"
        )


celery_logger = CeleryLogger()


def setup_celery_logging(**kwargs):
    celery_logger.setup_celery_logging(**kwargs)


def get_celery_logger() -> CeleryLogger:
    return celery_logger


def reset_celery_file_handler(log_filename: str):
    """清空 Celery 日志文件后重置对应 WatchedFileHandler。"""
    handler = _file_handlers.get(log_filename)
    if handler is not None and isinstance(handler, logging.handlers.WatchedFileHandler):
        handler.acquire()
        try:
            if handler.stream:
                handler.stream.close()
                handler.stream = None
            handler._open()
        finally:
            handler.release()


def log_task_execution(task_name: str = None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            actual_task_name = task_name or func.__name__
            task_id = f"{actual_task_name}_{id(args)}"

            try:
                celery_logger.log_task_started(task_id, actual_task_name)
                result = func(*args, **kwargs)
                celery_logger.log_task_succeeded(task_id, actual_task_name, result)
                return result
            except Exception as e:
                celery_logger.log_task_failed(task_id, actual_task_name, str(e))
                raise

        return wrapper

    return decorator
