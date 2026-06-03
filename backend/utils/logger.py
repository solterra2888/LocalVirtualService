# -*- coding: utf-8 -*-
"""
统一日志工具模块

架构要点：
- 进程内仅 universal_glance 根 logger 持有唯一文件/控制台 handler
- 子 logger（get_logger(name)）仅设置 name 并 propagate 到根，不再各自 addHandler
- 文件输出使用 WatchedFileHandler，轮转交给外部 logrotate，避免多进程 rename 竞争
- 可选 QueueHandler + QueueListener 异步写盘（LOG_ASYNC，默认开启）
"""

import os
import sys
import logging
import logging.handlers
import queue
from pathlib import Path
from typing import Optional, List

ROOT_LOGGER_NAME = "universal_glance"

_logging_initialized = False
_queue_listener: Optional[logging.handlers.QueueListener] = None
_app_log_handler: Optional[logging.Handler] = None
_root_log_level = logging.INFO


def _is_celery_worker_env() -> bool:
    try:
        return len(logging.getLogger("celery").handlers) > 0
    except Exception:
        return False


def _log_async_enabled() -> bool:
    return os.getenv("LOG_ASYNC", "true").lower() in ("1", "true", "yes")


class UnifiedLogger:
    """统一日志管理类"""

    def __init__(self, name: str = ROOT_LOGGER_NAME):
        self.name = name
        self.logger = logging.getLogger(name)
        self._is_root = name == ROOT_LOGGER_NAME

        if not self._is_root:
            self.logger.propagate = True
            self.logger.handlers.clear()
            # 显式挂到应用根 logger，否则默认 parent 是 logging.root，无法共享 handler
            self.logger.parent = logging.getLogger(ROOT_LOGGER_NAME)

    def initialize(
        self,
        level: str = "INFO",
        log_to_file: bool = True,
        log_to_console: bool = True,
        log_file_days: int = 30,
        log_dir: str = "logs",
        log_async: Optional[bool] = None,
    ):
        """
        初始化日志配置（仅根 logger 执行一次）

        log_file_days 保留以兼容配置接口；实际轮转由 logrotate 负责。
        """
        global _logging_initialized, _queue_listener, _app_log_handler, _root_log_level

        if not self._is_root:
            logger.initialize(
                level=level,
                log_to_file=log_to_file,
                log_to_console=log_to_console,
                log_file_days=log_file_days,
                log_dir=log_dir,
                log_async=log_async,
            )
            return

        if _logging_initialized:
            return

        log_level = getattr(logging, level.upper(), logging.INFO)
        _root_log_level = log_level
        self.logger.setLevel(log_level)
        self.logger.propagate = False
        self.logger.handlers.clear()

        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        is_celery_worker = _is_celery_worker_env()
        real_handlers: List[logging.Handler] = []

        if log_to_console and not is_celery_worker:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            real_handlers.append(console_handler)

        if log_to_file:
            log_path = Path(log_dir)
            log_path.mkdir(exist_ok=True)
            file_handler = logging.handlers.WatchedFileHandler(
                log_path / "app.log",
                encoding="utf-8",
            )
            file_handler.setFormatter(formatter)
            real_handlers.append(file_handler)
            _app_log_handler = file_handler

        use_async = log_async if log_async is not None else _log_async_enabled()
        if use_async and real_handlers:
            log_queue: queue.Queue = queue.Queue(-1)
            queue_handler = logging.handlers.QueueHandler(log_queue)
            self.logger.addHandler(queue_handler)
            _queue_listener = logging.handlers.QueueListener(
                log_queue, *real_handlers, respect_handler_level=True
            )
            _queue_listener.start()
        else:
            for handler in real_handlers:
                self.logger.addHandler(handler)

        _logging_initialized = True

        if not is_celery_worker:
            self.info(
                f"日志系统初始化完成 - 级别: {level}, 文件输出: {log_to_file}, "
                f"控制台输出: {log_to_console}, 异步: {use_async and bool(real_handlers)}"
            )

    def debug(self, msg: str, source: str = None, **kwargs):
        self._log(logging.DEBUG, msg, source, **kwargs)

    def info(self, msg: str, source: str = None, **kwargs):
        self._log(logging.INFO, msg, source, **kwargs)

    def warning(self, msg: str, source: str = None, **kwargs):
        self._log(logging.WARNING, msg, source, **kwargs)

    def error(self, msg: str, source: str = None, **kwargs):
        self._log(logging.ERROR, msg, source, **kwargs)

    def _log(self, level: int, msg: str, source: str = None, **kwargs):
        global _logging_initialized

        if not _logging_initialized:
            if self._is_root:
                self.initialize()
            else:
                logger.initialize()

        if source:
            formatted_message = f"[{source}] {msg}"
        else:
            formatted_message = msg

        if kwargs:
            extra_info = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            formatted_message += f" | {extra_info}"

        self.logger.setLevel(_root_log_level)
        self.logger.log(level, formatted_message)

    def log_task_start(self, task_id: str, task_name: str, files_count: int = 0):
        self.info(
            f"任务开始: {task_name}",
            source="task",
            task_id=task_id,
            files_count=files_count,
        )

    def log_task_progress(self, task_id: str, current_file: str, progress: float):
        self.debug(
            f"处理进度: {progress:.1f}% - {current_file}",
            source="task",
            task_id=task_id,
        )

    def log_task_complete(
        self,
        task_id: str,
        files_processed: int,
        files_failed: int,
        total_time: float,
    ):
        self.info(
            f"任务完成 - 成功: {files_processed}, 失败: {files_failed}, "
            f"耗时: {total_time:.2f}秒",
            source="task",
            task_id=task_id,
        )

    def log_file_processing(
        self,
        filename: str,
        status: str,
        processing_time: float = None,
        error_message: str = None,
    ):
        if status == "success":
            self.info(
                f"文件处理成功: {filename}",
                source="file_processor",
                processing_time=processing_time,
            )
        elif status == "failed":
            self.error(
                f"文件处理失败: {filename} - {error_message}",
                source="file_processor",
            )
        elif status == "start":
            self.debug(f"开始处理文件: {filename}", source="file_processor")

    def log_api_request(
        self,
        endpoint: str,
        method: str,
        user: str = None,
        status_code: int = None,
        response_time: float = None,
    ):
        message = f"{method} {endpoint}"
        if status_code:
            message += f" - {status_code}"
        if response_time:
            message += f" ({response_time:.3f}s)"

        if status_code and status_code >= 400:
            self.warning(message, source="api", user=user)
        else:
            self.debug(message, source="api", user=user)

    def log_llama_service(self, message: str, level: str = "info"):
        log_method = getattr(self, level.lower(), self.info)
        log_method(message, source="llama")

    def log_rc_agent_service(self, message: str, level: str = "info"):
        log_method = getattr(self, level.lower(), self.info)
        log_method(message, source="rc_agent")


logger = UnifiedLogger()


def get_logger(name: str = None) -> UnifiedLogger:
    """获取日志实例；具名 logger 向 universal_glance 传播，共享单一 handler。"""
    if name:
        return UnifiedLogger(name)
    return logger


def init_logging(**kwargs):
    """初始化全局日志配置"""
    logger.initialize(**kwargs)


def shutdown_logging():
    """停止 QueueListener 并 flush 队列中的日志（进程退出时调用）。"""
    global _queue_listener
    if _queue_listener is not None:
        _queue_listener.stop()
        _queue_listener = None


def reset_file_handler_after_clear():
    """
    清空 app.log 后重置 WatchedFileHandler 的文件句柄，使下次写入从文件头开始。
    """
    global _app_log_handler
    if _app_log_handler is not None and isinstance(
        _app_log_handler, logging.handlers.WatchedFileHandler
    ):
        _app_log_handler.acquire()
        try:
            if _app_log_handler.stream:
                _app_log_handler.stream.close()
                _app_log_handler.stream = None
            _app_log_handler._open()
        finally:
            _app_log_handler.release()


def debug(msg: str, source: str = None, **kwargs):
    if "message" in kwargs:
        kwargs["extra_message"] = kwargs.pop("message")
    logger.debug(msg, source, **kwargs)


def info(msg: str, source: str = None, **kwargs):
    if "message" in kwargs:
        kwargs["extra_message"] = kwargs.pop("message")
    logger.info(msg, source, **kwargs)


def warning(msg: str, source: str = None, **kwargs):
    if "message" in kwargs:
        kwargs["extra_message"] = kwargs.pop("message")
    logger.warning(msg, source, **kwargs)


def error(msg: str, source: str = None, **kwargs):
    if "message" in kwargs:
        kwargs["extra_message"] = kwargs.pop("message")
    logger.error(msg, source, **kwargs)
