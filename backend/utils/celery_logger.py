# -*- coding: utf-8 -*-
"""
Celery专用日志配置模块
用于配置Celery worker和任务的日志记录
"""

import os
import logging
import logging.handlers
from pathlib import Path
from typing import Optional


class CeleryLogger:
    """Celery专用日志管理器"""
    
    def __init__(self, name: str = "celery"):
        self.name = name
        self.logger = logging.getLogger(name)
        self._initialized = False
        
    def setup_celery_logging(self, 
                           log_dir: str = "logs",
                           worker_log_file: str = "celery_worker.log",
                           task_log_file: str = "celery_tasks.log",
                           log_level: str = "INFO",
                           max_bytes: int = 10 * 1024 * 1024,  # 10MB
                           backup_count: int = 5):
        """
        设置Celery日志配置
        
        Args:
            log_dir: 日志目录
            worker_log_file: Worker日志文件名
            task_log_file: 任务日志文件名
            log_level: 日志级别
            max_bytes: 日志文件最大字节数
            backup_count: 备份文件数量
        """
        if self._initialized:
            return
            
        # 确保日志目录存在
        log_path = Path(log_dir)
        log_path.mkdir(exist_ok=True)
        
        # 设置日志级别
        level = getattr(logging, log_level.upper(), logging.INFO)
        self.logger.setLevel(level)
        
        # 清除已有的handlers
        self.logger.handlers.clear()
        
        # 定义日志格式
        formatter = logging.Formatter(
            '[%(asctime)s: %(levelname)s/%(processName)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Worker日志文件处理器
        worker_log_path = log_path / worker_log_file
        worker_handler = logging.handlers.RotatingFileHandler(
            filename=worker_log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        worker_handler.setFormatter(formatter)
        worker_handler.setLevel(level)
        self.logger.addHandler(worker_handler)
        
        # 任务日志文件处理器（单独文件）
        task_log_path = log_path / task_log_file
        task_handler = logging.handlers.RotatingFileHandler(
            filename=task_log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        task_formatter = logging.Formatter(
            '[%(asctime)s: %(levelname)s/%(processName)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        task_handler.setFormatter(task_formatter)
        task_handler.setLevel(level)
        self.logger.addHandler(task_handler)
        
        # 控制台输出（可选）
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(level)
        self.logger.addHandler(console_handler)
        
        self._initialized = True
        self.logger.info(f"Celery日志系统初始化完成 - 级别: {log_level}")
    
    def log_worker_start(self, worker_name: str, queues: list, concurrency: int):
        """记录Worker启动日志"""
        self.logger.info(f"Celery Worker启动 - 名称: {worker_name}, 队列: {queues}, 并发数: {concurrency}")
    
    def log_worker_shutdown(self, worker_name: str):
        """记录Worker关闭日志"""
        self.logger.info(f"Celery Worker关闭 - 名称: {worker_name}")
    
    def log_task_received(self, task_id: str, task_name: str, args: tuple = (), kwargs: dict = None):
        """记录任务接收日志"""
        kwargs = kwargs or {}
        self.logger.info(f"任务接收 - ID: {task_id}, 名称: {task_name}, 参数: {args}, 关键字参数: {kwargs}")
    
    def log_task_started(self, task_id: str, task_name: str):
        """记录任务开始日志"""
        self.logger.info(f"任务开始 - ID: {task_id}, 名称: {task_name}")
    
    def log_task_succeeded(self, task_id: str, task_name: str, result=None, runtime: float = None):
        """记录任务成功日志"""
        runtime_info = f", 运行时间: {runtime:.2f}秒" if runtime else ""
        
        # 如果是新闻分析任务，尝试解析结果并显示摘要
        if task_name in ['analyze_unanalyzed_news', 'analyze_news_batch'] and isinstance(result, dict):
            total = result.get('total', result.get('total_news', 0))
            success = result.get('success', result.get('news_analyzed', 0))
            failed = result.get('failed', result.get('news_failed', 0))
            template = result.get('template_code', 'unknown')
            
            if total > 0:
                success_rate = (success / total * 100)
                self.logger.info(f"✅ 任务成功 - ID: {task_id}, 名称: {task_name}{runtime_info}")
                self.logger.info(f"   📊 处理摘要: 总计 {total} 条, 成功 {success} 条 ({success_rate:.1f}%), 失败 {failed} 条")
                self.logger.info(f"   🔧 使用模板: {template}")
            else:
                self.logger.info(f"✅ 任务成功 - ID: {task_id}, 名称: {task_name}{runtime_info}")
                self.logger.info(f"   ℹ️ 没有需要处理的新闻")
        else:
            self.logger.info(f"✅ 任务成功 - ID: {task_id}, 名称: {task_name}{runtime_info}")
    
    def log_task_failed(self, task_id: str, task_name: str, exception: str, traceback: str = None):
        """记录任务失败日志"""
        self.logger.error(f"任务失败 - ID: {task_id}, 名称: {task_name}, 异常: {exception}")
        if traceback:
            self.logger.error(f"任务失败堆栈 - ID: {task_id}:\n{traceback}")
    
    def log_task_retry(self, task_id: str, task_name: str, reason: str, countdown: float = None):
        """记录任务重试日志"""
        countdown_info = f", 倒计时: {countdown}秒" if countdown else ""
        self.logger.warning(f"任务重试 - ID: {task_id}, 名称: {task_name}, 原因: {reason}{countdown_info}")
    
    def log_queue_info(self, queue_name: str, message_count: int):
        """记录队列信息日志"""
        self.logger.debug(f"队列状态 - 名称: {queue_name}, 消息数量: {message_count}")
    
    def log_connection_info(self, broker_url: str, result_backend: str):
        """记录连接信息日志"""
        self.logger.info(f"Celery连接配置 - Broker: {broker_url}, Result Backend: {result_backend}")


# 全局Celery日志实例
celery_logger = CeleryLogger()


def setup_celery_logging(**kwargs):
    """设置Celery日志的便捷函数"""
    celery_logger.setup_celery_logging(**kwargs)


def get_celery_logger() -> CeleryLogger:
    """获取Celery日志实例"""
    return celery_logger


# 任务装饰器，用于自动记录任务日志
def log_task_execution(task_name: str = None):
    """
    任务执行日志装饰器
    
    Args:
        task_name: 任务名称，如果不提供则使用函数名
    """
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
