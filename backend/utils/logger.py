# -*- coding: utf-8 -*-
"""
统一日志工具模块
"""

import os
import sys
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any


class UnifiedLogger:
    """统一日志管理类"""
    
    def __init__(self, name: str = "universal_glance"):
        self.name = name
        self.logger = logging.getLogger(name)
        self._initialized = False
        
    def initialize(self, 
                  level: str = "INFO",
                  log_to_file: bool = True,
                  log_to_console: bool = True,
                  log_file_days: int = 30,
                  log_dir: str = "logs"):
        """
        初始化日志配置
        
        Args:
            level: 日志级别 (DEBUG, INFO, WARN, ERROR)
            log_to_file: 是否输出到文件
            log_to_console: 是否输出到控制台
            log_file_days: 日志文件保留天数
            log_dir: 日志目录
        """
        if self._initialized:
            return
            
        # 设置日志级别
        log_level = getattr(logging, level.upper(), logging.INFO)
        self.logger.setLevel(log_level)
        
        # 清除已有的handlers
        self.logger.handlers.clear()
        
        # 定义日志格式
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 控制台输出
        if log_to_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # 文件输出
        if log_to_file:
            # 确保日志目录存在
            log_path = Path(log_dir)
            log_path.mkdir(exist_ok=True)
            
            # 配置按天轮转的文件处理器
            log_file = log_path / "app.log"
            file_handler = logging.handlers.TimedRotatingFileHandler(
                filename=log_file,
                when='midnight',
                interval=1,
                backupCount=log_file_days,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            file_handler.suffix = "%Y-%m-%d"  # 备份文件后缀格式
            self.logger.addHandler(file_handler)
        
        self._initialized = True
        self.info(f"日志系统初始化完成 - 级别: {level}, 文件输出: {log_to_file}, 控制台输出: {log_to_console}")
    
    def debug(self, message: str, source: str = None, **kwargs):
        """调试级别日志"""
        self._log(logging.DEBUG, message, source, **kwargs)
    
    def info(self, message: str, source: str = None, **kwargs):
        """信息级别日志"""
        self._log(logging.INFO, message, source, **kwargs)
    
    def warning(self, message: str, source: str = None, **kwargs):
        """警告级别日志"""
        self._log(logging.WARNING, message, source, **kwargs)
    
    def error(self, message: str, source: str = None, **kwargs):
        """错误级别日志"""
        self._log(logging.ERROR, message, source, **kwargs)
    
    def _log(self, level: int, message: str, source: str = None, **kwargs):
        """内部日志记录方法"""
        if not self._initialized:
            # 如果未初始化，使用默认配置
            self.initialize()
        
        # 格式化消息
        if source:
            formatted_message = f"[{source}] {message}"
        else:
            formatted_message = message
        
        # 添加额外信息
        if kwargs:
            extra_info = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            formatted_message += f" | {extra_info}"
        
        self.logger.log(level, formatted_message)
    
    def log_task_start(self, task_id: str, task_name: str, files_count: int = 0):
        """记录任务开始日志"""
        self.info(f"任务开始: {task_name}", source="task", 
                 task_id=task_id, files_count=files_count)
    
    def log_task_progress(self, task_id: str, current_file: str, progress: float):
        """记录任务进度日志"""
        self.debug(f"处理进度: {progress:.1f}% - {current_file}", 
                  source="task", task_id=task_id)
    
    def log_task_complete(self, task_id: str, files_processed: int, files_failed: int, 
                         total_time: float):
        """记录任务完成日志"""
        self.info(f"任务完成 - 成功: {files_processed}, 失败: {files_failed}, "
                 f"耗时: {total_time:.2f}秒", source="task", task_id=task_id)
    
    def log_file_processing(self, filename: str, status: str, processing_time: float = None,
                           error_message: str = None):
        """记录文件处理日志"""
        if status == "success":
            self.info(f"文件处理成功: {filename}", source="file_processor",
                     processing_time=processing_time)
        elif status == "failed":
            self.error(f"文件处理失败: {filename} - {error_message}", 
                      source="file_processor")
        elif status == "start":
            self.debug(f"开始处理文件: {filename}", source="file_processor")
    
    def log_api_request(self, endpoint: str, method: str, user: str = None, 
                       status_code: int = None, response_time: float = None):
        """记录API请求日志"""
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
        """LlamaService专用日志方法，兼容现有回调"""
        log_method = getattr(self, level.lower(), self.info)
        log_method(message, source="llama")
    
    def log_rc_agent_service(self, message: str, level: str = "info"):
        """RC Agent服务专用日志方法，兼容现有回调"""
        log_method = getattr(self, level.lower(), self.info)
        log_method(message, source="rc_agent")


# 全局日志实例
logger = UnifiedLogger()


def get_logger(name: str = None) -> UnifiedLogger:
    """获取日志实例"""
    if name:
        return UnifiedLogger(name)
    return logger


def init_logging(**kwargs):
    """初始化全局日志配置"""
    logger.initialize(**kwargs)


# 便捷函数
def debug(message: str, source: str = None, **kwargs):
    """调试日志"""
    logger.debug(message, source, **kwargs)


def info(message: str, source: str = None, **kwargs):
    """信息日志"""
    logger.info(message, source, **kwargs)


def warning(message: str, source: str = None, **kwargs):
    """警告日志"""
    logger.warning(message, source, **kwargs)


def error(message: str, source: str = None, **kwargs):
    """错误日志"""
    logger.error(message, source, **kwargs) 