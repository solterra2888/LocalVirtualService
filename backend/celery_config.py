# -*- coding: utf-8 -*-
"""
Celery配置文件
"""
import os
from backend.utils.env_loader import load_project_env

# 加载项目环境变量
load_project_env()

# Broker和结果后端配置
broker_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
result_backend = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# 任务序列化配置
task_serializer = 'json'
accept_content = ['json']
result_serializer = 'json'

# 异常处理配置
result_backend_transport_options = {
    'master_name': 'mymaster',
    'visibility_timeout': 3600,
}

# 确保异常信息可以被正确序列化
task_ignore_result = False
result_expires = 3600

# 时区配置
timezone = 'Asia/Shanghai'
enable_utc = True

# 任务路由配置
task_routes = {
    'backend.tasks.file_tasks.*': {'queue': 'file_processing'},
    'backend.tasks.news_tasks.*': {'queue': 'ai_processing'},
    # 明确指定新闻分析任务（使用任务名称而不是模块路径）
    'analyze_news_batch': {'queue': 'ai_processing'},
    'analyze_unanalyzed_news': {'queue': 'ai_processing'},
}

# Worker配置
worker_prefetch_multiplier = 1  # 防止内存占用过多
task_acks_late = True  # 任务完成后确认
worker_max_tasks_per_child = 1000  # 防止内存泄漏

# 任务时间限制
task_time_limit = 3600  # 1小时硬限制
task_soft_time_limit = 3000  # 50分钟软限制

# 结果过期时间
result_expires = 3600  # 1小时后过期


