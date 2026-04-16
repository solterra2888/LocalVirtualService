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

# Celery 6.0+ 兼容性配置
broker_connection_retry_on_startup = True  # 启动时重试broker连接
broker_connection_retry = True  # 保持向后兼容

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
    'backend.tasks.theme_tasks.*': {'queue': 'ai_processing'},
    'backend.tasks.data_tasks.*': {'queue': 'data_fetching'},  # 🔥 新增：数据获取任务
    'backend.tasks.subscription_tasks.*': {'queue': 'data_fetching'},  # 🔥 新增：订阅任务（Twitter API获取）
    # 明确指定新闻分析任务（使用任务名称而不是模块路径）
    'analyze_news_batch': {'queue': 'ai_processing'},
    'analyze_unanalyzed_news': {'queue': 'ai_processing'},
    # 明确指定投资主题任务
    'initialize_theme_embeddings': {'queue': 'ai_processing'},
    'fill_theme_descriptions': {'queue': 'ai_processing'},
    'regenerate_all_theme_embeddings': {'queue': 'ai_processing'},
    # 长时间运行的数据获取任务（data_fetching队列）
    'fetch_futu_themes': {'queue': 'data_fetching'},
    'fetch_eodhd_fundamentals': {'queue': 'data_fetching'},  # 🔥 新增：基本面数据获取
    'update_single_ticker_fundamentals': {'queue': 'data_fetching'},  # 🔥 新增：单个ticker基本面数据更新
    'update_watchlist_fundamentals_batch': {'queue': 'data_fetching'},  # 🔥 新增：Watchlist批量基本面数据更新
    'fetch_eodhd_eod_stock_prices': {'queue': 'data_fetching'},  # 🔥 新增：历史股价数据获取
    'fetch_single_ticker_eod_history': {'queue': 'data_fetching'},  # 🔥 新增：单个ticker历史数据获取
    'fetch_earnings_calendar': {'queue': 'data_fetching'},  # 🔥 新增：财报日历数据获取（13000+条记录）
    'fetch_ipo_calendar': {'queue': 'data_fetching'},  # 🔥 新增：IPO日历数据获取
    # 订阅相关任务（data_fetching队列）
    'fetch_twitter_subscription': {'queue': 'data_fetching'},  # Twitter订阅内容获取
    'fetch_youtube_subscription': {'queue': 'youtube_fetching'},  # YouTube订阅内容获取（远程 Worker）；创建订阅时由 API 显式 override 到 data_fetching
    'fetch_all_youtube_subscriptions': {'queue': 'youtube_fetching'},  # YouTube批量订阅获取（远程 Worker，Scheduler 定时调用）
    'fetch_youtube_transcripts_batch': {'queue': 'youtube_fetching'},  # YouTube transcript 字幕拉取（远程 Worker，住宅 IP）
    'transcribe_youtube_feed_task': {'queue': 'youtube_transcription'},  # YouTube Feed ASR 转写（本地虚拟机 Worker）
    'transcribe_youtube_file_asr_task': {'queue': 'youtube_transcription'},  # Upload YouTube Link ASR 转写（本地虚拟机 Worker）
    'add_user_to_existing_twitter_content': {'queue': 'data_fetching'},
    'smart_wait_and_add_user': {'queue': 'data_fetching'},
    'generate_author_summary': {'queue': 'ai_processing'},
    'generate_all_author_summaries': {'queue': 'ai_processing'},
    'generate_twitter_daily_report': {'queue': 'ai_processing'},
    'generate_all_twitter_daily_reports': {'queue': 'ai_processing'},
    'generate_yt_author_summary': {'queue': 'ai_processing'},
    'generate_all_yt_author_summaries': {'queue': 'ai_processing'},
    'generate_youtube_daily_report': {'queue': 'ai_processing'},
    'generate_all_youtube_daily_reports': {'queue': 'ai_processing'},
    'cleanup_llm_usage_logs': {'queue': 'data_fetching'},
}

# Worker配置
worker_prefetch_multiplier = 1  # 防止内存占用过多
task_acks_late = True  # 任务完成后确认
worker_max_tasks_per_child = 1000  # 防止内存泄漏

# 任务时间限制
task_time_limit = 7200  # 2小时硬限制（Futu主题爬取需要约1小时）
task_soft_time_limit = 6600  # 110分钟软限制

# 结果过期时间
result_expires = 3600  # 1小时后过期

# 日志配置
worker_log_format = '[%(asctime)s: %(levelname)s/%(processName)s] %(message)s'
worker_task_log_format = '[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s'

# 日志文件配置
worker_log_file = 'logs/celery_worker.log'
worker_logfile = worker_log_file

# 任务日志配置
task_log_file = 'logs/celery_tasks.log'
task_logfile = task_log_file

# 日志级别
worker_log_level = 'INFO'
task_log_level = 'INFO'

# 日志轮转配置
worker_log_max_bytes = 10 * 1024 * 1024  # 10MB
worker_log_backup_count = 5

# Celery Beat 定时任务
from celery.schedules import crontab

beat_schedule = {
    'cleanup-llm-usage-logs-daily': {
        'task': 'cleanup_llm_usage_logs',
        'schedule': crontab(hour=3, minute=0),  # 每天凌晨 3:00 执行
        'kwargs': {'retain_days': 7},
    },
}


