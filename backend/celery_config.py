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
    'backend.tasks.theme_tasks.*': {'queue': 'ai_processing'},
    'backend.tasks.data_tasks.*': {'queue': 'data_fetching'},  # 🔥 新增：数据获取任务
    'backend.tasks.subscription_tasks.*': {'queue': 'data_fetching'},  # 🔥 新增：订阅任务（Twitter API获取）
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
    # Upload Link 链路单独走 priority 队列，让用户上传不被批量 Feed 任务阻塞
    # （HK Worker concurrency=1，批量任务实测要 30–60 min 串行处理，期间用户上传
    # 必须能"插队"到独立 worker。详见 deployment/local_virtual_service/README.md
    # 「上传插队队列」一节）。
    'fetch_youtube_file_transcript_task': {'queue': 'youtube_priority'},          # Upload Link 字幕抓取
    'transcribe_youtube_file_asr_task': {'queue': 'youtube_transcription_priority'},  # Upload Link ASR 转写
    'finalize_youtube_file_task': {'queue': 'file_processing'},  # Upload YouTube Link 完成回调（远程 Worker，触发 auto-tag/name/classify）
    'finalize_youtube_feed_task': {'queue': 'file_processing'},  # YouTube Feed 字幕落库回调（家用 Worker → 远程，触发 auto-feed-tagging）
    'tag_feed_content': {'queue': 'file_processing'},            # Feeds 单条内容自动打标 + 摘要
    'batch_tag_feed_content': {'queue': 'file_processing'},      # Feeds 批量打标派发器
    'personalize_feed_summary': {'queue': 'file_processing'},    # PR-6: Feeds per-user 个性化 summary
    'fan_out_personalize_summary': {'queue': 'file_processing'}, # PR-N2: tag 完成后扇出 per-user 个性化派发器
    # 内容主题匹配（tag 完成后链式触发）
    'match_content_themes_ticker': {'queue': 'file_processing'},  # Phase 1: ticker 重叠匹配（纯 SQL）
    'match_content_themes_vector': {'queue': 'ai_processing'},    # Phase 2: 向量语义匹配（Qwen embedding）
    'backfill_content_theme_matches': {'queue': 'ai_processing'}, # 批量回填历史内容
    'fetch_zhishixingqiu_subscription': {'queue': 'data_fetching'},  # 知识星球订阅抓取
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
    'check_hk_worker_health': {'queue': 'data_fetching'},  # HK YouTube Worker 心跳探测（主站消费）
    # 向量化任务 (OSS Vector Bucket / 语义检索)
    'vectorize_file_content': {'queue': 'ai_processing'},
    'vectorize_feed_content': {'queue': 'ai_processing'},
    'delete_file_vectors': {'queue': 'ai_processing'},
    'delete_feed_vectors': {'queue': 'ai_processing'},
    'delete_vectors_by_keys': {'queue': 'ai_processing'},
    'reconcile_vector_bucket': {'queue': 'ai_processing'},
    'generate_all_theme_reports': {'queue': 'data_fetching'},        # Theme Report 静态 HTML 生成
    'backfill_theme_report_eod_history': {'queue': 'data_fetching'}, # Theme Report EOD 历史回填
    'generate_all_market_weekly_reports': {'queue': 'data_fetching'},  # Market Weekly Report 批量静态 HTML 生成
    'generate_market_weekly_report': {'queue': 'data_fetching'},       # Market Weekly Report 单用户静态 HTML 生成
    # Daily Market Report — 批量 fan-out（所有用户）+ 单用户（手动/API）
    'generate_all_daily_market_reports': {'queue': 'data_fetching'},
    'generate_daily_market_report': {'queue': 'data_fetching'},
    # Ebook Map-Reduce 章节摘要任务
    'ebook_map_reduce': {'queue': 'ai_processing'},
    # The Close Summary — Bloomberg The Close 每日 PM Brief
    'generate_the_close_summary': {'queue': 'ai_processing'},
    # CNBC Closing Bell — CNBC Closing Bell 每日 Stock News
    'generate_cnbc_closing_bell_summary': {'queue': 'ai_processing'},
    # 知识星球每日摘要 — Phase 1 全局 + Phase 2 按用户
    'generate_zhishixingqiu_summary': {'queue': 'ai_processing'},
    'generate_all_zhishixingqiu_user_reports': {'queue': 'ai_processing'},
    'generate_zhishixingqiu_user_report': {'queue': 'ai_processing'},
    # Podcast 订阅抓取（公网 CDN，无需住宅 IP）
    'fetch_podcast_subscription': {'queue': 'data_fetching'},
    'fetch_all_podcast_subscriptions': {'queue': 'data_fetching'},
    'podcast_transcribe_episode': {'queue': 'data_fetching'},
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
    # 每天 04:00 (UTC) reconcile OSS Vector Bucket 与 PG document_chunks:
    # 删除孤儿向量, 重置 vector_status='done' 但实际 chunk 数为 0 的脏行.
    # 即使日常链路工作正常, 这里兜底处理任何 PG/VB 状态偏差.
    'reconcile-vector-bucket-daily': {
        'task': 'reconcile_vector_bucket',
        'schedule': crontab(hour=4, minute=0),
    },
    # 每 10 分钟探测 HK YouTube Worker 是否在线；连续 2 次未命中 (~20min) 发 Lark 告警
    'check-hk-worker-health': {
        'task': 'check_hk_worker_health',
        'schedule': crontab(minute='*/10'),
    },
    # Theme Report 改由 Scheduler Management「报告生成类」配置（默认每周一 14:00 CST）
}


