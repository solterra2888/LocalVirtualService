[Unit]
Description={{APP_NAME}} Celery Worker - Data Fetching Queue
After=network.target postgresql.service redis.service
Wants=postgresql.service redis.service

[Service]
Type=simple
User=root
Group=root
WorkingDirectory={{PROJECT_DIR}}

EnvironmentFile={{PROJECT_DIR}}/.env

Environment=PATH={{CONDA_ENV}}/bin:/usr/local/bin:/usr/bin:/bin
Environment=PYTHONPATH={{PROJECT_DIR}}

# 每次启动（含 systemd 自动重启）前清理 Redis 旧 task meta，
# 防止损坏记录导致 worker 启动即崩、触发无限重启循环。
# 前缀 - 表示失败时不阻止服务启动。
ExecStartPre=-{{CONDA_ENV}}/bin/python -c "\
import redis, os, sys; \
url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0'); \
r = redis.from_url(url, socket_connect_timeout=3); \
keys = r.keys('celery-task-meta-*') + r.keys('unacked*'); \
[r.delete(k) for k in keys]; \
print(f'[pre-start] cleared {len(keys)} stale task meta keys')"

# Worker CLI --time-limit 会覆盖 @task(time_limit=...)。
# form13f_import 流式导入约 10-20 分钟，须与 celery_config.task_time_limit=7200 对齐。
ExecStart={{CONDA_ENV}}/bin/celery -A backend.celery_app worker \
    --loglevel=info \
    --concurrency={{CELERY_DATA_CONCURRENCY}} \
    --queues=data_fetching \
    --hostname=data-worker@%h \
    --max-tasks-per-child=10 \
    --prefetch-multiplier=1 \
    --soft-time-limit=7200 \
    --time-limit=7260 \
    --logfile={{LOG_DIR}}/celery_data_worker.log \
    --pidfile={{LOG_DIR}}/celery_data_worker.pid

Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

LimitNOFILE=65536
LimitNPROC=4096

# 关键：停止时给 worker 足够时间走完优雅关闭流程
# - SIGTERM → worker 等当前任务完成 → 正确写 result → 再退出
# - 30s 太短，LLM 长任务（Claude/qwen3-max 长文本）会被 SIGKILL 腰斩，
#   导致 Redis 中留下损坏 meta，触发下次启动崩溃循环
KillSignal=SIGTERM
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
