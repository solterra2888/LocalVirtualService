[Unit]
Description={{APP_NAME}} Celery Worker - ZSXQ File Download/Upload Queue (single concurrency)
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

# 单并发（concurrency=1）+ 单队列：知识星球附件文件下载/上传专用 worker。
# 单线程天然限速，降低 ZSXQ 封号风险；大文件 mp3 串行处理。
ExecStart={{CONDA_ENV}}/bin/celery -A backend.celery_app worker \
    --loglevel=info \
    --concurrency=1 \
    --queues=zsxq_file_processing \
    --hostname=zsxq-files-worker@%h \
    --max-tasks-per-child=50 \
    --prefetch-multiplier=1 \
    --logfile={{LOG_DIR}}/celery_zsxq_files_worker.log \
    --pidfile={{LOG_DIR}}/celery_zsxq_files_worker.pid

Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

LimitNOFILE=65536
LimitNPROC=4096

# 停止时给 worker 时间走完优雅关闭（下载/上传中的任务可被 reaper 回退续传）
KillSignal=SIGTERM
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
