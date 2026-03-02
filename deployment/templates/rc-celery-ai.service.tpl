[Unit]
Description={{APP_NAME}} Celery Worker - AI Processing Queue
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

ExecStart={{CONDA_ENV}}/bin/celery -A backend.celery_app worker \
    --loglevel=info \
    --concurrency={{CELERY_AI_CONCURRENCY}} \
    --queues=file_processing,ai_processing \
    --hostname=ai-worker@%h \
    --prefetch-multiplier=1 \
    --logfile={{LOG_DIR}}/celery_ai_worker.log \
    --pidfile={{LOG_DIR}}/celery_ai_worker.pid

Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

LimitNOFILE=65536
LimitNPROC=4096

KillSignal=SIGTERM
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
