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

ExecStart={{CONDA_ENV}}/bin/celery -A backend.celery_app worker \
    --loglevel=info \
    --concurrency={{CELERY_DATA_CONCURRENCY}} \
    --queues=data_fetching \
    --hostname=data-worker@%h \
    --max-tasks-per-child=10 \
    --prefetch-multiplier=1 \
    --logfile={{LOG_DIR}}/celery_data_worker.log \
    --pidfile={{LOG_DIR}}/celery_data_worker.pid

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
