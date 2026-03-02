[Unit]
Description={{APP_NAME}} Web Application
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

ExecStart={{CONDA_ENV}}/bin/python -m uvicorn backend.main:app \
    --host 0.0.0.0 \
    --port {{WEBAPP_PORT}} \
    --workers {{UVICORN_WORKERS}}

Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

KillSignal=SIGTERM
TimeoutStopSec=30

LimitNOFILE=65536
LimitNPROC=4096

[Install]
WantedBy=multi-user.target
