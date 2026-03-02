[Unit]
Description={{APP_NAME}} Scheduler Service
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

ExecStart={{CONDA_ENV}}/bin/python {{PROJECT_DIR}}/scheduler_main.py

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
