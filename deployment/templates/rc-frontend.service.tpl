[Unit]
Description={{APP_NAME}} Frontend Application
After=network.target rc-webapp.service
Wants=rc-webapp.service

[Service]
Type=simple
User=root
Group=root
WorkingDirectory={{PROJECT_DIR}}/web-frontend

Environment=PATH={{VOLTA_DIR}}:/usr/local/bin:/usr/bin:/bin
Environment=BROWSER=none
Environment=REACT_APP_API_BASE_URL=/api
Environment=REACT_APP_SCHEDULER_BASE_URL=/scheduler
Environment=REACT_APP_APP_NAME={{APP_NAME}}
Environment=REACT_APP_APP_VERSION={{APP_VERSION}}
Environment=REACT_APP_RC_AGENT_BASE_URL={{RC_AGENT_BASE_URL}}

ExecStart={{VOLTA_DIR}}/npm start

Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

KillSignal=SIGTERM
TimeoutStopSec=15

LimitNOFILE=65536
LimitNPROC=4096

[Install]
WantedBy=multi-user.target
