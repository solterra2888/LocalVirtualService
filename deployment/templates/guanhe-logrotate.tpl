# GuanHe 应用日志轮转（由 deploy.sh 安装到 /etc/logrotate.d/guanhe）
# 使用 copytruncate 避免多进程 rename 竞争；WatchedFileHandler 可感知文件变化。

{{LOG_DIR}}/app.log
{{LOG_DIR}}/celery_*.log {
    daily
    rotate {{LOG_FILE_DAYS}}
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
    dateext
    dateformat -%Y-%m-%d
}
