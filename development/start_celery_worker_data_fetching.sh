#!/bin/bash

# Celery Worker for Data Fetching Queue (Futu Themes, etc.)
# 专门处理数据爬取任务的Worker

# 获取脚本所在目录的父目录（项目根目录）
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# 切换到项目根目录
cd "$PROJECT_ROOT"

echo "=========================================="
echo "Starting Celery Worker - Data Fetching Queue"
echo "=========================================="
echo "Project Root: $PROJECT_ROOT"
echo "Queue: data_fetching"
echo "Concurrency: 1 (only 1 task at a time)"
echo "Log File: logs/celery_worker_data_fetching.log"
echo "=========================================="
echo ""

# 确保日志目录存在
mkdir -p logs

# 启动Celery Worker
# -Q data_fetching: 只处理data_fetching队列的任务
# --concurrency=1: 同时只运行1个任务（避免并发爬取导致IP被封）
# --max-tasks-per-child=10: 每个子进程最多处理10个任务后重启（防止内存泄漏）
# --loglevel=info: 日志级别
# --logfile: 日志文件路径
# -P solo: 使用solo pool（单进程模式，适合开发环境）

celery -A backend.celery_app worker \
    -Q data_fetching \
    --concurrency=1 \
    --max-tasks-per-child=10 \
    --loglevel=info \
    --logfile=logs/celery_worker_data_fetching.log \
    --pidfile=logs/celery_worker_data_fetching.pid \
    -n data_fetching_worker@%h

# Note: 
# - 这个Worker专门处理长时间运行的数据爬取任务（如Futu主题爬取，约1小时）
# - 使用concurrency=1避免并发爬取导致的问题
# - 如果需要更详细的日志，可以将loglevel改为debug
# - 生产环境建议使用systemd或supervisor管理Worker进程

