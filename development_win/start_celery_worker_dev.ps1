# RC Project Celery Worker 开发启动脚本 (Windows PowerShell)
# 用于本地开发环境启动 Celery Worker
# 用法: 先 conda activate rcTool，再 .\development_win\start_celery_worker_dev.ps1

$ErrorActionPreference = "Stop"
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$PROJECT_ROOT = Split-Path -Parent $SCRIPT_DIR

Write-Host "RC Project - Celery Worker 开发模式启动" -ForegroundColor Cyan
Write-Host "=============================================="
Write-Host "项目根目录: $PROJECT_ROOT"
Write-Host ""

# 加载环境变量
$ENV_FILE = Join-Path $PROJECT_ROOT ".env"
if (Test-Path $ENV_FILE) {
    Write-Host "加载环境配置: $ENV_FILE"
    Get-Content $ENV_FILE | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#") -and $line -match "^([^#=]+)=(.*)$") {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim().Trim('"').Trim("'")
            Set-Item -Path "Env:$key" -Value $value
        }
    }
    Write-Host "环境变量加载完成" -ForegroundColor Green
} else {
    Write-Host "环境配置文件不存在: $ENV_FILE" -ForegroundColor Yellow
    Write-Host "请确保项目根目录有 .env 文件"
    exit 1
}

$PYTHON_CMD = "python"
try {
    $null = & $PYTHON_CMD --version 2>&1
} catch {
    Write-Host "Python未安装或未在PATH中，请先 conda activate rcTool" -ForegroundColor Red
    exit 1
}
Write-Host "Python版本: $(& $PYTHON_CMD --version)" -ForegroundColor Green

Set-Location $PROJECT_ROOT
$env:PYTHONPATH = "$PROJECT_ROOT;$env:PYTHONPATH"

# 检查 Redis
$REDIS_URL = if ($env:REDIS_URL) { $env:REDIS_URL } else { "redis://localhost:6379/0" }
Write-Host ""
Write-Host "检查Redis服务... Redis地址: $REDIS_URL"
$redisTest = @"
import redis
try:
    r = redis.from_url('$REDIS_URL')
    if r.ping():
        print('OK')
    else:
        exit(1)
except Exception as e:
    print(e)
    exit(1)
"@
try {
    $out = & $PYTHON_CMD -c $redisTest 2>&1
    if ($LASTEXITCODE -ne 0) { throw $out }
    Write-Host "Redis服务正常" -ForegroundColor Green
} catch {
    Write-Host "Redis服务不可用，请先启动 Redis（如 Windows 版或 WSL）" -ForegroundColor Red
    exit 1
}

# 检查 Celery 配置（当前已在 PROJECT_ROOT，PYTHONPATH 已设置）
Write-Host ""
Write-Host "检查Celery配置..."
$celeryCheck = @'
try:
    from backend.celery_app import app as celery_app
    from backend.celery_config import broker_url
    print("OK", broker_url)
except Exception as e:
    print(e)
    exit(1)
'@
try {
    $out = & $PYTHON_CMD -c $celeryCheck 2>&1
    if ($LASTEXITCODE -ne 0) { throw $out }
    Write-Host "Celery配置正常" -ForegroundColor Green
} catch {
    Write-Host "Celery配置错误: $_" -ForegroundColor Red
    exit 1
}

# 创建日志目录
$LOG_DIR = Join-Path $PROJECT_ROOT "logs"
if (-not (Test-Path $LOG_DIR)) {
    New-Item -ItemType Directory -Path $LOG_DIR | Out-Null
    Write-Host "已创建日志目录: $LOG_DIR" -ForegroundColor Green
}

$CELERY_LOG_LEVEL = if ($env:CELERY_LOG_LEVEL) { $env:CELERY_LOG_LEVEL } else { "info" }
$CELERY_CONCURRENCY = if ($env:CELERY_CONCURRENCY) { $env:CELERY_CONCURRENCY } else { "2" }

Write-Host ""
Write-Host "Celery Worker 启动配置:"
Write-Host "  - Worker数量: $CELERY_CONCURRENCY"
Write-Host "  - 日志级别: $CELERY_LOG_LEVEL"
Write-Host "  - 队列: file_processing, ai_processing"
Write-Host "  - 按 Ctrl+C 停止服务"
Write-Host ""

& $PYTHON_CMD -m celery -A backend.celery_app worker `
    --loglevel=$CELERY_LOG_LEVEL `
    --concurrency=$CELERY_CONCURRENCY `
    --queues=file_processing,ai_processing `
    --hostname=dev-worker@%h `
    --logfile="$LOG_DIR\celery_worker.log" `
    --pidfile="$LOG_DIR\celery_worker.pid"
