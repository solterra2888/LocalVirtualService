# RC Project 本地开发环境停止脚本 (Windows PowerShell)
# 停止所有本地开发服务
# 用法: .\development_win\stop_local_dev.ps1

$ErrorActionPreference = "SilentlyContinue"
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$PROJECT_ROOT = Split-Path -Parent $SCRIPT_DIR

Write-Host "RC Project - 本地开发环境停止脚本" -ForegroundColor Red
Write-Host "=============================================="
Write-Host ""

function Stop-ProcessByPort {
    param([int]$Port, [string]$ServiceName)
    $conn = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue | Where-Object { $_.State -eq "Listen" }
    if ($conn) {
        $pids = $conn.OwningProcess | Sort-Object -Unique
        foreach ($pid in $pids) {
            Write-Host "停止 $ServiceName (端口 $Port, PID: $pid)..." -ForegroundColor Yellow
            Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
        }
        Write-Host "$ServiceName 已停止" -ForegroundColor Green
    } else {
        Write-Host "端口 $Port ($ServiceName) 没有活动进程" -ForegroundColor Gray
    }
}

# 按端口停止
Write-Host "检查并停止端口上的进程..." -ForegroundColor Blue
Stop-ProcessByPort -Port 3000 -ServiceName "前端应用"
Stop-ProcessByPort -Port 8000 -ServiceName "Web应用"
Stop-ProcessByPort -Port 5555 -ServiceName "Flower监控"
Stop-ProcessByPort -Port 6379 -ServiceName "Redis服务"

# 通过进程名清理
Write-Host ""
Write-Host "清理相关进程..." -ForegroundColor Blue

Get-Process -Name "node" -ErrorAction SilentlyContinue | Where-Object {
    try {
        $cmd = (Get-CimInstance Win32_Process -Filter "ProcessId=$($_.Id)").CommandLine
        $cmd -match "npm|react-scripts|webpack"
    } catch { $false }
} | ForEach-Object {
    Write-Host "停止 Node 进程 (PID: $($_.Id))..." -ForegroundColor Yellow
    Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
}

Get-Process | Where-Object {
    try {
        $cmd = (Get-CimInstance Win32_Process -Filter "ProcessId=$($_.Id)").CommandLine
        $cmd -match "uvicorn.*main:app|celery.*worker|celery.*flower"
    } catch { $false }
} | ForEach-Object {
    Write-Host "停止进程 (PID: $($_.Id)): $($_.ProcessName)..." -ForegroundColor Yellow
    Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
}

# Redis（Windows 上多为独立进程，端口已处理）
$redis = Get-Process -Name "redis-server" -ErrorAction SilentlyContinue
if ($redis) {
    Write-Host "停止 Redis 进程..." -ForegroundColor Yellow
    $redis | Stop-Process -Force -ErrorAction SilentlyContinue
    Write-Host "Redis 已停止" -ForegroundColor Green
}

# 最终检查
Write-Host ""
Write-Host "最终检查端口状态..." -ForegroundColor Blue
@(
    @(3000, "前端应用"),
    @(8000, "Web应用"),
    @(5555, "Flower监控"),
    @(6379, "Redis服务")
) | ForEach-Object {
    $port = $_[0]; $name = $_[1]
    $c = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue | Where-Object { $_.State -eq "Listen" }
    if ($c) { Write-Host "端口 $port ($name) 仍有进程在运行" -ForegroundColor Red }
    else    { Write-Host "端口 $port ($name) 已释放" -ForegroundColor Green }
}

Write-Host ""
Write-Host "开发服务已停止" -ForegroundColor Green
Write-Host "=============================================="
Write-Host "重新启动: .\development_win\start_webapp_dev.ps1 等脚本"
Write-Host "=============================================="
