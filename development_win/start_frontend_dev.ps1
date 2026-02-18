# RC Project 前端应用本地开发启动脚本 (Windows PowerShell)
# 用于本地开发环境
# 用法: .\development_win\start_frontend_dev.ps1

$ErrorActionPreference = "Stop"
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$PROJECT_ROOT = Split-Path -Parent $SCRIPT_DIR
$FRONTEND_DIR = Join-Path $PROJECT_ROOT "web-frontend"

Write-Host "RC Project - 前端应用开发模式启动" -ForegroundColor Cyan
Write-Host "=========================================="
Write-Host "项目根目录: $PROJECT_ROOT"
Write-Host "前端目录: $FRONTEND_DIR"
Write-Host ""

if (-not (Test-Path $FRONTEND_DIR)) {
    Write-Host "前端目录不存在: $FRONTEND_DIR" -ForegroundColor Red
    exit 1
}

# 加载开发环境变量（可选）
$ENV_FILE = Join-Path $SCRIPT_DIR "..\development\env.development"
if (-not (Test-Path $ENV_FILE)) { $ENV_FILE = Join-Path $PROJECT_ROOT ".env.development" }
if (Test-Path $ENV_FILE) {
    Write-Host "加载开发环境配置: $ENV_FILE"
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
    Write-Host "未找到开发环境配置文件，使用默认配置" -ForegroundColor Yellow
}

# 检查 Node.js / npm
Write-Host ""
Write-Host "检查前端环境..."
try {
    $null = Get-Command node -ErrorAction Stop
    $null = Get-Command npm -ErrorAction Stop
} catch {
    Write-Host "Node.js 或 npm 未安装或未在 PATH 中" -ForegroundColor Red
    exit 1
}
Write-Host "Node.js: $(node --version)" -ForegroundColor Green
Write-Host "npm: $(npm --version)" -ForegroundColor Green

Set-Location $FRONTEND_DIR

if (-not (Test-Path "package.json")) {
    Write-Host "package.json 不存在" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path "node_modules")) {
    Write-Host ""
    Write-Host "安装前端依赖..."
    npm install
    Write-Host "依赖安装完成" -ForegroundColor Green
}

$env:PORT = "3000"
$env:BROWSER = if ($env:BROWSER) { $env:BROWSER } else { "none" }
$env:REACT_APP_API_BASE_URL = if ($env:REACT_APP_API_BASE_URL) { $env:REACT_APP_API_BASE_URL } else { "http://localhost:8000/api" }
$env:REACT_APP_SCHEDULER_BASE_URL = if ($env:REACT_APP_SCHEDULER_BASE_URL) { $env:REACT_APP_SCHEDULER_BASE_URL } else { "http://localhost:8001/api" }

Write-Host ""
Write-Host "前端启动配置:"
Write-Host "  - API地址: $($env:REACT_APP_API_BASE_URL)"
Write-Host "  - 开发服务器: http://localhost:3000"
Write-Host "  - 按 Ctrl+C 停止服务"
Write-Host ""

npm start
