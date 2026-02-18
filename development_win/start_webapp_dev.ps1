# RC Project Web应用本地开发启动脚本 (Windows PowerShell)
# 用于本地开发环境
# 用法: 先 conda activate rcTool，再 .\development_win\start_webapp_dev.ps1

$ErrorActionPreference = "Stop"
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$PROJECT_ROOT = Split-Path -Parent $SCRIPT_DIR

Write-Host "RC Project - Web应用开发模式启动" -ForegroundColor Cyan
Write-Host "======================================"
Write-Host "项目根目录: $PROJECT_ROOT"
Write-Host ""

# 加载环境变量（从项目根目录的 .env 文件）
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
    Write-Host "可以从 example.env 复制: Copy-Item example.env .env"
    exit 1
}

# 检查Python环境
Write-Host ""
Write-Host "检查Python环境..."
$PYTHON_CMD = "python"
try {
    $null = & $PYTHON_CMD --version 2>&1
    Write-Host "使用当前Python: $PYTHON_CMD" -ForegroundColor Green
} catch {
    Write-Host "Python未安装或未在PATH中，请先 conda activate rcTool" -ForegroundColor Red
    exit 1
}
Write-Host "Python版本: $(& $PYTHON_CMD --version)"

# 检查项目根目录
Set-Location $PROJECT_ROOT

if (-not (Test-Path "backend\main.py")) {
    Write-Host "backend\main.py 不存在" -ForegroundColor Red
    exit 1
}

$env:PYTHONPATH = "$PROJECT_ROOT;$env:PYTHONPATH"

# 显示启动配置
Write-Host ""
Write-Host "测试数据库连接..."
Write-Host "  数据库地址: $($env:DB_HOST):$($env:DB_PORT)"
Write-Host "  数据库名称: $($env:DB_NAME)"
Write-Host ""
Write-Host "Web应用启动配置:"
Write-Host "  - 服务地址: http://localhost:8000"
Write-Host "  - API文档: http://localhost:8000/docs"
Write-Host "  - 按 Ctrl+C 停止服务"
Write-Host ""

# 启动Web应用（uvicorn，热重载）
& $PYTHON_CMD -m uvicorn backend.main:app `
    --host 0.0.0.0 `
    --port 8000 `
    --reload `
    --reload-dir backend `
    --reload-dir config `
    --log-level info `
    --access-log
