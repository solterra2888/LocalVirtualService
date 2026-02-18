# Windows 开发脚本说明

本目录为 Linux/macOS 下 `development/*.sh` 的 **Windows PowerShell** 版本，在 PowerShell 中运行。

## 为什么 .sh 在 Windows 没反应？

`.sh` 是 Bash 脚本，PowerShell 不会直接执行。请使用本目录下的 `.ps1` 脚本。

## 使用方法

在项目根目录打开 **PowerShell**，然后：

### 1. 启动 Web 应用（后端）

```powershell
conda activate rcTool
.\development_win\start_webapp_dev.ps1
```

### 2. 启动前端（另开一个终端）

```powershell
.\development_win\start_frontend_dev.ps1
```

### 3. 启动 Celery Worker（另开一个终端，需先有 Redis）

```powershell
conda activate rcTool
.\development_win\start_celery_worker_dev.ps1
```

### 4. 停止所有本地开发服务

```powershell
.\development_win\stop_local_dev.ps1
```

## 执行策略

若提示“无法加载，因为在此系统上禁止运行脚本”，请先执行：

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## 依赖

- 项目根目录有 `.env` 文件（可从 `example.env` 复制）
- Web / Celery：已激活 conda 环境 `rcTool`
- 前端：已安装 Node.js 和 npm
- Celery：本机或可访问的 Redis 服务
