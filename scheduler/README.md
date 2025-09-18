# RC Project 调度器管理

这个文件夹包含了RC Project调度器服务的独立管理脚本。

## 📁 文件说明

- `start_scheduler.sh` - 启动调度器服务
- `stop_scheduler.sh` - 停止调度器服务  
- `restart_scheduler.sh` - 重启调度器服务
- `status_scheduler.sh` - 查看调度器状态
- `logs_scheduler.sh` - 查看调度器日志
- `README.md` - 本说明文件

## 🚀 快速开始

### 启动调度器
```bash
sudo ./scheduler/start_scheduler.sh
```

### 停止调度器
```bash
sudo ./scheduler/stop_scheduler.sh
```

### 重启调度器
```bash
sudo ./scheduler/restart_scheduler.sh
```

### 查看状态
```bash
sudo ./scheduler/status_scheduler.sh
```

### 查看日志
```bash
# 查看最后50行日志
sudo ./scheduler/logs_scheduler.sh

# 实时跟踪日志
sudo ./scheduler/logs_scheduler.sh -f

# 查看最后100行日志
sudo ./scheduler/logs_scheduler.sh -n 100

# 查看1小时前的日志
sudo ./scheduler/logs_scheduler.sh -s "1 hour ago"
```

## 🌐 服务信息

- **服务地址**: http://111.230.48.119:8001
- **API文档**: http://111.230.48.119:8001/scheduler-docs
- **健康检查**: http://111.230.48.119:8001/api/health
- **详细状态**: http://111.230.48.119:8001/api/status

## 🔧 系统服务管理

调度器作为systemd服务运行，服务名为 `rc-scheduler`。

### 常用命令
```bash
# 查看服务状态
systemctl status rc-scheduler

# 启动服务
systemctl start rc-scheduler

# 停止服务
systemctl stop rc-scheduler

# 重启服务
systemctl restart rc-scheduler

# 查看日志
journalctl -u rc-scheduler -f

# 查看服务配置
systemctl cat rc-scheduler
```

## 📊 监控和调试

### 端口检查
```bash
# 检查8001端口占用
lsof -i:8001

# 检查所有相关端口
netstat -tlnp | grep -E ":(8000|8001|3000|5432)"
```

### 进程检查
```bash
# 查看调度器进程
ps aux | grep scheduler_main.py

# 查看所有RC项目进程
ps aux | grep -E "(rc-|scheduler|uvicorn)"
```

### 日志分析
```bash
# 查看错误日志
journalctl -u rc-scheduler --since "1 hour ago" | grep -i error

# 查看警告日志
journalctl -u rc-scheduler --since "1 hour ago" | grep -i warn

# 查看特定时间段的日志
journalctl -u rc-scheduler --since "2024-01-01 00:00:00" --until "2024-01-01 23:59:59"
```

## ⚠️ 注意事项

1. **权限要求**: 所有脚本都需要root权限运行
2. **依赖服务**: 调度器依赖PostgreSQL数据库，启动前会检查数据库状态
3. **端口冲突**: 调度器使用8001端口，确保该端口未被其他服务占用
4. **日志轮转**: 系统会自动轮转日志，避免日志文件过大
5. **服务重启**: 如果调度器服务异常退出，systemd会自动重启

## 🆘 故障排除

### 常见问题

1. **调度器启动失败**
   - 检查PostgreSQL是否运行
   - 检查8001端口是否被占用
   - 查看错误日志: `journalctl -u rc-scheduler -n 50`

2. **API无响应**
   - 检查服务状态: `systemctl status rc-scheduler`
   - 检查端口占用: `lsof -i:8001`
   - 测试网络连接: `curl http://localhost:8001/api/health`

3. **任务不执行**
   - 检查调度器状态: `curl http://localhost:8001/api/status`
   - 查看任务配置是否正确
   - 检查数据库连接

### 联系支持

如果遇到无法解决的问题，请提供以下信息：
- 系统版本: `uname -a`
- 服务状态: `systemctl status rc-scheduler`
- 错误日志: `journalctl -u rc-scheduler -n 100`
- 端口占用: `lsof -i:8001`
