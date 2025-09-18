# RC Project 本地开发环境指南

> 🚀 本指南帮助您在本地开发环境中运行RC Project，同时连接远程数据库

## 📋 概述

本地开发环境配置允许您：
- ✅ 在本地运行前端应用 (React)
- ✅ 在本地运行Web应用 (FastAPI)
- ✅ 在本地运行调度器服务
- ✅ 连接远程数据库 (111.230.48.119)
- ✅ 支持热重载和实时调试

## 🏗️ 架构对比

### 生产环境 (111.230.48.119)
```
远程服务器 (111.230.48.119)
├── PostgreSQL (localhost:5432)
├── 调度器 (localhost:8001)
├── Web应用 (localhost:8000)
├── 前端应用 (localhost:3000)
└── Nginx (80端口)
```

### 开发环境 (本地)
```
本地开发机器
├── 调度器 (localhost:8001) ────┐
├── Web应用 (localhost:8000) ────┼─── 连接 ──→ 远程PostgreSQL (111.230.48.119:5432)
└── 前端应用 (localhost:3000) ───┘
```

## 🔧 环境要求

### 必需软件
- **Python 3.8+** (推荐使用conda环境: `/Users/kevinliu/anaconda3/envs/rcTool`)
- **Node.js 16+** 和 **npm**
- **网络连接** 到远程数据库服务器

### 端口要求
确保以下端口未被占用：
- `3000` - 前端开发服务器
- `8000` - Web应用API服务器
- `8001` - 调度器服务器

## 🚀 快速开始

### 1. 一键启动所有服务
```bash
cd /home/kevin/rcproject
./development/start_local_dev.sh
```

### 2. 分别启动单个服务
```bash
# 启动调度器
./development/start_scheduler_dev.sh

# 启动Web应用
./development/start_webapp_dev.sh

# 启动前端应用
./development/start_frontend_dev.sh
```

### 3. 停止所有服务
```bash
./development/stop_local_dev.sh
```

## 🌐 访问地址

启动成功后，可通过以下地址访问服务：

| 服务 | 地址 | 说明 |
|------|------|------|
| 前端应用 | http://localhost:3000 | React开发服务器 |
| Web应用 | http://localhost:8000 | FastAPI应用 |
| API文档 | http://localhost:8000/docs | Swagger文档 |
| 调度器 | http://localhost:8001 | 独立调度器服务 |
| 调度器文档 | http://localhost:8001/scheduler-docs | 调度器API文档 |

## ⚙️ 配置说明

### 环境变量文件
配置文件位置：`development/env.development`

主要配置项：
```bash
# 数据库连接 (远程)
DB_HOST=111.230.48.119
DB_PORT=5432
DB_NAME=rc_database

# 前端API地址 (本地)
REACT_APP_API_BASE_URL=http://localhost:8000/api
REACT_APP_SCHEDULER_BASE_URL=http://localhost:8001/api

# 部署模式
DEPLOYMENT_MODE=development
```

### 与生产环境的差异
| 配置项 | 生产环境 | 开发环境 |
|--------|----------|----------|
| DB_HOST | localhost | 111.230.48.119 |
| 前端API地址 | http://111.230.48.119/api | http://localhost:8000/api |
| 部署模式 | unified | development |
| 热重载 | 关闭 | 开启 |

## 📝 日志管理

### 日志文件位置
```
development/logs/
├── scheduler.log    # 调度器日志
├── webapp.log       # Web应用日志
└── frontend.log     # 前端应用日志
```

### 查看实时日志
```bash
# 查看所有日志
tail -f development/logs/*.log

# 查看特定服务日志
tail -f development/logs/webapp.log
tail -f development/logs/scheduler.log
tail -f development/logs/frontend.log
```

## 🔍 故障排除

### 常见问题

#### 1. 端口被占用
```bash
# 检查端口占用
lsof -i :3000
lsof -i :8000
lsof -i :8001

# 停止所有开发服务
./development/stop_local_dev.sh
```

#### 2. 数据库连接失败
```bash
# 检查网络连接
ping 111.230.48.119

# 检查数据库连接
telnet 111.230.48.119 5432
```

#### 3. Python环境问题
```bash
# 检查conda环境
/Users/kevinliu/anaconda3/envs/rcTool/bin/python --version

# 激活conda环境
conda activate rcTool
```

#### 4. Node.js依赖问题
```bash
# 重新安装依赖
cd web-frontend
rm -rf node_modules
npm install
```

### 手动进程管理
```bash
# 查看相关进程
ps aux | grep -E 'uvicorn|npm|scheduler_main'

# 手动停止进程
pkill -f uvicorn
pkill -f "npm start"
pkill -f scheduler_main
```

## 🛠️ 开发工作流

### 1. 代码修改流程
1. 修改代码
2. 服务自动重载（Web应用和前端支持热重载）
3. 浏览器自动刷新或手动刷新

### 2. 调试建议
- **前端调试**: 使用浏览器开发者工具
- **API调试**: 访问 http://localhost:8000/docs
- **调度器调试**: 访问 http://localhost:8001/scheduler-docs
- **日志调试**: 实时查看 `development/logs/` 下的日志文件

### 3. 数据库操作
```bash
# 连接远程数据库
psql -h 111.230.48.119 -p 5432 -U postgres -d rc_database
```

## 🔒 安全注意事项

1. **数据库连接**: 确保远程数据库允许您的IP连接
2. **防火墙**: 检查本地防火墙设置
3. **敏感信息**: 不要提交包含密码的配置文件到版本控制
4. **网络安全**: 考虑使用VPN或SSH隧道

## 📚 相关文档

- [项目主README](../README.md)
- [生产环境部署指南](../deployment/)
- [API文档](http://localhost:8000/docs) (启动后访问)
- [调度器文档](http://localhost:8001/scheduler-docs) (启动后访问)

## 🤝 团队协作

### 开发环境隔离
- 每个开发者使用独立的本地环境
- 共享远程数据库（注意数据同步）
- 避免同时修改数据库结构

### 版本控制
```bash
# 生产环境启动脚本
sudo ./start_services.sh          # 不要修改

# 开发环境启动脚本  
./development/start_local_dev.sh  # 可以自定义
```

## 📞 获取帮助

如遇问题，请：
1. 查看日志文件 `development/logs/`
2. 检查环境配置 `development/env.development`
3. 参考故障排除章节
4. 联系项目维护者

---

**Happy Coding! 🎉**
