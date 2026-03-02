# GuanHe Project 新服务器部署手册

> 本文档覆盖从一台全新 Ubuntu 服务器到项目完全运行的每一步操作。

---

## 0. 环境要求

| 项目 | 最低要求 | 推荐 |
|------|----------|------|
| 操作系统 | Ubuntu 20.04+ | Ubuntu 22.04 LTS |
| CPU | 2 核 | 4 核 |
| 内存 | 4 GB | 8 GB |
| 磁盘 | 20 GB | 50 GB |
| 网络 | 开放 80 端口 | 80 + 443 |

---

## 1. 基础软件安装

以 root 用户登录服务器，依次执行：

### 1.1 系统更新

```bash
apt update && apt upgrade -y
```

### 1.2 安装 Docker

```bash
curl -fsSL https://get.docker.com | sh
systemctl enable docker && systemctl start docker
```

### 1.3 安装 Nginx

```bash
apt install -y nginx
```

### 1.4 安装 Miniconda（Python 环境管理）

```bash
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -b -p /root/miniconda3
eval "$(/root/miniconda3/bin/conda init bash)"
source ~/.bashrc
```

### 1.5 安装 Volta（Node.js 版本管理）

```bash
curl https://get.volta.sh | bash
source ~/.bashrc
volta install node@24
```

---

## 2. 创建数据库和缓存容器

### 2.1 PostgreSQL

```bash
mkdir -p /opt/postgres/data

docker run -d \
  --name postgres \
  --restart always \
  -e POSTGRES_PASSWORD=你的数据库密码 \
  -v /opt/postgres/data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:17
```

等待几秒后，创建项目数据库：

```bash
docker exec -it postgres psql -U postgres -c "CREATE DATABASE 你的数据库名;"
```

### 2.2 Redis

```bash
mkdir -p /opt/redis/conf /opt/redis/data

# 创建 Redis 配置文件（设置密码）
cat > /opt/redis/conf/redis.conf << 'EOF'
requirepass 你的Redis密码
appendonly yes
EOF

docker run -d \
  --name redis \
  --restart always \
  -v /opt/redis/conf:/usr/local/etc/redis/conf \
  -v /opt/redis/data:/data \
  -v /etc/localtime:/etc/localtime:ro \
  -p 6379:6379 \
  redis:7.4.6-alpine redis-server /usr/local/etc/redis/conf/redis.conf
```

### 2.3 验证容器运行

```bash
docker ps
# 应看到 postgres 和 redis 两个容器状态为 Up
```

---

## 3. 部署项目代码

### 3.1 克隆仓库

```bash
cd /opt
git clone <你的仓库地址> guanhetech
cd guanhetech
```

### 3.2 创建 Python 环境

```bash
# 创建 conda 环境（环境名称自定义，需与后续 deploy.conf 一致）
conda create -n guanhe python=3.13 -y
conda activate guanhe

# 安装 Python 依赖
pip install -r requirements.txt
pip install python-dotenv llama-index
```

### 3.3 安装前端依赖

```bash
cd web-frontend
npm install
cd ..
```

---

## 4. 配置部署参数

这是最关键的一步。整个项目的配置集中在 `deployment/` 目录下的两个文件中。

### 4.1 编辑部署配置

```bash
cd deployment
cp deploy.conf.example deploy.conf
vim deploy.conf
```

逐项修改：

```ini
# ===== 项目标识 =====
APP_NAME=你的项目名称          # 例如: My Company

# ===== 服务器路径（根据实际安装路径修改）=====
PROJECT_DIR=/opt/guanhetech    # 项目克隆到的目录
CONDA_ENV=/root/miniconda3/envs/guanhe  # conda 环境路径
VOLTA_DIR=/root/.volta/bin     # Volta 安装路径

# ===== 网络 =====
SERVER_HOST=你的服务器公网IP    # 例如: 47.83.165.99

# ===== 数据库（与第 2 步创建的一致）=====
DB_HOST=localhost              # 如果数据库在本机用 localhost
DB_NAME=你的数据库名
DB_USER=postgres

# ===== Redis =====
REDIS_HOST=localhost
REDIS_PORT=6379

# ===== RC Agent（如有 Dify 服务填写，否则保持默认）=====
RC_AGENT_BASE_URL=http://localhost:81/v1/chat-messages

# ===== 代理（国内服务器访问境外 API 需要配置）=====
# HTTP_PROXY=http://127.0.0.1:7890
# HTTPS_PROXY=http://127.0.0.1:7890
```

### 4.2 编辑密钥配置

```bash
cp .env.secrets.example .env.secrets
vim .env.secrets
```

填入所有密钥：

```ini
DB_PASSWORD=你的数据库密码
DEFAULT_ADMIN_PASSWORD=管理员登录密码
REDIS_PASSWORD=你的Redis密码
LLAMA_CLOUD_API_KEY=你的LlamaIndex密钥
EODHD_API_TOKEN=你的EODHD密钥
TWITTER_API_KEY=你的Twitter密钥
DASHSCOPE_API_KEY=你的通义千问密钥
OSS_ACCESS_KEY_ID=你的OSS AccessKey
OSS_ACCESS_KEY_SECRET=你的OSS Secret
RC_AGENT_TOKEN=你的RC Agent令牌
```

生成 JWT 密钥：

```bash
JWT_SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(48))")
# 将输出的值填入 .env.secrets 的 JWT_SECRET_KEY 字段
echo $JWT_SECRET_KEY
```

### 4.3 运行部署脚本

```bash
sudo bash deploy.sh
```

脚本会自动完成：
- 合并三层配置，生成 `.env`
- 从模板渲染 5 个 systemd service 文件 + nginx.conf
- 安装到 `/etc/systemd/system/` 和 `/etc/nginx/`
- 执行 `systemctl daemon-reload`

---

## 5. 启动服务

```bash
cd /opt/guanhetech
sudo ./start_services.sh
```

预期输出：

```
✅ PostgreSQL (Docker: postgres) 已在运行
✅ Redis (Docker: redis) 已在运行
✅ Web应用已启动
✅ 前端应用已启动
✅ Celery AI Worker已启动
✅ Celery Data Worker已启动
✅ 调度器服务已启动
✅ Nginx已启动
```

---

## 6. 验证部署

### 6.1 检查所有服务状态

```bash
for svc in rc-webapp rc-frontend rc-celery-ai rc-celery-data rc-scheduler nginx; do
    printf "%-20s %s\n" "$svc" "$(systemctl is-active $svc)"
done
```

全部应显示 `active`。

### 6.2 测试 API

```bash
# 后端 API
curl http://localhost:8000/
# 预期: {"message":"你的项目名 API","version":"2.0.0"}

# 调度器
curl http://localhost:8001/api/health
# 预期: {"status":"healthy",...}

# Nginx 代理
curl http://你的公网IP/api/health
# 预期: 同上
```

### 6.3 访问前端

浏览器打开 `http://你的公网IP`，应看到登录页面。使用配置的管理员账号密码登录。

---

## 7. 设置开机自启（可选）

```bash
systemctl enable rc-webapp rc-frontend rc-celery-ai rc-celery-data rc-scheduler nginx
```

---

## 8. 常用运维命令

| 操作 | 命令 |
|------|------|
| 启动所有服务 | `sudo ./start_services.sh` |
| 停止所有服务 | `sudo ./stop_services.sh` |
| 重启所有服务 | `sudo ./restart_services.sh` |
| 仅重启后端 | `sudo ./restart_backend.sh` |
| 修改配置后重新部署 | `sudo bash deployment/deploy.sh && sudo ./restart_services.sh` |
| 查看某服务日志 | `journalctl -u rc-webapp -f` |
| 查看 Celery 日志 | `tail -f logs/celery_ai_worker.log` |
| 查看 Nginx 日志 | `tail -f /var/log/nginx/rc_project_access.log` |

---

## 9. 故障排查

### 服务启动失败 (exit code 203)

路径配置错误。检查 `deploy.conf` 中的 `CONDA_ENV` 和 `VOLTA_DIR` 是否与实际路径一致：

```bash
which python    # 应在 CONDA_ENV/bin/ 下
which npm       # 应在 VOLTA_DIR/ 下
```

修正后重新运行 `sudo bash deployment/deploy.sh`。

### Celery 报 PID 文件已存在

```bash
rm -f logs/celery_*.pid
sudo systemctl restart rc-celery-ai rc-celery-data
```

### 数据库连接失败

```bash
# 检查 Docker 容器
docker ps | grep postgres

# 检查连接
docker exec -it postgres psql -U postgres -c "SELECT 1;"

# 检查 .env 中的 DB_HOST / DB_PASSWORD
grep DB_ /opt/guanhetech/.env
```

### 前端页面空白或 API 404

检查 Nginx 是否正确代理：

```bash
nginx -t                           # 语法检查
curl http://localhost:8000/api/health  # 绕过 Nginx 直接访问后端
curl http://localhost:3000/            # 绕过 Nginx 直接访问前端
```

---

## 附录: 配置文件层级说明

```
deployment/deploy.conf        ← 第 1 层：服务器基础设施（路径、IP、端口）
deployment/.env.secrets        ← 第 2 层：敏感密钥（密码、API Key）
deployment/config.defaults.env ← 第 3 层：应用默认参数（通用，不需要改）
         ↓
    deploy.sh 合并
         ↓
    ┌────┼────┐
    ↓    ↓    ↓
  .env  *.service  nginx.conf    ← 自动生成，不要手动编辑
```

修改任何配置后，都只需运行：

```bash
sudo bash deployment/deploy.sh && sudo ./restart_services.sh
```
