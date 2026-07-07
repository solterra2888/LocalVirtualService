# GuanHe Project 新服务器部署及故障排查手册

> 本文档覆盖从一台全新 Ubuntu 服务器到项目完全运行的每一步操作。

---

## 0. 环境要求


| 项目   | 最低要求          | 推荐               |
| ---- | ------------- | ---------------- |
| 操作系统 | Ubuntu 20.04+ | Ubuntu 22.04 LTS |
| CPU  | 2 核           | 4 核              |
| 内存   | 4 GB          | 8 GB             |
| 磁盘   | 20 GB         | 50 GB            |
| 网络   | 开放 80 端口      | 80 + 443         |


> **部署场景说明**：
>
>
> | 场景        | 条件           | 使用的站点配置                   | 端口要求     |
> | --------- | ------------ | ------------------------- | -------- |
> | **A（推荐）** | 有域名 + SSL 证书 | `web.conf`（HTTP→HTTPS 跳转） | 80 + 443 |
> | **B**     | 纯 IP / 无 SSL | `web.conf.http`（纯 HTTP）   | 仅 80     |
>
>
> 本手册默认按**场景 A** 说明；场景 B 的差异处会标注「**纯 HTTP 部署**」。

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

### 1.2.1 配置 Docker 镜像加速（国内 / 访问 Docker Hub 较慢时）

首次执行 `docker run` 时会从 Docker Hub 拉取镜像。若服务器访问 `registry-1.docker.io` **超时**（见第 10 章「Docker 拉取镜像失败」），可配置**镜像加速**（`registry-mirrors`），让拉取走国内或就近节点。

1. 编辑 Docker 守护进程配置（若文件不存在则新建）：

```bash
sudo mkdir -p /etc/docker
sudo nano /etc/docker/daemon.json
```

1. 写入或合并如下内容（**保留 JSON 合法**：最后一项后不要有逗号）：

```json
{
  "registry-mirrors": [
    "https://docker.1ms.run",
    "https://docker.xuanyuan.me"
  ]
}
```

> **说明**：公开镜像站地址可能变更，若失效请搜索「Docker 镜像加速 2025」等关键词替换为当前可用地址；云厂商（阿里云、腾讯云等）控制台通常也提供**专属加速地址**，优先使用。

1. 保存并退出 **nano**：

- `Ctrl + O` → 回车（保存）
- `Ctrl + X`（退出）

1. 重载并重启 Docker 使配置生效：

```bash
sudo systemctl daemon-reload
sudo systemctl restart docker
```

1. 验证：

```bash
docker info | grep -A5 "Registry Mirrors"
```

配置完成后再执行本手册中的 `docker run` / `docker pull`（如 `postgres:17`、`redis:7.4.6-alpine`、`nginx:alpine`）。

### 1.3 安装 Nginx（Docker 版本）

Nginx 通过 Docker 部署，无需 apt 安装。配置目录准备见第 2.3 节，容器创建见第 3.4 节。

### 1.4 安装 Miniconda（Python 环境管理）

```bash
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -b -p /root/miniconda3

# 将 conda 写入 ~/.bashrc（不要对 conda init 使用 eval，否则会误执行输出里的「no change」等提示）
/root/miniconda3/bin/conda init bash

# 当前终端立即启用 conda（二选一）
source ~/.bashrc
# 或: eval "$(/root/miniconda3/bin/conda shell.bash hook)"
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
  -e POSTGRES_PASSWORD='你的数据库密码' \
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

# 创建 Redis 配置文件
cat > /opt/redis/conf/redis.conf << 'EOF'
bind 0.0.0.0
protected-mode yes
port 6379
daemonize no
requirepass 你的Redis密码
appendonly yes
dir ./
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

> **配置要点**：`bind 0.0.0.0` 允许容器内监听所有网卡（Docker 内必需）；`protected-mode yes` 配合 `requirepass` 确保密码保护；`daemonize no` 保持前台运行（Docker 要求）。
>
> **安全提示**：Redis 通过 `-p 6379:6379` 映射到宿主机。应通过服务器防火墙 / 安全组**限制 6379 端口的访问来源**，仅允许本机或可信 IP，切勿对公网完全开放。

### 2.3 Nginx 配置目录准备

Nginx 通过 Docker 部署。容器创建前，需先在宿主机准备好配置目录和核心文件。

#### 2.3.1 创建目录结构

```
/opt/nginx/
├── conf/
│   ├── nginx.conf          ← 主配置（含 include vhosts/*.conf）
│   ├── mime.types           ← MIME 类型映射
│   ├── cert/                ← SSL 证书（场景 A 需要）
│   │   ├── cert.pem
│   │   └── key.pem
│   └── vhosts/
│       └── web.conf         ← 项目站点配置（由 deploy.sh / start_services.sh 同步）
├── html/                    ← Nginx 默认站点目录
└── log/                     ← Nginx 日志
```

```bash
mkdir -p /opt/nginx/conf/vhosts /opt/nginx/conf/cert /opt/nginx/html /opt/nginx/log
```

#### 2.3.2 初始化 nginx.conf 和 mime.types

项目仓库中提供了参考主配置 `deployment/nginx/nginx.conf`，克隆代码后直接复制即可（见第 3.1 节）。如需提前初始化，可从 Docker 镜像提取默认配置再手动编辑：

```bash
# 提取 Nginx 默认配置（后续会被仓库中的参考配置覆盖）
docker run --rm nginx:alpine cat /etc/nginx/nginx.conf > /opt/nginx/conf/nginx.conf
docker run --rm nginx:alpine cat /etc/nginx/mime.types > /opt/nginx/conf/mime.types
```

> **说明**：仓库中 `deployment/nginx/nginx.conf` 已包含 gzip 压缩、epoll 优化、`include vhosts/*.conf` 等生产配置。`mime.types` 使用 Nginx 官方默认文件即可。

#### 2.3.3 准备 SSL 证书（场景 A）

使用 HTTPS 部署时，需将 SSL 证书放入 `/opt/nginx/conf/cert/`：

```bash
# 将证书文件上传到服务器后
cp /path/to/your/cert.pem  /opt/nginx/conf/cert/cert.pem
cp /path/to/your/key.pem   /opt/nginx/conf/cert/key.pem
chmod 600 /opt/nginx/conf/cert/key.pem
```

> **免费证书获取**：可通过 [Let's Encrypt](https://letsencrypt.org/) 免费申请，或从域名注册商 / 云服务商处获取。
>
> **纯 HTTP 部署**（场景 B）：跳过此步，无需证书。

> Nginx 容器将在第 3.4 节（项目代码克隆和前端构建完成后）创建。

### 2.4 验证容器运行

```bash
docker ps
# 应看到 postgres、redis 两个容器状态为 Up
```

---

## 3. 部署项目代码

### 3.1 克隆仓库

```bash
cd /opt
git clone <你的仓库地址> guanhetech
cd guanhetech
```

> **Git 配置（首次部署建议）**  
> 若还没配置过用户名和邮箱，建议先设置（提交时会用到）：
>
> ```bash
> # 全局配置（对所有仓库生效）
> git config --global user.name "你的名字"
> git config --global user.email "你的邮箱@example.com"
>
> # 查看当前配置
> git config --list
> ```

### 3.2 创建 Python 环境

```bash
# 新版 Miniconda 使用 defaults 源时，需先接受 Anaconda 仓库服务条款（否则 conda create 会报 CondaToSNonInteractiveError）
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r

# 创建 conda 环境（环境名称自定义，需与后续 deploy.conf 一致）
conda create -n guanhe python=3.13 -y
conda activate guanhe

# 安装 Python 依赖
pip install -r requirements.txt
pip install llama-index          # 文件解析（LlamaParse），未包含在 requirements.txt 中
```

### 3.3 安装前端依赖并构建

```bash
cd web-frontend
npm install
npm run build
cd ..
```

> **本地开发**：执行 `cd web-frontend && npm start` 启动开发服务器（端口 3000，支持热更新），无需 Nginx。
>
> **线上部署**：前端由 Docker Nginx 以静态文件方式提供服务，不使用 `npm start`。

### 3.4 配置 Nginx 站点并创建容器

#### 3.4.1 选择并定制站点配置（web.conf）

项目根目录提供了两份 Nginx 站点配置，根据部署场景选择：


| 文件              | 场景             | 说明                                                        |
| --------------- | -------------- | --------------------------------------------------------- |
| `web.conf`      | A：有域名 + HTTPS  | HTTP→HTTPS 跳转，SSL 加密，`server_name` 默认为 `guanhecapital.hk` |
| `web.conf.http` | B：纯 IP / 无 SSL | 仅 HTTP，`server_name _`（匹配任意），无需证书                         |


**场景 A（HTTPS）** — 编辑 `web.conf`，将域名替换为你的实际域名：

```bash
cd /opt/guanhetech
# 将 guanhecapital.hk 替换为你的域名（注意同时替换 www 和裸域）
sed -i 's/www\.guanhecapital\.hk/www.你的域名/g; s/guanhecapital\.hk/你的域名/g' web.conf
```

**纯 HTTP 部署（场景 B）** — 用 HTTP 版本覆盖默认配置：

```bash
cd /opt/guanhetech
cp web.conf.http web.conf
```

#### 3.4.2 复制 nginx.conf 到宿主机

如果在第 2.3.2 节尚未复制（当时代码未克隆），现在执行：

```bash
cp /opt/guanhetech/deployment/nginx/nginx.conf /opt/nginx/conf/nginx.conf

# 提取 mime.types（若尚未执行）
docker run --rm nginx:alpine cat /etc/nginx/mime.types > /opt/nginx/conf/mime.types
```

#### 3.4.3 创建 Nginx 容器

前端构建完成、配置文件就位后，即可创建 Nginx 容器。容器通过 Docker volume mount 将 `web-frontend/build/` 直接挂载为站点目录，无需手动复制文件。

```bash
docker run -d --name nginx \
  --network host \
  --restart always \
  -v /opt/nginx/conf:/etc/nginx \
  -v /opt/nginx/html:/usr/share/nginx/html \
  -v /opt/guanhetech/web-frontend/build:/usr/share/nginx/html/guanhe:ro \
  -v /opt/nginx/log:/var/log/nginx \
  -v /etc/localtime:/etc/localtime:ro \
  nginx:alpine
```

> **关键挂载**：`-v .../web-frontend/build:/usr/share/nginx/html/guanhe:ro` 将构建产物直接挂载为 Nginx 站点根目录。每次 `npm run build` 后 Nginx 自动读取最新文件，无需额外复制。如果项目目录不在 `/opt/guanhetech`，请将路径替换为实际的 `PROJECT_DIR`。

验证容器运行：

```bash
docker ps
# 应看到 postgres、redis、nginx 三个容器状态为 Up
```

> **排查**：若 Nginx 启动后立即退出，通常是配置文件缺失或语法错误。运行 `docker logs nginx` 查看错误信息，常见原因包括：`nginx.conf` 或 `mime.types` 不存在、`cert/cert.pem` 找不到（场景 A 忘记上传证书）、`web.conf` 中 `log_format acc` 未在 `nginx.conf` 中定义等。

日常前端更新可使用一键部署命令（包含构建 + 通知 Nginx 重载）：

```bash
cd web-frontend
npm run deploy
```

**deploy 脚本说明**：该命令对应 `web-frontend/package.json` 中的 `scripts.deploy`，实际执行：

1. `CI=false react-scripts build` — 构建前端（`CI=false` 避免 ESLint 警告导致构建失败）；
2. `docker exec nginx nginx -s reload` — 通知 Nginx 重载，使新构建产物生效。

因 Nginx 容器已将 `web-frontend/build/` 挂载为站点目录，构建会直接更新该目录，执行上述命令后用户访问到的即为最新前端。

---

## 4. 配置部署参数

这是最关键的一步。项目最终需要根目录下的 `.env` 文件来运行。根据使用场景不同，配置文件的位置和生成方式完全独立：


|      | 正式服务器部署（4.A）                         | 本地开发（4.B）                                      |
| ---- | ------------------------------------ | ---------------------------------------------- |
| 基础配置 | `deployment/deploy.conf`             | `development/deploy.conf`                      |
| 密钥配置 | `deployment/.env.secrets`            | `development/.env.secrets`                     |
| 默认参数 | `deployment/config.defaults.env`（共用） | `deployment/config.defaults.env`（共用）           |
| 生成脚本 | `sudo bash deployment/deploy.sh`     | `bash development/generate_env_from_deploy.sh` |
| 生成产物 | `.env` + systemd services + Nginx 配置 | 仅 `.env`                                       |


> 两套配置完全独立，互不影响。正式服务器和本地开发机各自维护自己的 `deploy.conf` 和 `.env.secrets`。

#### 配置文件层级说明

正式服务器和本地开发使用各自独立的配置文件，仅共享 `config.defaults.env`（应用默认参数）。

**正式服务器：**

```
deployment/deploy.conf         ← 第 1 层：服务器基础设施（路径、IP、端口）
deployment/.env.secrets         ← 第 2 层：敏感密钥（密码、API Key）
deployment/config.defaults.env  ← 第 3 层：应用默认参数（通用，不需要改）
         ↓
    deploy.sh 合并
         ↓
    ┌────┼────────────────┐
    ↓    ↓                ↓
  .env  *.service     web.conf → /opt/nginx/conf/vhosts/
        (4个)          (Docker Nginx 配置)
```

**本地开发：**

```
development/deploy.conf         ← 第 1 层：本地环境参数（路径、IP、端口）
development/.env.secrets         ← 第 2 层：本地密钥（密码、API Key）
deployment/config.defaults.env   ← 第 3 层：应用默认参数（共用，不需要改）
         ↓
    generate_env_from_deploy.sh 合并
         ↓
       .env（仅此一个文件，无 systemd / Nginx）
```

---

### 4.A 正式服务器部署

服务器上的配置文件位于 `deployment/` 目录。编辑后运行 `deploy.sh`，由脚本自动生成 `.env`、systemd 服务与 Nginx 配置。

#### 4.A.1 编辑部署配置

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

#### 4.A.2 编辑密钥配置

```bash
cd deployment
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

#### 4.A.3 运行部署脚本

```bash
sudo bash deployment/deploy.sh
```

脚本会自动完成：

- 合并 `deployment/deploy.conf` + `deployment/.env.secrets` + `deployment/config.defaults.env`，生成 `.env`
- 从模板渲染 4 个 systemd service 文件（rc-webapp、rc-celery-ai、rc-celery-data、rc-scheduler）
- 安装 service 文件到 `/etc/systemd/system/`
- 同步 `web.conf` 到 Docker Nginx 配置卷 `/opt/nginx/conf/vhosts/`
- 执行 `systemctl daemon-reload`

> **生成的 Celery service 关键参数**（`rc-celery-ai` / `rc-celery-data`）：
>
> | 参数 | AI Worker | Data Worker | 说明 |
> |------|-----------|-------------|------|
> | `ExecStartPre` | ✅ | ✅ | 每次启动（含 systemd 自动重启）前自动清理 Redis 旧 task meta，防止损坏记录触发无限崩溃循环 |
> | `TimeoutStopSec` | `720` | `120` | 给 worker 足够时间在 SIGTERM 后优雅退出（默认 30s 会硬杀长 LLM 任务，导致 Redis meta 损坏） |
> | `--soft-time-limit` | `600` | `110` | 单任务最长执行时间（秒），与 `FILE_PROCESSING_TIMEOUT_MINUTES=10` 对齐；超时后 Celery 抛 `SoftTimeLimitExceeded` 并正常写结果 |
> | `--time-limit` | `660` | `130` | 硬截止时间，超时强制杀进程 |
>
> 修改模板后须重新执行 `sudo bash deployment/deploy.sh && sudo systemctl daemon-reload && sudo ./restart_services.sh` 才能生效。

---

### 4.B 本地开发

本地开发的配置文件位于 `development/` 目录（与服务器的 `deployment/` 互不干扰）。只需生成 `.env`，不涉及 systemd / Nginx。

#### 4.B.1 准备本地配置文件

```bash
cd /path/to/guanhetech

# 1) Python 环境配置
cp development/dev.conf.example development/dev.conf
vim development/dev.conf
# 设置 DEV_CONDA_ENV=/你的/conda/路径，例如:
#   DEV_CONDA_ENV=/Users/你的用户名/miniconda3/envs/guanhe

# 2) 本地部署参数（development 目录下的独立配置）
cp development/deploy.conf.example development/deploy.conf
vim development/deploy.conf
# 按本地环境修改，例如:
#   PROJECT_DIR=/Users/你的用户名/Documents/guanhetech
#   DB_HOST=127.0.0.1（本地 PostgreSQL）或远程 IP（共享数据库）
#   REDIS_HOST=127.0.0.1
#   SERVER_HOST=127.0.0.1

# 3) 本地密钥配置（development 目录下的独立配置）
cp development/.env.secrets.example development/.env.secrets
vim development/.env.secrets
# 填写本地使用的 DB 密码、Redis 密码、各类 API Key
# 注意：本地无密码 Redis 需将 REDIS_PASSWORD 留空或注释掉
```

#### 4.B.2 生成 `.env`（无需 root）

```bash
bash development/generate_env_from_deploy.sh
```

该脚本会：

- 读取：`development/deploy.conf`、`development/.env.secrets`、`deployment/config.defaults.env`
- 生成：项目根目录 `.env`（结构与服务器上 `deploy.sh` 生成的基本一致）
- 不进行任何 systemd / Nginx 操作，适合本地开发环境

#### 4.B.3 启动本地开发环境

```bash
cd /path/to/guanhetech
./development/start_local_dev.sh
```

该脚本会：

- 加载 `development/dev.conf`（Python 环境）与项目根目录 `.env`
- 检查数据库、Redis、Celery 配置；Redis 不可用时尝试启动本地无密码实例
- 启动本地 Web 后端（默认 `http://localhost:8000`）与前端（默认 `http://localhost:3000`）
- **启动 Celery Worker 前自动清理 Redis 旧任务元数据**（`celery-task-meta-*` / `unacked*`），防止上次异常退出留下的损坏记录导致 Worker 启动即崩
- 通过读取日志文件（检测到 `ready.` 字样）判断 Worker 真正就绪，而非仅判断进程存活

> **停止**：`./development/stop_local_dev.sh`（会在关闭 Redis **之前**先清理 Celery 队列与 task meta，确保清理有效）。

---

## 5. 启动服务

```bash
cd /opt/guanhetech
sudo ./start_services.sh
```

脚本执行顺序：检查 PostgreSQL / Redis Docker 容器 → **清理 Redis 旧 task meta**（防止损坏记录导致 Worker 启动即崩）→ 启动 Web 应用 → Celery AI Worker → Celery Data Worker → 调度器 → Nginx。

预期输出：

```
✅ PostgreSQL (Docker: postgres) 已在运行
✅ Redis (Docker: redis) 已在运行
  ✅ Redis 无残留任务元数据
✅ Web应用已启动
✅ Celery AI Worker已启动
✅ Celery Data Worker已启动
✅ 调度器服务已启动
✅ Nginx (Docker: nginx) 已启动
```

> `stop_services.sh` 与 `restart_services.sh` 同样内置了 Redis 清理步骤（分别在停止/重启 Celery 前执行），日常运维直接使用这些脚本即可，无需手动清理 Redis。

---

## 6. 验证部署

### 6.1 检查所有服务状态

```bash
# systemd 服务
for svc in rc-webapp rc-celery-ai rc-celery-data rc-scheduler; do
    printf "%-20s %s\n" "$svc" "$(systemctl is-active $svc)"
done

# Docker 容器
for ctn in postgres redis nginx; do
    status=$(docker inspect -f '{{.State.Status}}' $ctn 2>/dev/null || echo "not found")
    printf "%-20s %s\n" "$ctn (Docker)" "$status"
done
```

systemd 服务应全部显示 `active`，Docker 容器应全部显示 `running`。

### 6.2 测试 API

```bash
# 后端 API（直连，绕过 Nginx）
curl http://localhost:8000/
# 预期: {"message":"你的项目名 API","version":"2.0.0"}

# 调度器（直连）
curl http://localhost:8001/api/health
# 预期: {"status":"healthy",...}

# 通过 Nginx 代理 — 场景 A（HTTPS）
curl -k https://你的域名/api/health
# 预期: 同上（-k 跳过自签名证书验证；正式证书无需 -k）

# 通过 Nginx 代理 — 场景 B（纯 HTTP）
curl http://你的公网IP/api/health
# 预期: 同上
```

> **注意**：场景 A 的 `web.conf` 会将 HTTP 80 → HTTPS 301 重定向。如果用 `curl http://...` 测试会返回 301，需加 `-L` 跟随重定向或直接用 `https://`。

### 6.3 访问前端

- **场景 A（HTTPS）**：浏览器打开 `https://你的域名`
- **场景 B（纯 HTTP）**：浏览器打开 `http://你的公网IP`

应看到登录页面。使用配置的管理员账号密码登录。

## 7. 设置开机自启（可选）

```bash
# systemd 服务
systemctl enable rc-webapp rc-celery-ai rc-celery-data rc-scheduler

# Docker 容器已设置 --restart always，会随 Docker 自动启动
```

---

## 8. 常用运维命令


| 操作           | 命令                                                                                    |
| ------------ | ------------------------------------------------------------------------------------- |
| 启动所有服务       | `sudo ./start_services.sh`                                                            |
| 停止所有服务       | `sudo ./stop_services.sh`                                                             |
| 重启所有服务       | `sudo ./restart_services.sh`                                                          |
| 仅重启后端        | `sudo ./restart_backend.sh`                                                           |
| 前端构建并部署      | `cd web-frontend && npm run deploy`                                                   |
| 修改配置后重新部署    | `sudo bash deployment/deploy.sh && sudo ./restart_services.sh`                        |
| 查看某服务日志      | `journalctl -u rc-webapp -f`                                                          |
| 查看 Celery 日志 | `tail -f logs/celery_ai_worker.log`（AI）；`tail -f logs/celery_data_worker.log`（数据/订阅等） |
| 查看 Nginx 日志  | `tail -f /opt/nginx/log/rc_project_access.log` 或 `docker logs nginx --tail 50`        |


---

## 9. 服务器配置

### 服务器端口（安全组 / 防火墙）

> **安全原则**：仅开放业务必需的最小端口集合。数据库和缓存端口**切勿对公网完全开放**。

实例 > 点击实例ID > 安全组 > 管理规则 > 入方向 > 增加规则：

**必须开放（访问来源：`0.0.0.0/0` 任意位置）**：


| 端口  | 用途                                |
| --- | --------------------------------- |
| 80  | HTTP（场景 B 直接提供服务；场景 A 用于跳转 HTTPS） |
| 443 | HTTPS（场景 A 必需；场景 B 可不开放）          |


**按需开放（访问来源：仅指定 IP）**：


| 端口   | 用途         | 安全建议                         |
| ---- | ---------- | ---------------------------- |
| 5432 | PostgreSQL | 仅在需要远程访问数据库时开放，来源限制为指定服务器 IP |
| 6379 | Redis      | 仅在需要远程访问时开放，来源限制为指定服务器 IP    |


> **警告**：将 Redis 6379 或 PostgreSQL 5432 对 `0.0.0.0/0`（任意位置）开放会带来严重安全风险。即使设置了密码，仍可能遭受暴力破解或漏洞利用。如无跨服务器访问需求，**不要开放**这两个端口。

## 10. 故障排查

### Docker 拉取镜像失败（Unable to find image locally / registry i/o timeout）

**症状**：执行 `docker run ... postgres:17`（或其它镜像）时出现类似错误：

```text
Unable to find image 'postgres:17' locally
failed to resolve reference "docker.io/library/postgres:17": ...
dial tcp ... registry-1.docker.io:443: i/o timeout
```

**原因**：本机无法稳定访问 **Docker Hub**（`registry-1.docker.io`），常见于国内网络或防火墙策略，**不是**因为少执行了一步 `docker pull`。

> 单独执行 `docker pull postgres:17` 仍会访问同一仓库，若网络问题未解决，**同样会超时**。

**解决方案**：

1. **推荐**：按第 **1.2.1 节**配置 `registry-mirrors` 镜像加速，重启 Docker 后再拉取/运行容器。
2. **备选**：为服务器配置 HTTP/HTTPS **代理**（企业出口、Clash 等），使 Docker 能访问外网注册表（需在 Docker 或系统层配置代理，此处不展开）。
3. **离线**：在能访问 Docker Hub 的机器上 `docker pull` + `docker save`，再 `scp` 到目标机 `docker load`。

镜像拉取成功后再继续第 2 节创建 PostgreSQL / Redis 等容器。

### `pip install -r requirements.txt` 时 numpy 报错（缺少 g++ / metadata-generation-failed）

**症状**：安装 `numpy` 时出现 `Unknown compiler(s)`、`No such file or directory: 'g++'`，或 `Preparing metadata ... error`。

**原因**：

1. **Python 3.13**：旧版 `numpy==1.26.x` 没有对应平台的预编译 wheel 时，pip 会尝试从源码编译，需要系统已安装 **C/C++ 编译器**（`build-essential`）。
2. **版本搭配**：`pandas 3.0.x` 需要 **NumPy 2.x**；仓库已使用 `numpy>=2.0,<3`，在 Python 3.12 / 3.13 上一般会直接下载 wheel，无需本地编译。

**解决方案**（任选其一或组合）：

1. **推荐**：拉取最新代码后重新安装依赖（已使用 NumPy 2.x）：
  ```bash
   conda activate guanhe
   pip install -U pip
   pip install -r requirements.txt
  ```
2. **仍从源码编译时**：安装编译工具后再 `pip install`：
  ```bash
   sudo apt update && sudo apt install -y build-essential
  ```
3. **备选环境**：若个别包仍与 Python 3.13 不兼容，可使用 **Python 3.12** 创建环境：
  ```bash
   conda create -n guanhe python=3.12 -y
   conda activate guanhe
   pip install -r requirements.txt
  ```

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

### Celery Worker 反复重启 / Redis 任务元数据异常

**症状**：Worker 日志出现 `Exception information must include the exception type`，或同一任务 ID 日志反复出现"任务失败"；上传 / AI 分析任务卡在「处理中」。

**根本原因**：

- Worker 正常处理任务时，结果写入 Redis `celery-task-meta-<task_id>`。若 Worker 被 SIGKILL 强杀（例如 `TimeoutStopSec` 太短、强制重启），当前任务的 Python 异常对象无法完整序列化，留下一条格式损坏的 meta。
- 下次 Worker 启动时尝试恢复此 meta → `ValueError: Exception information must include...` → Worker 崩溃 → systemd 自动重启 → 再次崩溃 → 无限循环。

**防御机制（已内置）**：

| 层级 | 机制 | 覆盖场景 |
|------|------|---------|
| **治本** | `--soft-time-limit`（AI: 600s，Data: 110s）+ `--time-limit`（AI: 660s，Data: 130s） | 任务超时后由 Celery 自行终止并正常写结果，不留损坏 meta |
| **第二道** | `TimeoutStopSec`（AI: 720s，Data: 120s） | 给 Worker 足够时间在收到 SIGTERM 后优雅退出 |
| **第三道** | `ExecStartPre` 启动前清理（两个 Celery service 均已配置） | 即使前两道失守，每次 systemd 启动时自动删除所有旧 meta |
| **第四道** | `result_expires = 3600`（celery_config.py） | 残留 meta 1 小时后自动过期，不会永久积累 |
| **运维入口** | start / stop / restart 脚本均内置清理 | 常规运维操作自动保证 Redis 干净 |

**日常运维处理**（优先）：

```bash
# 生产环境：重启脚本会自动清理 Redis，再重启 Worker
sudo ./restart_services.sh

# 本地开发：stop 脚本会在关 Redis 前先清理，start 脚本启 Worker 前也会清理
./development/stop_local_dev.sh
./development/start_local_dev.sh
```

**应急手动清理**（当脚本不可用或需要立即解决时）：

```bash
# 连接 Redis（生产环境带密码时替换 REDIS_URL）
source /opt/guanhetech/.env   # 加载 REDIS_URL

python3 - <<'EOF'
import redis, os
r = redis.from_url(os.environ.get('REDIS_URL', 'redis://localhost:6379/0'))
keys = r.keys('celery-task-meta-*') + r.keys('unacked*')
for k in keys:
    r.delete(k)
print(f"已清除 {len(keys)} 条损坏 meta")
EOF

# 清理后重启 Worker
sudo systemctl restart rc-celery-ai rc-celery-data
# 或本地：bash development/start_celery_worker_dev.sh
```

**如果 `ExecStartPre` / `TimeoutStopSec` 参数尚未生效**（旧部署未重新执行 deploy.sh）：

```bash
sudo bash deployment/deploy.sh        # 重新渲染 service 模板
sudo systemctl daemon-reload          # 让 systemd 读取新配置
sudo ./restart_services.sh            # 重启所有服务
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

### 上传文件/音频报错：AUTH  called without any password configured

**症状**：上传音频或文档时接口返回 500，日志中出现：

```
上传音频文件失败: AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?
```

或「上传文档文件失败」并带有相同 AUTH 相关错误。

**原因**：Celery 提交任务时使用 Redis 作为 broker。`.env` 中的 `REDIS_URL` 含有密码（由 `.env.secrets` 的 `REDIS_PASSWORD` 生成），但**本地 Redis 未设置密码**（例如用 `redis-server` 直接启动或由 `start_local_dev.sh` 启动的无密码实例）。Redis 服务端收到 AUTH 请求却未配置密码，因而报错。

**解决方案**（任选其一）：

1. **推荐（本地开发）**：若本地 Redis 无密码，在 `development/.env.secrets` 中将 `REDIS_PASSWORD` **留空或注释掉**，然后重新生成 `.env`：
  ```bash
   # 编辑 development/.env.secrets，将 REDIS_PASSWORD=xxx 改为空或注释掉
   bash development/generate_env_from_deploy.sh
  ```
   生成的 `REDIS_URL` 将变为 `redis://127.0.0.1:6379/0`，即可连接无密码 Redis。
2. **使用 start_local_dev.sh 启动**：脚本会先检查 Redis；若不可用会启动本地无密码 Redis，并写入 `development/.env.redis.local`（`REDIS_URL=redis://127.0.0.1:6379/0`）。之后启动的 Web 与 Celery 会先加载 `.env` 再加载该文件，从而覆盖为无密码连接。若你未通过该脚本启动（例如直接用 IDE 跑 uvicorn），则不会加载 `.env.redis.local`，需采用方案 1。
3. **生产/服务器**：若 Redis 已配置密码，请确保 `deployment/.env.secrets` 中 `REDIS_PASSWORD` 与 Redis 的 `requirepass` 一致，并重新运行 `sudo bash deployment/deploy.sh` 以更新 `.env`。

### 前端页面空白或 API 404

检查 Docker Nginx 是否正常运行以及后端是否可达：

```bash
docker ps | grep nginx                                        # 确认 Nginx 容器在运行
docker logs nginx --tail 20                                   # 查看 Nginx 近期日志
curl http://localhost:8000/api/health                         # 绕过 Nginx 直接访问后端
docker exec nginx ls /usr/share/nginx/html/guanhe/index.html  # 确认容器内能读到前端构建产物
ls /opt/guanhetech/web-frontend/build/index.html              # 确认宿主机 build 产物存在
```

### 音频上传后 ASR 语音识别返回 FILE_403_FORBIDDEN

**症状**：音频文件已成功上传到 OSS，但语音识别（ASR）结果为空，日志出现：

```
ASR 子任务失败: {'code': 'FILE_403_FORBIDDEN', 'message': 'FILE_403_FORBIDDEN', 'subtask_status': 'FAILED'}
```

**原因**：ASR 服务（DashScope）需要通过公网 URL 拉取音频文件。当 OSS Bucket 为**私有**时，未签名的 `public_url`（如 `https://bucket.oss-cn-hongkong.aliyuncs.com/audio/...`）会被 OSS 拒绝访问（403）。

**解决方案**（已在代码中修复）：提交 ASR 任务时使用带签名的 `signed_url`（48 小时有效期）而非 `public_url`。对应代码位于 `backend/tasks/file_tasks.py` 的 `upload_and_transcribe_audio_task`：

```python
# 优先使用签名 URL，否则 ASR 拉取私有桶会 403
audio_url = upload_result.get("signed_url") or upload_result["public_url"]
```

> **注意**：此问题仅影响音频文件（ASR 需要外部拉取）。PDF/Word 等文档的处理流程不受影响，因为文档解析在本地服务器完成，不依赖公网 URL。

### YouTube Feed ASR（AI 转写）故障排查

Feeds 模块中「YouTube Feeds → 点击 AI Transcribe」依赖：yt-dlp 下载音频 → OSS 上传 → DashScope Fun-ASR 转写 → 结果写回 `subscription_content`。以下为已实施修复与常见问题汇总。

#### 已实施的修复汇总


| 层次                                  | 现象 / 报错                                                                                         | 处理方式                                                                                                                                                                                                                                  |
| ----------------------------------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1. 反机器人检测**                       | `Sign in to confirm you're not a bot`                                                           | 使用 Netscape 格式 `youtube-cookies.txt`，通过环境变量 `YOUTUBE_COOKIES_FILE` 指定路径（见 `deploy.conf.example`）。需在浏览器登录 YouTube 后用扩展导出，上传到服务器如 `config/youtube-cookies.txt`。                                                                         |
| **2. n-challenge / 签名求解**           | `Signature solving failed` / `n challenge solving failed` / `Requested format is not available` | 安装 `yt-dlp[default]`（含 `yt-dlp-ejs`）并在代码中启用 Node.js 运行时（`js_runtimes`），使 yt-dlp 能解开 YouTube 的 JS 挑战。                                                                                                                                  |
| **3. PO Token（GVS）**                | 仅剩 storyboard 等缩略图、无音频格式                                                                        | 部署 `bgutil-ytdlp-pot-provider`：clone 至 `/opt/bgutil-ytdlp-pot-provider`，编译 server，安装 Python 插件，并以 systemd 服务 `bgutil-pot` 运行（默认端口 4416）。                                                                                              |
| **4. ffmpeg 缺失**                    | `ffprobe and ffmpeg not found`                                                                  | 在 conda 环境中安装：`conda install -y -c conda-forge ffmpeg`。代码中通过 `ffmpeg_location` 指定路径兜底。                                                                                                                                                |
| **5. OSS 私有桶 403**                  | ASR 子任务 `FILE_403_FORBIDDEN`、转写完成但 `transcript` 为空                                              | 提交 ASR 时使用 OSS **签名 URL**（`signed_url`）而非 `public_url`，见上文「音频上传后 ASR 语音识别返回 FILE_403_FORBIDDEN」。                                                                                                                                      |
| **6. OSS 私有桶 FILE_DOWNLOAD_FAILED** | 已使用签名 URL 仍报 `FILE_DOWNLOAD_FAILED`、ASR 未返回有效转写                                                 | Fun-ASR 可能无法通过 OSS 签名 URL 拉取。在对应环境的 `.env.secrets`（服务器：`deployment/.env.secrets`；本地：`development/.env.secrets`）中增加一行 `OSS_ASR_PUBLIC_READ=true`，然后重新生成 `.env`。上传用于 ASR 的音频时会将该对象 ACL 设为 public-read，并改用公网 URL 提交 ASR。仅该次上传的音频对象会公网可读。 |


#### 常见错误速查

- **Sign in to confirm you're not a bot** → Cookie 失效或未配置。重新导出 `youtube-cookies.txt` 并更新服务器上 `config/youtube-cookies.txt`，必要时重启 Celery。
- **Invalid js_runtimes format** → yt-dlp Python API 要求 `js_runtimes` 为 `{'node': {'path': '...'}}` 的 dict 格式，已修复。
- **transcript 为空且 transcript_data.asr_data 为 null** → 多为 OSS 403（见上表第 5 项），或 DashScope 子任务失败，检查日志中的 `FILE_403_FORBIDDEN` 并确认使用 `signed_url`。
- **FILE_DOWNLOAD_FAILED（与 OSS 私有桶有关）** → 若已使用签名 URL 仍报此错，说明 Fun-ASR 可能不支持通过签名 URL 拉取。在对应环境的 `.env.secrets` 中增加 `OSS_ASR_PUBLIC_READ=true`，重新生成 `.env` 后重启服务（见上表第 6 项）。

#### Cookie 过期与长期维护

YouTube 的 session cookie 会过期（通常 1–2 周），且机房 IP 易触发风控，因此「Sign in to confirm you're not a bot」可能反复出现。可选方案：


| 方案                      | 说明                                                                                                                                                                   |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **A. 定期检测 + 告警 + 上传接口** | 定时任务用当前 cookies 测一次 yt-dlp；若失败则告警。提供仅管理员可用的 API（如 `POST /api/admin/youtube-cookies`）接收新 cookies 文件并写回 `config/youtube-cookies.txt`，管理员在浏览器导出后通过接口或本地脚本上传，无需 SSH 改文件。 |
| **B. 内网 Cookie 代理机**    | 在可长期登录 YouTube 的机器上跑服务（如 Playwright 导出 cookies），HK 服务器定时拉取并更新 `youtube-cookies.txt`。实现成本较高，需维护浏览器环境与接口安全。                                                            |
| **C. 第三方转写/下载服务**       | 使用不依赖本方 cookies 的 SaaS（仅提供视频 URL），由对方处理 YouTube 访问与合规。                                                                                                               |
| **D. 仅需字幕时**            | 若视频自带字幕，可考虑 YouTube 官方 API 或第三方字幕 API 拉取字幕，避免 yt-dlp 下载与 cookie 维护。                                                                                                  |


当前推荐先做 **方案 A**：检测 + 告警 + 管理员上传接口，将「发现失效 → 更新 cookies」半自动化，减少手动 SSH 与改文件的频率。

### YouTube 订阅视频获取：RSS 失败自动回退 YouTube Data API v3

YouTube 已逐步弃用频道 RSS（`/feeds/videos.xml?channel_id=...`），大量频道返回 404。代码已实现 **自动回退（fallback）**：RSS 失败时，若配置了 `YOUTUBE_DATA_API_KEY`，会改用 YouTube Data API v3 拉取视频列表。

#### 配置步骤

1. 在 [Google Cloud Console](https://console.cloud.google.com/apis/credentials) 创建 **Public data** 类型的 API Key，并在 Library 中启用 **YouTube Data API v3**。
2. 将 API Key 写入对应环境的 `.env.secrets`：
  - 服务器：`deployment/.env.secrets`
  - 本地：`development/.env.secrets`
3. 重新生成 `.env` 并重启服务：
  ```bash
   # 服务器
   sudo bash deployment/deploy.sh && sudo ./restart_services.sh
   # 本地
   bash development/generate_env_from_deploy.sh
  ```

#### 工作原理

`fetch_channel_videos()` 先尝试 RSS；若 RSS 返回 404 或空结果，且当前频道标识为 `UC...` 格式的 channel_id，则自动调用 API v3：

1. `channels.list(part=contentDetails)` → 获取 uploads 播放列表 ID（1 quota unit）
2. `playlistItems.list(part=snippet)` → 获取视频列表（1 quota unit）
3. `videos.list(part=snippet,statistics,contentDetails)` → 获取完整视频信息（1 quota unit）

返回的数据格式与 RSS 路径完全一致，下游 `YouTubeAdapter` 和 `SubscriptionContentService` 无需任何修改。

#### 配额说明

- 免费额度：**10,000 units/天**，仅对 RSS 失败的频道消耗配额（每频道 3 units）。
- 14 个频道全部 RSS 失败 ≈ 42 units/次，一天多次运行也远低于上限。
- 若未配置 `YOUTUBE_DATA_API_KEY`，行为与之前完全一致（RSS 失败则跳过该频道）。

### PDF/图片在右侧面板无法预览（自动下载或"未能加载 PDF 文档"）

**症状**：文件已上传到 OSS 且处理成功，但在 File List 点击文件后，右侧面板的 Original 标签页无法在线预览 PDF，而是触发浏览器下载或显示"未能加载 PDF 文档"。

**原因**：阿里云 OSS 默认域名（`bucket.oss-cn-xxx.aliyuncs.com`）有安全策略，会对特定地域和文件类型强制添加 `x-oss-force-download: true` 和 `Content-Disposition: attachment` 响应头，即使签名 URL 中指定了 `response-content-disposition=inline` 也无效。详见 [阿里云官方文档：如何配置访问OSS文件时的预览行为](https://help.aliyun.com/zh/oss/how-to-ensure-an-object-is-previewed-when-you-access-the-object)。

**解决方案**（已在代码中修复）：不再让前端 iframe 直接请求 OSS 签名 URL，改为通过后端 `/api/files/serve/{file_id}` 代理。后端从 OSS 下载文件内容，再以正确的 `Content-Type` 和 `Content-Disposition: inline` 头返回给浏览器。对应代码位于 `backend/api/files.py`：

- `get_original_file_info` 端点：无自定义域名时返回 `/api/files/serve/{file_id}`（而非 OSS 签名 URL）
- `serve_file` 端点：通过 `oss_service.bucket.get_object()` 从 OSS 读取完整文件内容，用 `Response` 返回

**备选方案**：如需绕过后端代理（避免大文件占用后端带宽），可绑定**已备案的自定义域名**到 OSS Bucket，然后在 `.env` 中配置：

```ini
OSS_CUSTOM_DOMAIN=your-domain.com
```

配置自定义域名后，文件 URL 会使用该域名，不受 OSS 默认域名的强制下载策略影响。

### 本地开发时 PDF/图片在 Original 面板显示主页而非文件内容

**症状**：在中国境内或本地运行前后端分离开发（前端 3000、后端 8000）时，点击 PDF/图片后，右侧 Original 标签页显示的是 localhost:3000 的主页内容，而不是文件预览。

**原因**：后端返回的 `file_url` 为相对路径 `/api/files/serve/{file_id}`。iframe 使用该相对路径时，浏览器会请求当前页面的 origin（即前端 dev server 的 3000 端口）。Create React App 的 proxy 在某些情况下未对该 iframe 请求生效，dev server 将请求当作 SPA 路由返回 `index.html`，导致 iframe 内显示的是前端主页。

**解决方案**（已在代码中修复）：在 `get_original_file_info` 中，当 `ENV=development` 且 `file_url` 为相对路径 `/api/files/serve/...` 时，将其改写为指向后端的绝对 URL（如 `http://localhost:8000/api/files/serve/{file_id}`），使 iframe 直接请求后端，避免经过前端 dev server。对应代码位于 `backend/api/files.py`，在返回 `file_url` 前统一做开发环境改写。

**要点**：本地开发时需确保后端 `ENV=development`（或未设置，默认 development），且前端 iframe 会加载 `http://localhost:8000/api/files/serve/...?token=...`，因此后端 8000 必须已启动。

### 远程部署后 PDF 预览显示 "localhost refused to connect"（Word/Excel 正常）

**症状**：按本指南在远程服务器部署后，Upload files 模块中 Word、Excel 可预览，但 PDF 无法预览，浏览器提示 "localhost refused to connect"。

**原因**：后端根据 `ENV` 判断是否为开发环境。未设置 `ENV=production` 时默认为 `development`，会将 PDF/图片的预览地址从相对路径 `/api/files/serve/{file_id}` 改写为 `http://localhost:8000/api/files/serve/...`，以便本地开发时 iframe 直连后端。在远程部署下，该 URL 被返回给用户浏览器，浏览器会请求**用户本机**的 localhost，导致连接被拒绝。Word/Excel 使用 OSS 签名 URL（公网地址），不受此影响。

**解决方案**：部署时需设置 `ENV=production`。已在 `deployment/config.defaults.env` 中默认加入 `ENV=production`，重新执行 `sudo bash deployment/deploy.sh` 后生成的 `.env` 会包含该变量。若已部署且未重新执行 deploy，可手动在项目根目录 `.env` 中增加一行 `ENV=production`，然后重启后端服务（如 `systemctl restart rc-webapp`）。确保 Nginx 已将 `/api` 代理到后端，PDF 预览即可正常。

### Word/Excel 在右侧 Original 面板无法预览（显示 "An error occurred"）

**症状**：本地开发时 Word/Excel 可预览，部署到远程服务器后，点击文件卡片，右侧 Original 标签页显示 "An error occurred - We're sorry, but for some reason we can't open this for you."

**原因**：与 **URL 可访问性** 有关，与代理、OSS、后端 serve 接口均相关：

1. **后端返回的 file_url**：未配置 `OSS_CUSTOM_DOMAIN` 时，OSS 文件会返回相对路径 `/api/files/serve/{file_id}`。
2. **前端判断**：原先用 `file_url.includes('localhost')` 区分本地/生产；远程环境下该 URL 不含 localhost，被误判为“生产环境”，从而走 Microsoft Office Online Viewer。
3. **Office Online Viewer 限制**：预览服务需要从 **公网可访问的绝对 HTTPS URL** 拉取文件。相对路径无法被 Microsoft 服务器解析；即使用绝对地址，`/api/files/serve/` 需要 JWT 鉴权，第三方服务无法带 token 访问，因此仍会失败。

**解决方案**（已在代码中修复）：


| 位置                                            | 修改要点                                                                                                                                 |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| **后端** `backend/api/files.py`                 | OSS 文件（无自定义域名时）改为生成 **OSS 签名 URL**（2 小时有效、公网可访问、强制 HTTPS），不再返回 `/api/files/serve/{file_id}`。Microsoft 可直接从 OSS 拉取文件进行预览。             |
| **前端** `web-frontend/src/pages/FilesPage.tsx` | 将“是否使用 Office Online Viewer”的判断从 `includes('localhost')` 改为 `file_url.startsWith('https://')`，只有公网 HTTPS URL 才送入 Office 预览；否则显示下载按钮。 |


**要点**：Word/Excel 在线预览依赖 **公网可访问的 HTTPS 文件地址**。若使用 OSS 且未配置自定义域名，必须依赖后端生成的**签名 URL**；配置了 `OSS_CUSTOM_DOMAIN` 时，会直接使用该域名的公共 URL，同样满足 Office 预览要求。

### Twitter 订阅推文拉取（优化说明与调度）

Twitter 订阅由 **APScheduler** 触发任务类型 `twitter_subscription`，实际拉取在 **Celery Data Worker**（`fetch_twitter_subscription`）中执行，避免阻塞调度器进程。数据源为 [twitterapi.io Advanced Search](https://docs.twitterapi.io/api-reference/endpoint/tweet_advanced_search)（按请求计费，每页最多约 20 条，需 `cursor` 分页）。

#### 行为要点（代码：`backend/services/twitter_subscription_service.py`）

1. **动态时间窗口**：定时任务（`force_fetch=False`）根据同一作者下各订阅的 `last_fetched_at` 计算 `since_hours`，避免每次固定拉取 24 小时全量。公式含约 1.5 小时缓冲，并封顶为环境变量 `TWITTER_DEFAULT_QUERY_HOURS`（默认 24）。
2. **时区与数据库**：`subscription_list.last_fetched_at` 为 **无时区** 的 `timestamp`（与服务器本地时间一致）。计算间隔时使用 **本地 naive `datetime.now()`** 与库中时间比较；若误用 UTC 与本地时间混比，会导致间隔为负、始终回退到 24 小时窗口。
3. **动态分页上限**：窗口较小时自动降低 `max_pages`（例如 ≤3 小时 → 2 页；≤12 小时 → 3 页；否则 5 页），减少无效翻页请求。
4. **空页提前结束**：某一页返回 0 条推文时不再继续请求下一页，避免浪费 token。
5. **批量去重写库**：拉取后先批量查询已存在的 `content_id`，对已知推文跳过 `save_content` 的 UPSERT；若仍需合并 `user_ids`，会走轻量 `UPDATE`。任务结果中的 `db_writes_saved` 表示因此跳过的写入次数。
6. **首次 / 强制拉取**：`force_fetch=True` 时仍按原逻辑（首次作者 7 天、非首次 12 小时等），不受上述增量窗口影响。

#### 环境变量（可选）


| 变量                            | 含义                          |
| ----------------------------- | --------------------------- |
| `TWITTER_API_KEY`             | twitterapi.io 密钥（必填）        |
| `TWITTER_DEFAULT_QUERY_HOURS` | 无历史或回退时的默认查询窗口（小时），默认 `24`  |
| `TWITTER_INITIAL_FETCH_HOURS` | 某作者首次入库时的回溯窗口，默认 `168`（7 天） |


#### 调度 Cron 建议

- 若对实时性不敏感、**每天两次**即可：在任务配置中使用 Cron `0 8,20 * * `*，时区 `America/New_York`（覆盖美东交易日 + 国内早晚查看习惯）。两次间隔约 12 小时时，增量窗口会自动扩大到约 13～14 小时并配合 `max_pages=5`，避免漏拉。
- 若仍使用 **每 30 分钟**（`*/30 * * * `*），增量窗口与 `max_pages` 会随间隔自动缩小，API 调用量显著低于「固定 24h」旧逻辑。

#### 部署后需重启

修改 `twitter_subscription_service.py` 后须重启 **Data Worker** 使 Celery 加载新代码：

```bash
sudo systemctl restart rc-celery-data
# 或: sudo ./restart_services.sh
```

#### 日志与自检

- 日志文件：`tail -f logs/celery_data_worker.log`
- 正常增量拉取会出现：`增量拉取` / `smart_hours` / `since_hours` / `max_pages`；完成行含 `api_calls`、`db_writes_saved`、`execution_duration_ms`。
- 参考脚本（与后端同一 Advanced Search）：`scripts/fetch_twitter_user_recent.py`。

---

## 11. Chrome Extension (Web Clipper) 部署

Web Clipper 是一个 Chrome 扩展，用户可在浏览器中一键将网页内容（含内嵌图片）保存到平台 OSS，自动提取文本并支持后续 AI 分析。

### 11.1 配置扩展默认服务器地址

部署前需将扩展代码中的占位域名替换为实际的生产地址：

```bash
cd /opt/guanhetech/chrome-extension
sed -i "s|https://your-domain.com|https://你的实际域名|g" popup.js background.js
```

> **纯 HTTP 部署（场景 B）**：将 `https://` 改为 `http://你的公网IP`。

### 11.2 替换扩展图标（可选）

默认提供蓝色占位图标。如需替换为品牌图标，准备 16×16、48×48、128×128 三个尺寸的 PNG 文件，覆盖 `chrome-extension/` 下的 `icon16.png`、`icon48.png`、`icon128.png`。

### 11.3 企业分发安装

本扩展不发布到 Chrome Web Store，通过企业内部分发方式安装：

1. 将 `chrome-extension/` 文件夹打包或直接分发给用户
2. 用户打开 Chrome → 地址栏输入 `chrome://extensions/`
3. 开启右上角 **开发者模式**
4. 点击 **加载已解压的扩展程序** → 选择 `chrome-extension/` 文件夹
5. 将扩展固定到工具栏

> **移动端**：Android Chrome 不支持扩展。移动端用户可通过 Kiwi Browser 或 Yandex Browser 加载 Chrome 扩展。iOS 不支持。

### 11.4 用户使用流程

1. 点击工具栏扩展图标，输入平台账号密码登录（JWT 会自动保存）
2. 打开任意网页，点击扩展图标 → **Capture Page**
3. 扩展自动提取内容、下载图片内嵌为 base64、发送到后端
4. 页面右上角显示绿色 toast 表示保存成功
5. 在平台 Sources 页面可看到新增的 **Web** 类型文件卡片，Original 标签页可预览完整网页

### 11.5 可选环境变量


| 变量                    | 含义                      | 默认值          |
| --------------------- | ----------------------- | ------------ |
| `OSS_WEB_CLIP_PREFIX` | Web Clip 文件在 OSS 中的存储前缀 | `web-clips/` |


如需修改，在对应环境的配置文件中添加后重新生成 `.env`：

```bash
# 服务器：编辑 deployment/config.defaults.env 或 deployment/deploy.conf
# 本地：编辑 development/deploy.conf
# 然后重新生成 .env
```

### 11.6 故障排查


| 症状                                | 原因与解决                                                                                        |
| --------------------------------- | -------------------------------------------------------------------------------------------- |
| Capture 后提示 "Not logged in"       | 扩展未登录。点击扩展图标，输入账号密码登录                                                                        |
| Capture 后提示 "Session expired"     | JWT 已过期（8 小时有效）。重新登录即可                                                                       |
| 提示 "Is the server running at ..." | 服务器地址配置错误或后端未启动。检查扩展 popup 中的 Server URL，确认后端可达                                              |
| 保存成功但 Sources 列表无新文件              | 刷新页面。若仍无，检查后端日志 `journalctl -u rc-webapp -f` 和 Celery 日志 `tail -f logs/celery_ai_worker.log` |


---

## 附录: Git 与 GitHub 推送配置

### 0. 配置 Git 用户信息（首次提交前必做）

提交代码前，必须配置 `user.name` 和 `user.email`，否则会提示无法提交：

```bash
# 全局配置（推荐，所有仓库生效）
git config --global user.name "你的名字"
git config --global user.email "your-email@example.com"

# 仅当前项目生效（去掉 --global）
git config user.name "你的名字"
git config user.email "your-email@example.com"
```

邮箱建议与 GitHub/GitLab 账号一致，便于识别提交者。

### 1. 创建 GitHub 仓库

前往 [https://github.com/new](https://github.com/new) 创建仓库：

- **Repository name**: `guanhetech`
- **Visibility**: Private
- 不要勾选 README / .gitignore / License（本地已有）

### 2. 生成 Personal Access Token

1. 前往 GitHub → Settings → Developer settings → [Personal access tokens (classic)](https://github.com/settings/tokens/new)
2. 点击 **Generate new token (classic)**
3. 设置：
  - **Note**: `guanhetech-server`（备注，随意填写）
  - **Expiration**: 按需选择（推荐 No expiration）
  - **Select scopes**: 勾选 `**repo`**（整个大类）
4. 点击 **Generate token**，复制生成的 token（仅显示一次）

### 3. 配置远程仓库并推送

```bash
cd /opt/guanhetech

# 添加 GitHub 远程仓库（如已有 Gitee 的 origin，先重命名）
git remote rename origin gitee
git remote add origin https://github.com/<你的用户名>/guanhetech.git

# 推送（首次需要输入用户名和 token 作为密码）
git push -u origin master
```

### 4. 缓存凭证（避免每次输入 token）

```bash
git config --global credential.helper store
```

运行后，下次 `git push` 输入一次用户名和 token，之后会自动记住。

### 5. 日常推送

```bash
cd /opt/guanhetech
git add .
git commit -m "描述你的变更"
git push origin master
```

> **安全提示**: 不要把 token 硬编码在 remote URL 中。使用 `credential.helper store` 或环境变量管理凭证。

