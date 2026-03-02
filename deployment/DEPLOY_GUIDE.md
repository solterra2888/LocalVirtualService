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

### 1.3 安装 Nginx（Docker 版本）

Nginx 通过 Docker 部署，无需 apt 安装。详见第 2.3 节。

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

### 2.3 Nginx（Docker）

Nginx Docker 容器由运维独立脚本部署，会自动初始化完整的配置目录结构：

```
/opt/nginx/
├── conf/
│   ├── nginx.conf          ← 主配置（由运维脚本部署，含 include vhosts/*.conf）
│   ├── mime.types           ← MIME 类型映射
│   └── vhosts/
│       └── web.conf         ← 项目站点配置（由 deploy.sh / start_services.sh 同步）
├── html/
│   └── guanhe/              ← 前端构建产物
└── log/                     ← Nginx 日志
```

> **注意**：不要手动执行 `docker run` 来创建 Nginx 容器。主配置 `nginx.conf` 通过 `include vhosts/*.conf;` 引入站点配置，容器启动前必须确保 `/opt/nginx/conf/` 下有完整的配置文件集。
>
> 项目只需管理 `web.conf`（站点配置），启动/部署脚本会自动将项目根目录的 `web.conf` 同步到 `/opt/nginx/conf/vhosts/web.conf`。

### 2.4 验证容器运行

```bash
docker ps
# 应看到 postgres、redis、nginx 三个容器状态为 Up
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

### 3.3 安装前端依赖并构建

```bash
cd web-frontend
npm install
npm run build
cd ..

# 将构建产物部署到 Nginx 静态文件目录
cp -r web-frontend/build/* /opt/nginx/html/guanhe/
```

> 线上环境不再使用 `npm start`（端口 3000），前端由 Docker Nginx 直接以静态文件方式提供服务。
> 如需本地前端开发调试，可手动执行 `cd web-frontend && npm start` 启动 3000 端口。

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
- 从模板渲染 4 个 systemd service 文件（rc-webapp、rc-celery-ai、rc-celery-data、rc-scheduler）
- 安装 service 文件到 `/etc/systemd/system/`
- 同步 `web.conf` 到 Docker Nginx 配置卷 `/opt/nginx/conf/vhosts/`
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
✅ Celery AI Worker已启动
✅ Celery Data Worker已启动
✅ 调度器服务已启动
✅ Nginx (Docker: nginx) 已启动
```

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
# systemd 服务
systemctl enable rc-webapp rc-celery-ai rc-celery-data rc-scheduler

# Docker 容器已设置 --restart always，会随 Docker 自动启动
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
| 查看 Nginx 日志 | `tail -f /opt/nginx/log/rc_project_access.log` 或 `docker logs nginx --tail 50` |

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

检查 Docker Nginx 是否正常运行以及后端是否可达：

```bash
docker ps | grep nginx                    # 确认 Nginx 容器在运行
docker logs nginx --tail 20               # 查看 Nginx 近期日志
curl http://localhost:8000/api/health      # 绕过 Nginx 直接访问后端
ls /opt/nginx/html/guanhe/index.html      # 确认前端构建产物存在
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

---

## 附录: 配置文件层级说明

```
deployment/deploy.conf        ← 第 1 层：服务器基础设施（路径、IP、端口）
deployment/.env.secrets        ← 第 2 层：敏感密钥（密码、API Key）
deployment/config.defaults.env ← 第 3 层：应用默认参数（通用，不需要改）
         ↓
    deploy.sh 合并
         ↓
    ┌────┼────────────────┐
    ↓    ↓                ↓
  .env  *.service     web.conf → /opt/nginx/conf/vhosts/
        (4个)          (Docker Nginx 配置)
```

修改任何配置后，都只需运行：

```bash
sudo bash deployment/deploy.sh && sudo ./restart_services.sh
```

---

## 附录: Git 与 GitHub 推送配置

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
   - **Select scopes**: 勾选 **`repo`**（整个大类）
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
