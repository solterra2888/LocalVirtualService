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

Nginx 通过 Docker 部署，无需 apt 安装。配置目录准备见第 2.3 节，容器创建见第 3.4 节。

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

### 2.3 Nginx 配置目录准备

Nginx 通过 Docker 部署。容器创建前，需先在宿主机准备好配置目录：

```
/opt/nginx/
├── conf/
│   ├── nginx.conf          ← 主配置（含 include vhosts/*.conf）
│   ├── mime.types           ← MIME 类型映射
│   └── vhosts/
│       └── web.conf         ← 项目站点配置（由 deploy.sh / start_services.sh 同步）
└── log/                     ← Nginx 日志
```

```bash
mkdir -p /opt/nginx/conf/vhosts /opt/nginx/html /opt/nginx/log
```

> 主配置 `nginx.conf`、`mime.types` 等文件由技术团队初始化（不在项目仓库中）。项目只需管理 `web.conf`（站点配置），部署脚本会自动同步到 `/opt/nginx/conf/vhosts/web.conf`。
>
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
```

> **本地开发**：执行 `cd web-frontend && npm start` 启动开发服务器（端口 3000，支持热更新），无需 Nginx。
>
> **线上部署**：前端由 Docker Nginx 以静态文件方式提供服务，不使用 `npm start`。

### 3.4 创建 Nginx 容器

前端构建完成后，即可创建 Nginx 容器。容器通过 Docker volume mount 将 `web-frontend/build/` 直接挂载为站点目录，无需手动复制文件。

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

## 4. 配置部署参数（两种场景）

这是最关键的一步。整个项目的配置集中在 `deployment/` 目录下的几个文件中，最终都会汇总到项目根目录的 `.env` 中。  
典型有两种使用场景：

- **场景 A：正式服务器部署（推荐本节 4.1–4.3 + 第 5–7 章）**
- **场景 B：本地单机开发（只需生成 `.env` + 使用 `development` 脚本）**

> ✅ 二者共享同一套 `deployment/deploy.conf` 与 `deployment/.env.secrets` 配置，只是生成 `.env` 的方式不同。

---

### 4.A 正式服务器部署：使用 `deployment/deploy.sh`（需要 root）

服务器上推荐的流程是：编辑 `deploy.conf` / `.env.secrets` → 运行 `sudo bash deployment/deploy.sh`，由脚本自动生成 `.env`、systemd 服务与 Nginx 配置。确保根目录有 `.env` 文件。

### 4.1 编辑部署配置（通用于服务器 & 本地）

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

### 4.2 编辑密钥配置（通用于服务器 & 本地）

密钥文件**仅放在 `deployment/` 目录**（部署脚本只读取此处），请勿在项目根目录创建 `.env.secrets`。

```bash
# 确保当前在 deployment 目录（若在 4.1 已 cd deployment 则无需再执行）
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

### 4.3 运行部署脚本（仅正式服务器需要）

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

### 4.B 本地开发：只生成 `.env`，不改 systemd / Nginx

在**本地开发机**上，一般不需要安装 systemd 服务和 Nginx，只需让根目录有一份完整的 `.env`，然后用 `development` 目录下的脚本启动即可。推荐步骤如下：

1. **准备开发机专用配置**

```bash
cd /opt/guanhetech

# 1) 本地 Python 环境等（development/dev.conf）
cp development/dev.conf.example development/dev.conf
vim development/dev.conf
# 设置 DEV_CONDA_ENV=/你的/conda/路径，例如:
#   DEV_CONDA_ENV=/opt/miniconda3/envs/guanhe

# 2) 复用 deployment 的示例配置（与服务器同一结构）
cp deployment/deploy.conf.example deployment/deploy.conf
cp deployment/.env.secrets.example deployment/.env.secrets
vim deployment/deploy.conf
# 建议本地修改：
#   PROJECT_DIR=/opt/guanhetech（或你的实际工程路径）
#   DB_HOST=127.0.0.1
#   REDIS_HOST=127.0.0.1
#   SERVER_HOST=127.0.0.1

vim deployment/.env.secrets
# 按需填写本地使用的 DB 密码、Redis 密码、各类 API Key（可与服务器不同）
```

2. **使用本地脚本生成 `.env`（无需 root）**

```bash
# 仍在项目根目录 /opt/guanhetech
bash development/generate_env_from_deploy.sh
```

该脚本会：

- 读取：`deployment/deploy.conf`、`deployment/.env.secrets`、`deployment/config.defaults.env`
- 生成：项目根目录 `.env`（结构与服务器上 `deploy.sh` 生成的基本一致）
- 不进行任何 systemd / Nginx 操作，适合本地开发环境

3. **启动本地开发环境**

```bash
cd /opt/guanhetech
./development/start_local_dev.sh
```

该脚本会：

- 加载 `development/dev.conf`（Python 环境）
- 加载项目根目录 `.env`
- 检查数据库、Redis、Celery 配置
- 启动本地 Web 后端（默认 `http://localhost:8000`）与前端（默认 `http://localhost:3000`）

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


| 操作           | 命令                                                                             |
| ------------ | ------------------------------------------------------------------------------ |
| 启动所有服务       | `sudo ./start_services.sh`                                                     |
| 停止所有服务       | `sudo ./stop_services.sh`                                                      |
| 重启所有服务       | `sudo ./restart_services.sh`                                                   |
| 仅重启后端        | `sudo ./restart_backend.sh`                                                    |
| 前端构建并部署      | `cd web-frontend && npm run deploy`                                            |
| 修改配置后重新部署    | `sudo bash deployment/deploy.sh && sudo ./restart_services.sh`                 |
| 查看某服务日志      | `journalctl -u rc-webapp -f`                                                   |
| 查看 Celery 日志 | `tail -f logs/celery_ai_worker.log`                                            |
| 查看 Nginx 日志  | `tail -f /opt/nginx/log/rc_project_access.log` 或 `docker logs nginx --tail 50` |


---

## 9. 服务器配置

### 服务器端口

实例 > 点击实例ID > 安全组 > 管理规则 > 入方向 > 增加规则：

- 访问来源选择任意位置
- 访问目的（端口）：
  - Redis: 6379
  - Postgres: 5432

## 10. 故障排查

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

### 上传文件/音频报错：AUTH &lt;password&gt; called without any password configured

**症状**：上传音频或文档时接口返回 500，日志中出现：

```
上传音频文件失败: AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?
```

或「上传文档文件失败」并带有相同 AUTH 相关错误。

**原因**：Celery 提交任务时使用 Redis 作为 broker。`.env` 中的 `REDIS_URL` 含有密码（由 `deployment/.env.secrets` 的 `REDIS_PASSWORD` 生成），但**本地 Redis 未设置密码**（例如用 `redis-server` 直接启动或由 `start_local_dev.sh` 启动的无密码实例）。Redis 服务端收到 AUTH 请求却未配置密码，因而报错。

**解决方案**（任选其一）：

1. **推荐（本地开发）**：若本地 Redis 无密码，在 `deployment/.env.secrets` 中将 `REDIS_PASSWORD` **留空或删除**，然后重新生成 `.env`：
   ```bash
   # 编辑 deployment/.env.secrets，将 REDIS_PASSWORD=xxx 改为空或注释掉
   bash development/generate_env_from_deploy.sh
   ```
   生成的 `REDIS_URL` 将变为 `redis://127.0.0.1:6379/0`，即可连接无密码 Redis。

2. **使用 start_local_dev.sh 启动**：脚本会先检查 Redis；若不可用会启动本地无密码 Redis，并写入 `development/.env.redis.local`（`REDIS_URL=redis://127.0.0.1:6379/0`）。之后启动的 Web 与 Celery 会先加载 `.env` 再加载该文件，从而覆盖为无密码连接。若你未通过该脚本启动（例如直接用 IDE 跑 uvicorn），则不会加载 `.env.redis.local`，需采用方案 1。

3. **生产/服务器**：若 Redis 已配置密码，请确保 `deployment/.env.secrets` 中 `REDIS_PASSWORD` 与 Redis 的 `requirepass` 一致，并重新运行 `deploy.sh` 或 `generate_env_from_deploy.sh` 以更新 `.env`。

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


| 层次                                  | 现象 / 报错                                                                                         | 处理方式                                                                                                                                                                                                                  |
| ----------------------------------- | ----------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1. 反机器人检测**                       | `Sign in to confirm you're not a bot`                                                           | 使用 Netscape 格式 `youtube-cookies.txt`，通过环境变量 `YOUTUBE_COOKIES_FILE` 指定路径（见 `deploy.conf.example`）。需在浏览器登录 YouTube 后用扩展导出，上传到服务器如 `config/youtube-cookies.txt`。                                                         |
| **2. n-challenge / 签名求解**           | `Signature solving failed` / `n challenge solving failed` / `Requested format is not available` | 安装 `yt-dlp[default]`（含 `yt-dlp-ejs`）并在代码中启用 Node.js 运行时（`js_runtimes`），使 yt-dlp 能解开 YouTube 的 JS 挑战。                                                                                                                  |
| **3. PO Token（GVS）**                | 仅剩 storyboard 等缩略图、无音频格式                                                                        | 部署 `bgutil-ytdlp-pot-provider`：clone 至 `/opt/bgutil-ytdlp-pot-provider`，编译 server，安装 Python 插件，并以 systemd 服务 `bgutil-pot` 运行（默认端口 4416）。                                                                              |
| **4. ffmpeg 缺失**                    | `ffprobe and ffmpeg not found`                                                                  | 在 conda 环境中安装：`conda install -y -c conda-forge ffmpeg`。代码中通过 `ffmpeg_location` 指定路径兜底。                                                                                                                                |
| **5. OSS 私有桶 403**                  | ASR 子任务 `FILE_403_FORBIDDEN`、转写完成但 `transcript` 为空                                              | 提交 ASR 时使用 OSS **签名 URL**（`signed_url`）而非 `public_url`，见上文「音频上传后 ASR 语音识别返回 FILE_403_FORBIDDEN」。                                                                                                                      |
| **6. OSS 私有桶 FILE_DOWNLOAD_FAILED** | 已使用签名 URL 仍报 `FILE_DOWNLOAD_FAILED`、ASR 未返回有效转写                                                 | Fun-ASR 可能无法通过 OSS 签名 URL 拉取。在 **deployment/.env.secrets** 中增加一行 `OSS_ASR_PUBLIC_READ=true`，然后重新运行 `sudo bash deployment/deploy.sh` 生成 `.env`。上传用于 ASR 的音频时会将该对象 ACL 设为 public-read，并改用公网 URL 提交 ASR。仅该次上传的音频对象会公网可读。 |


#### 常见错误速查

- **Sign in to confirm you're not a bot** → Cookie 失效或未配置。重新导出 `youtube-cookies.txt` 并更新服务器上 `config/youtube-cookies.txt`，必要时重启 Celery。
- **Invalid js_runtimes format** → yt-dlp Python API 要求 `js_runtimes` 为 `{'node': {'path': '...'}}` 的 dict 格式，已修复。
- **transcript 为空且 transcript_data.asr_data 为 null** → 多为 OSS 403（见上表第 5 项），或 DashScope 子任务失败，检查日志中的 `FILE_403_FORBIDDEN` 并确认使用 `signed_url`。
- **FILE_DOWNLOAD_FAILED（与 OSS 私有桶有关）** → 若已使用签名 URL 仍报此错，说明 Fun-ASR 可能不支持通过签名 URL 拉取。在 **deployment/.env.secrets** 中增加 `OSS_ASR_PUBLIC_READ=true`，重新执行 `deploy.sh` 后重启服务（见上表第 6 项）。

#### Cookie 过期与长期维护

YouTube 的 session cookie 会过期（通常 1–2 周），且机房 IP 易触发风控，因此「Sign in to confirm you're not a bot」可能反复出现。可选方案：


| 方案                      | 说明                                                                                                                                                                   |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **A. 定期检测 + 告警 + 上传接口** | 定时任务用当前 cookies 测一次 yt-dlp；若失败则告警。提供仅管理员可用的 API（如 `POST /api/admin/youtube-cookies`）接收新 cookies 文件并写回 `config/youtube-cookies.txt`，管理员在浏览器导出后通过接口或本地脚本上传，无需 SSH 改文件。 |
| **B. 内网 Cookie 代理机**    | 在可长期登录 YouTube 的机器上跑服务（如 Playwright 导出 cookies），HK 服务器定时拉取并更新 `youtube-cookies.txt`。实现成本较高，需维护浏览器环境与接口安全。                                                            |
| **C. 第三方转写/下载服务**       | 使用不依赖本方 cookies 的 SaaS（仅提供视频 URL），由对方处理 YouTube 访问与合规。                                                                                                               |
| **D. 仅需字幕时**            | 若视频自带字幕，可考虑 YouTube 官方 API 或第三方字幕 API 拉取字幕，避免 yt-dlp 下载与 cookie 维护。                                                                                                  |


当前推荐先做 **方案 A**：检测 + 告警 + 管理员上传接口，将「发现失效 → 更新 cookies」半自动化，减少手动 SSH 与改文件的频率。

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

