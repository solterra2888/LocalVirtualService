# Ubuntu 虚拟机部署 YouTube Remote Worker 手册

本手册面向：**Windows 主机 + VMware Workstation + Ubuntu Server 24.04 虚拟机**，在虚拟机内部署 `youtube_fetching` Celery Worker，通过 SSH 隧道连接远程服务器（`43.99.37.76`）上的 Redis 与 PostgreSQL，用虚拟机出口 IP 拉取 YouTube RSS 与字幕。

---

## 一、开始前你需要知道的事

| 项目 | 说明 |
|------|------|
| 角色 | 虚拟机只负责 **执行** YouTube 抓取任务；**定时触发** 仍在服务器上的 APScheduler |
| 网络 | 建议虚拟机使用 **桥接网络（Bridged）**，与家里路由器同一网段，出口更像家庭宽带（若只用 NAT 一般也可，但桥接更直观） |
| 服务器 | 需已部署本仓库最新代码，并已 **重启后端**（含 Scheduler），使 `youtube_subscription` 任务改为投递到 `youtube_fetching` 队列 |
| 账号 | 文档示例使用服务器 SSH 用户 **`root@43.99.37.76`**；若你改用其他用户，需保证该用户能 SSH 登录，且隧道目标仍是本机 `127.0.0.1:6379` 与 `127.0.0.1:5432`（与服务器上 Redis/PG 监听一致） |

---

## 二、VMware 与虚拟机网络（建议）

1. 关闭虚拟机电源。
2. 虚拟机设置 → **网络适配器** → 选择 **桥接模式（Bridged）**，并勾选与主机实际联网的网卡（有线/无线）。
3. 给虚拟机分配 **至少 2GB 内存、20GB 磁盘**（Python 环境与依赖足够用）。
4. 启动虚拟机，完成 Ubuntu Server 安装向导；**务必勾选安装 OpenSSH server**，以便后续 SSH 进虚拟机操作。

安装完成后建议：

```bash
sudo apt update && sudo apt upgrade -y
sudo reboot
```

---

## 三、确认虚拟机网络与 SSH

在虚拟机里登录后执行：

```bash
ip a
ping -c 3 8.8.8.8
ping -c 3 43.99.37.76
```

- 能 ping 通公网与服务器，说明网络正常。
- 若你打算从 Windows 用 **PuTTY / VS Code Remote** 连虚拟机，记下虚拟机 IP（如 `192.168.x.x`）。

---

## 四、安装系统依赖

在虚拟机中执行（整段复制即可）：

```bash
sudo apt update
sudo apt install -y \
  git \
  curl \
  python3 \
  python3-venv \
  python3-pip \
  autossh \
  openssh-client \
  build-essential \
  libpq-dev
```

说明：`build-essential` / `libpq-dev` 便于部分 Python 包编译；若使用 `psycopg2-binary` 多数情况可省略 `libpq-dev`，保留更稳妥。

---

## 五、准备 Linux 用户与项目目录

建议使用普通用户（非 root）运行 Worker，示例用户名为 `ubuntu`（安装时若创建的其他用户名，请替换下面路径中的用户名）。

```bash
whoami
cd ~
mkdir -p ~/guanhetech
```

### 5.1 获取代码（二选一）

**方式 A：从服务器拷贝（适合仓库未公开）**

在 **Windows 主机** 或 **能 SSH 到服务器的机器** 上，用 `scp` 把服务器上的项目拷到虚拟机；或在虚拟机里若已配好到服务器的密钥：

```bash
# 在虚拟机中（需已能 ssh 登录服务器）
scp -r root@43.99.37.76:/opt/guanhetech ~/guanhetech-src
mv ~/guanhetech-src ~/guanhetech
```

**方式 B：git clone（仓库可访问时）**

```bash
cd ~
git clone <你的仓库地址> guanhetech
cd ~/guanhetech
```

---

## 六、Python 虚拟环境与依赖

```bash
cd ~/guanhetech
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

若 `requirements.txt` 安装报错，可至少先装：

```bash
pip install "celery[redis]==5.3.1" redis psycopg2-binary feedparser requests youtube-transcript-api
```

（完整项目仍建议解决全部依赖，以免导入其他模块失败。）

```bash
mkdir -p ~/guanhetech/logs
```

---

## 七、配置 SSH 免密登录服务器

Worker 依赖 **长期稳定的 SSH 隧道**，必须使用密钥，避免每次输入密码。

在虚拟机中：

```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ""
ssh-copy-id root@43.99.37.76
```

按提示输入一次服务器 root 密码。然后测试：

```bash
ssh -o BatchMode=yes root@43.99.37.76 "echo OK"
```

应输出 `OK`。若失败，检查服务器 `sshd` 是否允许公钥登录、`/root/.ssh/authorized_keys` 权限等。

---

## 八、手动测试 SSH 隧道（重要）

先确认隧道能通，再写 systemd。

```bash
# 前台测试（另开一个终端窗口观察，或用 Ctrl+C 结束）
autossh -M 0 -N \
  -o "ServerAliveInterval=30" \
  -o "ServerAliveCountMax=3" \
  -L 16379:127.0.0.1:6379 \
  -L 15432:127.0.0.1:5432 \
  root@43.99.37.76
```

保持该进程运行，**再开一个 SSH 会话** 到同一台虚拟机，执行：

```bash
# 测试 Redis（需安装 redis-tools：sudo apt install -y redis-tools）
redis-cli -h 127.0.0.1 -p 16379 ping
# 期望：PONG

# 测试 PostgreSQL（需安装 postgresql-client：sudo apt install -y postgresql-client）
# 密码与服务器 DB 一致，见下一节
psql "host=127.0.0.1 port=15432 user=postgres dbname=guanhe_database" -c "SELECT 1;"
```

若 `redis-cli`/`psql` 失败，先不要继续，检查隧道与服务器防火墙是否放行本机 SSH（本方案不需要对外开放 Redis/PG 端口，仅 SSH 22）。

---

## 九、编写虚拟机上的 `.env`

在 `~/guanhetech/.env` 中至少需要 **数据库** 与 **Redis**，且指向 **隧道本地端口**。

1. **数据库密码**：与服务器 `/opt/guanhetech/.env` 中 `DB_PASSWORD` 一致。
2. **REDIS_URL**：在服务器上查看 `REDIS_URL`，把其中的 **`127.0.0.1:6379` 改成 `127.0.0.1:16379`**（只改主机端口，密码保持 URL 编码形式不变）。

示例结构（**请用你自己的密码替换占位符**）：

```bash
nano ~/guanhetech/.env
```

内容示例：

```env
DB_HOST=127.0.0.1
DB_PORT=15432
DB_NAME=guanhe_database
DB_USER=postgres
DB_PASSWORD=你的数据库明文密码

REDIS_URL=redis://:URL编码后的Redis密码@127.0.0.1:16379/0

LOG_DIR=./logs
LOG_LEVEL=INFO
LOG_TO_FILE=true
LOG_TO_CONSOLE=true
```

保存后：

```bash
chmod 600 ~/guanhetech/.env
```

---

## 十、前台手动跑一次 Celery Worker（验证）

确保上一节的手动 `autossh` 仍在运行，或另开一个终端先拉起隧道。

```bash
cd ~/guanhetech
source venv/bin/activate
export PYTHONPATH=~/guanhetech
celery -A backend.celery_app worker \
  --queues=youtube_fetching \
  --concurrency=1 \
  --hostname=youtube-worker@test \
  --loglevel=info
```

- 若能看到 `connected to redis` / `ready` 类日志，说明 **Redis 隧道与 Celery 正常**。
- 在服务器上触发一次 YouTube 调度或等待整点任务，观察虚拟机终端是否出现 `fetch_all_youtube_subscriptions` 相关日志。

按 `Ctrl+C` 停止前台 Worker。

---

## 十一、使用 systemd 用户服务（开机自启）

项目内已提供模板（与仓库同步）：

- [`ssh-tunnel.service`](ssh-tunnel.service)
- [`youtube-worker.service`](youtube-worker.service)

### 11.1 安装服务文件

```bash
mkdir -p ~/.config/systemd/user
cp ~/guanhetech/deployment/remote-youtube-worker/ssh-tunnel.service ~/.config/systemd/user/
cp ~/guanhetech/deployment/remote-youtube-worker/youtube-worker.service ~/.config/systemd/user/
```

确认 `youtube-worker.service` 里 **`ExecStart`** 路径为 `~/guanhetech/venv/bin/celery`（若你使用 conda 或其他路径，需用 `nano` 修改该文件）。

### 11.2 允许用户级服务在登录前也能跑（推荐）

```bash
sudo loginctl enable-linger $USER
```

### 11.3 加载并启动

```bash
systemctl --user daemon-reload
systemctl --user enable ssh-tunnel.service
systemctl --user enable youtube-worker.service
systemctl --user start ssh-tunnel.service
sleep 3
systemctl --user start youtube-worker.service
```

### 11.4 查看状态

```bash
systemctl --user status ssh-tunnel.service
systemctl --user status youtube-worker.service
tail -f ~/guanhetech/logs/celery_youtube_worker.log
```

---

## 十二、服务器端必须完成的操作

在 **43.99.37.76** 上：

1. 已合并/部署包含 `youtube_fetching` 队列改动的代码。
2. 重启相关服务，例如：

```bash
sudo ./restart_backend.sh
```

3. 确认本机 **`rc-celery-data` 只消费 `data_fetching`**，**不再消费 `youtube_fetching`**（当前实现下 YouTube 任务已单独队列，由虚拟机 Worker 消费）。

---

## 十三、常见问题

| 现象 | 可能原因 | 处理 |
|------|----------|------|
| `Connection refused` 连 Redis | 隧道未建立或端口错 | `systemctl --user status ssh-tunnel.service`，检查 `16379` |
| Celery 无法连 PostgreSQL | `DB_PASSWORD` 错误或隧道未通 | 核对 `.env`，再测 `psql` |
| 任务永远不执行 | 虚拟机未开机或 Worker 未启动 | 开机后 `systemctl --user start ...` |
| 仍无 transcript | 非 IP 问题（如视频无字幕） | 看 `celery_youtube_worker.log` 与服务器 `subscription_content` |
| VMware 挂起/休眠 | 隧道断开 | 恢复后 `autossh` 会重连；确认 `Restart=always` |

---

## 十四、安全建议

- 虚拟机 `.env` 含数据库与 Redis 密码，**勿提交到 Git**，权限保持 `600`。
- 仅通过 SSH 隧道访问服务器 Redis/PG，**不要在公网开放 5432/6379**。
- 定期 `sudo apt upgrade` 更新虚拟机系统。

---

## 十五、相关文件索引

| 文件 | 说明 |
|------|------|
| [`.env.template`](.env.template) | 环境变量字段说明 |
| [`setup.sh`](setup.sh) | Linux 上一键脚本（可选；若路径与本文档一致可对照手工执行） |
| [`ssh-tunnel.service`](ssh-tunnel.service) | SSH 隧道 systemd 模板 |
| [`youtube-worker.service`](youtube-worker.service) | Celery Worker systemd 模板 |

---

部署完成后，YouTube 定时任务将由服务器 **投递到 Redis**，由本虚拟机 **Worker 拉取并执行**，数据写回服务器数据库，前端无需改动。
