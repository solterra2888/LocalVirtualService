# Ubuntu 虚拟机部署 YouTube Remote Worker 手册

本手册面向：**Windows 主机 + VMware Workstation + Ubuntu Server 24.04 虚拟机**，在虚拟机内部署 `youtube_fetching` Celery Worker，**通过服务器公网 IP 直连** Redis 与 PostgreSQL，用虚拟机出口 IP 拉取 YouTube RSS 与字幕。

> **说明**：当前默认方案为 **公网直连**（你已在服务器侧做好安全策略，例如防火墙仅放行家里公网 IP、强密码等）。若你希望 **不开放** 6379/5432，可改用 SSH 本地转发，见文末 [附录：可选 SSH 隧道](#附录可选-ssh-隧道)。

---

## 一、开始前你需要知道的事

| 项目 | 说明 |
|------|------|
| 角色 | 虚拟机只负责 **执行** YouTube 抓取任务；**定时触发** 仍在服务器上的 APScheduler |
| 网络 | 建议虚拟机使用 **桥接网络（Bridged）**，出口为家庭宽带 IP |
| 服务器 | 已部署含 `youtube_fetching` 队列的代码，并已 **重启后端**；**Redis、PostgreSQL 需对 Worker 可达**（通常监听 `0.0.0.0` 或经安全组/NAT 映射，且防火墙 **限源** 至你家宽带公网 IP） |
| 连接方式 | Worker 使用 `DB_HOST` / `REDIS_URL` **直接指向服务器公网 IP**（如 `43.99.37.76`），**不需要**在虚拟机里跑 SSH 隧道 |

---

## 二、服务器端网络前提（必读）

在 **43.99.37.76** 上需同时满足：

1. **Redis**（默认 6379）与 **PostgreSQL**（默认 5432）对来自 **家用出口 IP** 的连接可用（监听地址、安全组、iptables 等已按你的方案配置）。
2. 应用使用的账号密码与服务器 `/opt/guanhetech/.env` 中 **`DB_*`、`REDIS_URL`** 一致；虚拟机上的 `.env` 仅把 Redis URL 里的 **`127.0.0.1` 换成公网 IP**，端口一般仍为 `6379` / `5432`。
3. 若 PostgreSQL 要求 **SSL**，需在应用侧支持（当前 `DatabaseManager` 为常规 `psycopg2` 连接；若你启用了强制 SSL，请确认驱动与参数，必要时单独调整）。

---

## 三、VMware 与虚拟机网络（建议）

1. 虚拟机设置 → **网络适配器** → **桥接模式（Bridged）**。
2. 至少 **2GB 内存、20GB 磁盘**。
3. 安装 Ubuntu Server 时勾选 **OpenSSH server**，便于从 Windows SSH 进虚拟机。

```bash
sudo apt update && sudo apt upgrade -y
sudo reboot
```

---

## 四、确认虚拟机网络

```bash
ip a
ping -c 3 8.8.8.8
ping -c 3 43.99.37.76
```

---

## 五、安装系统依赖

```bash
sudo apt update
sudo apt install -y \
  git \
  curl \
  python3 \
  python3-venv \
  python3-pip \
  build-essential \
  libpq-dev \
  redis-tools \
  postgresql-client
```

说明：`redis-tools` / `postgresql-client` 用于下一节 **直连测试**（可选但推荐）。

---

## 六、获取代码

**方式 A：git clone**（仓库可访问时）

```bash
cd ~
git clone <你的仓库地址> guanhetech
```

**方式 B：从服务器拷贝**

```bash
scp -r root@43.99.37.76:/opt/guanhetech ~/guanhetech
```

---

## 七、Python 虚拟环境与依赖

```bash
cd ~/guanhetech
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
mkdir -p ~/guanhetech/logs
```

---

## 八、测试公网 Redis / PostgreSQL（推荐）

将下面命令中的 **`你的服务器公网 IP`**、**密码** 换成真实值（与服务器 `.env` 一致）。

```bash
# Redis — 与 REDIS_URL 中密码一致；端口若改过请改 -p
redis-cli -h 43.99.37.76 -p 6379 -a '你的Redis密码' ping
# 期望：PONG

# PostgreSQL
PGPASSWORD='你的数据库密码' psql -h 43.99.37.76 -p 5432 -U postgres -d guanhe_database -c "SELECT 1;"
```

若此处失败，先修服务器防火墙/监听/安全组，再部署 Worker。

---

## 九、编写虚拟机上的 `.env`

与 [`.env.template`](.env.template) 一致：**`DB_HOST` 为公网 IP**，**`REDIS_URL` 与服务器相同，仅把 `127.0.0.1` 换成公网 IP**。

```bash
nano ~/guanhetech/.env
```

示例（密码请替换）：

```env
DB_HOST=43.99.37.76
DB_PORT=5432
DB_NAME=guanhe_database
DB_USER=postgres
DB_PASSWORD=你的数据库明文密码

REDIS_URL=redis://:URL编码后的Redis密码@43.99.37.76:6379/0

LOG_DIR=./logs
LOG_LEVEL=INFO
LOG_TO_FILE=true
LOG_TO_CONSOLE=true
```

```bash
chmod 600 ~/guanhetech/.env
```

---

## 十、前台跑一次 Celery Worker（验证）

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

应出现已连接 broker、ready 等日志。服务器触发一次 YouTube 调度后，此处应收到 `fetch_all_youtube_subscriptions` 类任务。按 `Ctrl+C` 停止。

---

## 十一、systemd 开机自启（仅 Worker）

[`youtube-worker.service`](youtube-worker.service) 已 **不依赖** SSH 隧道服务。

```bash
mkdir -p ~/.config/systemd/user
cp ~/guanhetech/deployment/remote-youtube-worker/youtube-worker.service ~/.config/systemd/user/
# 若 Celery 不在 ~/guanhetech/venv/bin/celery，请编辑该文件中的 ExecStart

sudo loginctl enable-linger "$USER"
systemctl --user daemon-reload
systemctl --user enable --now youtube-worker.service
systemctl --user status youtube-worker.service
tail -f ~/guanhetech/logs/celery_youtube_worker.log
```

---

## 十二、一键脚本（可选）

若虚拟机 **已能免密 SSH 到服务器 root**（仅用于脚本从服务器读取 `.env` 生成本地配置），可执行：

```bash
bash ~/guanhetech/deployment/remote-youtube-worker/setup.sh
```

环境变量 **`SERVER_HOST`** 可覆盖默认 IP，例如：

```bash
SERVER_HOST=43.99.37.76 bash ~/guanhetech/deployment/remote-youtube-worker/setup.sh
```

若无法 SSH 到服务器，请按 **第八、九节** 手工测试并手写 `.env`。

---

## 十三、服务器端应用部署

1. 代码已包含 `youtube_fetching` 路由与 Scheduler 投递逻辑。
2. 重启：

```bash
sudo ./restart_backend.sh
```

3. 服务器上的 `rc-celery-data` 只消费 `data_fetching`，**不消费** `youtube_fetching`；YouTube 任务由虚拟机 Worker 消费。

---

## 十四、常见问题

| 现象 | 可能原因 | 处理 |
|------|----------|------|
| Celery 连不上 Redis | 防火墙未放行家里 IP 或端口错 | 查服务器 `ss -tlnp`、云安全组、`redis-cli` 自外网测 |
| 连不上 Postgres | 同上或 `pg_hba.conf` 未允许 | 查 PG 日志、`psql` 自外网测 |
| 任务不执行 | Worker 未启动或队列名不一致 | `systemctl --user status youtube-worker` |
| 家里公网 IP 变动 | 防火墙限源失效 | 更新服务器规则或改固定出口 |

---

## 十五、安全建议

- 虚拟机 `chmod 600 ~/.env`，**勿提交 Git**。
- 公网暴露 Redis/PG 时务必 **最小权限 + 强密码 + 防火墙限源**；条件允许时用 **VPN/专线** 替代对全网开放。
- 定期 `sudo apt upgrade`。

---

## 附录：可选 SSH 隧道

若你 **不想** 对公网开放 6379/5432，可在虚拟机用 **SSH 本地转发** 把流量转到服务器本机 Redis/PG，此时 `.env` 使用 `DB_HOST=127.0.0.1`、`DB_PORT=15432`、`REDIS_URL` 指向 `127.0.0.1:16379`，并配合 [`ssh-tunnel.service`](ssh-tunnel.service) 与 `youtube-worker.service` 中 `After=` 隧道服务（需自行按需改回依赖关系）。旧版步骤可参考仓库历史或单独文档。

---

部署完成后，YouTube 定时任务由服务器 **写入 Redis**，虚拟机 **Worker 拉取并执行**，数据写回同一 PostgreSQL，前端无需改动。
