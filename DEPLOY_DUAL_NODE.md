# 双节点快速部署指南

新加坡 VPS（字幕）+ 香港家用机（ASR 下载）分工部署。两台机器共享香港主站的 **Redis** 与 **PostgreSQL**，通过队列拆分能力，避免新加坡 IP 下载视频、避免香港带宽被字幕任务占用。

```
香港主站 ──任务──► Redis ◄── 新加坡 caption 节点（字幕）
                    │              │
                    │              └── 字幕失败 → 派发 ASR 队列
                    │
                    └── 香港 asr 节点（yt-dlp 下载 + OSS + Fun-ASR）
```

---

## 一、队列分工一览

| 队列 | 新加坡 `caption` | 香港 `asr` |
|------|:----------------:|:----------:|
| `youtube_fetching` | ✅ 消费 | ❌ |
| `youtube_priority` | ✅ 消费 | ❌ |
| `youtube_transcription` | ❌ 只派发 | ✅ 消费 |
| `youtube_transcription_long` | ❌ | ✅ 消费 |
| `youtube_transcription_priority` | ❌ 只派发 | ✅ 消费 |

---

## 配置模板对照

| 机器 | 模板文件 | 一键初始化 |
|------|----------|------------|
| 新加坡 VPS | `.env.template.caption` | `bash setup.sh caption` |
| 香港家用机 | `.env.template.asr` | `bash setup.sh asr` |
| 单机全栈 | `.env.template` | `bash setup.sh` |

---

## 二、新加坡 VPS（字幕节点）

### 1. 克隆与配置

```bash
git clone https://github.com/solterra2888/LocalVirtualService.git /opt/local_virtual_service
cd /opt/local_virtual_service

# 方式 A：专用模板（推荐）
cp .env.template.caption .env

# 方式 B：setup 自动选模板（仅 .env 不存在时）
# bash setup.sh caption

nano .env   # 填 DB / Redis / Webshare
```

### 2. 必填项检查

- `DB_HOST` / `DB_PASSWORD` / `REDIS_URL` → 指向香港主站
- `WEBSHARE_PROXY_USERNAME` / `WEBSHARE_PROXY_PASSWORD` → 已预填结构，填入真实值
- `CAPTION_YTDLP_USE_WEBSHARE=true` / `CAPTION_ASR_FALLBACK_ENABLED=true` → 模板已默认开启
- **不需要** 配置 `OSS_*` / `DASHSCOPE_API_KEY`（ASR 在香港执行）

### 3. 一键部署

```bash
bash setup.sh
```

`setup.sh` 会读取 `WORKER_NODE_ROLE=caption`，自动启用 `yt-worker-caption.target`（`fetch` + `priority-transcript`）。

### 4. 验证

```bash
systemctl status yt-worker-fetch yt-worker-priority-transcript
tail -30 /opt/local_virtual_service/logs/worker.log
```

日志中应出现 Webshare 初始化、`[via=webshare]` 等字样。

---

## 三、香港家用机（ASR 节点）

### 1. 克隆与配置

```bash
git clone https://github.com/solterra2888/LocalVirtualService.git /opt/local_virtual_service
cd /opt/local_virtual_service

cp .env.template.asr .env
# 或: bash setup.sh asr   # 仅 .env 不存在时

nano .env   # 填 DB / Redis / OSS / DashScope / 可选 cookies
```

### 2. 必填项检查

- `DB_HOST` / `REDIS_URL` → 指向香港主站
- `OSS_*` + `DASHSCOPE_API_KEY` → ASR 必需
- `WORKER_*_CONCURRENCY=1` → 模板已默认，保护家用带宽
- **不要** 配置 `WEBSHARE_PROXY_*`（音频下载不走代理）
- 建议配置 `YOUTUBE_COOKIES_FILE` 提升下载成功率

### 3. 一键部署

```bash
bash setup.sh
```

自动启用 `yt-worker-asr-node.target`（`asr` + `long` + `priority-asr`）。

### 4. 验证

```bash
systemctl status yt-worker-asr yt-worker-long yt-worker-priority-asr

# 确认本机不消费字幕队列（应无 fetch / priority-transcript 服务）
systemctl is-active yt-worker-fetch 2>/dev/null || echo "fetch 未运行 ✓"
```

用一条**无字幕**的测试视频触发 ASR，日志中应出现 `── Feed ASR 开始` 或 `── Upload ASR 开始` 及 `[1/5]`~`[5/5]` 步骤。

---

## 四、日常运维

### 重启

```bash
# 新加坡
sudo systemctl restart yt-worker-caption.target

# 香港
sudo systemctl restart yt-worker-asr-node.target
```

### 代码更新

```bash
cd /opt/local_virtual_service
git pull
sudo systemctl restart yt-worker-caption.target   # 或 yt-worker-asr-node.target
```

### 切换节点角色（已部署机器）

1. 修改 `.env` 中的 `WORKER_NODE_ROLE`
2. 重新运行 `bash setup.sh`（会按新角色 stop/disable 旧 unit 并启用新 target）

### 健康检查

```bash
bash /opt/local_virtual_service/scripts/health_check.sh
systemctl list-timers yt-worker-health
```

---

## 五、从单机迁移到双节点

若某台机器当前跑的是 `WORKER_NODE_ROLE=all`（`yt-worker.target` + `main`）：

| 机器 | 操作 |
|------|------|
| 新加坡 | `.env` 设 `WORKER_NODE_ROLE=caption`，`bash setup.sh` |
| 香港家用 | `.env` 设 `WORKER_NODE_ROLE=asr`，`bash setup.sh` |

迁移后 `yt-worker-main` 不再使用；由 `fetch` + `asr` 分别承担原 `main` 的两半职责。

---

## 六、故障与降级

| 场景 | 影响 | 处理 |
|------|------|------|
| 香港 ASR 节点离线 | 字幕正常，ASR 队列积压 | 恢复香港节点后自动消化积压 |
| 新加坡字幕节点离线 | Feed 抓取停止 | 恢复新加坡节点；香港 ASR 不受影响 |
| 香港带宽饱和 | ASR 变慢 | 提高新加坡字幕成功率；临时设 `CAPTION_ASR_FALLBACK_ENABLED=false` 暂停新 ASR 派发 |
| 长视频压垮带宽 | 长队列积压 | 可夜间才启 `yt-worker-long`：`systemctl stop/start yt-worker-long` |

---

## 七、systemd Target 对照

| `WORKER_NODE_ROLE` | systemd target | 包含的 worker |
|--------------------|----------------|---------------|
| `all`（默认） | `yt-worker.target` | main, long, priority-transcript, priority-asr |
| `caption` | `yt-worker-caption.target` | fetch, priority-transcript |
| `asr` | `yt-worker-asr-node.target` | asr, long, priority-asr |

---

## 八、调试模式（前台）

```bash
# 在对应机器上，按 .env 的 WORKER_NODE_ROLE 启动
bash start.sh
```

---

## 九、检查清单

- [ ] 两台机器 `REDIS_URL` / `DB_*` 指向同一主站
- [ ] 新加坡 `WORKER_NODE_ROLE=caption`，香港 `WORKER_NODE_ROLE=asr`
- [ ] 新加坡已配 Webshare，香港已配 OSS + DashScope
- [ ] 新加坡 `CAPTION_ASR_FALLBACK_ENABLED=true`
- [ ] 两台 `celery inspect active_queues` 消费的队列互不重叠
- [ ] 测试 Feed 字幕成功 → 写库 → 主站打标
- [ ] 测试无字幕视频 → 香港 ASR 完成 → 写库 → 回调主站
