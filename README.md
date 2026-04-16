# Local Virtual Service — YouTube 转录独立 Worker

将 YouTube 视频获取、字幕拉取、yt-dlp 音频下载和 Fun-ASR 语音识别打包为**独立可部署的 Celery Worker**，部署在家庭网络虚拟机上，解决远程服务器无法直接访问 YouTube 的问题。

## 架构

```
┌──────────────────────────────────────────┐
│           远程服务器 (43.99.37.76)         │
│  ┌─────────┐  ┌───────┐  ┌───────────┐  │
│  │ Web API  │  │ Redis │  │PostgreSQL │  │
│  │Scheduler │  │Broker │  │  Database │  │
│  └────┬─────┘  └───┬───┘  └─────┬─────┘  │
│       │ dispatch    │            │        │
└───────┼─────────────┼────────────┼────────┘
        │             │            │
   ─ ─ ─│─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─  公网
        │             │            │
┌───────┼─────────────┼────────────┼────────┐
│       ▼             ▼            ▼        │
│  ┌──────────────────────────────────────┐ │
│  │   YouTube Transcription Worker       │ │
│  │                                      │ │
│  │  youtube_fetching 队列:              │ │
│  │    ├ RSS/API 获取频道视频列表         │ │
│  │    ├ youtube-transcript-api 字幕     │ │
│  │    └ 字幕失败 → 自动派发 ASR ──┐     │ │
│  │                                │     │ │
│  │  youtube_transcription 队列:   │     │ │
│  │    ├ yt-dlp 下载音频 ◄─────────┘     │ │
│  │    ├ OSS 上传 → Fun-ASR 转录         │ │
│  │    └ 结果写回数据库                   │ │
│  └──────────────────────────────────────┘ │
│          家庭虚拟机 (可访问 YouTube)        │
└───────────────────────────────────────────┘
```

## 消费的队列

| 队列 | 任务 | 说明 |
|------|------|------|
| `youtube_fetching` | `fetch_all_youtube_subscriptions` | 批量获取订阅频道视频 |
| `youtube_fetching` | `fetch_youtube_subscription` | 获取单个订阅视频 |
| `youtube_transcription` | `transcribe_youtube_feed_task` | Feed 视频 Fun-ASR 转录 |
| `youtube_transcription` | `transcribe_youtube_file_asr_task` | 上传链接视频 Fun-ASR 转录 |

## 与主项目的区别

- **完全独立** — 不依赖 `backend/` 目录，可单独复制部署
- **零冗余** — 仅包含 YouTube/ASR 相关代码，不加载其他 40+ 任务
- **任务名兼容** — 任务名称与主项目一致，服务器 dispatch 后本 Worker 直接消费

## 快速部署

```bash
# 需要已安装 Miniconda/Anaconda
bash setup.sh

# 编辑配置
nano ~/local_virtual_service/.env
```

## 启动

```bash
# 前台运行（直接查看日志，适合调试）
bash ~/local_virtual_service/start.sh

# 后台运行（日志写入文件）
nohup bash ~/local_virtual_service/start.sh > ~/local_virtual_service/logs/worker.log 2>&1 &

# 查看后台日志
tail -f ~/local_virtual_service/logs/worker.log
```

## 字幕获取与 ASR 自动降级

```
新视频入库
  │
  ├─ youtube-transcript-api 拉取字幕
  │    │
  │    ├─ 成功 → transcript 写入 DB ✓
  │    │
  │    └─ 失败
  │         │
  │         ├─ IP封锁 / 无字幕 / 429 → 自动派发 ASR 转录任务
  │         │    │
  │         │    ├─ yt-dlp 下载音频
  │         │    ├─ OSS 上传
  │         │    ├─ Fun-ASR 语音识别
  │         │    └─ transcript 写入 DB ✓
  │         │
  │         └─ 直播未开始 / 首播未开始 / 私有视频 → 跳过（yt-dlp 也无法下载）
```

## 日志输出示例

前台运行时日志直接输出到终端，后台运行时写入 `logs/worker.log`。

**批量订阅获取 + 自动 ASR 降级**：
```
══════════════════════════════════════════════════
  批量获取开始: 31 个订阅, 26 个频道
══════════════════════════════════════════════════
── [1/26] 频道: Bloomberg Television (UCIALMKvObZNtJ6AmdCLP7Lg) ──
  RSS 返回 15 个视频
  [新] video=FLmZ6HnOSkE「Trump Tariff Pause Sends Markets Higher」
  ✓ 字幕获取成功: video=FLmZ6HnOSkE lang=en 字数=5230
  ✗ 字幕获取失败: video=zkHFpVXoLdA 原因=IP 被 YouTube 封锁 [→ ASR]
  ✗ 字幕获取失败: video=51WGag3jPKg 原因=直播未开始 [跳过]
  小计: 新增=3 已有=12 字幕成功=1 待ASR=1
  ...
══════════════════════════════════════════════════
  批量获取完成 (23142 ms)
  频道: 成功=26 / 总计=26
  视频: 新增=5 已有=379
  字幕: 成功=2 / 新增=5
  ASR:  派发=3
══════════════════════════════════════════════════
  📡 派发 ASR 转录: 3 个视频
    → ASR 任务已派发: video=zkHFpVXoLdA content=1234
    → ASR 任务已派发: video=RRwO9QOi8c8 content=1235
    → ASR 任务已派发: video=gNC5_2K8bn8 content=1236

── Feed ASR 开始: content=1234 video=zkHFpVXoLdA ──
  [1/5] yt-dlp 下载音频...
  [2/5] 音频下载完成: 12.3 MB (youtube_zkHFpVXoLdA.mp3)
  [3/5] 上传到 OSS...
  [4/5] 提交 Fun-ASR 转录...
  [5/5] 处理 ASR 结果...
── Feed ASR 完成: content=1234 video=zkHFpVXoLdA 字数=8520 ──
```

## 依赖的外部服务

| 服务 | 环境变量 | 必需 | 说明 |
|------|----------|------|------|
| Redis | `REDIS_URL` | 是 | Celery Broker |
| PostgreSQL | `DB_HOST` 等 | 是 | 数据存储 |
| 阿里云 OSS | `OSS_ACCESS_KEY_ID` 等 | ASR 时 | 音频中转 |
| DashScope | `DASHSCOPE_API_KEY` | ASR 时 | Fun-ASR 引擎 |
| YouTube Data API | `YOUTUBE_DATA_API_KEY` | 否 | RSS 失败时回退 |

## 服务器端配置

需要在服务器的 `backend/celery_config.py` 中将 ASR 任务路由到 `youtube_transcription` 队列（已更新）：

```python
'transcribe_youtube_feed_task': {'queue': 'youtube_transcription'},
'transcribe_youtube_file_asr_task': {'queue': 'youtube_transcription'},
```
