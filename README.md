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
│  │    └ youtube-transcript-api 字幕     │ │
│  │                                      │ │
│  │  youtube_transcription 队列:         │ │
│  │    ├ yt-dlp 下载音频                 │ │
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
# 在虚拟机上执行
bash deployment/local_virtual_service/setup.sh

# 编辑配置
nano ~/local_virtual_service/.env

# 启动
systemctl --user start youtube-worker.service
```

## 手动调试

```bash
cd ~/local_virtual_service
source venv/bin/activate
celery -A worker.celery_app worker \
  --queues=youtube_fetching,youtube_transcription \
  --concurrency=1 \
  --loglevel=info
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
