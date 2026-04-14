# -*- coding: utf-8 -*-
"""
独立 Worker 的 Celery 任务定义。
任务名称与主项目完全一致，以便服务器端 dispatch 后由本 Worker 消费执行。
"""

import json
import logging
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime

from celery import current_task

from .celery_app import app
from .services import (
    ASRService,
    ContentStore,
    OSSService,
    YouTubeCaptionService,
    YouTubeFeedService,
    build_ydl_opts,
)

log = logging.getLogger("worker")


# =====================================================================
# YouTube Feed 订阅获取 (youtube_fetching 队列)
# =====================================================================

@app.task(bind=True, name="fetch_youtube_subscription", queue="youtube_fetching")
def fetch_youtube_subscription(self, subscription_id=None, user_id=None, force_fetch=False):
    """获取单个 YouTube 订阅的最新视频"""
    store = ContentStore()

    if not subscription_id:
        return {"success": False, "message": "subscription_id is required"}

    from .db import DB
    db = DB()
    with db.connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, user_id, channel_identifier, channel_name, max_items_per_fetch
            FROM subscription_list WHERE id=%s
        """, (subscription_id,))
        row = cur.fetchone()
    if not row:
        return {"success": False, "message": "Subscription not found"}

    sub_id, uid, ch_ident, ch_name, max_items = row
    user_id = uid
    max_items = max_items or 15

    log.info("fetch_youtube_subscription: sub=%s channel=%s", sub_id, ch_ident)

    raw_videos = YouTubeFeedService.fetch_channel_videos(ch_ident, max_items)
    if not raw_videos:
        return {"success": True, "videos_new": 0, "videos_existing": 0}

    videos_new = videos_existing = 0
    for rv in raw_videos:
        try:
            cid, is_new = store.save_youtube_content(rv, [user_id])
            if is_new:
                videos_new += 1
                _fetch_caption(cid, rv, store)
            else:
                videos_existing += 1
        except Exception as e:
            log.warning("保存视频失败: %s", e)

    store.update_subscription_stats(sub_id, videos_new)
    return {"success": True, "videos_new": videos_new, "videos_existing": videos_existing}


@app.task(bind=True, name="fetch_all_youtube_subscriptions", queue="youtube_fetching")
def fetch_all_youtube_subscriptions(self):
    """批量获取所有 YouTube 订阅的最新视频"""
    start = time.time()
    store = ContentStore()
    subs = store.get_youtube_subscriptions(enabled_only=True)
    if not subs:
        return {"success": True, "channels_count": 0, "videos_new": 0}

    channels_map: dict = {}
    for s in subs:
        cid = s["channel_identifier"]
        if cid not in channels_map:
            channels_map[cid] = {
                "channel_name": s.get("channel_name", ""),
                "max_items": s.get("max_items_per_fetch", 15),
                "sub_ids": [],
                "user_ids": [],
            }
        channels_map[cid]["sub_ids"].append(s["id"])
        channels_map[cid]["user_ids"].append(s["user_id"])

    log.info("找到 %d 个订阅, %d 个频道", len(subs), len(channels_map))

    total_new = total_existing = ch_ok = 0
    items = list(channels_map.items())
    for idx, (cid, info) in enumerate(items):
        if idx > 0:
            time.sleep(0.5)
        try:
            log.info("获取频道: %s (%s)", info["channel_name"], cid)
            raw_videos = YouTubeFeedService.fetch_channel_videos(cid, info["max_items"])
            if not raw_videos:
                log.warning("频道 %s 未获取到视频", cid)
                continue

            vnew = vexist = 0
            for rv in raw_videos:
                try:
                    dbid, is_new = store.save_youtube_content(rv, info["user_ids"])
                    if is_new:
                        vnew += 1
                        _fetch_caption(dbid, rv, store)
                    else:
                        vexist += 1
                except Exception as e:
                    log.warning("保存视频失败: %s", e)

            total_new += vnew
            total_existing += vexist
            ch_ok += 1

            for sid in info["sub_ids"]:
                store.update_subscription_stats(sid, vnew)
        except Exception as e:
            log.error("频道 %s 失败: %s", cid, e)

    dur = int((time.time() - start) * 1000)
    log.info("YouTube 批量获取完成: channels=%d, new=%d, existing=%d, %dms",
             ch_ok, total_new, total_existing, dur)
    return {
        "success": True,
        "subscriptions_count": len(subs),
        "channels_count": len(channels_map),
        "channels_success": ch_ok,
        "videos_new": total_new,
        "videos_existing": total_existing,
        "execution_duration_ms": dur,
    }


def _fetch_caption(content_db_id: int, raw_video: dict, store: ContentStore):
    """为新视频尝试拉取字幕 (不抛异常)"""
    vid = raw_video.get("id", "")
    url = f"https://www.youtube.com/watch?v={vid}"
    try:
        result = YouTubeCaptionService.fetch_transcript(url)
        full_text = YouTubeCaptionService.get_full_text(result)
        store.update_transcript(content_db_id, full_text, result)
        log.info("Transcript OK: content=%d video=%s lang=%s", content_db_id, vid, result.get("language"))
    except ImportError as e:
        log.warning(
            "youtube-transcript-api 不可用 (运行本进程的解释器: %s), 跳过 caption: %s",
            sys.executable,
            e,
        )
    except Exception as e:
        log.warning("Caption 获取失败 (video=%s): %s", vid, e)


# =====================================================================
# YouTube ASR 转录 (youtube_transcription 队列)
# =====================================================================

def _progress(status="processing", progress=0, msg=None, url="", total=1, failed=0):
    try:
        meta = {
            "status": status, "progress": progress,
            "current_file": url, "files_processed": 0,
            "files_failed": failed, "total_files": total, "logs": [],
        }
        if msg:
            meta["logs"].append({"timestamp": datetime.now().isoformat(), "level": "info", "message": msg})
        current_task.update_state(state="PROGRESS", meta=meta)
    except Exception:
        pass


@app.task(bind=True, name="transcribe_youtube_feed_task", queue="youtube_transcription")
def transcribe_youtube_feed_task(self, content_db_id: int, video_url: str, video_id: str):
    """对 Feed 中无字幕的 YouTube 视频, 下载音频 → Fun-ASR 转录 → 写回 DB"""
    task_id = self.request.id
    temp_dir = None
    try:
        log.info("Feed ASR 开始: content=%d video=%s", content_db_id, video_id)
        import yt_dlp

        _progress(progress=10, msg="Downloading audio via yt-dlp...", url=video_url)
        temp_dir = tempfile.mkdtemp(prefix="yt_feed_asr_")
        out_tmpl = os.path.join(temp_dir, f"youtube_{video_id}.%(ext)s")
        with yt_dlp.YoutubeDL(build_ydl_opts(out_tmpl)) as ydl:
            ydl.download([video_url])

        files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.startswith(f"youtube_{video_id}")]
        if not files:
            raise FileNotFoundError("yt-dlp 未生成音频文件")
        audio_path = files[0]
        _progress(progress=30, msg=f"Audio downloaded ({os.path.getsize(audio_path)/1024/1024:.1f} MB)", url=video_url)

        _progress(progress=40, msg="Uploading to OSS...", url=video_url)
        oss = OSSService()
        if not oss.is_configured():
            raise ValueError("OSS 未配置")
        up = oss.upload_audio(audio_path, os.path.basename(audio_path))
        if not up["success"]:
            raise Exception(f"OSS 上传失败: {up.get('error')}")

        _progress(progress=55, msg="Submitting to Fun-ASR...", url=video_url)
        asr = ASRService()
        result = asr.transcribe(up["audio_url"])
        if not result["success"]:
            raise ValueError(f"ASR 失败: {result['error']}")
        if not result.get("transcriptions"):
            raise ValueError("ASR 未返回结果")

        _progress(progress=80, msg="Processing ASR results...", url=video_url)
        text_parts = [ASRService.format_text(t) for t in result["transcriptions"]]
        full_text = "\n".join(p for p in text_parts if p)
        asr_structured = ASRService.build_structured_data(result["transcriptions"])
        transcript_data = {"source": "asr", "language": "zh", "is_generated": True, "asr_data": asr_structured}

        _progress(progress=90, msg="Saving transcript...", url=video_url)
        ContentStore().update_transcript(content_db_id, full_text, transcript_data)
        _progress(status="completed", progress=100, msg="Done!", url=video_url)

        return {"task_id": task_id, "status": "completed", "content_db_id": content_db_id}

    except Exception as e:
        err = f"Feed ASR 失败: {e}"
        log.error("%s content=%d", err, content_db_id)
        _progress(status="failed", msg=err, url=video_url, failed=1)
        return {"task_id": task_id, "status": "failed", "error": err}
    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)


@app.task(bind=True, name="transcribe_youtube_file_asr_task", queue="youtube_transcription")
def transcribe_youtube_file_asr_task(self, file_id: int, youtube_url: str, video_id: str,
                                     max_duration_seconds: int = 0):
    """对 Upload YouTube Link 中无字幕的视频, 下载 → Fun-ASR → 写回 file_processing_details"""
    task_id = self.request.id
    temp_dir = None
    store = ContentStore()
    try:
        log.info("File ASR 开始: file_id=%d video=%s", file_id, video_id)
        store.update_file_status(file_id, "processing")
        import yt_dlp

        _progress(progress=10, msg="Downloading audio...", url=youtube_url)
        temp_dir = tempfile.mkdtemp(prefix="yt_file_asr_")
        out_tmpl = os.path.join(temp_dir, f"youtube_{video_id}.%(ext)s")
        with yt_dlp.YoutubeDL(build_ydl_opts(out_tmpl)) as ydl:
            ydl.download([youtube_url])

        files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.startswith(f"youtube_{video_id}")]
        if not files:
            raise FileNotFoundError("yt-dlp 未生成音频文件")
        audio_path = files[0]
        _progress(progress=40, msg=f"Audio: {os.path.getsize(audio_path)/1024/1024:.1f} MB", url=youtube_url)

        _progress(progress=50, msg="Uploading to OSS...", url=youtube_url)
        oss = OSSService()
        if not oss.is_configured():
            raise ValueError("OSS 未配置")
        up = oss.upload_audio(audio_path, os.path.basename(audio_path))
        if not up["success"]:
            raise Exception(f"OSS: {up.get('error')}")

        _progress(progress=60, msg="Submitting Fun-ASR...", url=youtube_url)
        asr = ASRService()
        result = asr.transcribe(up["audio_url"])
        if not result["success"]:
            raise ValueError(f"ASR: {result['error']}")
        if not result.get("transcriptions"):
            raise ValueError("ASR 无结果")

        _progress(progress=85, msg="Processing results...", url=youtube_url)
        text_parts = [ASRService.format_text(t) for t in result["transcriptions"]]
        full_text = "\n".join(p for p in text_parts if p)
        asr_struct = ASRService.build_structured_data(result["transcriptions"])

        store.update_file_asr(file_id, full_text, asr_struct)
        _progress(status="completed", progress=100, msg="Done!", url=youtube_url)

        return {"task_id": task_id, "status": "completed", "file_id": file_id, "content_length": len(full_text)}

    except Exception as e:
        err = f"File ASR 失败: {e}"
        log.error("%s file_id=%d", err, file_id)
        store.update_file_status(file_id, "failed")
        _progress(status="failed", msg=err, url=youtube_url, failed=1)
        return {"task_id": task_id, "status": "failed", "error": err}
    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
