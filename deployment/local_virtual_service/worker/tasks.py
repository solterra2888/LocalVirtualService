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

    log.info("── 获取单个订阅: sub=%s channel=%s (%s) ──", sub_id, ch_name, ch_ident)

    raw_videos = YouTubeFeedService.fetch_channel_videos(ch_ident, max_items)
    if not raw_videos:
        log.info("  频道无新视频")
        return {"success": True, "videos_new": 0, "videos_existing": 0}

    log.info("  RSS 返回 %d 个视频", len(raw_videos))
    videos_new = videos_existing = caption_ok = 0
    asr_pending = []
    for rv in raw_videos:
        title = rv.get("snippet", {}).get("title", "")
        vid = rv.get("id", "")
        try:
            cid, is_new = store.save_youtube_content(rv, [user_id])
            if is_new:
                videos_new += 1
                log.info("  [新] video=%s「%s」", vid, title[:50])
                success, should_asr = _fetch_caption(cid, rv, store)
                if success:
                    caption_ok += 1
                elif should_asr:
                    asr_pending.append({
                        "content_db_id": cid,
                        "video_id": vid,
                        "video_url": f"https://www.youtube.com/watch?v={vid}",
                    })
            else:
                videos_existing += 1
        except Exception as e:
            log.warning("  保存失败: video=%s error=%s", vid, e)

    _dispatch_asr_fallback(asr_pending)
    store.update_subscription_stats(sub_id, videos_new)
    log.info("── 订阅 %s 完成: 新增=%d 已有=%d 字幕成功=%d ASR派发=%d ──",
             ch_name, videos_new, videos_existing, caption_ok, len(asr_pending))
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

    log.info("══════════════════════════════════════════════════")
    log.info("  批量获取开始: %d 个订阅, %d 个频道", len(subs), len(channels_map))
    log.info("══════════════════════════════════════════════════")

    total_new = total_existing = total_caption_ok = total_asr = ch_ok = 0
    all_asr_pending = []
    items = list(channels_map.items())
    for idx, (cid, info) in enumerate(items):
        if idx > 0:
            time.sleep(0.5)
        ch_name = info["channel_name"] or cid
        try:
            log.info("── [%d/%d] 频道: %s (%s) ──", idx + 1, len(items), ch_name, cid)
            raw_videos = YouTubeFeedService.fetch_channel_videos(cid, info["max_items"])
            if not raw_videos:
                log.warning("  频道 %s 未获取到视频", ch_name)
                continue

            log.info("  RSS 返回 %d 个视频", len(raw_videos))
            vnew = vexist = vcaption = 0
            ch_asr_pending = []
            for rv in raw_videos:
                title = rv.get("snippet", {}).get("title", "")
                vid = rv.get("id", "")
                try:
                    dbid, is_new = store.save_youtube_content(rv, info["user_ids"])
                    if is_new:
                        vnew += 1
                        log.info("  [新] video=%s「%s」", vid, title[:50])
                        success, should_asr = _fetch_caption(dbid, rv, store)
                        if success:
                            vcaption += 1
                        elif should_asr:
                            ch_asr_pending.append({
                                "content_db_id": dbid,
                                "video_id": vid,
                                "video_url": f"https://www.youtube.com/watch?v={vid}",
                            })
                    else:
                        vexist += 1
                except Exception as e:
                    log.warning("  保存失败: video=%s error=%s", vid, e)

            total_new += vnew
            total_existing += vexist
            total_caption_ok += vcaption
            total_asr += len(ch_asr_pending)
            all_asr_pending.extend(ch_asr_pending)
            ch_ok += 1

            for sid in info["sub_ids"]:
                store.update_subscription_stats(sid, vnew)

            log.info("  小计: 新增=%d 已有=%d 字幕成功=%d 待ASR=%d",
                     vnew, vexist, vcaption, len(ch_asr_pending))
        except Exception as e:
            log.error("  频道 %s 异常: %s", ch_name, e)

    _dispatch_asr_fallback(all_asr_pending)

    dur = int((time.time() - start) * 1000)
    log.info("══════════════════════════════════════════════════")
    log.info("  批量获取完成 (%d ms)", dur)
    log.info("  频道: 成功=%d / 总计=%d", ch_ok, len(items))
    log.info("  视频: 新增=%d 已有=%d", total_new, total_existing)
    log.info("  字幕: 成功=%d / 新增=%d", total_caption_ok, total_new)
    log.info("  ASR:  派发=%d", total_asr)
    log.info("══════════════════════════════════════════════════")
    return {
        "success": True,
        "subscriptions_count": len(subs),
        "channels_count": len(channels_map),
        "channels_success": ch_ok,
        "videos_new": total_new,
        "videos_existing": total_existing,
        "execution_duration_ms": dur,
    }


def _classify_caption_error(err: Exception) -> tuple:
    """将 youtube-transcript-api 的冗长异常归类为 (简短描述, 是否应尝试ASR)"""
    msg = str(err)
    if "blocking requests from your IP" in msg:
        return "IP 被 YouTube 封锁", True
    if "unplayable" in msg.lower():
        if "live event will begin" in msg:
            return "直播未开始", False
        if "Premieres in" in msg:
            return "首播未开始", False
        if "private video" in msg.lower():
            return "私有视频", False
        return "视频不可播放", False
    if "No transcript" in msg or "disabled" in msg.lower():
        return "该视频无字幕", True
    if "Too Many Requests" in msg or "429" in msg:
        return "请求过于频繁 (429)", True
    return msg.split("\n")[0][:120], True


def _fetch_caption(content_db_id: int, raw_video: dict, store: ContentStore) -> tuple:
    """
    为新视频尝试拉取字幕 (不抛异常)。
    返回 (success: bool, should_try_asr: bool)
    """
    vid = raw_video.get("id", "")
    title = raw_video.get("snippet", {}).get("title", "")
    url = f"https://www.youtube.com/watch?v={vid}"
    try:
        result = YouTubeCaptionService.fetch_transcript(url)
        full_text = YouTubeCaptionService.get_full_text(result)
        store.update_transcript(content_db_id, full_text, result)
        char_count = len(full_text)
        log.info("  ✓ 字幕获取成功: video=%s lang=%s 字数=%d「%s」",
                 vid, result.get("language"), char_count, title[:40])
        return True, False
    except ImportError as e:
        log.warning("youtube-transcript-api 不可用 (解释器: %s): %s", sys.executable, e)
        return False, True
    except Exception as e:
        reason, should_asr = _classify_caption_error(e)
        tag = "→ ASR" if should_asr else "跳过"
        log.warning("  ✗ 字幕获取失败: video=%s 原因=%s [%s]「%s」", vid, reason, tag, title[:40])
        return False, should_asr


def _dispatch_asr_fallback(asr_pending: list):
    """将字幕失败的视频派发到 ASR 转录队列"""
    if not asr_pending:
        return
    log.info("  📡 派发 ASR 转录: %d 个视频", len(asr_pending))
    for item in asr_pending:
        try:
            transcribe_youtube_feed_task.apply_async(
                args=[item["content_db_id"], item["video_url"], item["video_id"]],
                queue="youtube_transcription",
            )
            log.info("    → ASR 任务已派发: video=%s content=%d", item["video_id"], item["content_db_id"])
        except Exception as e:
            log.error("    ASR 派发失败: video=%s error=%s", item["video_id"], e)


# =====================================================================
# YouTube transcript 字幕拉取 (youtube_fetching 队列)
# 由云端服务器在保存视频后派发，使用住宅 IP 拉取字幕
# =====================================================================

@app.task(bind=True, name="fetch_youtube_transcripts_batch", queue="youtube_fetching")
def fetch_youtube_transcripts_batch(self, video_items: list):
    """批量为 YouTube 视频拉取 transcript，失败且可重试的自动派发 ASR"""
    store = ContentStore()
    log.info("── 批量字幕拉取开始: %d 个视频 ──", len(video_items))
    ok = fail = 0
    asr_pending = []
    for i, item in enumerate(video_items):
        vid = item["video_id"]
        url = item["video_url"]
        cid = item["content_db_id"]
        try:
            result = YouTubeCaptionService.fetch_transcript(url)
            full_text = YouTubeCaptionService.get_full_text(result)
            store.update_transcript(cid, full_text, result)
            ok += 1
            log.info("  [%d/%d] ✓ video=%s lang=%s 字数=%d",
                     i + 1, len(video_items), vid, result.get("language"), len(full_text))
        except Exception as e:
            fail += 1
            reason, should_asr = _classify_caption_error(e)
            tag = "→ ASR" if should_asr else "跳过"
            log.warning("  [%d/%d] ✗ video=%s 原因=%s [%s]",
                        i + 1, len(video_items), vid, reason, tag)
            if should_asr:
                asr_pending.append(item)

    _dispatch_asr_fallback(asr_pending)
    log.info("── 批量字幕完成: 成功=%d 失败=%d ASR派发=%d / 总计=%d ──",
             ok, fail, len(asr_pending), len(video_items))
    return {"success": True, "total": len(video_items), "transcripts_ok": ok,
            "transcripts_failed": fail, "asr_dispatched": len(asr_pending)}


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
        log.info("── Feed ASR 开始: content=%d video=%s url=%s ──", content_db_id, video_id, video_url)
        import yt_dlp

        log.info("  [1/5] yt-dlp 下载音频...")
        _progress(progress=10, msg="Downloading audio via yt-dlp...", url=video_url)
        temp_dir = tempfile.mkdtemp(prefix="yt_feed_asr_")
        out_tmpl = os.path.join(temp_dir, f"youtube_{video_id}.%(ext)s")
        with yt_dlp.YoutubeDL(build_ydl_opts(out_tmpl)) as ydl:
            ydl.download([video_url])

        files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.startswith(f"youtube_{video_id}")]
        if not files:
            raise FileNotFoundError("yt-dlp 未生成音频文件")
        audio_path = files[0]
        size_mb = os.path.getsize(audio_path) / 1024 / 1024
        log.info("  [2/5] 音频下载完成: %.1f MB (%s)", size_mb, os.path.basename(audio_path))
        _progress(progress=30, msg=f"Audio downloaded ({size_mb:.1f} MB)", url=video_url)

        log.info("  [3/5] 上传到 OSS...")
        _progress(progress=40, msg="Uploading to OSS...", url=video_url)
        oss = OSSService()
        if not oss.is_configured():
            raise ValueError("OSS 未配置")
        up = oss.upload_audio(audio_path, os.path.basename(audio_path))
        if not up["success"]:
            raise Exception(f"OSS 上传失败: {up.get('error')}")
        log.info("  OSS 上传完成: %s", up.get("object_name", ""))

        log.info("  [4/5] 提交 Fun-ASR 转录...")
        _progress(progress=55, msg="Submitting to Fun-ASR...", url=video_url)
        asr = ASRService()
        result = asr.transcribe(up["audio_url"])
        if not result["success"]:
            raise ValueError(f"ASR 失败: {result['error']}")
        if not result.get("transcriptions"):
            raise ValueError("ASR 未返回结果")

        log.info("  [5/5] 处理 ASR 结果...")
        _progress(progress=80, msg="Processing ASR results...", url=video_url)
        text_parts = [ASRService.format_text(t) for t in result["transcriptions"]]
        full_text = "\n".join(p for p in text_parts if p)
        asr_structured = ASRService.build_structured_data(result["transcriptions"])
        transcript_data = {"source": "asr", "language": "zh", "is_generated": True, "asr_data": asr_structured}

        _progress(progress=90, msg="Saving transcript...", url=video_url)
        ContentStore().update_transcript(content_db_id, full_text, transcript_data)
        _progress(status="completed", progress=100, msg="Done!", url=video_url)

        log.info("── Feed ASR 完成: content=%d video=%s 字数=%d ──",
                 content_db_id, video_id, len(full_text))
        return {"task_id": task_id, "status": "completed", "content_db_id": content_db_id}

    except Exception as e:
        err = f"Feed ASR 失败: {e}"
        log.error("── %s content=%d video=%s ──", err, content_db_id, video_id)
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
        log.info("── File ASR 开始: file_id=%d video=%s url=%s ──", file_id, video_id, youtube_url)
        store.update_file_status(file_id, "processing")
        import yt_dlp

        log.info("  [1/5] yt-dlp 下载音频...")
        _progress(progress=10, msg="Downloading audio...", url=youtube_url)
        temp_dir = tempfile.mkdtemp(prefix="yt_file_asr_")
        out_tmpl = os.path.join(temp_dir, f"youtube_{video_id}.%(ext)s")
        with yt_dlp.YoutubeDL(build_ydl_opts(out_tmpl)) as ydl:
            ydl.download([youtube_url])

        files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.startswith(f"youtube_{video_id}")]
        if not files:
            raise FileNotFoundError("yt-dlp 未生成音频文件")
        audio_path = files[0]
        size_mb = os.path.getsize(audio_path) / 1024 / 1024
        log.info("  [2/5] 音频下载完成: %.1f MB (%s)", size_mb, os.path.basename(audio_path))
        _progress(progress=40, msg=f"Audio: {size_mb:.1f} MB", url=youtube_url)

        log.info("  [3/5] 上传到 OSS...")
        _progress(progress=50, msg="Uploading to OSS...", url=youtube_url)
        oss = OSSService()
        if not oss.is_configured():
            raise ValueError("OSS 未配置")
        up = oss.upload_audio(audio_path, os.path.basename(audio_path))
        if not up["success"]:
            raise Exception(f"OSS: {up.get('error')}")
        log.info("  OSS 上传完成: %s", up.get("object_name", ""))

        log.info("  [4/5] 提交 Fun-ASR 转录...")
        _progress(progress=60, msg="Submitting Fun-ASR...", url=youtube_url)
        asr = ASRService()
        result = asr.transcribe(up["audio_url"])
        if not result["success"]:
            raise ValueError(f"ASR: {result['error']}")
        if not result.get("transcriptions"):
            raise ValueError("ASR 无结果")

        log.info("  [5/5] 处理 ASR 结果...")
        _progress(progress=85, msg="Processing results...", url=youtube_url)
        text_parts = [ASRService.format_text(t) for t in result["transcriptions"]]
        full_text = "\n".join(p for p in text_parts if p)
        asr_struct = ASRService.build_structured_data(result["transcriptions"])

        store.update_file_asr(file_id, full_text, asr_struct)
        _progress(status="completed", progress=100, msg="Done!", url=youtube_url)

        log.info("── File ASR 完成: file_id=%d video=%s 字数=%d ──",
                 file_id, video_id, len(full_text))
        return {"task_id": task_id, "status": "completed", "file_id": file_id, "content_length": len(full_text)}

    except Exception as e:
        err = f"File ASR 失败: {e}"
        log.error("── %s file_id=%d video=%s ──", err, file_id, video_id)
        store.update_file_status(file_id, "failed")
        _progress(status="failed", msg=err, url=youtube_url, failed=1)
        return {"task_id": task_id, "status": "failed", "error": err}
    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
