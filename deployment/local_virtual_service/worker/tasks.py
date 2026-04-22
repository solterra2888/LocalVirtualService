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
from typing import Optional

from celery import current_task
from celery.exceptions import SoftTimeLimitExceeded

from .celery_app import app
from .services import (
    ASRService,
    ContentStore,
    OSSService,
    YouTubeCaptionService,
    YouTubeFeedService,
    build_ydl_opts,
    probe_youtube_metadata,
)

log = logging.getLogger("worker")


# 长视频阈值（秒）：超过此值的 ASR 任务会被转发到 youtube_transcription_long 队列，
# 由独立 worker 处理，避免阻塞短视频。默认 30 分钟。
_LONG_VIDEO_THRESHOLD = int(os.getenv("ASR_LONG_VIDEO_THRESHOLD_SECONDS", "1800"))
_SHORT_QUEUE = "youtube_transcription"
_LONG_QUEUE = "youtube_transcription_long"

# 长视频任务专属的 Celery 超时（覆盖 celery_app.py 里 2h 的全局默认值）。
# Fun-ASR 允许 ≤12h 音频，默认给 6h 硬上限、5.5h 软上限，足以覆盖 10h+ 的直播回放。
_LONG_TASK_HARD_LIMIT = int(os.getenv("ASR_LONG_TASK_TIME_LIMIT_SECONDS", "21600"))
_LONG_TASK_SOFT_LIMIT = int(os.getenv("ASR_LONG_TASK_SOFT_TIME_LIMIT_SECONDS", "19800"))

# Fun-ASR 异步轮询最长等待时间（分钟），长/短队列分别配置。
# 短队列复用 ASRService 默认值（读 ASR_POLL_TIMEOUT_MINUTES，5 min）；
# 长队列在此处显式传入，避免大文件只跑 5 min 就被本地判定超时、
# 触发"重下载 500MB+ 音频 → 重新上传 OSS → 重新提交 ASR"的浪费。
_ASR_POLL_TIMEOUT_LONG_MIN = float(os.getenv("ASR_POLL_TIMEOUT_MINUTES_LONG", "60"))

# 重试策略：瞬时错误（超时/429/网络/OSS 5xx）最多重试此次数，并走指数退避。
# 永久错误（视频已删除/私有/年龄限制/无人声）不重试。
_MAX_RETRIES = int(os.getenv("ASR_MAX_RETRIES", "3"))
_RETRY_BACKOFF_BASE = int(os.getenv("ASR_RETRY_BACKOFF_BASE_SECONDS", "60"))
_RETRY_BACKOFF_MAX = int(os.getenv("ASR_RETRY_BACKOFF_MAX_SECONDS", "600"))


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

    try:
        raw_videos, video_source = YouTubeFeedService.fetch_channel_videos(ch_ident, max_items)
    except Exception as e:
        log.error("  视频列表抓取失败: sub=%s channel=%s error=%s", sub_id, ch_name, e)
        store.increment_subscription_failures(sub_id)
        return {"success": False, "message": f"RSS fetch failed: {e}",
                "videos_new": 0, "videos_existing": 0}

    if not raw_videos:
        log.info("  频道无新视频")
        return {"success": True, "videos_new": 0, "videos_existing": 0}

    source_label = "RSS" if video_source == "rss" else "Data API v3"
    log.info("  %s 返回 %d 个视频", source_label, len(raw_videos))
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
            raw_videos, video_source = YouTubeFeedService.fetch_channel_videos(cid, info["max_items"])
            if not raw_videos:
                log.warning("  频道 %s 未获取到视频", ch_name)
                continue

            source_label = "RSS" if video_source == "rss" else "Data API v3"
            log.info("  %s 返回 %d 个视频", source_label, len(raw_videos))
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


def _ytdlp_caption_fallback_enabled() -> bool:
    """是否启用 yt-dlp 作为 youtube-transcript-api 的字幕 fallback。"""
    return os.getenv("CAPTION_YTDLP_FALLBACK_ENABLED", "true").strip().lower() in \
        ("1", "true", "yes", "on")


def _fetch_transcript_with_fallback(video_url: str) -> dict:
    """
    统一的字幕拉取入口：transcript-api → yt-dlp 双路径。

    两条路径走的是**不同的 YouTube endpoint**（/api/timedtext vs player endpoint），
    被独立限流。当 IP 在 transcript-api 上被封禁时 yt-dlp 路径通常仍可用。

    返回字典:
      success:     最终是否拿到字幕
      result:      成功时的字幕数据 (与 fetch_transcript 同结构)
      source:      'API' 或 'yt-dlp'，表示最终由哪条路径拿到（失败时为 None）
      should_asr:  失败时是否应降级到 ASR（永久失败如私有/直播未开始则 False）
      api_reason:  transcript-api 的失败原因（未调用时为 None）
      yt_reason:   yt-dlp 的失败原因（未调用时为 None）
    """
    api_reason: Optional[str] = None
    api_should_asr: bool = True

    # ── Stage 1: transcript-api（快） ──────────────────────────
    try:
        result = YouTubeCaptionService.fetch_transcript(video_url)
        return {"success": True, "result": result, "source": "API",
                "should_asr": False, "api_reason": None, "yt_reason": None}
    except ImportError:
        api_reason = "transcript-api 库未安装"
    except Exception as e:
        api_reason, api_should_asr = _classify_caption_error(e)

    # 永久失败（直播未开始/私有/首播等）→ yt-dlp 也无能为力，直接返回
    if not api_should_asr:
        return {"success": False, "result": None, "source": None,
                "should_asr": False, "api_reason": api_reason, "yt_reason": None}

    # ── Stage 2: yt-dlp fallback（慢 2-5s，但走不同 endpoint） ──
    if not _ytdlp_caption_fallback_enabled():
        return {"success": False, "result": None, "source": None,
                "should_asr": True, "api_reason": api_reason,
                "yt_reason": "fallback 未启用"}

    try:
        result = YouTubeCaptionService.fetch_transcript_via_ytdlp(video_url)
        return {"success": True, "result": result, "source": "yt-dlp",
                "should_asr": False, "api_reason": api_reason, "yt_reason": None}
    except Exception as e:
        yt_reason = str(e).split("\n")[0][:120]
        return {"success": False, "result": None, "source": None,
                "should_asr": True, "api_reason": api_reason, "yt_reason": yt_reason}


def _format_caption_failure_reason(outcome: dict) -> str:
    """把 outcome 里的 api_reason / yt_reason 格式化成一行日志文字。"""
    api_r, yt_r = outcome.get("api_reason"), outcome.get("yt_reason")
    if api_r and yt_r:
        return f"API={api_r} | yt-dlp={yt_r}"
    return api_r or yt_r or "unknown"


def _fetch_caption(content_db_id: int, raw_video: dict, store: ContentStore) -> tuple:
    """
    为新视频尝试拉取字幕 (不抛异常)。
    返回 (success: bool, should_try_asr: bool)
    """
    vid = raw_video.get("id", "")
    title = raw_video.get("snippet", {}).get("title", "")
    url = f"https://www.youtube.com/watch?v={vid}"

    outcome = _fetch_transcript_with_fallback(url)

    if outcome["success"]:
        result = outcome["result"]
        full_text = YouTubeCaptionService.get_full_text(result)
        store.update_transcript(content_db_id, full_text, result)
        source_type = "自动生成" if result.get("is_generated") else "人工字幕"
        via = outcome["source"]
        extra = f" (API→{outcome['api_reason']})" if via == "yt-dlp" and outcome["api_reason"] else ""
        log.info("  ✓ 字幕获取成功[%s]: video=%s lang=%s(%s) 字数=%d「%s」%s",
                 via, vid, result.get("language"), source_type,
                 len(full_text), title[:40], extra)
        return True, False

    reason = _format_caption_failure_reason(outcome)
    tag = "→ ASR" if outcome["should_asr"] else "跳过"
    log.warning("  ✗ 字幕获取失败: video=%s 原因=%s [%s]「%s」", vid, reason, tag, title[:40])
    return False, outcome["should_asr"]


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
    ok_via_api = ok_via_ytdlp = 0
    asr_pending = []
    for i, item in enumerate(video_items):
        vid = item["video_id"]
        url = item["video_url"]
        cid = item["content_db_id"]

        outcome = _fetch_transcript_with_fallback(url)

        if outcome["success"]:
            result = outcome["result"]
            full_text = YouTubeCaptionService.get_full_text(result)
            store.update_transcript(cid, full_text, result)
            ok += 1
            source = "自动生成" if result.get("is_generated") else "人工字幕"
            via = outcome["source"]
            if via == "yt-dlp":
                ok_via_ytdlp += 1
            else:
                ok_via_api += 1
            extra = f" (API→{outcome['api_reason']})" if via == "yt-dlp" and outcome["api_reason"] else ""
            log.info("  [%d/%d] ✓[%s] video=%s lang=%s(%s) 字数=%d%s",
                     i + 1, len(video_items), via, vid,
                     result.get("language"), source, len(full_text), extra)
        else:
            fail += 1
            reason = _format_caption_failure_reason(outcome)
            tag = "→ ASR" if outcome["should_asr"] else "跳过"
            log.warning("  [%d/%d] ✗ video=%s 原因=%s [%s]",
                        i + 1, len(video_items), vid, reason, tag)
            if outcome["should_asr"]:
                asr_pending.append(item)

    _dispatch_asr_fallback(asr_pending)
    log.info("── 批量字幕完成: 成功=%d(API=%d,yt-dlp=%d) 失败=%d ASR派发=%d / 总计=%d ──",
             ok, ok_via_api, ok_via_ytdlp, fail, len(asr_pending), len(video_items))
    return {"success": True, "total": len(video_items),
            "transcripts_ok": ok, "transcripts_ok_via_api": ok_via_api,
            "transcripts_ok_via_ytdlp": ok_via_ytdlp,
            "transcripts_failed": fail, "asr_dispatched": len(asr_pending)}


# =====================================================================
# Upload YouTube Link 字幕拉取 (youtube_fetching 队列)
#
# 由远程 `/api/files/upload-youtube` 在同步建完 file_processing_details 行后
# 派发, 与 Feed 字幕拉取完全复用 `_fetch_transcript_with_fallback` 逻辑
# (transcript-api → yt-dlp 两路径), 但落库目标不同:
#   Feed   → subscription_content.transcript
#   Upload → file_processing_details.content + asr_with_diarization
#
# 失败且可降级时, 会自动派发 `transcribe_youtube_file_asr_task` 到
# youtube_transcription 队列 (由同一家庭 Worker 消费).
# 终态 (completed / 永久 failed) 会通过 signal_upload_file_ready 回调远程
# `finalize_youtube_file_task`, 触发 auto-tag / auto-name / auto-classify.
# =====================================================================

@app.task(bind=True, name="fetch_youtube_file_transcript_task",
          queue="youtube_fetching", max_retries=_MAX_RETRIES)
def fetch_youtube_file_transcript_task(self, file_id: int, youtube_url: str,
                                        video_id: str, username: str = "guest"):
    """
    对 Upload YouTube Link 中的视频拉字幕, 失败则派发 file ASR.

    Args:
        file_id:     file_processing_details 主键 (远程 API 已预先 INSERT)
        youtube_url: 原始 YouTube URL (存在 file_processing_details.original_file_path)
        video_id:    YouTube video_id (用于日志、临时文件命名)
        username:    文件所属用户, 透传给后续 ASR 任务与 finalize 回调
    """
    task_id = self.request.id
    store = ContentStore()
    task_start = time.time()

    # ── 幂等保护: 已完成的 file_id 直接返回 ──────────────────────────
    try:
        current_status = store.get_file_status(file_id)
    except Exception:
        current_status = None
    if current_status == "completed":
        log.info("── Upload Caption 跳过: file_id=%d 已完成 ──", file_id)
        return {"task_id": task_id, "status": "skipped",
                "reason": "already_completed", "file_id": file_id}

    log.info("── Upload Caption 开始: file_id=%d video=%s url=%s ──",
             file_id, video_id, youtube_url)
    store.update_file_status(file_id, "processing")
    _progress(progress=10, msg="Fetching YouTube transcript...", url=youtube_url)

    outcome = _fetch_transcript_with_fallback(youtube_url)

    # ── 成功: 写回 file_processing_details + 回调远程 ──────────────────
    if outcome["success"]:
        result = outcome["result"]
        full_text = YouTubeCaptionService.get_full_text(result)
        store.update_file_transcript(file_id, full_text, result, status="completed")

        via = outcome["source"]  # 'API' 或 'yt-dlp'
        source_tag = "caption_api" if via == "API" else "caption_ytdlp"
        source_type = "自动生成" if result.get("is_generated") else "人工字幕"
        extra = (f" (API→{outcome['api_reason']})"
                 if via == "yt-dlp" and outcome["api_reason"] else "")
        elapsed = round(time.time() - task_start, 2)
        log.info(
            "── Upload Caption 完成[%s]: file_id=%d lang=%s(%s) 字数=%d%s ──",
            via, file_id, result.get("language"), source_type,
            len(full_text), extra,
        )
        _progress(status="completed", progress=100,
                  msg=f"Caption fetched via {via}", url=youtube_url)

        store.signal_upload_file_ready(
            file_id, username, "completed",
            source=source_tag,
            length=len(full_text),
            language=result.get("language"),
            is_generated=bool(result.get("is_generated")),
            elapsed_seconds=elapsed,
        )
        return {
            "task_id": task_id, "status": "completed", "file_id": file_id,
            "source": source_tag, "length": len(full_text),
        }

    # ── 永久失败 (直播 / 私有 / 首播未开始 ...): 不降级 ASR ─────────────
    if not outcome["should_asr"]:
        reason = _format_caption_failure_reason(outcome)
        elapsed = round(time.time() - task_start, 2)
        log.warning(
            "── Upload Caption 永久失败: file_id=%d video=%s 原因=%s ──",
            file_id, video_id, reason,
        )
        store.update_file_status(file_id, "failed")
        _progress(status="failed", msg=f"Caption failed (permanent): {reason}",
                  url=youtube_url, failed=1)
        store.signal_upload_file_ready(
            file_id, username, "failed",
            source="caption_permanent_fail",
            reason=reason,
            elapsed_seconds=elapsed,
        )
        return {"task_id": task_id, "status": "failed", "permanent": True,
                "reason": reason, "file_id": file_id}

    # ── 可降级: 派发 ASR 任务 (不 signal, 终态由 ASR 任务负责 signal) ──
    reason = _format_caption_failure_reason(outcome)
    log.info(
        "── Upload Caption 降级 ASR: file_id=%d video=%s 原因=%s ──",
        file_id, video_id, reason,
    )
    _progress(progress=40,
              msg=f"Caption unavailable ({reason}), switching to Fun-ASR...",
              url=youtube_url)
    try:
        self.app.send_task(
            "transcribe_youtube_file_asr_task",
            args=(file_id, youtube_url, video_id, username, 0),
            queue="youtube_transcription",
        )
        log.info("  → File ASR 任务已派发: file_id=%d video=%s", file_id, video_id)
    except Exception as e:
        # 派发失败 = 永久失败, 必须 signal, 否则远程 processing_stats 卡在 processing
        elapsed = round(time.time() - task_start, 2)
        log.error("  ASR 派发失败: file_id=%d error=%s", file_id, e)
        store.update_file_status(file_id, "failed")
        _progress(status="failed", msg=f"ASR dispatch failed: {e}",
                  url=youtube_url, failed=1)
        store.signal_upload_file_ready(
            file_id, username, "failed",
            source="asr_dispatch_fail",
            reason=f"ASR dispatch failed: {e}",
            elapsed_seconds=elapsed,
        )
        return {"task_id": task_id, "status": "failed", "permanent": True,
                "reason": f"ASR dispatch failed: {e}", "file_id": file_id}

    return {"task_id": task_id, "status": "asr_dispatched",
            "reason": reason, "file_id": file_id}


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


def _current_routing_key(task) -> str:
    """取当前任务消费时所在的 routing_key（== 队列名），失败时返回短队列名。"""
    try:
        info = getattr(task.request, "delivery_info", None) or {}
        return info.get("routing_key") or _SHORT_QUEUE
    except Exception:
        return _SHORT_QUEUE


def _classify_asr_failure(err: Exception) -> tuple:
    """
    将 ASR 流水线抛出的异常归类为 (简短描述, 是否可重试)。
    永久性错误不应重试，立即标记 failed；瞬时错误走指数退避。
    """
    if isinstance(err, SoftTimeLimitExceeded):
        # 软超时绝大多数是「直播流 / 异常大文件 / yt-dlp 卡死」这类重试也治不好的
        # 场景。反复重试只会让同一个视频占用 worker 数小时。标记永久失败, 直接
        # 放弃该视频, 避免卡住整个 worker 池。
        return "处理超时 (soft time limit, 可能是直播或异常大文件)", False

    msg = str(err)
    low = msg.lower()

    # ── 永久失败（视频层面）─────────────────────────────
    if "video is not available" in low or "video unavailable" in low:
        return "视频不可用", False
    if "private video" in low:
        return "私有视频", False
    if "sign in to confirm" in low or "age-restricted" in low or "age restricted" in low:
        return "年龄限制", False
    if "members-only" in low or "members only content" in low:
        return "会员专属", False
    if "removed by the uploader" in low or "account has been terminated" in low:
        return "视频已删除", False
    if "asr_response_have_no_words" in low or "asr 未返回结果" in low:
        return "ASR 无识别结果（视频无人声）", False

    # ── 永久失败（配置/权限层面，需人工介入，不应自动重试）──
    if "accessdenied" in low or ("403" in msg and "oss" in low):
        return "OSS 权限拒绝 (需检查 ACL)", False
    if "oss 未配置" in msg or "asr 未配置" in low:
        return "服务未配置", False
    # curl-cffi impersonate 配置错误是永久的配置问题，重试无意义
    if "impersonate target" in low and "not available" in low:
        return "impersonate 配置错误 (检查 curl-cffi 版本/YTDLP_IMPERSONATE 值)", False

    # ── 瞬时失败（网络/限流/外部服务抖动）────────────────
    if "429" in msg or "too many requests" in low:
        return "请求过多 (429)", True
    if "503" in msg or "service unavailable" in low:
        return "服务暂不可用 (503)", True
    if "502" in msg or "bad gateway" in low:
        return "网关错误 (502)", True
    if "timeout" in low or "timed out" in low or "read timed out" in low:
        return "网络超时", True
    if "connection reset" in low or "connection aborted" in low:
        return "连接被重置", True
    if "yt-dlp 未生成音频文件" in msg:
        return "yt-dlp 下载未产出文件", True

    # ── 默认策略：未分类异常走重试（保守策略，避免漏过可恢复错误）──
    short = msg.split("\n")[0][:100]
    return short, True


def _compute_retry_countdown(retry_count: int) -> int:
    """指数退避，最大不超过 _RETRY_BACKOFF_MAX。
    retry_count=0 → 60s, =1 → 120s, =2 → 240s, 封顶 600s。
    """
    countdown = _RETRY_BACKOFF_BASE * (2 ** retry_count)
    return min(countdown, _RETRY_BACKOFF_MAX)


@app.task(bind=True, name="transcribe_youtube_feed_task", queue=_SHORT_QUEUE,
          max_retries=_MAX_RETRIES)
def transcribe_youtube_feed_task(self, content_db_id: int, video_url: str, video_id: str,
                                 _skip_duration_check: bool = False):
    """对 Feed 中无字幕的 YouTube 视频, 下载音频 → Fun-ASR 转录 → 写回 DB

    若当前在短队列且探测到视频超过 `_LONG_VIDEO_THRESHOLD`，会自动转发到长视频队列，
    避免大文件阻塞短视频处理。`_skip_duration_check=True` 用于长队列 worker 消费时跳过二次检测。
    """
    task_id = self.request.id
    temp_dir = None

    current_queue = _current_routing_key(self)

    # ── 预探测: 获取元信息 (is_live / live_status / duration) ──────────────
    # 目的有两个:
    #   1) 直播类视频直接永久失败, 避免 yt-dlp 无限等流卡死 worker 10+ 小时
    #   2) 顺便拿到时长用于长短队列路由 (以前用 probe_youtube_duration 做)
    # 注意: 只在短队列+首次进入时 probe; 长队列二次消费跳过, 不浪费时间
    if not _skip_duration_check and current_queue == _SHORT_QUEUE:
        meta = probe_youtube_metadata(video_url)
        live_status = meta.get("live_status")

        # ── P0 修复: 直播 / 首播未开始 / post_live 直接永久失败 ──
        if meta.get("is_live") or live_status in ("is_live", "is_upcoming", "post_live"):
            err_msg = f"直播视频无法转录 (live_status={live_status or 'is_live'})"
            log.warning(
                "── 直播跳过: content=%d video=%s live_status=%s「%s」──",
                content_db_id, video_id, live_status or "is_live",
                (meta.get("title") or "")[:40],
            )
            _progress(status="failed", msg=err_msg, url=video_url, failed=1)
            return {"task_id": task_id, "status": "failed", "error": err_msg,
                    "permanent": True, "reason": "is_live",
                    "content_db_id": content_db_id}

        # ── 长视频路由: 超阈值转发到长队列 ──
        duration = meta.get("duration")
        if duration and duration > _LONG_VIDEO_THRESHOLD:
            log.info(
                "── 长视频检测: content=%d video=%s 时长=%dm%02ds 超过阈值%dm → 转发长视频队列 ──",
                content_db_id, video_id,
                duration // 60, duration % 60,
                _LONG_VIDEO_THRESHOLD // 60,
            )
            try:
                transcribe_youtube_feed_task.apply_async(
                    args=[content_db_id, video_url, video_id],
                    kwargs={"_skip_duration_check": True},
                    queue=_LONG_QUEUE,
                    time_limit=_LONG_TASK_HARD_LIMIT,
                    soft_time_limit=_LONG_TASK_SOFT_LIMIT,
                )
                return {"task_id": task_id, "status": "rerouted_to_long",
                        "content_db_id": content_db_id, "duration_seconds": duration}
            except Exception as e:
                log.warning("转发长视频队列失败，继续在短队列处理: %s", e)

    try:
        log.info("── Feed ASR 开始: content=%d video=%s url=%s queue=%s ──",
                 content_db_id, video_id, video_url, current_queue)
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
        if current_queue == _LONG_QUEUE:
            result = asr.transcribe(up["audio_url"], timeout_min=_ASR_POLL_TIMEOUT_LONG_MIN)
        else:
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
        # 先清理临时目录，避免重试前残留占用磁盘
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
            temp_dir = None

        reason, retriable = _classify_asr_failure(e)
        retries_done = self.request.retries
        tag = "可重试" if retriable else "永久"
        log.error("── Feed ASR 失败[%s]: reason=%s content=%d video=%s (尝试 %d/%d) ──",
                  tag, reason, content_db_id, video_id, retries_done + 1, _MAX_RETRIES + 1)

        if retriable and retries_done < _MAX_RETRIES:
            countdown = _compute_retry_countdown(retries_done)
            log.info("  → %ds 后重试 (第 %d 次)", countdown, retries_done + 2)
            _progress(status="processing", msg=f"Retrying in {countdown}s: {reason}", url=video_url)
            raise self.retry(exc=e, countdown=countdown)

        err = f"Feed ASR 失败: {reason}"
        _progress(status="failed", msg=err, url=video_url, failed=1)
        return {"task_id": task_id, "status": "failed", "error": err,
                "permanent": not retriable, "retries": retries_done}
    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)


@app.task(bind=True, name="transcribe_youtube_file_asr_task", queue="youtube_transcription",
          max_retries=_MAX_RETRIES)
def transcribe_youtube_file_asr_task(self, file_id: int, youtube_url: str, video_id: str,
                                     username: str = "guest",
                                     max_duration_seconds: int = 0,
                                     _skip_duration_check: bool = False):
    """对 Upload YouTube Link 中无字幕的视频, 下载 → Fun-ASR → 写回 file_processing_details.

    Args:
        file_id:              file_processing_details 主键
        youtube_url:          YouTube 完整 URL
        video_id:             YouTube video_id
        username:             文件所属用户, 用于终态 signal 的 finalize 任务
        max_duration_seconds: 预留参数 (0 = 完整视频), 当前未使用
        _skip_duration_check: 长队列二次消费时置 True, 跳过元信息 probe

    与 Feed 侧一致, 本任务:
      - 短队列首次进入 → probe 元信息, 直播永久失败, 长视频转发长队列
      - 成功 / 永久失败 均通过 `signal_upload_file_ready` 回调远程
        `finalize_youtube_file_task`, 触发 auto-tag / auto-name / auto-classify
    """
    task_id = self.request.id
    temp_dir = None
    store = ContentStore()
    task_start = time.time()

    current_queue = _current_routing_key(self)

    # ── 预探测: 直播永久失败 + 长视频路由 ────────────────────────────
    # 与 Feed 的 transcribe_youtube_feed_task 完全对齐, 只在短队列首次进入时做.
    if not _skip_duration_check and current_queue == _SHORT_QUEUE:
        try:
            meta = probe_youtube_metadata(youtube_url) or {}
        except Exception as e:
            log.warning("probe_youtube_metadata 失败, 继续走默认路径: %s", e)
            meta = {}
        live_status = meta.get("live_status")

        if meta.get("is_live") or live_status in ("is_live", "is_upcoming", "post_live"):
            err_msg = f"直播视频无法转录 (live_status={live_status or 'is_live'})"
            log.warning(
                "── Upload ASR 直播跳过: file_id=%d video=%s live_status=%s「%s」──",
                file_id, video_id, live_status or "is_live",
                (meta.get("title") or "")[:40],
            )
            store.update_file_status(file_id, "failed")
            _progress(status="failed", msg=err_msg, url=youtube_url, failed=1)
            elapsed = round(time.time() - task_start, 2)
            store.signal_upload_file_ready(
                file_id, username, "failed",
                source="asr_live_skip",
                reason=err_msg,
                elapsed_seconds=elapsed,
            )
            return {"task_id": task_id, "status": "failed", "error": err_msg,
                    "permanent": True, "reason": "is_live", "file_id": file_id}

        duration = meta.get("duration")
        if duration and duration > _LONG_VIDEO_THRESHOLD:
            log.info(
                "── Upload ASR 长视频检测: file_id=%d video=%s 时长=%dm%02ds 超过阈值%dm → 转发长视频队列 ──",
                file_id, video_id,
                duration // 60, duration % 60,
                _LONG_VIDEO_THRESHOLD // 60,
            )
            try:
                transcribe_youtube_file_asr_task.apply_async(
                    args=[file_id, youtube_url, video_id, username, max_duration_seconds],
                    kwargs={"_skip_duration_check": True},
                    queue=_LONG_QUEUE,
                    time_limit=_LONG_TASK_HARD_LIMIT,
                    soft_time_limit=_LONG_TASK_SOFT_LIMIT,
                )
                return {"task_id": task_id, "status": "rerouted_to_long",
                        "file_id": file_id, "duration_seconds": duration}
            except Exception as e:
                log.warning("转发长视频队列失败，继续在短队列处理: %s", e)

    try:
        log.info(
            "── Upload ASR 开始: file_id=%d video=%s url=%s queue=%s ──",
            file_id, video_id, youtube_url, current_queue,
        )
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
        if current_queue == _LONG_QUEUE:
            result = asr.transcribe(up["audio_url"], timeout_min=_ASR_POLL_TIMEOUT_LONG_MIN)
        else:
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

        elapsed = round(time.time() - task_start, 2)
        log.info(
            "── Upload ASR 完成: file_id=%d video=%s 字数=%d 耗时=%.1fs ──",
            file_id, video_id, len(full_text), elapsed,
        )
        store.signal_upload_file_ready(
            file_id, username, "completed",
            source="asr",
            length=len(full_text),
            elapsed_seconds=elapsed,
        )
        return {"task_id": task_id, "status": "completed", "file_id": file_id,
                "content_length": len(full_text)}

    except Exception as e:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
            temp_dir = None

        reason, retriable = _classify_asr_failure(e)
        retries_done = self.request.retries
        tag = "可重试" if retriable else "永久"
        log.error(
            "── Upload ASR 失败[%s]: reason=%s file_id=%d video=%s (尝试 %d/%d) ──",
            tag, reason, file_id, video_id, retries_done + 1, _MAX_RETRIES + 1,
        )

        if retriable and retries_done < _MAX_RETRIES:
            countdown = _compute_retry_countdown(retries_done)
            log.info("  → %ds 后重试 (第 %d 次)", countdown, retries_done + 2)
            store.update_file_status(file_id, "processing")
            _progress(status="processing", msg=f"Retrying in {countdown}s: {reason}", url=youtube_url)
            # 重试不触发 signal, 终态由最终成功/永久失败分支负责
            raise self.retry(exc=e, countdown=countdown)

        err = f"Upload ASR 失败: {reason}"
        store.update_file_status(file_id, "failed")
        _progress(status="failed", msg=err, url=youtube_url, failed=1)
        elapsed = round(time.time() - task_start, 2)
        store.signal_upload_file_ready(
            file_id, username, "failed",
            source="asr",
            reason=reason,
            retries=retries_done,
            permanent=not retriable,
            elapsed_seconds=elapsed,
        )
        return {"task_id": task_id, "status": "failed", "error": err,
                "permanent": not retriable, "retries": retries_done,
                "file_id": file_id}
    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
