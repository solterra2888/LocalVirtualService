# -*- coding: utf-8 -*-
"""
Surveillance Flash Service — Bloomberg Surveillance 速览版（精华集）AM Brief

数据源：Bloomberg Surveillance 官方播客（Omny RSS）。节目当晚（北京 21:32–22:06）
发布 30–53 分钟精华集，RSS 自带官方字幕（TextWithTimestamps），无需 ASR。

与完整版（BloombergSurveillanceService，YouTube 全场转录）的关系：
- 同一 report_date（北京自然日 D+1，与完整版配对）：速览版在 D 日 22:30 先写入，
  次日早间完整版通过"窗口内出现新视频"的幂等重跑机制自然升级覆盖。
- 摘要/报告共用 bloomberg_surveillance_summaries / surveillance_reports 的
  report_date 行；速览版 metadata.variant='flash' 供前端/模板区分。
- 精华集字幕为编辑精选（约覆盖完整版六成主题），升级前的过渡产物。
"""

import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import feedparser
import requests

from backend.services.bloomberg_surveillance_service import (
    BloombergSurveillanceService,
    _is_placeholder_summary,
)
from backend.utils.datetime_utils import utcnow, window_label_date
from backend.utils.logger import get_logger

logger = get_logger("surveillance_flash_service")

# Bloomberg Surveillance 官方播客（iTunes 目录解析所得，Omny Studio 托管）
PODCAST_FEED_URL = (
    "https://www.omnycontent.com/d/playlist/e73c998e-6e60-432f-8610-ae210140c5b1/"
    "8e704079-ca57-4eac-9741-ae27003e2b7f/9739700c-72c3-4176-ae55-ae27003e2b96/"
    "podcast.rss"
)
# 入库用的稳定频道标识（channel_type='podcast'，与 YouTube 管线互不干扰）
PODCAST_AUTHOR_ID = "bloomberg_surveillance_podcast"
PODCAST_AUTHOR_NAME = "Bloomberg Surveillance (Podcast)"
DEFAULT_REPORT_TIMEZONE = "Asia/Shanghai"

# 精华集时长窗口（秒）：25–75 分钟。每日版 "Surveillance TV: ..." 约 21–23 分钟，
# 通过时长下限排除；周末特辑偶发超窗口，宁可漏出不误收每日版。
HIGHLIGHT_MIN_SECONDS = 25 * 60
HIGHLIGHT_MAX_SECONDS = 75 * 60

DAILY_EDITION_MARK = "surveillance tv"

_TRANSCRIPT_URL_RE = re.compile(
    r'<podcast:transcript url="([^"]*TextWithTimestamps[^"]*)"'
)
_TIMESTAMP_LINE_RE = re.compile(r"^\d{1,2}:\d{2}:\d{2}$")


def _parse_itunes_duration(val: Any) -> int:
    """解析 itunes:duration（H:MM:SS / MM:SS / 秒数）为秒。"""
    try:
        parts = [int(p) for p in str(val).split(":")]
    except (ValueError, TypeError):
        return 0
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    if len(parts) == 2:
        return parts[0] * 60 + parts[1]
    return int(float(val)) if parts else 0


def flash_report_date(now_utc: Optional[datetime] = None) -> date:
    """速览版 report_date：北京自然日 + 1（与同场节目的完整版同一口径）。

    精华集在北京 D 日晚间生成，对应美东 D 日节目；完整版次日（D+1）早间
    生成并使用 report_date=D+1，速览版对齐到 D+1 以便成对升级。
    """
    label = window_label_date(now_utc or utcnow(), DEFAULT_REPORT_TIMEZONE)
    return label + timedelta(days=1)


def select_evening_highlight(
    episodes: List[Dict[str, Any]],
    now_utc: datetime,
    lookback_hours: float = 4.0,
) -> Optional[Dict[str, Any]]:
    """从播客 feed 剧集中挑选当晚精华集（纯函数，供单测）。

    规则：发布于最近 lookback_hours 内、时长 25–75 分钟、非 "Surveillance TV"
    每日版、带官方字幕 URL；取发布最新的一条。
    """
    cutoff = now_utc - timedelta(hours=lookback_hours)
    candidates = []
    for ep in episodes:
        published = ep.get("published_utc")
        duration = ep.get("duration_seconds") or 0
        title = (ep.get("title") or "").lower()
        if not published or not ep.get("transcript_url"):
            continue
        if not (cutoff <= published <= now_utc):
            continue
        if not (HIGHLIGHT_MIN_SECONDS <= duration <= HIGHLIGHT_MAX_SECONDS):
            continue
        if DAILY_EDITION_MARK in title:
            continue
        candidates.append(ep)
    if not candidates:
        return None
    return max(candidates, key=lambda e: e["published_utc"])


class SurveillanceFlashService(BloombergSurveillanceService):
    """Bloomberg Surveillance 速览版摘要服务（复用完整版的 LLM 与持久化）。"""

    # ── Public API ─────────────────────────────────────────────────────────

    async def generate(
        self,
        report_date: Optional[date] = None,
        lookback_hours: float = 4.0,
        force: bool = False,
    ) -> Dict[str, Any]:
        """生成速览版摘要。

        Args:
            report_date: 报告日期，默认北京自然日 + 1（与完整版配对）。
            lookback_hours: 精华集检索回溯窗口（小时），默认 4。
            force: 是否强制重新生成。
        """
        start_time = time.time()
        if report_date is None:
            report_date = flash_report_date()
        logger.info(
            f"开始生成 Surveillance 速览版: report_date={report_date}, "
            f"lookback={lookback_hours}h"
        )

        episodes = self._fetch_feed_episodes()
        now = utcnow()
        episode = select_evening_highlight(episodes, now, lookback_hours)

        # 幂等：完整版已存在勿降级；同集速览已生成则跳过。
        if not force and episode:
            existing = self._get_existing_summary(report_date)
            if self._should_skip_flash(existing, episode["clip_id"]):
                logger.info(f"速览版摘要已存在, 跳过: {report_date}")
                return {
                    "success": True,
                    "summary_id": existing["id"],
                    "message": "摘要已存在",
                    "skipped": True,
                }

        summary_id = self._upsert_summary_status(report_date, "generating")

        try:
            if not episode:
                self._update_summary(
                    summary_id,
                    status="completed",
                    summary_content="精华集尚未发布。",
                    metadata={
                        "reason": "episode_not_yet_published",
                        "variant": "flash",
                    },
                )
                logger.info(f"未找到当晚精华集, 记占位: {report_date}")
                return {
                    "success": True,
                    "summary_id": summary_id,
                    "message": "精华集尚未发布",
                    "skipped": True,
                    "reason": "episode_not_yet_published",
                }

            transcript_raw = self._download_transcript(episode)
            transcript = self._clean_transcript(transcript_raw)
            word_count = len(transcript.split())
            if word_count < 500:
                self._update_summary(
                    summary_id,
                    status="completed",
                    video_id=episode["clip_id"],
                    video_title=episode["title"],
                    video_url=episode.get("link"),
                    summary_content="精华集字幕内容异常（过短）。",
                    metadata={"reason": "no_transcript", "variant": "flash"},
                )
                return {
                    "success": True,
                    "summary_id": summary_id,
                    "message": "精华集字幕过短",
                    "skipped": True,
                    "reason": "no_transcript",
                }

            self._upsert_podcast_content(episode, transcript)

            summary_text = await self._call_llm_with_retry(
                transcript, episode["title"], report_date
            )

            elapsed = round(time.time() - start_time, 2)
            metadata = {
                "variant": "flash",
                "generation_time_seconds": elapsed,
                "transcript_word_count": word_count,
                "transcript_char_count": len(transcript),
                "video_id": episode["clip_id"],
                "video_ids": [episode["clip_id"]],
                "video_count": 1,
                "transcript_segment_count": 1,
                "source": "podcast_highlight",
                "episode_title": episode["title"],
                "episode_published_utc": episode["published_utc"].isoformat(),
            }
            self._update_summary(
                summary_id,
                status="completed",
                video_id=episode["clip_id"],
                video_title=episode["title"],
                video_url=episode.get("link"),
                transcript_word_count=word_count,
                summary_content=summary_text,
                metadata=metadata,
            )
            logger.info(
                f"Surveillance 速览版完成: {episode['title'][:60]}, "
                f"{word_count} words, 耗时 {elapsed}s"
            )
            return {
                "success": True,
                "summary_id": summary_id,
                "message": f"速览版生成成功（{word_count} words, {elapsed}s）",
                "video_count": 1,
                "transcript_word_count": word_count,
                "generation_time_seconds": elapsed,
                "skipped": False,
            }

        except Exception as e:
            error_msg = f"速览版生成失败: {e}"
            logger.error(error_msg, exception=str(e))
            self._update_summary(summary_id, status="failed", error_message=error_msg)
            return {
                "success": False,
                "summary_id": summary_id,
                "message": error_msg,
                "skipped": False,
            }

    # ── Episode picking ────────────────────────────────────────────────────

    def _fetch_feed_episodes(self) -> List[Dict[str, Any]]:
        """拉取播客 RSS，解析近期剧集（含官方字幕 URL）。"""
        resp = requests.get(PODCAST_FEED_URL, timeout=30)
        resp.raise_for_status()
        raw = resp.content.decode("utf-8", errors="replace").replace("&amp;", "&")
        feed = feedparser.parse(resp.content)
        blocks = raw.split("<item>")[1:]

        episodes: List[Dict[str, Any]] = []
        for idx, entry in enumerate(feed.entries[:30]):
            published = None
            for key in ("published_parsed", "updated_parsed"):
                st = entry.get(key)
                if st:
                    published = datetime(*st[:6], tzinfo=timezone.utc)
                    break
            audio_url = ""
            for enc in entry.get("enclosures", []):
                audio_url = enc.get("href") or enc.get("url") or ""
                break
            block = blocks[idx] if idx < len(blocks) else ""
            m = _TRANSCRIPT_URL_RE.search(block)
            transcript_url = m.group(1) if m else None
            clip_id = ""
            if transcript_url:
                cm = re.search(r"/clips/([0-9a-f-]{36})/", transcript_url)
                clip_id = cm.group(1) if cm else ""
            episodes.append(
                {
                    "title": entry.get("title", ""),
                    "link": entry.get("link", ""),
                    "guid": entry.get("id", ""),
                    "published_utc": published,
                    "duration_seconds": _parse_itunes_duration(
                        entry.get("itunes_duration", "")
                    ),
                    "audio_url": audio_url,
                    "transcript_url": transcript_url,
                    "clip_id": clip_id,
                    "summary": (entry.get("summary") or entry.get("description") or "")[:2000],
                }
            )
        logger.info(f"播客 feed 解析完成: {len(episodes)} 条近期剧集")
        return episodes

    @staticmethod
    def _should_skip_flash(
        existing: Optional[Dict[str, Any]], clip_id: str
    ) -> bool:
        """速览幂等：完整版已存在勿降级；同集速览已生成则跳过。"""
        if (
            not existing
            or existing.get("status") != "completed"
            or _is_placeholder_summary(existing)
        ):
            return False
        meta = existing.get("metadata") or {}
        if meta.get("variant") != "flash":
            return True  # 完整版（或旧数据）已存在
        ids = set(meta.get("video_ids") or [])
        if existing.get("video_id"):
            ids.add(existing["video_id"])
        return clip_id in ids

    # ── Transcript & persistence ───────────────────────────────────────────

    @staticmethod
    def _download_transcript(episode: Dict[str, Any]) -> str:
        """下载官方字幕（TextWithTimestamps）。"""
        resp = requests.get(episode["transcript_url"], timeout=30)
        resp.raise_for_status()
        return resp.text

    @staticmethod
    def _clean_transcript(text: str) -> str:
        """去掉独立时间戳行，保留 'Speaker N: ...' 对话文本。"""
        lines = [
            line.rstrip()
            for line in text.splitlines()
            if line.strip() and not _TIMESTAMP_LINE_RE.match(line.strip())
        ]
        return "\n".join(lines).strip()

    def _upsert_podcast_content(
        self, episode: Dict[str, Any], transcript: str
    ) -> None:
        """精华集字幕入库 subscription_content（channel_type='podcast'）。"""
        from psycopg2.extras import Json

        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO subscription_content (
                    channel_type, content_id, title, content, url,
                    author_name, author_identifier, metadata,
                    fetched_at, published_at, duration_seconds,
                    feed_url, guid, transcript
                ) VALUES (
                    'podcast', %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (channel_type, content_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    transcript = EXCLUDED.transcript,
                    metadata = EXCLUDED.metadata,
                    fetched_at = EXCLUDED.fetched_at
                """,
                (
                    episode["clip_id"],
                    episode["title"],
                    episode.get("summary"),
                    episode.get("link"),
                    PODCAST_AUTHOR_NAME,
                    PODCAST_AUTHOR_ID,
                    Json(
                        {
                            "show_name": "Bloomberg Surveillance",
                            "source": "podcast_highlight",
                            "transcript_url": episode.get("transcript_url"),
                            "audio_url": episode.get("audio_url"),
                        }
                    ),
                    utcnow(),
                    episode.get("published_utc"),
                    episode.get("duration_seconds"),
                    PODCAST_FEED_URL,
                    episode.get("guid"),
                    transcript,
                ),
            )
            conn.commit()
            logger.info(
                f"精华集字幕已入库: content_id={episode['clip_id']}, "
                f"{len(transcript)} chars"
            )
        finally:
            cursor.close()
