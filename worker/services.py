# -*- coding: utf-8 -*-
"""
独立 Worker 所需的全部服务模块：
  - YouTubeCaptionService   : 通过 youtube-transcript-api 获取字幕
  - YouTubeFeedService      : 通过 RSS / Data API v3 获取频道视频列表 + 存储
  - OSSService              : 上传音频到阿里云 OSS
  - ASRService              : 通过 DashScope Fun-ASR 语音识别
  - ContentStore            : 读写 subscription_content / file_processing_details
"""

import json
import logging
import os
import re
import shutil
import tempfile
import time
import uuid
from datetime import datetime
from http import HTTPStatus
from typing import Any, Dict, List, Optional
from urllib import request as urllib_request

import feedparser
import requests
from psycopg2.extras import Json

from .db import DB, get_db_config

log = logging.getLogger("worker")

# ---------------------------------------------------------------------------
# YouTubeCaptionService
# ---------------------------------------------------------------------------

class YouTubeCaptionService:
    """通过 youtube-transcript-api 获取 YouTube 字幕"""

    _TIMEOUT = 30  # 秒

    @staticmethod
    def extract_video_id(url: str) -> Optional[str]:
        patterns = [
            r'(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/|youtube\.com\/shorts\/)([a-zA-Z0-9_-]{11})',
            r'youtube\.com\/watch\?.*v=([a-zA-Z0-9_-]{11})',
        ]
        for p in patterns:
            m = re.search(p, url)
            if m:
                return m.group(1)
        return None

    @staticmethod
    def fetch_transcript(video_url: str, languages: Optional[List[str]] = None) -> Dict[str, Any]:
        from youtube_transcript_api import YouTubeTranscriptApi

        video_id = YouTubeCaptionService.extract_video_id(video_url)
        if not video_id:
            raise ValueError(f"无法从 URL 提取 video_id: {video_url}")

        if languages is None:
            languages = ["zh-Hans", "zh-CN", "zh", "zh-Hant", "zh-TW", "en", "en-US"]

        import concurrent.futures

        def _do_fetch():
            api = YouTubeTranscriptApi()
            tl = api.list(video_id)
            transcript = None
            lang_used = None
            for lang in languages:
                try:
                    transcript = tl.find_transcript([lang])
                    lang_used = lang
                    break
                except Exception:
                    continue
            if transcript is None:
                for t in tl:
                    transcript = t
                    lang_used = t.language_code
                    break
            if transcript is None:
                raise Exception("No transcript available")
            data = transcript.fetch()
            return {
                "video_id": video_id,
                "video_url": video_url,
                "language": lang_used,
                "is_generated": getattr(transcript, "is_generated", False),
                "transcript": [{"text": e.text, "start": e.start, "duration": e.duration} for e in data],
            }

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_do_fetch)
            return future.result(timeout=YouTubeCaptionService._TIMEOUT)

    @staticmethod
    def get_full_text(transcript_data: Dict[str, Any]) -> str:
        if not transcript_data or "transcript" not in transcript_data:
            return ""
        return "\n".join(seg["text"] for seg in transcript_data["transcript"])


# ---------------------------------------------------------------------------
# YouTubeFeedService  — RSS 获取 + Data API v3 fallback
# ---------------------------------------------------------------------------

def _get_proxies() -> Optional[Dict[str, str]]:
    proxy = os.getenv("YOUTUBE_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
    if proxy:
        return {"http": proxy, "https": proxy}
    return None


def _get_youtube_api_key() -> Optional[str]:
    key = os.getenv("YOUTUBE_DATA_API_KEY", "").strip()
    return key or None


def _normalize_channel_id(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    c = str(raw).strip()
    if len(c) >= 24 and c.startswith("UC"):
        return c
    if len(c) == 22 and re.match(r"^[A-Za-z0-9_-]{22}$", c):
        return "UC" + c
    return c


class YouTubeFeedService:
    """通过 RSS / YouTube Data API v3 获取频道视频列表"""

    REQUEST_TIMEOUT = 5
    RSS_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    RSS_USER = "https://www.youtube.com/feeds/videos.xml?user={username}"
    API_BASE = "https://www.googleapis.com/youtube/v3"
    MAX_RETRIES = 3

    # ── RSS ─────────────────────────────────────────────────────

    @classmethod
    def fetch_channel_videos(cls, channel_identifier: str, max_items: int = 15) -> List[Dict[str, Any]]:
        ident = channel_identifier.strip().lstrip("@")
        is_cid = len(ident) >= 20 and ident.startswith("UC")

        videos = cls._fetch_via_rss(ident, max_items, is_cid)
        if videos:
            return videos

        api_key = _get_youtube_api_key()
        if not api_key:
            log.warning("RSS 未获取到视频且未配置 YOUTUBE_DATA_API_KEY (channel=%s)", ident)
            return []
        if is_cid:
            log.info("RSS 失败, 尝试 Data API v3 fallback (channel=%s)", ident)
            return cls._fetch_via_api(ident, max_items, api_key)
        return []

    @classmethod
    def _fetch_via_rss(cls, ident: str, max_items: int, is_cid: bool) -> List[Dict[str, Any]]:
        rss_url = (cls.RSS_URL.format(channel_id=ident) if is_cid
                   else cls.RSS_USER.format(username=ident))
        proxies = _get_proxies()
        resp = None
        for attempt in range(cls.MAX_RETRIES):
            try:
                resp = requests.get(rss_url, headers={"User-Agent": "feedparser/6.0"},
                                    timeout=cls.REQUEST_TIMEOUT, proxies=proxies)
                if resp.status_code in (404, 500, 502, 503, 504) and attempt < cls.MAX_RETRIES - 1:
                    log.warning("RSS HTTP %d, 重试 %d/%d: %s", resp.status_code, attempt + 1, cls.MAX_RETRIES, rss_url)
                    time.sleep(1.0 * (attempt + 1))
                    continue
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt < cls.MAX_RETRIES - 1:
                    retryable = False
                    if isinstance(e, requests.HTTPError) and e.response is not None:
                        retryable = e.response.status_code in (404, 500, 502, 503, 504)
                    if retryable:
                        time.sleep(1.0 * (attempt + 1))
                        continue
                log.error("无法获取 RSS Feed: %s", e)
                return []
        if resp is None:
            return []

        feed = feedparser.parse(resp.content)
        if not feed or not hasattr(feed, "entries") or not feed.entries:
            return []

        videos: List[Dict[str, Any]] = []
        for entry in feed.entries[:max_items]:
            vid = getattr(entry, "yt_videoid", None)
            if not vid and "link" in entry:
                m = re.search(r"watch\?v=([a-zA-Z0-9_-]{11})", entry.link)
                vid = m.group(1) if m else None
            if not vid:
                continue

            thumb = f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
            if hasattr(entry, "media_thumbnail") and entry.media_thumbnail:
                thumb = entry.media_thumbnail[0].get("url", thumb)

            view_count = int(getattr(entry, "media_statistics", {}).get("views", 0) if hasattr(entry, "media_statistics") else 0)

            cid = None
            author_name = entry.get("author", "Unknown")
            if hasattr(entry, "yt_channelid"):
                cid = entry.yt_channelid
            elif hasattr(entry, "author_detail") and "href" in entry.author_detail:
                m2 = re.search(r"/channel/([^/]+)", entry.author_detail["href"])
                cid = m2.group(1) if m2 else None
            cid = _normalize_channel_id(cid) if cid else None

            videos.append({
                "id": vid,
                "snippet": {
                    "title": entry.get("title", ""),
                    "description": entry.get("summary", ""),
                    "publishedAt": entry.get("published", ""),
                    "channelId": cid or ident,
                    "channelTitle": author_name,
                    "thumbnails": {"high": {"url": thumb}},
                },
                "statistics": {"viewCount": str(view_count), "likeCount": "0", "commentCount": "0"},
                "contentDetails": {"duration": ""},
            })
        return videos

    # ── YouTube Data API v3 ─────────────────────────────────────

    @classmethod
    def _fetch_via_api(cls, channel_id: str, max_items: int, api_key: str) -> List[Dict[str, Any]]:
        proxies = _get_proxies()
        headers = {"Accept": "application/json"}
        timeout = 10
        try:
            r = requests.get(f"{cls.API_BASE}/channels",
                             params={"part": "contentDetails", "id": channel_id, "key": api_key},
                             headers=headers, timeout=timeout, proxies=proxies)
            if r.status_code == 403:
                log.warning("YouTube Data API 配额已用尽")
                return []
            r.raise_for_status()
            items = r.json().get("items", [])
            if not items:
                return []
            uploads_pid = items[0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
            if not uploads_pid:
                return []

            r = requests.get(f"{cls.API_BASE}/playlistItems",
                             params={"part": "snippet", "playlistId": uploads_pid,
                                     "maxResults": min(max_items, 50), "key": api_key},
                             headers=headers, timeout=timeout, proxies=proxies)
            r.raise_for_status()
            video_ids = [it["snippet"]["resourceId"]["videoId"]
                         for it in r.json().get("items", [])
                         if it.get("snippet", {}).get("resourceId", {}).get("videoId")]
            if not video_ids:
                return []

            r = requests.get(f"{cls.API_BASE}/videos",
                             params={"part": "snippet,statistics,contentDetails",
                                     "id": ",".join(video_ids), "key": api_key},
                             headers=headers, timeout=timeout, proxies=proxies)
            r.raise_for_status()
            videos = []
            for item in r.json().get("items", []):
                sn = item.get("snippet", {})
                st = item.get("statistics", {})
                cd = item.get("contentDetails", {})
                videos.append({
                    "id": item["id"],
                    "snippet": {
                        "title": sn.get("title", ""),
                        "description": sn.get("description", ""),
                        "publishedAt": sn.get("publishedAt", ""),
                        "channelId": sn.get("channelId", channel_id),
                        "channelTitle": sn.get("channelTitle", ""),
                        "thumbnails": sn.get("thumbnails", {}),
                    },
                    "statistics": {
                        "viewCount": st.get("viewCount", "0"),
                        "likeCount": st.get("likeCount", "0"),
                        "commentCount": st.get("commentCount", "0"),
                    },
                    "contentDetails": {"duration": cd.get("duration", "")},
                })
            return videos
        except Exception as e:
            log.error("API v3 fallback 失败: %s", e)
            return []


# ---------------------------------------------------------------------------
# OSSService  — 上传音频到阿里云 OSS (供 Fun-ASR 拉取)
# ---------------------------------------------------------------------------

class OSSService:
    """阿里云 OSS — 仅包含音频上传功能"""

    def __init__(self):
        import oss2
        self.access_key_id = os.getenv("OSS_ACCESS_KEY_ID")
        self.access_key_secret = os.getenv("OSS_ACCESS_KEY_SECRET")
        self.bucket_name = os.getenv("OSS_BUCKET_NAME", "rc-audio-files")
        self.endpoint = os.getenv("OSS_ENDPOINT", "oss-cn-shenzhen.aliyuncs.com")
        self.audio_prefix = os.getenv("OSS_AUDIO_PREFIX", "audio/")
        self.bucket = None
        if self.access_key_id and self.access_key_secret:
            auth = oss2.Auth(self.access_key_id, self.access_key_secret)
            self.bucket = oss2.Bucket(auth, self.endpoint, self.bucket_name)

    def is_configured(self) -> bool:
        return self.bucket is not None

    def upload_audio(self, file_path: str, filename: str, username: str = "system") -> Dict[str, Any]:
        import oss2
        if not self.is_configured():
            return {"success": False, "error": "OSS 未配置"}
        safe_name = re.sub(r"[^\w\u4e00-\u9fff\-.]", "_", filename)
        date_str = datetime.now().strftime("%Y-%m-%d")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        uid = uuid.uuid4().hex[:8]
        obj_name = f"{self.audio_prefix}{username}/{date_str}/{uid}_{ts}_{safe_name}"
        with open(file_path, "rb") as f:
            self.bucket.put_object(obj_name, f, headers={"Content-Type": "audio/mpeg"})

        asr_public = os.getenv("OSS_ASR_PUBLIC_READ", "").strip().lower() in ("1", "true", "yes")
        if asr_public:
            try:
                self.bucket.put_object_acl(obj_name, oss2.OBJECT_ACL_PUBLIC_READ)
            except Exception as e:
                log.warning("设置 public-read 失败: %s", e)

        public_url = f"https://{self.bucket_name}.{self.endpoint}/{obj_name}"
        signed_url = self.bucket.sign_url("GET", obj_name, 3600 * 48)
        audio_url = public_url if asr_public else signed_url

        return {
            "success": True,
            "public_url": public_url,
            "signed_url": signed_url,
            "audio_url": audio_url,
            "object_name": obj_name,
            "file_size": os.path.getsize(file_path),
        }


# ---------------------------------------------------------------------------
# ASRService  — DashScope Fun-ASR
# ---------------------------------------------------------------------------

class ASRService:
    """阿里云 DashScope Fun-ASR 语音识别"""

    def __init__(self):
        self.api_key = os.getenv("DASHSCOPE_API_KEY")
        self._available = False
        try:
            from dashscope.audio.asr import Transcription
            import dashscope
            dashscope.base_http_api_url = "https://dashscope.aliyuncs.com/api/v1"
            if self.api_key:
                dashscope.api_key = self.api_key
            self._available = True
        except ImportError:
            pass

    def is_configured(self) -> bool:
        return self._available and bool(self.api_key)

    def transcribe(self, audio_url: str, language_hints: Optional[List[str]] = None,
                   diarization: bool = True, timeout_min: float = 5) -> Dict[str, Any]:
        """一键转录: 提交 → 轮询 → 下载结果"""
        from dashscope.audio.asr import Transcription

        if not self.is_configured():
            return {"success": False, "error": "ASR 未配置 (DASHSCOPE_API_KEY)"}

        # 提交
        resp = Transcription.async_call(
            model="fun-asr",
            file_urls=[audio_url],
            language_hints=language_hints or ["zh", "en"],
            diarization_enabled=diarization,
        )
        if not resp or not resp.output:
            return {"success": False, "error": "提交 ASR 任务失败"}
        task_id = resp.output.task_id
        log.info("ASR 任务提交: task_id=%s", task_id)

        # 轮询
        deadline = time.monotonic() + timeout_min * 60
        while time.monotonic() < deadline:
            r = Transcription.fetch(task=task_id)
            sc = getattr(r, "status_code", HTTPStatus.OK)
            if sc != HTTPStatus.OK:
                time.sleep(5)
                continue
            if not r or not r.output:
                time.sleep(5)
                continue
            out = r.output if isinstance(r.output, dict) else vars(r.output) if hasattr(r.output, "__dict__") else {}
            status = out.get("task_status", "UNKNOWN")
            if status == "SUCCEEDED":
                results = out.get("results", [])
                transcriptions = []
                for sub in results:
                    if sub.get("subtask_status") == "SUCCEEDED":
                        turl = sub.get("transcription_url")
                        if turl:
                            data = json.loads(urllib_request.urlopen(turl).read().decode("utf8"))
                            transcriptions.append(data)
                return {"success": True, "transcriptions": transcriptions, "task_id": task_id}
            if status == "FAILED":
                return {"success": False, "error": f"ASR FAILED: {out.get('message', '')}"}
            time.sleep(5)

        return {"success": False, "error": f"ASR 超时 ({timeout_min} min)"}

    @staticmethod
    def format_text(transcription_data: Dict[str, Any], include_speaker: bool = True) -> str:
        lines = []
        for transcript in transcription_data.get("transcripts", []):
            for sent in transcript.get("sentences", []):
                begin = sent.get("begin_time", 0) / 1000
                end = sent.get("end_time", 0) / 1000
                text = sent.get("text", "")
                spk = ""
                if include_speaker and "speaker" in sent:
                    spk = f"[{sent['speaker']}] "
                lines.append(f"{spk}[{begin:.2f}s - {end:.2f}s] {text}")
        return "\n".join(lines)

    @staticmethod
    def build_structured_data(transcriptions: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not transcriptions:
            return None
        t = transcriptions[0]
        props = {}
        if "properties" in t:
            p = t["properties"]
            props = {
                "duration_ms": p.get("original_duration_in_milliseconds", 0),
                "audio_format": p.get("audio_format", ""),
                "sampling_rate": p.get("original_sampling_rate", 0),
            }
        sentences, speaker_ids = [], set()
        for tr in t.get("transcripts", []):
            for s in tr.get("sentences", []):
                sid = s.get("speaker_id")
                if sid is not None:
                    speaker_ids.add(sid)
                sentences.append({
                    "sentence_id": s.get("sentence_id"),
                    "begin_time": s.get("begin_time", 0),
                    "end_time": s.get("end_time", 0),
                    "text": s.get("text", ""),
                    "speaker_id": sid,
                })
        sentences.sort(key=lambda x: x["begin_time"])
        speakers = [{"speaker_id": sid, "label": f"speaker {sid}"} for sid in sorted(speaker_ids)]
        return {
            "properties": props,
            "speakers": speakers,
            "speaker_count": len(speakers),
            "sentences": sentences,
            "total_sentences": len(sentences),
        }


# ---------------------------------------------------------------------------
# ContentStore  — 读写数据库
# ---------------------------------------------------------------------------

class ContentStore:
    """subscription_content + file_processing_details 的读写操作"""

    def __init__(self, db: Optional[DB] = None):
        self.db = db or DB()

    # -- YouTube adapter (内联, 不再依赖 content_adapters 包) --

    @staticmethod
    def _normalize_youtube(raw: Dict[str, Any]) -> Dict[str, Any]:
        vid = raw.get("id", "")
        sn = raw.get("snippet", {})
        st = raw.get("statistics", {})
        cd = raw.get("contentDetails", {})

        thumbs = sn.get("thumbnails", {})
        thumb_url = (thumbs.get("maxres", {}).get("url")
                     or thumbs.get("high", {}).get("url")
                     or thumbs.get("medium", {}).get("url")
                     or thumbs.get("default", {}).get("url"))

        published = sn.get("publishedAt", "")
        if published:
            from dateutil import parser as dp
            published_dt = dp.parse(published)
        else:
            published_dt = datetime.now()

        view = int(st.get("viewCount", 0) or 0)
        like = int(st.get("likeCount", 0) or 0)
        comment = int(st.get("commentCount", 0) or 0)

        return {
            "channel_type": "youtube",
            "content_id": vid,
            "title": sn.get("title", ""),
            "content": sn.get("description", ""),
            "author_name": sn.get("channelTitle", ""),
            "author_identifier": sn.get("channelId", ""),
            "url": f"https://www.youtube.com/watch?v={vid}",
            "media_urls": [thumb_url] if thumb_url else [],
            "published_at": published_dt,
            "engagement_count": like + comment,
            "view_count": view,
            "metadata": {
                "video_id": vid,
                "channel_id": sn.get("channelId", ""),
                "duration": cd.get("duration", ""),
                "view_count": view,
                "like_count": like,
                "comment_count": comment,
            },
        }

    @staticmethod
    def _heat_level(engagement: int) -> int:
        if engagement >= 10000:
            return 4
        if engagement >= 1000:
            return 3
        if engagement >= 100:
            return 2
        return 1

    def save_youtube_content(self, raw_video: Dict[str, Any], user_ids: List[int]) -> tuple:
        """保存 YouTube 视频到 subscription_content，返回 (content_db_id, is_new)"""
        n = self._normalize_youtube(raw_video)
        heat = self._heat_level(n["engagement_count"])
        with self.db.connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO subscription_content (
                    channel_type, content_id, title, content,
                    author_name, author_identifier, url, media_urls,
                    published_at, engagement_count, view_count, metadata,
                    user_ids, fetched_at, heat_level,
                    is_retweet, is_quote, is_reply
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    CURRENT_TIMESTAMP, %s, false, false, false
                )
                ON CONFLICT (channel_type, content_id) DO UPDATE SET
                    user_ids = ARRAY(SELECT DISTINCT UNNEST(
                        subscription_content.user_ids || EXCLUDED.user_ids)),
                    engagement_count = EXCLUDED.engagement_count,
                    view_count = EXCLUDED.view_count,
                    metadata = EXCLUDED.metadata,
                    heat_level = EXCLUDED.heat_level,
                    fetched_at = CURRENT_TIMESTAMP
                RETURNING id, (xmax = 0) AS inserted
            """, (
                n["channel_type"], n["content_id"], n.get("title"), n["content"],
                n.get("author_name"), n["author_identifier"], n.get("url"),
                n.get("media_urls", []),
                n["published_at"], n["engagement_count"], n.get("view_count", 0),
                Json(n.get("metadata", {})),
                user_ids, heat,
            ))
            row = cur.fetchone()
            return row[0], row[1]

    def update_transcript(self, content_db_id: int, transcript: str,
                          transcript_data: Dict[str, Any]) -> None:
        with self.db.connection() as conn:
            conn.cursor().execute(
                "UPDATE subscription_content SET transcript=%s, transcript_data=%s WHERE id=%s",
                (transcript, Json(transcript_data), content_db_id),
            )

    def update_file_asr(self, file_id: int, full_text: str,
                        asr_structured: Optional[Dict[str, Any]], status: str = "completed") -> None:
        with self.db.connection() as conn:
            conn.cursor().execute("""
                UPDATE file_processing_details
                SET content=%s, asr_with_diarization=%s, content_length=%s, status=%s
                WHERE id=%s
            """, (
                full_text,
                json.dumps(asr_structured) if asr_structured else None,
                len(full_text), status, file_id,
            ))

    def update_file_status(self, file_id: int, status: str) -> None:
        try:
            with self.db.connection() as conn:
                conn.cursor().execute(
                    "UPDATE file_processing_details SET status=%s WHERE id=%s",
                    (status, file_id),
                )
        except Exception:
            pass

    def get_youtube_subscriptions(self, enabled_only: bool = True) -> List[Dict[str, Any]]:
        with self.db.connection() as conn:
            cur = conn.cursor()
            q = """SELECT id, user_id, channel_type, channel_identifier,
                          channel_name, max_items_per_fetch, enabled
                   FROM subscription_list WHERE channel_type='youtube'"""
            if enabled_only:
                q += " AND enabled=true"
            cur.execute(q)
            return [
                {"id": r[0], "user_id": r[1], "channel_type": r[2],
                 "channel_identifier": r[3], "channel_name": r[4],
                 "max_items_per_fetch": r[5], "enabled": r[6]}
                for r in cur.fetchall()
            ]

    def update_subscription_stats(self, sub_id: int, videos_new: int) -> None:
        try:
            with self.db.connection() as conn:
                conn.cursor().execute("""
                    UPDATE subscription_list
                    SET last_fetched_at=CURRENT_TIMESTAMP,
                        total_items_fetched=total_items_fetched+%s,
                        last_fetch_items_count=%s,
                        consecutive_failures=0
                    WHERE id=%s
                """, (videos_new, videos_new, sub_id))
        except Exception as e:
            log.error("更新订阅统计失败: %s", e)


# ---------------------------------------------------------------------------
# yt-dlp helpers
# ---------------------------------------------------------------------------

def _find_binary(name: str, extra_paths: List[str] | None = None) -> str | None:
    env_key = f"{name.upper()}_BINARY"
    val = os.getenv(env_key)
    if val and os.path.isfile(val):
        return val
    for p in (extra_paths or []):
        if p and os.path.isfile(p):
            return p
    return shutil.which(name)


def build_ydl_opts(out_template: str) -> dict:
    opts: dict = {
        "format": "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best",
        "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "128"}],
        "outtmpl": out_template,
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 30,
        "retries": 5,
        "fragment_retries": 5,
        "retry_sleep_functions": {"http": lambda n: 2 ** n},
    }
    node = _find_binary("node", ["/usr/local/bin/node", "/usr/bin/node"])
    if node:
        opts["js_runtimes"] = {"node": {"path": node}}
    ffmpeg = _find_binary("ffmpeg", ["/usr/local/bin/ffmpeg", "/usr/bin/ffmpeg"])
    if ffmpeg:
        opts["ffmpeg_location"] = os.path.dirname(ffmpeg)

    proxy = os.getenv("YOUTUBE_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
    if proxy:
        opts["proxy"] = proxy
    cookies = os.getenv("YOUTUBE_COOKIES_FILE")
    if cookies and os.path.isfile(cookies):
        opts["cookiefile"] = cookies
    po = os.getenv("YOUTUBE_PO_TOKEN")
    vd = os.getenv("YOUTUBE_VISITOR_DATA")
    if po and vd:
        opts["extractor_args"] = {"youtube": {"po_token": [f"web+{po}"], "visitor_data": [vd]}}
    return opts
