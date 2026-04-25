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
from datetime import datetime, timezone
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

class CaptionFetchError(Exception):
    """
    包裹 youtube-transcript-api 抛出的原始异常，额外带上:
      - original_class : 原始异常类名（如 IpBlocked / YouTubeRequestFailed / TooManyRequests）
      - via            : 'webshare' 或 'direct'，标注这次调用走的是哪条链路
    这样 tasks 侧归类时能看出 "429 是谁返的"（YouTube 还是 Webshare）。
    """
    def __init__(self, message: str, original_class: str, via: str):
        super().__init__(message)
        self.original_class = original_class
        self.via = via


class YouTubeCaptionService:
    """通过 youtube-transcript-api 获取 YouTube 字幕"""

    # 单次字幕拉取的硬超时（秒）。
    # 默认 60s 考虑了 Webshare Rotating Residential 的典型场景：
    #   - WebshareProxyConfig.retries_when_blocked=10，触发 IP 旋转最多 10 次
    #   - 每次新 IP 要重建 TCP + TLS，再加 YouTube /watch 元数据 + /api/timedtext 两跳
    #   - 单次完整重试成本 ~3–5s × 10 次 ≈ 30–50s
    # 30s 在旧的直连场景够用，切到 Webshare 后偶发视频会在单视频级别被硬超时砍掉
    # （日志里表现为 "transcript-api 硬超时[via=webshare]"），然后掉到 yt-dlp 直连
    # 又撞 429。抬到 60s 可以把这一类偶发视频留在 transcript-api 路径内消化。
    # 若仍频繁出现该类超时，可通过 env 调大到 90s。
    _TIMEOUT = int(os.getenv("YOUTUBE_TRANSCRIPT_TIMEOUT_SECONDS", "60"))

    # Webshare 住宅代理配置缓存。约定:
    #   "__unset__" : 还没读过环境变量
    #   None        : 未启用 / 不可用，走直连
    #   <object>    : 已经构造好的 WebshareProxyConfig 实例
    # 这里**只**用于 youtube-transcript-api，不会被传给 yt-dlp / oss2 / dashscope，
    # 所以 yt-dlp 的音频下载和 OSS 上传不会产生 Webshare 流量计费。
    _PROXY_CONFIG_CACHE: Any = "__unset__"
    _PROXY_LOG_DONE: bool = False
    _PROXY_PROBE_DONE: bool = False

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
    def _get_webshare_proxy_config():
        """
        读取 Webshare 住宅代理配置（仅给 youtube-transcript-api 用）。

        约定通过环境变量启用:
            WEBSHARE_PROXY_USERNAME / WEBSHARE_PROXY_PASSWORD
        可选:
            WEBSHARE_FILTER_IP_LOCATIONS  逗号分隔的 ISO-3166 国家码 (如 "jp,tw,us")
                                          只在该列表的国家里轮换出口 IP，可降低延迟。
                                          留空则使用 Webshare 全球池。

        缺一不可：用户名 / 密码任意一个为空都视为禁用，函数返回 None，
        调用方继续走直连——这样既不会因为忘配密码而误用代理，也保证
        本机 IP 没被 ban 时能继续零成本工作。
        """
        if YouTubeCaptionService._PROXY_CONFIG_CACHE != "__unset__":
            return YouTubeCaptionService._PROXY_CONFIG_CACHE

        user = os.getenv("WEBSHARE_PROXY_USERNAME", "").strip()
        pwd = os.getenv("WEBSHARE_PROXY_PASSWORD", "").strip()
        if not user or not pwd:
            YouTubeCaptionService._PROXY_CONFIG_CACHE = None
            return None

        try:
            from youtube_transcript_api.proxies import WebshareProxyConfig
        except ImportError:
            log.warning(
                "youtube-transcript-api 版本过低，缺失 WebshareProxyConfig 支持；"
                "Webshare 代理被忽略，走直连。请升级到 >= 1.0.0 (pip install -U youtube-transcript-api)"
            )
            YouTubeCaptionService._PROXY_CONFIG_CACHE = None
            return None

        kwargs: Dict[str, Any] = {"proxy_username": user, "proxy_password": pwd}
        locations_raw = os.getenv("WEBSHARE_FILTER_IP_LOCATIONS", "").strip()
        if locations_raw:
            loc_list = [x.strip().lower() for x in locations_raw.split(",") if x.strip()]
            if loc_list:
                kwargs["filter_ip_locations"] = loc_list

        try:
            cfg = WebshareProxyConfig(**kwargs)
        except TypeError:
            # 老版本可能不认 filter_ip_locations，降级成只用账号
            cfg = WebshareProxyConfig(proxy_username=user, proxy_password=pwd)

        YouTubeCaptionService._PROXY_CONFIG_CACHE = cfg
        if not YouTubeCaptionService._PROXY_LOG_DONE:
            locations_applied = kwargs.get("filter_ip_locations") or "global"
            log.info(
                "✓ youtube-transcript-api 已启用 Webshare 住宅代理 (locations=%s)",
                locations_applied,
            )
            if locations_applied == "global":
                log.warning(
                    "  ⚠  Webshare 使用的是全球池，里面包含大量被 YouTube 风控过的住宅 IP "
                    "(巴西/印度/东南亚等)，会有相当比例仍然 429。"
                    "强烈建议在 .env 里设置 WEBSHARE_FILTER_IP_LOCATIONS=jp,tw,us "
                    "（或 jp,tw,sg,kr 等 YouTube 可直连的干净区域）再重启 worker。"
                )
            YouTubeCaptionService._PROXY_LOG_DONE = True

        # 首次加载后跑一次自检，用 Webshare 通道打 ipify 把 exit IP 打印出来，
        # 让运维一眼看出代理有没有真的生效以及拿到了哪个国家/地区的 IP。
        if not YouTubeCaptionService._PROXY_PROBE_DONE:
            YouTubeCaptionService._probe_webshare_exit(cfg)
            YouTubeCaptionService._PROXY_PROBE_DONE = True

        return cfg

    @staticmethod
    def _probe_webshare_exit(cfg: Any) -> None:
        """
        通过 Webshare 代理打一个公共 echo-IP 服务，把出口 IP 打印出来。
        这是用来证明 "代理真的在工作 + 真的是 Rotating Residential" 的最直接手段:
          - exit_ip ≠ 本机公网 IP   → 代理链路 OK
          - exit_ip == 本机公网 IP  → 代理配置传了但没生效（严重 bug）
          - 407/401 认证失败        → 用户名密码错，或买的不是 Rotating Residential
                                      （`-rotate` 后缀只有 Rotating Residential 识别）
          - 连接超时                → Webshare 入口从本机不可达，查防火墙/DNS
          - 连续 N 次都是同一 exit_ip → 你买的是 Static Residential，不是 Rotating

        **复用 WebshareProxyConfig.url 属性**，保证自检走的 URL 跟真实业务请求
        字节级一致（同样带 `-rotate` 后缀、同样的 location codes 编码），不会
        因为格式拼错误报。

        永远不会抛异常，失败只会打 warning，不影响主流程。
        可通过 WEBSHARE_STARTUP_PROBE=false 关闭（不建议）。
        """
        if os.getenv("WEBSHARE_STARTUP_PROBE", "true").strip().lower() \
                not in ("1", "true", "yes", "on"):
            return

        # 直接拿库构造好的 URL；cfg.url 形如:
        #   http://USER[-JP-TW-US]-rotate:PASS@p.webshare.io:80/
        # 这里的 `-rotate` 后缀就是 Rotating Residential 的触发标记，
        # 只有 Rotating Residential 套餐会识别并每次换 IP。
        try:
            proxy_url = cfg.url
        except Exception as e:
            log.warning("⚠ 无法从 WebshareProxyConfig 读取 .url 属性 (库版本过旧?): %s", e)
            return

        # 脱敏打印 URL 格式，让运维一眼看出是不是 Rotating Residential 的典型形态
        # （必须带 `-rotate`，走 p.webshare.io）
        import re as _re
        masked_url = _re.sub(r":[^:@/]+@", ":***@", proxy_url)
        log.info("  Webshare 代理 URL 形态: %s", masked_url)
        if "-rotate" not in masked_url:
            log.warning(
                "  ⚠ 代理 URL 缺少 '-rotate' 后缀，这不是 Rotating Residential 的标准形态！"
                "请升级 youtube-transcript-api >= 1.0.3"
            )
        if "p.webshare.io" not in masked_url:
            log.warning(
                "  ⚠ 代理入口不是 p.webshare.io，这不是 Rotating Residential 的网关。"
                "请确认你在 Webshare Dashboard 买的是 'Rotating Residential'，"
                "不是 'Proxy Server' 或 'Static Residential'。"
            )

        target_urls = [
            "https://api.ipify.org?format=json",
            "https://ifconfig.me/ip",  # 备选，ipify 有时被滥用导致 429
        ]
        timeout_s = int(os.getenv("WEBSHARE_STARTUP_PROBE_TIMEOUT", "10"))

        for url in target_urls:
            t0 = time.time()
            try:
                resp = requests.get(
                    url,
                    proxies={"http": proxy_url, "https": proxy_url},
                    timeout=timeout_s,
                    headers={"User-Agent": "local-virtual-service/webshare-probe"},
                )
                elapsed_ms = int((time.time() - t0) * 1000)
                if resp.status_code == 200:
                    body = resp.text.strip()
                    # 兼容 JSON 和纯文本两种响应
                    exit_ip = body
                    if body.startswith("{"):
                        try:
                            exit_ip = json.loads(body).get("ip", body)
                        except Exception:
                            pass
                    log.info(
                        "✓ Webshare 自检通过: exit_ip=%s 延迟=%dms (via %s)",
                        exit_ip, elapsed_ms, url,
                    )
                    return
                log.warning(
                    "⚠ Webshare 自检返回 HTTP %d: %s  (url=%s, %dms)",
                    resp.status_code, resp.text[:120].replace("\n", " "),
                    url, elapsed_ms,
                )
                if resp.status_code in (401, 407):
                    log.warning(
                        "  → 407/401 = 代理认证失败。请检查 WEBSHARE_PROXY_USERNAME / "
                        "WEBSHARE_PROXY_PASSWORD 是否与 Webshare Dashboard 中一致，"
                        "以及套餐类型是否为 'Rotating Residential'（不能是 Proxy Server 或 Static Residential）"
                    )
                    return  # 认证错不必再试下一个 url
            except requests.exceptions.ProxyError as e:
                log.warning("⚠ Webshare 自检代理层错误 (url=%s): %s", url, str(e)[:200])
            except requests.exceptions.ConnectTimeout:
                log.warning(
                    "⚠ Webshare 自检连接超时: p.webshare.io:80 在 %ds 内建不起 TCP。"
                    "检查本机能否直连 p.webshare.io（家庭网络可能有防火墙/限制）",
                    timeout_s,
                )
                return
            except Exception as e:
                log.warning(
                    "⚠ Webshare 自检失败 (url=%s): %s: %s",
                    url, type(e).__name__, str(e)[:200],
                )

    @staticmethod
    def fetch_transcript(video_url: str, languages: Optional[List[str]] = None) -> Dict[str, Any]:
        from youtube_transcript_api import YouTubeTranscriptApi

        video_id = YouTubeCaptionService.extract_video_id(video_url)
        if not video_id:
            raise ValueError(f"无法从 URL 提取 video_id: {video_url}")

        if languages is None:
            languages = ["zh-Hans", "zh-CN", "zh", "zh-Hant", "zh-TW", "en", "en-US"]

        import concurrent.futures

        proxy_config = YouTubeCaptionService._get_webshare_proxy_config()
        via = "webshare" if proxy_config is not None else "direct"

        def _do_fetch():
            # 仅这一条链路（/api/timedtext）走 Webshare；
            # 其它任何 yt-dlp / OSS / DashScope 调用都不受影响。
            if proxy_config is not None:
                api = YouTubeTranscriptApi(proxy_config=proxy_config)
            else:
                api = YouTubeTranscriptApi()
            try:
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
            except CaptionFetchError:
                raise
            except Exception as e:
                # 把原始异常类名 + via 标注上去，tasks 侧归类时能看出
                # 是 YouTube 直连 429 还是 Webshare 通道 429。
                raise CaptionFetchError(
                    str(e),
                    original_class=type(e).__name__,
                    via=via,
                ) from e

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_do_fetch)
            try:
                return future.result(timeout=YouTubeCaptionService._TIMEOUT)
            except concurrent.futures.TimeoutError as e:
                raise CaptionFetchError(
                    f"transcript-api 调用超过 {YouTubeCaptionService._TIMEOUT}s 硬超时",
                    original_class="TimeoutError",
                    via=via,
                ) from e

    @staticmethod
    def get_full_text(transcript_data: Dict[str, Any]) -> str:
        if not transcript_data or "transcript" not in transcript_data:
            return ""
        return "\n".join(seg["text"] for seg in transcript_data["transcript"])

    # ---------------------------------------------------------------------
    # yt-dlp 字幕拉取（当 youtube-transcript-api 的 /api/timedtext endpoint
    # 被风控时的回退方案。yt-dlp 走 player endpoint，两条路径被 YouTube
    # 独立限流，因此一条挂了另一条通常还能用）
    # ---------------------------------------------------------------------

    @staticmethod
    def fetch_transcript_via_ytdlp(video_url: str,
                                   languages: Optional[List[str]] = None,
                                   timeout: Optional[int] = None) -> Dict[str, Any]:
        """
        使用 yt-dlp **仅下载字幕文件**（不下载视频/音频）作为 fallback。
        返回结构与 fetch_transcript 完全一致，调用方无需改动：
            {video_id, video_url, language, is_generated, transcript: [{text, start, duration}]}
        """
        try:
            import yt_dlp  # noqa: F401
        except ImportError:
            raise ImportError("yt-dlp 未安装")

        video_id = YouTubeCaptionService.extract_video_id(video_url)
        if not video_id:
            raise ValueError(f"无法从 URL 提取 video_id: {video_url}")

        if languages is None:
            languages = ["zh-Hans", "zh-CN", "zh", "zh-Hant", "zh-TW",
                         "en", "en-US", "en-GB", "ja", "ko"]
        if timeout is None:
            timeout = int(os.getenv("CAPTION_YTDLP_TIMEOUT_SECONDS", "60"))

        tmp_dir = tempfile.mkdtemp(prefix="yt_sub_")
        try:
            opts: dict = {
                "skip_download": True,          # 不要视频/音频
                "writesubtitles": True,         # 人工字幕
                "writeautomaticsub": True,      # 自动生成字幕
                "subtitleslangs": languages,
                # vtt 最易解析，json3 有更精细时间戳；留 best 兜底
                "subtitlesformat": "vtt/json3/srv3/best",
                "outtmpl": os.path.join(tmp_dir, "%(id)s.%(ext)s"),
                "quiet": True,
                "no_warnings": True,
                "noprogress": True,
                "logger": _YdlLogger(),
                "socket_timeout": int(os.getenv("YTDLP_SOCKET_TIMEOUT_SECONDS", "30")),
                "retries": int(os.getenv("YTDLP_RETRIES", "5")),
                # 同时覆盖 http / extractor / file_access 的指数退避
                "retry_sleep_functions": {
                    "http": lambda n: 2 ** n,
                    "extractor": lambda n: 2 ** n,
                    "file_access": lambda n: 2 ** n,
                },
                # 反爬延迟: 字幕下载前 / 元信息提取请求间
                "sleep_interval_subtitles": float(
                    os.getenv("CAPTION_YTDLP_SLEEP_SUBTITLES_SECONDS", "0.5")
                ),
                "sleep_interval_requests": float(
                    os.getenv("CAPTION_YTDLP_SLEEP_REQUESTS_SECONDS", "0.5")
                ),
            }
            # 复用与下载音频相同的代理/认证配置
            proxy = os.getenv("YOUTUBE_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
            if proxy:
                opts["proxy"] = proxy
            cookies = os.getenv("YOUTUBE_COOKIES_FILE")
            if cookies and os.path.isfile(cookies):
                opts["cookiefile"] = cookies
            # extractor_args: PO Token (player+subs context) + 跳过翻译字幕
            yt_args = _build_yt_extractor_args(
                contexts=["player", "subs"],
                extra_skip=["translated_subs"],
            )
            if yt_args:
                opts["extractor_args"] = {"youtube": yt_args}
            # TLS 指纹伪装：YouTube 字幕 CDN 对 Python urllib3 指纹会直接 429，
            # 用 curl-cffi 伪装成真浏览器后通常能绕过
            impersonate = _get_impersonate_target()
            if impersonate is not None:
                opts["impersonate"] = impersonate

            import concurrent.futures
            import yt_dlp as _ytdlp

            def _run():
                with _ytdlp.YoutubeDL(opts) as ydl:
                    return ydl.extract_info(video_url, download=True)

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                info = pool.submit(_run).result(timeout=timeout)

            if not info:
                raise Exception("yt-dlp 未返回视频元信息")

            # 找出 tmp_dir 里 yt-dlp 写下的字幕文件。命名格式: {id}.{lang}.{ext}
            sub_files: List[str] = []
            for fn in os.listdir(tmp_dir):
                if fn.startswith(video_id + "."):
                    sub_files.append(os.path.join(tmp_dir, fn))

            if not sub_files:
                manual = set((info.get("subtitles") or {}).keys())
                auto = set((info.get("automatic_captions") or {}).keys())
                if not manual and not auto:
                    raise Exception("该视频无字幕")
                raise Exception(f"无目标语言字幕，仅有: {sorted((manual | auto))[:10]}")

            manual_langs = set((info.get("subtitles") or {}).keys())

            # 按优先语言顺序挑选，找不到就选第一个
            chosen_path: Optional[str] = None
            chosen_lang: Optional[str] = None
            for lang in languages:
                for f in sub_files:
                    parts = os.path.basename(f).split(".")
                    # 形如 VIDEOID.en.vtt -> parts[1] == 'en'
                    if len(parts) >= 3 and parts[1] == lang:
                        chosen_path = f
                        chosen_lang = lang
                        break
                if chosen_path:
                    break
            if chosen_path is None:
                chosen_path = sub_files[0]
                parts = os.path.basename(chosen_path).split(".")
                chosen_lang = parts[1] if len(parts) >= 3 else "unknown"

            is_generated = chosen_lang not in manual_langs
            entries = _parse_subtitle_file(chosen_path)
            if not entries:
                raise Exception(f"字幕解析为空: {os.path.basename(chosen_path)}")

            return {
                "video_id": video_id,
                "video_url": video_url,
                "language": chosen_lang,
                "is_generated": is_generated,
                "transcript": entries,
            }
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# 字幕文件解析器（支持 vtt / json3 / srv3）
# ---------------------------------------------------------------------------

def _parse_subtitle_file(path: str) -> List[Dict[str, Any]]:
    """按扩展名分发到具体解析器。解析失败会抛异常。"""
    ext = path.rsplit(".", 1)[-1].lower()
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()
    if not content.strip():
        return []
    if ext == "json3" or (content.lstrip().startswith("{") and '"events"' in content[:200]):
        return _parse_json3(content)
    if ext == "vtt" or content.lstrip().startswith("WEBVTT"):
        return _parse_vtt(content)
    if ext.startswith("srv") or content.lstrip().startswith("<?xml"):
        return _parse_srv(content)
    # 兜底：按顺序尝试
    for parser in (_parse_vtt, _parse_json3, _parse_srv):
        try:
            out = parser(content)
            if out:
                return out
        except Exception:
            continue
    raise ValueError(f"不支持的字幕格式: {os.path.basename(path)}")


def _parse_vtt(content: str) -> List[Dict[str, Any]]:
    """解析 WebVTT 格式。对内联 <c.style> 风格标签做清理。"""
    ts_re = re.compile(
        r"(\d{2}):(\d{2}):(\d{2})\.(\d{3})\s+-->\s+(\d{2}):(\d{2}):(\d{2})\.(\d{3})"
    )
    tag_re = re.compile(r"<[^>]+>")
    entries: List[Dict[str, Any]] = []
    lines = content.splitlines()
    i, n = 0, len(lines)
    while i < n:
        m = ts_re.search(lines[i])
        if not m:
            i += 1
            continue
        h1, m1, s1, ms1, h2, m2, s2, ms2 = m.groups()
        start = int(h1) * 3600 + int(m1) * 60 + int(s1) + int(ms1) / 1000.0
        end = int(h2) * 3600 + int(m2) * 60 + int(s2) + int(ms2) / 1000.0
        i += 1
        buf: List[str] = []
        while i < n and lines[i].strip():
            buf.append(lines[i].strip())
            i += 1
        text = tag_re.sub("", " ".join(buf)).strip()
        if text:
            entries.append({
                "text": text,
                "start": start,
                "duration": max(0.0, end - start),
            })
        i += 1
    # YouTube 自动字幕的 VTT 经常有重复的"滚动"行，做轻量去重
    dedup: List[Dict[str, Any]] = []
    last_text = None
    for e in entries:
        if e["text"] != last_text:
            dedup.append(e)
            last_text = e["text"]
    return dedup


def _parse_json3(content: str) -> List[Dict[str, Any]]:
    """解析 YouTube json3 字幕格式。"""
    data = json.loads(content)
    entries: List[Dict[str, Any]] = []
    for e in data.get("events") or []:
        segs = e.get("segs") or []
        text = "".join((s.get("utf8") or "") for s in segs).strip()
        if not text:
            continue
        start_ms = int(e.get("tStartMs") or 0)
        duration_ms = int(e.get("dDurationMs") or 0)
        entries.append({
            "text": text,
            "start": start_ms / 1000.0,
            "duration": duration_ms / 1000.0,
        })
    # 同样做相邻重复去重
    dedup: List[Dict[str, Any]] = []
    last_text = None
    for e in entries:
        if e["text"] != last_text:
            dedup.append(e)
            last_text = e["text"]
    return dedup


def _parse_srv(content: str) -> List[Dict[str, Any]]:
    """解析 srv1/srv2/srv3 格式（YouTube 专有 XML）。"""
    entries: List[Dict[str, Any]] = []
    # srv3: <p t="12345" d="2500">text</p>
    for m in re.finditer(r'<p\s+[^>]*t="(\d+)"[^>]*d="(\d+)"[^>]*>([^<]*)</p>', content):
        start_ms, dur_ms, text = m.groups()
        text = text.strip()
        if text:
            entries.append({
                "text": text,
                "start": int(start_ms) / 1000.0,
                "duration": int(dur_ms) / 1000.0,
            })
    # srv1 (transcript v1): <text start="12.34" dur="2.5">text</text>
    if not entries:
        for m in re.finditer(
            r'<text\s+[^>]*start="([\d.]+)"[^>]*dur="([\d.]+)"[^>]*>([^<]*)</text>',
            content,
        ):
            s, d, text = m.groups()
            text = text.strip()
            if text:
                entries.append({
                    "text": text,
                    "start": float(s),
                    "duration": float(d),
                })
    return entries


# ---------------------------------------------------------------------------
# YouTubeFeedService  — RSS 获取 + Data API v3 fallback
# ---------------------------------------------------------------------------

def _get_proxies() -> Optional[Dict[str, str]]:
    proxy = os.getenv("YOUTUBE_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
    if proxy:
        return {"http": proxy, "https": proxy}
    return None


def _get_youtube_api_keys() -> List[str]:
    """返回所有配置的 YouTube Data API v3 Key（主 Key 优先，BACKUP 兜底）。"""
    keys = []
    for env_var in ("YOUTUBE_DATA_API_KEY", "YOUTUBE_DATA_API_KEY_BACKUP"):
        k = os.getenv(env_var, "").strip()
        if k:
            keys.append(k)
    return keys


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

    # 单次 RSS / API 请求超时（秒）
    REQUEST_TIMEOUT = int(os.getenv("YOUTUBE_FEED_TIMEOUT_SECONDS", "5"))
    RSS_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    RSS_USER = "https://www.youtube.com/feeds/videos.xml?user={username}"
    API_BASE = "https://www.googleapis.com/youtube/v3"
    # RSS 5xx 瞬时错误重试次数（超过后 fallback 到 Data API v3）。
    # 注意: 404 不在重试范围内（YouTube RSS 对某些频道/IP 直接 404，重试无效）。
    # 默认 2 = 共 2 次尝试 / 1 次重试。
    MAX_RETRIES = int(os.getenv("YOUTUBE_FEED_MAX_RETRIES", "2"))

    # ── RSS ─────────────────────────────────────────────────────

    @classmethod
    def fetch_channel_videos(cls, channel_identifier: str, max_items: int = 15) -> tuple:
        """获取频道视频列表，返回 (videos, source)。
        source 为 'rss' | 'api_v3' | '' (空串表示两路均失败)。
        """
        ident = channel_identifier.strip().lstrip("@")
        is_cid = len(ident) >= 20 and ident.startswith("UC")

        videos = cls._fetch_via_rss(ident, max_items, is_cid)
        if videos:
            return videos, "rss"

        if not is_cid:
            return [], ""

        api_keys = _get_youtube_api_keys()
        if not api_keys:
            log.warning("RSS 未获取到视频且未配置 YOUTUBE_DATA_API_KEY (channel=%s)", ident)
            return [], ""

        log.info("RSS 失败, 尝试 Data API v3 fallback (channel=%s)", ident)
        for i, api_key in enumerate(api_keys):
            # _fetch_via_api 返回 None 表示该 Key 配额已耗尽（HTTP 403），
            # 返回 [] 表示 API 正常但频道无视频或请求失败。
            api_videos = cls._fetch_via_api(ident, max_items, api_key)
            if api_videos is not None:
                return api_videos, "api_v3" if api_videos else ""
            # 当前 Key 配额耗尽，若还有备用 Key 则切换
            if i < len(api_keys) - 1:
                log.warning(
                    "Data API v3 Key #%d 配额已用尽，切换备用 Key (channel=%s)",
                    i + 1, ident,
                )

        log.error("所有 Data API v3 Key 配额均已用尽 (channel=%s)", ident)
        return [], ""

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
                # 404 不重试——YouTube RSS 对某些频道/IP 固定返回 404，重试无效；
                # 5xx 属于瞬时错误，在剩余次数内可重试。
                if resp.status_code in (500, 502, 503, 504) and attempt < cls.MAX_RETRIES - 1:
                    log.warning("RSS HTTP %d, 重试 %d/%d: %s", resp.status_code, attempt + 1, cls.MAX_RETRIES, rss_url)
                    time.sleep(1.0 * (attempt + 1))
                    continue
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt < cls.MAX_RETRIES - 1:
                    retryable = False
                    if isinstance(e, requests.HTTPError) and e.response is not None:
                        retryable = e.response.status_code in (500, 502, 503, 504)
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
    def _fetch_via_api(cls, channel_id: str, max_items: int, api_key: str) -> Optional[List[Dict[str, Any]]]:
        """调用 YouTube Data API v3 获取频道视频。
        返回值语义:
          None      — HTTP 403，该 Key 配额已耗尽，调用方应切换到备用 Key
          []        — API 正常响应但无结果，或其他非配额类错误
          [...]     — 成功
        """
        proxies = _get_proxies()
        headers = {"Accept": "application/json"}
        timeout = 10
        try:
            r = requests.get(f"{cls.API_BASE}/channels",
                             params={"part": "contentDetails", "id": channel_id, "key": api_key},
                             headers=headers, timeout=timeout, proxies=proxies)
            if r.status_code == 403:
                log.warning(
                    "YouTube Data API 配额已用尽 (key=...%s channel=%s)",
                    api_key[-6:], channel_id,
                )
                return None
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
        ttl = int(os.getenv("OSS_SIGNED_URL_TTL_SECONDS", str(3600 * 48)))
        signed_url = self.bucket.sign_url("GET", obj_name, ttl)
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
                   diarization: bool = True, timeout_min: Optional[float] = None) -> Dict[str, Any]:
        """一键转录: 提交 → 轮询 → 下载结果

        timeout_min 若为 None，则读取环境变量 ASR_POLL_TIMEOUT_MINUTES（默认 5）。
        长视频（>2h）建议调到 30 以上，否则 Fun-ASR 排队+处理可能撑不过默认 5min。
        """
        if timeout_min is None:
            timeout_min = float(os.getenv("ASR_POLL_TIMEOUT_MINUTES", "5"))
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
            # 时区契约: 缺失 tz 的 ISO 字符串视为 UTC (YouTube API 文档保证 publishedAt 为 UTC).
            # 与家用 Worker db.py 适配器约定 (naive=HK) 不同, 此处显式贴 UTC tz 防误判.
            if published_dt.tzinfo is None:
                published_dt = published_dt.replace(tzinfo=timezone.utc)
        else:
            # 时区契约: fallback 必须为 UTC-aware, 切勿使用 datetime.now() (HK 本地裸时间).
            published_dt = datetime.now(timezone.utc)

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

        # ── Feeds 自动打标: 通知远程主站"字幕已落库, 可以触发 auto-feed-tagging" ──
        # 与 signal_upload_file_ready 的跨集群回调模式一致.
        # 远程 backend/tasks/subscription_tasks.py::finalize_youtube_feed_task 消费.
        # 失败静默 — 打标是增值能力, 不应阻断字幕入库主流程.
        self._signal_feed_transcript_ready(content_db_id)

    def _signal_feed_transcript_ready(self, content_db_id: int) -> None:
        """
        通知远程 finalize_youtube_feed_task: YouTube Feed 字幕已写入.

        主 repo 对标任务: backend/tasks/subscription_tasks.py::finalize_youtube_feed_task
        设计文档:        docs/instructions/FEEDS_AUTO_TAGGING_PLAN.md §3.3 §6.5
        """
        try:
            from .celery_app import app
            app.send_task(
                "finalize_youtube_feed_task",
                args=(content_db_id,),
                queue="file_processing",
            )
            log.info(
                "signal_feed_transcript_ready → finalize_youtube_feed_task: content=%d",
                content_db_id,
            )
        except Exception as e:
            log.warning(
                "signal_feed_transcript_ready 派发失败 (不阻断主流程): content=%d error=%s",
                content_db_id, e,
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

    # ------------------------------------------------------------------
    # Upload YouTube Link 专用：字幕抓取结果写回 file_processing_details
    # ------------------------------------------------------------------

    def get_file_status(self, file_id: int) -> Optional[str]:
        """读取 file_processing_details 的当前 status，用于幂等保护。"""
        try:
            with self.db.connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT status FROM file_processing_details WHERE id=%s",
                    (file_id,),
                )
                row = cur.fetchone()
                return row[0] if row else None
        except Exception as e:
            log.warning("get_file_status 失败: file_id=%s error=%s", file_id, e)
            return None

    def update_file_transcript(self, file_id: int, content: str,
                               transcript_data: Dict[str, Any],
                               status: str = "completed") -> None:
        """
        Upload YouTube Link 走字幕路径时写回:
          content                = 字幕全文（纯文本）
          asr_with_diarization   = 字幕结构化 JSON（复用此字段承载 caption JSON，
                                   与远程旧实现一致，前端 OriginalPanel 已有兼容）
        """
        with self.db.connection() as conn:
            conn.cursor().execute("""
                UPDATE file_processing_details
                SET content=%s, asr_with_diarization=%s,
                    content_length=%s, status=%s
                WHERE id=%s
            """, (
                content,
                Json(transcript_data) if transcript_data is not None else None,
                len(content or ""), status, file_id,
            ))

    def signal_upload_file_ready(self, file_id: int, username: str,
                                 outcome: str, **meta) -> None:
        """
        通知远程 `finalize_youtube_file_task`: Upload Link 的字幕/ASR 已写入终态。

        outcome: 'completed' | 'failed'
        meta:    任意附加信息（source / length / reason / elapsed_seconds ...）

        注意: 本地 Worker 不依赖远程代码包，通过 Celery 任务名跨集群派发。
        """
        try:
            from .celery_app import app
            app.send_task(
                "finalize_youtube_file_task",
                args=(file_id, username, outcome, meta or {}),
                queue="file_processing",
            )
            log.info("signal_upload_file_ready → finalize: file_id=%d outcome=%s",
                     file_id, outcome)
        except Exception as e:
            # 失败不影响当前任务: file_processing_details.content 已写入,
            # 用户仍可通过 /api/file-tags/retag 等接口手动补偿 auto-* 行为.
            log.error("signal_upload_file_ready 派发失败: file_id=%d outcome=%s error=%s",
                      file_id, outcome, e)

    def increment_subscription_failures(self, sub_id: int) -> None:
        """订阅抓取失败时累加 consecutive_failures (与远程旧实现对齐)。"""
        try:
            with self.db.connection() as conn:
                conn.cursor().execute(
                    "UPDATE subscription_list "
                    "SET consecutive_failures=consecutive_failures+1 "
                    "WHERE id=%s",
                    (sub_id,),
                )
        except Exception as e:
            log.warning("increment_subscription_failures 失败: sub_id=%s error=%s",
                        sub_id, e)

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


_impersonate_warned_keys: set = set()  # 用 key 区分不同告警，每个只输出一次
_impersonate_probe_done = False
_impersonate_available_targets: Optional[list] = None


def _warn_impersonate_once(key: str, msg: str) -> None:
    """同一 key 的 warning 只打印一次，避免刷屏。"""
    if key not in _impersonate_warned_keys:
        log.warning(msg)
        _impersonate_warned_keys.add(key)


def _probe_available_impersonate_targets() -> list:
    """
    探测当前 yt-dlp + curl-cffi 组合下实际支持的 impersonate target 列表，
    结果缓存在 worker 进程生命周期内。探测失败或没有任何可用后端时返回空列表。

    yt-dlp 的 `_get_available_impersonate_targets()` 返回的是
    `[(ImpersonateTarget, handler_name), ...]` 元组列表，这里统一剥出
    `ImpersonateTarget`，调用方不用关心 handler。
    """
    global _impersonate_probe_done, _impersonate_available_targets
    if _impersonate_probe_done:
        return _impersonate_available_targets or []
    _impersonate_probe_done = True
    try:
        import yt_dlp
        probe_opts = {"quiet": True, "no_warnings": True, "logger": _YdlLogger()}
        with yt_dlp.YoutubeDL(probe_opts) as ydl:
            if not hasattr(ydl, "_get_available_impersonate_targets"):
                _impersonate_available_targets = []
                return []
            raw = list(ydl._get_available_impersonate_targets())
            # 归一化: 老版本 yt-dlp 可能直接返回 ImpersonateTarget 列表，
            # 新版本返回 (ImpersonateTarget, handler_name) 元组列表
            _impersonate_available_targets = [
                item[0] if isinstance(item, tuple) else item
                for item in raw
            ]
    except Exception as e:
        log.debug("probe impersonate targets 失败: %s", e)
        _impersonate_available_targets = []
    return _impersonate_available_targets or []


def _get_impersonate_target():
    """
    解析 YTDLP_IMPERSONATE env 得到 yt-dlp 的 impersonate 配置对象。

    解析链 (逐层降级，保证不会让 yt-dlp 挂掉):
      1. env 未配置 / 显式禁用        → 返回 None (不启用 impersonation)
      2. curl-cffi 未装               → 返回 None + 一次性 warning
      3. yt-dlp 没有注册 impersonate  → 返回 None + 一次性 warning
      4. env 指定的具体 target 不在   → 降级为 ImpersonateTarget() (auto)
         当前 curl-cffi 后端支持列表
      5. 指定 target 可用             → 返回该 target

    env 取值:
      - '' / 'false' / 'off' / '0' / 'no' → 禁用
      - 'true' / 'any' / 'auto'           → 自动挑一个可用 target (推荐)
      - 'chrome' / 'safari' / 'edge' / 'firefox' / 'chrome:windows' 等 → 指定
    """
    target_str = os.getenv("YTDLP_IMPERSONATE", "").strip()
    if not target_str or target_str.lower() in ("false", "0", "no", "off", "disable", "disabled"):
        return None

    # 1) curl-cffi 可用性
    try:
        import curl_cffi  # noqa: F401
    except ImportError:
        _warn_impersonate_once(
            "no_curl_cffi",
            f"YTDLP_IMPERSONATE={target_str} 已配置但 curl-cffi 未安装，"
            f"impersonation 将跳过。安装命令: pip install 'curl-cffi>=0.7.0'",
        )
        return None

    # 2) yt-dlp impersonate API 可用性
    try:
        from yt_dlp.networking.impersonate import ImpersonateTarget
    except ImportError:
        _warn_impersonate_once(
            "no_ytdlp_api",
            "当前 yt-dlp 版本不支持 impersonate，impersonation 将跳过",
        )
        return None

    # 3) 探测 yt-dlp 能用的 target 列表（检查是否真有后端注册）
    available = _probe_available_impersonate_targets()
    if not available:
        # 尝试从 yt-dlp bridge 拿到具体原因（最常见是 curl-cffi 版本超出支持范围）
        hint = ""
        try:
            from yt_dlp.networking import _curlcffi  # noqa: F401
        except ImportError as bridge_err:
            hint = f" 原因: {bridge_err}"
        try:
            import curl_cffi as _cc
            ver = getattr(_cc, "__version__", "unknown")
            hint += f" (当前 curl-cffi={ver})"
        except Exception:
            pass
        _warn_impersonate_once(
            "no_backend",
            f"yt-dlp 未注册任何 impersonate 后端，impersonation 将跳过。{hint} "
            f"修复命令: pip install 'curl-cffi>=0.10,<0.15'",
        )
        return None

    # 4) auto / any → 空 ImpersonateTarget 让 yt-dlp 自己挑
    low = target_str.lower()
    if low in ("true", "any", "auto"):
        return ImpersonateTarget()

    # 5) 解析用户指定的 target
    try:
        user_target = ImpersonateTarget.from_str(target_str)
    except Exception as e:
        _warn_impersonate_once(
            f"parse_fail:{target_str}",
            f"解析 YTDLP_IMPERSONATE={target_str} 失败 ({e})，回退为 auto",
        )
        return ImpersonateTarget()

    # 6) 校验 user_target 在 available 里有能满足它的 backend
    #    (avail in user_target) 表示 avail 是 user_target 的一个具体化
    if any(avail in user_target for avail in available):
        return user_target

    # 7) 指定 target 在当前 curl-cffi 中不可用 → 降级为 auto
    _warn_impersonate_once(
        f"target_unavail:{target_str}",
        f"YTDLP_IMPERSONATE={target_str} 在当前 curl-cffi 中不可用。"
        f"可用 target 示例: {sorted({str(a) for a in available})[:5]}；"
        f"已自动回退为 auto（yt-dlp 会挑一个支持的）",
    )
    return ImpersonateTarget()


def _po_tokens_for_context(po: str, contexts: List[str]) -> List[str]:
    """
    把 YOUTUBE_PO_TOKEN env 展开成 yt-dlp 新格式 (CLIENT.CONTEXT+TOKEN) 的列表。

    支持两种输入:
      a) **老格式**(兼容): 单个裸 token 字符串 "MnRh..."
         → 自动扩展到每个 context: ['web.gvs+MnRh...', 'web.subs+MnRh...']
      b) **新格式**: 已经是完整 token, 可以逗号分隔
         如 "web.gvs+XXX,web.subs+YYY,web_safari.gvs+ZZZ"
         → 按逗号拆分原样返回, 忽略 contexts 参数

    参考: https://github.com/yt-dlp/yt-dlp (extractor_args -> youtube.po_token)
    """
    po = (po or "").strip()
    if not po:
        return []
    # 检测是否已经是新格式: 第一个 token 在 '+' 左边含 '.' (CLIENT.CONTEXT)
    first_token = po.split(",")[0].strip()
    if "+" in first_token and "." in first_token.split("+", 1)[0]:
        return [t.strip() for t in po.split(",") if t.strip()]
    # 老格式: 单个裸 token, 自动扩展到各 context
    return [f"web.{ctx}+{po}" for ctx in contexts]


def _build_yt_extractor_args(contexts: List[str],
                             extra_skip: Optional[List[str]] = None) -> dict:
    """
    根据环境变量生成 yt-dlp 的 extractor_args.youtube 字典。
    统一三处用法 (音频下载 / 字幕拉取 / 元信息探测) 的 PO Token 处理逻辑。

    Args:
        contexts: PO Token 需要注入的 context 列表 (如 ['gvs']/['player','subs'])
        extra_skip: extractor_args.skip 额外要跳过的东西 (如 ['translated_subs'])
    """
    yt: dict = {}
    if extra_skip:
        yt["skip"] = extra_skip
    po = os.getenv("YOUTUBE_PO_TOKEN")
    tokens = _po_tokens_for_context(po, contexts) if po else []
    if tokens:
        yt["po_token"] = tokens
    vd = os.getenv("YOUTUBE_VISITOR_DATA")
    if vd:
        yt["visitor_data"] = [vd]
    return yt


class _YdlLogger:
    """Redirect yt-dlp output to Python logging instead of stderr."""

    def __init__(self) -> None:
        self._log = logging.getLogger("worker.yt_dlp")

    def debug(self, msg: str) -> None:
        if msg.startswith("[download]"):
            return
        self._log.debug(msg)

    def info(self, msg: str) -> None:
        if msg.startswith("[download]"):
            return
        self._log.debug(msg)

    def warning(self, msg: str) -> None:
        if "No supported JavaScript runtime" in msg:
            return
        self._log.warning(msg)

    def error(self, msg: str) -> None:
        self._log.debug(msg)


def probe_youtube_metadata(video_url: str, timeout: int = 10) -> Dict[str, Any]:
    """
    用 yt-dlp 仅获取视频元信息（不下载），返回探测到的元信息字典。

    返回结构:
        {
            'ok':          True/False   — 探测是否成功
            'duration':    int|None     — 视频时长秒数，直播或未知为 None
            'is_live':     bool         — 当前是否在直播
            'live_status': str          — yt-dlp 原生字段:
                                          'not_live' | 'is_live' | 'is_upcoming'
                                          | 'was_live' | 'post_live' | None
            'title':       str|None
            'error':       str|None     — 探测失败时的原因
        }
    """
    empty = {
        "ok": False, "duration": None, "is_live": False,
        "live_status": None, "title": None, "error": None,
    }
    try:
        import yt_dlp  # noqa: F401
    except ImportError:
        empty["error"] = "yt-dlp 未安装"
        return empty

    opts: dict = {
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "logger": _YdlLogger(),
        "skip_download": True,
        "socket_timeout": timeout,
        "retries": 1,
    }
    proxy = os.getenv("YOUTUBE_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
    if proxy:
        opts["proxy"] = proxy
    cookies = os.getenv("YOUTUBE_COOKIES_FILE")
    if cookies and os.path.isfile(cookies):
        opts["cookiefile"] = cookies
    # 元信息探测走 player context
    yt_args = _build_yt_extractor_args(contexts=["player"])
    if yt_args:
        opts["extractor_args"] = {"youtube": yt_args}
    impersonate = _get_impersonate_target()
    if impersonate is not None:
        opts["impersonate"] = impersonate

    try:
        import yt_dlp
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
        if not isinstance(info, dict):
            empty["error"] = "yt-dlp 未返回 info dict"
            return empty
        duration_raw = info.get("duration")
        return {
            "ok": True,
            "duration": int(duration_raw) if duration_raw else None,
            "is_live": bool(info.get("is_live")),
            "live_status": info.get("live_status"),
            "title": info.get("title"),
            "error": None,
        }
    except Exception as e:
        log.debug("probe_youtube_metadata 失败 url=%s: %s", video_url, e)
        empty["error"] = str(e).split("\n")[0][:120]
        return empty


def probe_youtube_duration(video_url: str, timeout: int = 10) -> Optional[int]:
    """向后兼容: 仅返回视频时长 (秒)。直播 / 未知 / 探测失败均返回 None。"""
    return probe_youtube_metadata(video_url, timeout).get("duration")


def _reject_live_filter(info_dict, *, incomplete: bool = False) -> Optional[str]:
    """
    yt-dlp 的 match_filter: 直播 / 首播未开始 / 流式视频 返回一个非 None 字符串
    就会被 yt-dlp 直接跳过下载（不会无限等流）。这是防 worker 卡死的兜底保险。
    """
    live_status = info_dict.get("live_status")
    if info_dict.get("is_live") or live_status == "is_live":
        return "跳过直播视频 (yt-dlp 会无限等流)"
    if live_status == "is_upcoming":
        return "跳过未开始的首播 / 直播"
    if info_dict.get("live_status") == "post_live":
        # post_live: 直播刚结束但回放还没生成完，下载会卡
        return "跳过 post_live (回放未完成)"
    return None


def build_ydl_opts(out_template: str) -> dict:
    opts: dict = {
        "format": "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best",
        "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "128"}],
        "outtmpl": out_template,
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "logger": _YdlLogger(),
        "socket_timeout": int(os.getenv("YTDLP_SOCKET_TIMEOUT_SECONDS", "30")),
        "retries": int(os.getenv("YTDLP_RETRIES", "5")),
        "fragment_retries": int(os.getenv("YTDLP_FRAGMENT_RETRIES", "5")),
        # http / extractor 两级退避: extractor 专门覆盖 YouTube 元信息提取阶段的限流
        "retry_sleep_functions": {
            "http": lambda n: 2 ** n,
            "extractor": lambda n: 2 ** n,
        },
        # 直播防护: 直播/首播未开始/post_live 都直接跳过, 避免 yt-dlp 无限等流
        "match_filter": _reject_live_filter,
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
    # 音频下载走 gvs(Google Video Server) context
    yt_args = _build_yt_extractor_args(contexts=["gvs"])
    if yt_args:
        opts["extractor_args"] = {"youtube": yt_args}
    impersonate = _get_impersonate_target()
    if impersonate is not None:
        opts["impersonate"] = impersonate
    return opts
