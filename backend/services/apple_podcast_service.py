# -*- coding: utf-8 -*-
"""
Apple Podcasts 单集音频下载服务

策略（优先级降序）：
  1. iTunes Lookup API  —— 最可靠，直接返回 episodeUrl（MP3 CDN 链接），无需 yt-dlp
  2. yt-dlp fallback    —— iTunes API 查不到该集时兜底

Apple Podcasts 走的是 iTunes 公开 API + 第三方 Podcast CDN，
无需家庭 IP，可在远程服务器直接运行。
"""

import os
import re
import time
from typing import Any, Callable, Dict, Optional

from backend.utils.logger import error, info, warning


class PermanentDownloadError(Exception):
    """不可重试的永久性错误（付费订阅、地区锁、404 等）"""


class TransientDownloadError(Exception):
    """可重试的瞬时错误（网络超时、CDN 5xx 等）"""


# Apple Podcasts 单集 URL 模式：
#   https://podcasts.apple.com/{locale}/podcast/{title}/id{podcast_id}?i={episode_id}
_APPLE_PODCAST_URL_RE = re.compile(
    r'^https?://podcasts\.apple\.com/[^/]+/podcast/.+/id(\d+)(?:/\S*)?\?.*\bi=(\d+)',
    re.IGNORECASE,
)

# 宽松兜底：只要含 podcasts.apple.com 并有 id<digits> 和 ?i=<digits>
_PODCAST_ID_RE = re.compile(r'/id(\d+)', re.IGNORECASE)
_EPISODE_ID_RE = re.compile(r'[?&]i=(\d+)', re.IGNORECASE)

_DEFAULT_DOWNLOAD_TIMEOUT = int(os.getenv("APPLE_PODCAST_DOWNLOAD_TIMEOUT", "1800"))
_HTTP_TIMEOUT = int(os.getenv("APPLE_PODCAST_HTTP_TIMEOUT", "30"))

# 从 Apple Podcasts URL 中提取两字母 locale（如 /hk/、/us/、/gb/）
_LOCALE_RE = re.compile(r'podcasts\.apple\.com/([a-z]{2})/', re.IGNORECASE)


class ApplePodcastService:
    """Apple Podcasts 音频下载服务"""

    # ─────────────────────────────────────────────────────────────────────
    # URL 解析
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def extract_ids(url: str) -> Optional[tuple[str, str]]:
        """
        从 URL 同时提取 (podcast_id, episode_id)。
        两者都必须存在，否则返回 None。
        """
        if not url:
            return None
        url = url.strip()
        m = _APPLE_PODCAST_URL_RE.match(url)
        if m:
            return m.group(1), m.group(2)
        # 宽松兜底
        pm = _PODCAST_ID_RE.search(url)
        em = _EPISODE_ID_RE.search(url)
        if pm and em:
            return pm.group(1), em.group(1)
        return None

    @staticmethod
    def extract_episode_id(url: str) -> Optional[str]:
        """
        从 Apple Podcasts URL 中提取 episode_id（?i=<digits>）。

        Returns:
            episode_id 字符串或 None（URL 格式不合法）
        """
        result = ApplePodcastService.extract_ids(url)
        return result[1] if result else None

    # ─────────────────────────────────────────────────────────────────────
    # iTunes Lookup API
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _itunes_lookup(
        podcast_id: str,
        episode_id: str,
        country: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        通过 iTunes Lookup API 查询单集信息，返回包含 episodeUrl 的字典。

        country 参数用于指定地区（如 'hk'、'us'、'gb'），iTunes 部分播客
        只在特定地区目录中存在。查询策略：
          1. 若提供 country，先以该 country 查询
          2. 若未找到且 country != 'us'，再以 us 补查一次
          3. 若仍未找到，以无 country 参数再查一次（iTunes 默认 US 目录）

        Returns:
            dict with keys: episode_id, title, series, episode, duration,
                            thumbnail, audio_url, ext
            Or None if the episode is not found in the API response.
        """
        # 构造要尝试的 country 列表（去重、保序）
        countries_to_try: list[Optional[str]] = []
        if country:
            countries_to_try.append(country.lower())
        if 'us' not in countries_to_try:
            countries_to_try.append('us')
        countries_to_try.append(None)  # 无 country 参数兜底

        def _build_url(c: Optional[str]) -> str:
            base = f"https://itunes.apple.com/lookup?id={podcast_id}&entity=podcastEpisode&limit=300"
            return base + (f"&country={c}" if c else "")

        podcast_has_episodes = False  # 该播客在 iTunes 里是否存在其他公开集

        try:
            import httpx
            with httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
                for c in countries_to_try:
                    try:
                        resp = client.get(_build_url(c))
                        resp.raise_for_status()
                        data = resp.json()
                        if ApplePodcastService._podcast_has_any_episodes(data):
                            podcast_has_episodes = True
                        result = ApplePodcastService._parse_itunes_results(data, episode_id)
                        if result:
                            return result
                    except Exception as e:
                        warning(f"iTunes Lookup (country={c}) 失败: {e}", source="apple_podcast")
        except ImportError:
            # httpx 不可用，退回 urllib
            try:
                import urllib.request as urllib_request
                import json as _json
                for c in countries_to_try:
                    try:
                        with urllib_request.urlopen(_build_url(c), timeout=_HTTP_TIMEOUT) as resp:
                            data = _json.loads(resp.read().decode())
                            if ApplePodcastService._podcast_has_any_episodes(data):
                                podcast_has_episodes = True
                            result = ApplePodcastService._parse_itunes_results(data, episode_id)
                            if result:
                                return result
                    except Exception as e:
                        warning(f"iTunes Lookup urllib (country={c}) 失败: {e}", source="apple_podcast")
            except Exception as e:
                warning(f"iTunes Lookup (urllib fallback) 完全失败: {e}", source="apple_podcast")
                return None

        # 所有地区都查过了，仍未找到该集
        if podcast_has_episodes:
            # 播客存在且有公开集，但此集不在目录中 → 付费/订阅专属内容
            raise PermanentDownloadError(
                f"该集为付费或订阅专属内容，Apple Podcasts 不对外暴露音频链接 (episode_id={episode_id})"
            )
        return None

    @staticmethod
    def _podcast_has_any_episodes(data: Dict[str, Any]) -> bool:
        """判断 iTunes Lookup 响应中是否存在任意公开的播客集（用于区分"付费集"和"播客不存在"）。"""
        return any(
            item.get("wrapperType") == "podcastEpisode"
            for item in data.get("results", [])
        )

    @staticmethod
    def _parse_itunes_results(data: Dict[str, Any], episode_id: str) -> Optional[Dict[str, Any]]:
        """从 iTunes Lookup 响应中找到目标 episode_id 并构建元数据字典。"""
        results = data.get("results", [])
        target_id = int(episode_id)
        for item in results:
            if item.get("wrapperType") == "podcastEpisode" and item.get("trackId") == target_id:
                audio_url = item.get("episodeUrl") or item.get("episodeFileExtension")
                if not audio_url:
                    continue
                duration_ms = item.get("trackTimeMillis")
                duration_s = (duration_ms / 1000.0) if duration_ms else None
                collection_name = item.get("collectionName") or item.get("artistName") or ""
                return {
                    "episode_id": episode_id,
                    "title": item.get("trackName") or f"podcast_{episode_id}",
                    "series": collection_name,
                    "episode": item.get("trackName") or "",
                    "episode_number": item.get("episodeNumber"),
                    "duration": duration_s,
                    "thumbnail": item.get("artworkUrl600") or item.get("artworkUrl160"),
                    "audio_url": audio_url,
                    "ext": "mp3",
                    "webpage_url": item.get("trackViewUrl") or "",
                }
        return None

    # ─────────────────────────────────────────────────────────────────────
    # 公共 API
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def fetch_metadata(url: str) -> Dict[str, Any]:
        """
        获取 Apple Podcasts 单集元数据，不下载音频。

        策略：先走 iTunes Lookup API，失败时走 yt-dlp（skip_download=True）。

        Returns:
            {episode_id, title, series, episode, episode_number,
             duration, thumbnail, webpage_url, ext, audio_url(可选)}

        Raises:
            PermanentDownloadError / TransientDownloadError / ImportError
        """
        ids = ApplePodcastService.extract_ids(url)
        if not ids:
            raise PermanentDownloadError(f"无效的 Apple Podcasts URL: {url}")
        podcast_id, episode_id = ids

        # 从 URL 提取地区代码（如 /hk/、/us/），用于 iTunes 地区目录查询
        locale_match = _LOCALE_RE.search(url)
        country = locale_match.group(1).lower() if locale_match else None

        # ── 1. iTunes Lookup API（首选）
        meta = ApplePodcastService._itunes_lookup(podcast_id, episode_id, country=country)
        if meta:
            info(
                f"iTunes Lookup API 获取元数据成功: episode_id={episode_id} title={meta['title']}",
                source="apple_podcast",
            )
            return meta

        # ── 2. yt-dlp fallback
        warning(
            f"iTunes Lookup 未找到 episode_id={episode_id}，尝试 yt-dlp 元数据提取",
            source="apple_podcast",
        )
        return ApplePodcastService._ytdlp_metadata(url, episode_id)

    @staticmethod
    def _ytdlp_metadata(url: str, episode_id: str) -> Dict[str, Any]:
        """使用 yt-dlp skip_download 提取元数据（备用路径）。"""
        try:
            import yt_dlp
        except ImportError:
            raise ImportError("yt_dlp 未安装: pip install -U yt-dlp")

        opts: Dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "socket_timeout": _HTTP_TIMEOUT,
        }
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                raw = ydl.extract_info(url, download=False)
        except yt_dlp.utils.DownloadError as exc:
            msg = str(exc)
            if any(kw in msg.lower() for kw in (
                'not found', '404', 'unavailable', 'private', 'premium',
                'subscription', 'geo', 'region', 'blocked',
            )):
                raise PermanentDownloadError(f"内容不可访问: {msg}")
            raise TransientDownloadError(f"yt-dlp 元数据提取失败: {msg}")
        except Exception as exc:
            raise TransientDownloadError(f"yt-dlp 元数据提取异常: {exc}")

        if not isinstance(raw, dict):
            raise PermanentDownloadError("yt-dlp 未返回有效元数据")

        return {
            "episode_id": episode_id,
            "title": raw.get("title") or f"podcast_{episode_id}",
            "series": raw.get("series") or raw.get("playlist_title") or "",
            "episode": raw.get("episode") or raw.get("title") or "",
            "episode_number": raw.get("episode_number"),
            "duration": raw.get("duration"),
            "thumbnail": raw.get("thumbnail"),
            "webpage_url": raw.get("webpage_url") or url,
            "ext": raw.get("ext") or "mp3",
        }

    @staticmethod
    def download_audio(
        url: str,
        dest_dir: str,
        progress_hook: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """
        下载 Apple Podcasts 单集音频到 dest_dir。

        策略：先走 iTunes Lookup API 拿到 episodeUrl 直接用 httpx/urllib 下载，
        失败时回退到 yt-dlp。

        Returns:
            {file_path, filename, metadata}

        Raises:
            PermanentDownloadError / TransientDownloadError / ImportError
        """
        ids = ApplePodcastService.extract_ids(url)
        if not ids:
            raise PermanentDownloadError(f"无效的 Apple Podcasts URL: {url}")
        podcast_id, episode_id = ids

        os.makedirs(dest_dir, exist_ok=True)

        locale_match = _LOCALE_RE.search(url)
        country = locale_match.group(1).lower() if locale_match else None

        # ── 1. iTunes Lookup API → direct HTTP download
        meta = ApplePodcastService._itunes_lookup(podcast_id, episode_id, country=country)
        if meta and meta.get("audio_url"):
            try:
                result = ApplePodcastService._direct_download(
                    audio_url=meta["audio_url"],
                    dest_dir=dest_dir,
                    metadata=meta,
                    progress_hook=progress_hook,
                )
                info(
                    f"iTunes 直接下载完成: {result['filename']}",
                    source="apple_podcast",
                )
                return result
            except Exception as exc:
                warning(
                    f"iTunes 直接下载失败，回退 yt-dlp: {exc}",
                    source="apple_podcast",
                )

        # ── 2. yt-dlp fallback
        warning(
            f"iTunes Lookup 未找到音频 URL (episode_id={episode_id})，改用 yt-dlp 下载",
            source="apple_podcast",
        )
        return ApplePodcastService._ytdlp_download(url, dest_dir, episode_id, progress_hook)

    # ─────────────────────────────────────────────────────────────────────
    # 内部：直接 HTTP 下载（iTunes episodeUrl）
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _direct_download(
        audio_url: str,
        dest_dir: str,
        metadata: Dict[str, Any],
        progress_hook: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """从 episodeUrl 直接流式下载 MP3 到磁盘，支持 progress_hook。"""
        safe_title = re.sub(r'[^\w\s\-\[\]().,]', '_', metadata.get("title") or "episode")[:180]
        episode_id = metadata.get("episode_id") or "unknown"
        ext = metadata.get("ext") or "mp3"
        filename = f"{safe_title} [{episode_id}].{ext}"
        file_path = os.path.join(dest_dir, filename)

        # 优先 httpx，其次 urllib
        try:
            import httpx as _httpx

            with _httpx.Client(timeout=_DEFAULT_DOWNLOAD_TIMEOUT, follow_redirects=True) as client:
                with client.stream("GET", audio_url) as resp:
                    resp.raise_for_status()
                    total = int(resp.headers.get("content-length", 0))
                    downloaded = 0
                    last_hook_t = 0.0
                    with open(file_path, "wb") as f:
                        for chunk in resp.iter_bytes(chunk_size=65536):
                            f.write(chunk)
                            downloaded += len(chunk)
                            now = time.monotonic()
                            if progress_hook and now - last_hook_t >= 2.0:
                                last_hook_t = now
                                pct = (downloaded / total * 100) if total else 0
                                progress_hook({
                                    "status": "downloading",
                                    "_percent_str": f"{pct:.1f}%",
                                    "_speed_str": "",
                                    "_eta_str": "",
                                })
        except ImportError:
            # urllib fallback（无进度回调）
            import urllib.request as _urllib_req
            _urllib_req.urlretrieve(audio_url, file_path)

        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            raise TransientDownloadError(f"直接下载后文件不存在或为空: {file_path}")

        if progress_hook:
            progress_hook({"status": "finished", "filename": file_path})

        return {"file_path": file_path, "filename": filename, "metadata": metadata}

    # ─────────────────────────────────────────────────────────────────────
    # 内部：yt-dlp 下载（备用）
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _ytdlp_download(
        url: str,
        dest_dir: str,
        episode_id: str,
        progress_hook: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        try:
            import yt_dlp
        except ImportError:
            raise ImportError("yt_dlp 未安装: pip install -U yt-dlp")

        hooks = []
        if progress_hook:
            hooks.append(progress_hook)

        downloaded_files: list[str] = []

        def _collect_hook(d: Dict[str, Any]) -> None:
            if d.get("status") == "finished":
                fname = d.get("filename") or d.get("info_dict", {}).get("_filename")
                if fname:
                    downloaded_files.append(fname)

        hooks.append(_collect_hook)

        opts: Dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "paths": {"home": dest_dir},
            "outtmpl": {"default": "%(title).180B [%(id)s].%(ext)s"},
            "progress_hooks": hooks,
            "socket_timeout": 60,
        }

        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info_dict = ydl.extract_info(url, download=True)
        except yt_dlp.utils.DownloadError as exc:
            msg = str(exc)
            if any(kw in msg.lower() for kw in (
                "not found", "404", "unavailable", "private", "premium",
                "subscription", "geo", "region", "blocked",
            )):
                raise PermanentDownloadError(f"内容不可访问: {msg}")
            raise TransientDownloadError(f"yt-dlp 下载失败: {msg}")
        except Exception as exc:
            raise TransientDownloadError(f"yt-dlp 下载异常: {exc}")

        file_path: Optional[str] = None
        if info_dict and isinstance(info_dict, dict):
            rds = info_dict.get("requested_downloads") or []
            if rds:
                file_path = rds[0].get("filepath")

        if not file_path and downloaded_files:
            file_path = downloaded_files[-1]

        if not file_path or not os.path.exists(file_path):
            try:
                candidates = [
                    os.path.join(dest_dir, f)
                    for f in os.listdir(dest_dir)
                    if os.path.isfile(os.path.join(dest_dir, f))
                ]
                if candidates:
                    file_path = max(candidates, key=os.path.getmtime)
            except Exception:
                pass

        if not file_path or not os.path.exists(file_path):
            raise TransientDownloadError("yt-dlp 完成但未找到输出文件，dest_dir=" + dest_dir)

        metadata = {
            "episode_id": episode_id,
            "title": (info_dict.get("title") if info_dict else "") or f"podcast_{episode_id}",
            "series": (info_dict.get("series") or info_dict.get("playlist_title") if info_dict else "") or "",
            "episode": (info_dict.get("episode") or info_dict.get("title") if info_dict else "") or "",
            "episode_number": (info_dict.get("episode_number") if info_dict else None),
            "duration": (info_dict.get("duration") if info_dict else None),
            "thumbnail": (info_dict.get("thumbnail") if info_dict else None),
            "webpage_url": (info_dict.get("webpage_url") if info_dict else url) or url,
            "ext": (info_dict.get("ext") if info_dict else "mp3") or "mp3",
        }

        return {
            "file_path": file_path,
            "filename": os.path.basename(file_path),
            "metadata": metadata,
        }
