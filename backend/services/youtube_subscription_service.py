# -*- coding: utf-8 -*-
"""
YouTube 订阅服务
通过 RSS Feed 获取 YouTube 频道视频，RSS 失败时自动回退到 YouTube Data API v3
"""

import os
import re
import feedparser
import requests
from typing import Dict, Any, List, Optional
from datetime import datetime
import time
from backend.utils.logger import info, error, warning


def _get_proxies() -> Optional[Dict[str, str]]:
    """
    从环境变量读取代理配置。
    优先级：YOUTUBE_PROXY > HTTPS_PROXY > HTTP_PROXY
    在国内服务器访问 YouTube 时需要配置代理，例如在 .env 中设置：
      YOUTUBE_PROXY=http://127.0.0.1:7890
    """
    proxy = (
        os.getenv('YOUTUBE_PROXY')
        or os.getenv('HTTPS_PROXY')
        or os.getenv('HTTP_PROXY')
    )
    if proxy:
        return {'http': proxy, 'https': proxy}
    return None


def _is_network_unreachable(exc: Exception) -> bool:
    """判断异常是否属于"服务器无法访问外网"（需要配置代理）"""
    msg = str(exc).lower()
    return any(kw in msg for kw in [
        'network is unreachable',
        'errno 101',
        'failed to establish a new connection',
        'newconnectionerror',
    ])


def _get_youtube_api_key() -> Optional[str]:
    """从环境变量读取 YouTube Data API v3 密钥，未配置时返回 None（fallback 不启用）。"""
    key = os.getenv('YOUTUBE_DATA_API_KEY', '').strip()
    return key or None


def normalize_youtube_channel_id(raw: Optional[str]) -> Optional[str]:
    """
    规范化 YouTube channel_id（应为 UC + 22 位字符，共 24 位）。
    部分 RSS / feedparser 对 yt:channelId 会丢掉「UC」前缀，只剩 22 字符，导致后续用错 identifier。
    """
    if raw is None:
        return None
    c = str(raw).strip()
    if not c:
        return None
    if len(c) >= 24 and c.startswith('UC'):
        return c
    if len(c) == 22 and re.match(r'^[A-Za-z0-9_-]{22}$', c):
        return 'UC' + c
    return c


class YouTubeSubscriptionService:
    """YouTube 订阅服务类 - 使用 RSS Feed"""

    REQUEST_TIMEOUT = 5  # 单次请求超时（秒）

    # RSS Feed URL 模板
    RSS_BY_USER = "https://www.youtube.com/feeds/videos.xml?user={username}"
    RSS_BY_CHANNEL_ID = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    
    # 频道页面 URL 模板（用于获取 channel_id）
    CHANNEL_URL_TEMPLATES = [
        "https://www.youtube.com/@{username}",
        "https://www.youtube.com/c/{username}",
        "https://www.youtube.com/user/{username}",
    ]
    
    @staticmethod
    def clean_username(username: str) -> str:
        """
        清理用户名：去除 @ 前缀和空格
        
        Args:
            username: 原始用户名
            
        Returns:
            清理后的用户名
        """
        return username.strip().lstrip('@')
    
    @staticmethod
    def extract_channel_id_from_page(username: str) -> Optional[str]:
        """
        从频道页面 HTML 中提取 channel_id
        
        Args:
            username: YouTube 用户名
            
        Returns:
            channel_id 或 None
        """
        proxies = _get_proxies()
        for url_template in YouTubeSubscriptionService.CHANNEL_URL_TEMPLATES:
            try:
                url = url_template.format(username=username)
                info(f"尝试获取频道ID: url={url}", source="youtube_subscription")
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
                
                response = requests.get(
                    url, headers=headers,
                    timeout=YouTubeSubscriptionService.REQUEST_TIMEOUT,
                    proxies=proxies,
                )
                
                if response.status_code == 200:
                    html = response.text

                    # 提取 channel_id：必须严格匹配 UC + 22 字符，且优先使用「当前页面」字段，
                    # 避免误匹配到侧栏推荐 / 相关视频 / 评论作者的 channelId（YouTube 页面里
                    # 第一处出现的 "channelId":"..." 经常不是当前频道）。
                    # 优先级：canonical link > externalId > og:url > meta itemprop
                    patterns = [
                        r'<link\s+rel="canonical"\s+href="https://www\.youtube\.com/channel/(UC[A-Za-z0-9_-]{22})"',
                        r'"externalId":"(UC[A-Za-z0-9_-]{22})"',
                        r'<meta\s+property="og:url"\s+content="https://www\.youtube\.com/channel/(UC[A-Za-z0-9_-]{22})"',
                        r'<meta\s+itemprop="(?:identifier|channelId)"\s+content="(UC[A-Za-z0-9_-]{22})"',
                    ]

                    for pattern in patterns:
                        match = re.search(pattern, html)
                        if match:
                            channel_id = match.group(1)
                            info(f"成功提取 channel_id: {channel_id}", source="youtube_subscription")
                            return channel_id
                
            except Exception as e:
                warning(f"从 {url_template} 提取 channel_id 失败: {str(e)}", source="youtube_subscription")
                if _is_network_unreachable(e):
                    raise  # 网络不可达：立即终止，不再尝试后续 URL
                continue
        
        return None
    
    @staticmethod
    def validate_youtube_channel(channel_identifier: str) -> Dict[str, Any]:
        """
        验证 YouTube 频道是否存在

        ⚠️ 重要：YouTube 的旧 RSS endpoint `?user=<x>` 是基于注册时的 legacy username，
        与当前的 `@handle` 是两套完全不同的体系。同一个字符串在两个 endpoint 里
        可能指向完全不同的频道。例如：
            - `?user=markets`      → 一个无关的小频道 (UCxPf25VbKZx_xF942Lh3jYQ)
            - `youtube.com/@markets` → Bloomberg Television (UCIALMKvObZNtJ6AmdCLP7Lg)
        因此：
            - 用户输入 `@xxx` (前缀有 @) → **优先**走页面解析（@handle 路径），
              不能用 `?user=` RSS，否则会订阅到错误频道。
            - 没带 @ 的裸字符串 → 兼容老逻辑（先 ?user= RSS，再 fallback 页面解析）。

        Args:
            channel_identifier: YouTube 用户名 / handle (`@xxx`) / 频道ID (`UC...`)

        Returns:
            验证结果字典：
            {
                'exists': bool,
                'channel_id': str,
                'channel_name': str,
                'channel_url': str,
                'error': str (if exists=False)
            }
        """
        original_input = (channel_identifier or '').strip()
        had_at_prefix = original_input.startswith('@')
        username = YouTubeSubscriptionService.clean_username(channel_identifier)

        info(
            f"验证 YouTube 频道: identifier={username}, had_at_prefix={had_at_prefix}",
            source="youtube_subscription",
        )

        PROXY_HINT = (
            "服务器无法访问 YouTube，请在 .env 中配置 YOUTUBE_PROXY，"
            "例如：YOUTUBE_PROXY=http://127.0.0.1:7890"
        )

        def _check_unreachable(result: Dict[str, Any]) -> bool:
            """若网络不可达则记录日志并返回 True"""
            if result.get('network_unreachable'):
                warning(PROXY_HINT, source="youtube_subscription")
                return True
            return False

        def _try_via_page_then_channel_rss() -> Optional[Dict[str, Any]]:
            """先抓 @handle 页面拿真实 channel_id，再用 channel_id RSS 取信息。"""
            try:
                channel_id = YouTubeSubscriptionService.extract_channel_id_from_page(username)
            except Exception:
                warning(PROXY_HINT, source="youtube_subscription")
                return {'exists': False, 'network_unreachable': True, 'error': PROXY_HINT}

            if not channel_id:
                return None

            rss_url = YouTubeSubscriptionService.RSS_BY_CHANNEL_ID.format(channel_id=channel_id)
            result = YouTubeSubscriptionService._parse_rss_feed(rss_url)
            if _check_unreachable(result):
                return {'exists': False, 'network_unreachable': True, 'error': PROXY_HINT}
            if result['exists']:
                result['channel_id'] = channel_id
                return result
            api_result = YouTubeSubscriptionService._validate_channel_via_data_api(channel_id)
            if api_result:
                return api_result
            return None

        # 尝试1：如果看起来像 channel_id（UC开头），直接用 channel_id RSS
        if len(username) >= 20 and username.startswith('UC'):
            info(f"识别为 channel_id 格式", source="youtube_subscription")
            rss_url = YouTubeSubscriptionService.RSS_BY_CHANNEL_ID.format(channel_id=username)
            result = YouTubeSubscriptionService._parse_rss_feed(rss_url)
            if result['exists']:
                result['channel_id'] = username
                return result
            if _check_unreachable(result):
                return {'exists': False, 'network_unreachable': True, 'error': PROXY_HINT}
            api_result = YouTubeSubscriptionService._validate_channel_via_data_api(username)
            if api_result:
                return api_result

        # 尝试2：带 @ 前缀的输入 → 直接走页面解析（避免 ?user= 路由错误频道）
        if had_at_prefix:
            page_result = _try_via_page_then_channel_rss()
            if page_result is not None:
                if page_result.get('network_unreachable'):
                    return page_result
                if page_result.get('exists'):
                    return page_result
            # @handle 解析失败时不再 fallback 到 ?user=（语义不同），直接报错
            error_msg = (
                f"YouTube handle '@{username}' not found. "
                "请确认 https://www.youtube.com/@{username} 能正常访问。"
            ).format(username=username)
            error(error_msg, source="youtube_subscription")
            return {'exists': False, 'error': error_msg}

        # 尝试3：裸字符串（无 @）→ 先用 ?user= RSS（老逻辑，兼容历史订阅）
        rss_url = YouTubeSubscriptionService.RSS_BY_USER.format(username=username)
        result = YouTubeSubscriptionService._parse_rss_feed(rss_url)

        if _check_unreachable(result):
            return {'exists': False, 'network_unreachable': True, 'error': PROXY_HINT}

        if result['exists']:
            channel_id = result.get('channel_id')
            if not channel_id:
                try:
                    channel_id = YouTubeSubscriptionService.extract_channel_id_from_page(username)
                except Exception:
                    # 网络不可达在内层已记录，忽略
                    pass
                if channel_id:
                    result['channel_id'] = channel_id
            return result

        # 尝试4：?user= 失败 → fallback 到页面解析
        page_result = _try_via_page_then_channel_rss()
        if page_result is not None:
            if page_result.get('network_unreachable'):
                return page_result
            if page_result.get('exists'):
                return page_result

        # 所有尝试都失败
        error_msg = (
            f"YouTube channel '{username}' not found or RSS feed unavailable. "
            "YouTube 官方 RSS 常返回 404/500；若已配置 YOUTUBE_DATA_API_KEY，将自动用 API 验证。"
        )
        error(error_msg, source="youtube_subscription")
        return {
            'exists': False,
            'error': error_msg
        }
    
    @staticmethod
    def _parse_rss_feed(rss_url: str) -> Dict[str, Any]:
        """
        解析 RSS Feed 并提取频道信息
        
        Args:
            rss_url: RSS Feed URL
            
        Returns:
            解析结果
        """
        try:
            info(f"解析 RSS Feed: url={rss_url}", source="youtube_subscription")

            # 通过 requests 获取 RSS 内容（支持代理），再交由 feedparser 解析
            proxies = _get_proxies()
            rss_headers = {'User-Agent': 'feedparser/6.0'}
            max_rss_attempts = 3
            resp = None
            last_req_err = None
            for attempt in range(max_rss_attempts):
                try:
                    resp = requests.get(
                        rss_url,
                        headers=rss_headers,
                        timeout=YouTubeSubscriptionService.REQUEST_TIMEOUT,
                        proxies=proxies,
                    )
                    if resp.status_code in (500, 502, 503, 504) and attempt < max_rss_attempts - 1:
                        warning(
                            f"RSS Feed 暂时不可用 (HTTP {resp.status_code})，"
                            f"重试 {attempt + 1}/{max_rss_attempts}...",
                            source="youtube_subscription",
                        )
                        time.sleep(0.6 * (attempt + 1))
                        continue
                    resp.raise_for_status()
                    break
                except requests.exceptions.RequestException as req_err:
                    last_req_err = req_err
                    retryable = False
                    if isinstance(req_err, requests.exceptions.HTTPError) and req_err.response is not None:
                        code = req_err.response.status_code
                        retryable = code in (500, 502, 503, 504)
                    if retryable and attempt < max_rss_attempts - 1:
                        warning(
                            f"RSS Feed 请求失败，重试 {attempt + 1}/{max_rss_attempts}: {req_err}",
                            source="youtube_subscription",
                        )
                        time.sleep(0.6 * (attempt + 1))
                        continue
                    warning(f"RSS Feed 请求失败: {req_err}", source="youtube_subscription")
                    if _is_network_unreachable(req_err):
                        return {'exists': False, 'network_unreachable': True,
                                'error': 'network_unreachable'}
                    return {'exists': False, 'error': f'RSS feed request failed: {req_err}'}

            if resp is None:
                if last_req_err:
                    warning(f"RSS Feed 请求失败: {last_req_err}", source="youtube_subscription")
                    if _is_network_unreachable(last_req_err):
                        return {'exists': False, 'network_unreachable': True,
                                'error': 'network_unreachable'}
                    return {'exists': False, 'error': f'RSS feed request failed: {last_req_err}'}
                return {'exists': False, 'error': 'RSS feed request failed'}

            feed = feedparser.parse(resp.content)

            # 检查是否成功获取
            if not feed or not hasattr(feed, 'entries') or len(feed.entries) == 0:
                warning(f"RSS Feed 为空或无法访问", source="youtube_subscription")
                return {'exists': False, 'error': 'RSS feed is empty or unavailable'}
            
            # 提取频道信息
            channel_name = feed.feed.get('title', 'Unknown Channel')
            
            # channel_id：优先从条目 author 链接解析（最可靠），其次 yt:channelId（可能被 feedparser 截断缺 UC）
            channel_id = None
            channel_url = f"https://www.youtube.com/@{channel_name}"
            if feed.entries and hasattr(feed.entries[0], 'author_detail'):
                author_detail = feed.entries[0].author_detail
                if 'href' in author_detail:
                    href = author_detail['href']
                    m_ch = re.search(r'/channel/(UC[A-Za-z0-9_-]{22})', href)
                    if m_ch:
                        channel_id = m_ch.group(1)
                    channel_url = href
            if not channel_id and hasattr(feed.feed, 'yt_channelid') and feed.feed.yt_channelid:
                channel_id = normalize_youtube_channel_id(str(feed.feed.yt_channelid).strip())
            channel_id = normalize_youtube_channel_id(channel_id)
            if channel_id:
                channel_url = f"https://www.youtube.com/channel/{channel_id}"
            
            info(f"频道验证成功: name={channel_name}, channel_id={channel_id}", source="youtube_subscription")
            
            return {
                'exists': True,
                'channel_id': channel_id,
                'channel_name': channel_name,
                'channel_url': channel_url,
                'video_count': len(feed.entries)
            }
            
        except Exception as e:
            error(f"解析 RSS Feed 失败: {str(e)}", source="youtube_subscription")
            return {
                'exists': False,
                'error': f"Failed to parse RSS feed: {str(e)}"
            }
    
    YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
    YOUTUBE_API_TIMEOUT = 10

    @staticmethod
    def _validate_channel_via_data_api(channel_id: str) -> Optional[Dict[str, Any]]:
        """
        当官方 RSS 返回 404/500 等时，用 YouTube Data API v3 `channels.list` 验证频道并取标题。
        需配置环境变量 YOUTUBE_DATA_API_KEY；未配置或调用失败时返回 None。
        """
        api_key = _get_youtube_api_key()
        if not api_key:
            return None
        base = YouTubeSubscriptionService.YOUTUBE_API_BASE
        timeout = YouTubeSubscriptionService.YOUTUBE_API_TIMEOUT
        proxies = _get_proxies()
        try:
            resp = requests.get(
                f"{base}/channels",
                params={"part": "snippet", "id": channel_id, "key": api_key},
                headers={"Accept": "application/json"},
                timeout=timeout,
                proxies=proxies,
            )
            if resp.status_code == 403:
                warning(
                    "YouTube Data API 配额已用尽或 key 无权限，无法用作频道验证回退",
                    source="youtube_subscription",
                )
                return None
            resp.raise_for_status()
            data = resp.json()
            items = data.get("items", [])
            if not items:
                return None
            snippet = items[0].get("snippet", {})
            title = snippet.get("title", "Unknown Channel")
            channel_url = f"https://www.youtube.com/channel/{channel_id}"
            info(
                f"频道验证成功 (Data API): name={title}, channel_id={channel_id}",
                source="youtube_subscription",
            )
            return {
                "exists": True,
                "channel_id": channel_id,
                "channel_name": title,
                "channel_url": channel_url,
                "video_count": 0,
            }
        except requests.exceptions.RequestException as e:
            warning(f"Data API 验证频道失败: {e}", source="youtube_subscription")
            return None

    @staticmethod
    def _fetch_channel_videos_via_api(
        channel_id: str,
        max_items: int,
        api_key: str,
    ) -> List[Dict[str, Any]]:
        """
        通过 YouTube Data API v3 获取频道最新视频（RSS fallback）。

        调用链：channels.list → playlistItems.list → videos.list
        共消耗 3 quota units。
        """
        base = YouTubeSubscriptionService.YOUTUBE_API_BASE
        timeout = YouTubeSubscriptionService.YOUTUBE_API_TIMEOUT
        proxies = _get_proxies()
        headers = {'Accept': 'application/json'}

        try:
            # Step 1: channels.list → uploads playlist ID
            resp = requests.get(
                f"{base}/channels",
                params={'part': 'contentDetails', 'id': channel_id, 'key': api_key},
                headers=headers, timeout=timeout, proxies=proxies,
            )
            if resp.status_code == 403:
                warning("YouTube Data API 配额已用尽或 key 无权限", source="youtube_subscription")
                return []
            resp.raise_for_status()
            channels_data = resp.json()
            items = channels_data.get('items', [])
            if not items:
                warning(f"API v3: 频道 {channel_id} 未找到", source="youtube_subscription")
                return []
            uploads_playlist_id = (
                items[0].get('contentDetails', {})
                .get('relatedPlaylists', {})
                .get('uploads')
            )
            if not uploads_playlist_id:
                warning(f"API v3: 频道 {channel_id} 无 uploads 播放列表", source="youtube_subscription")
                return []

            # Step 2: playlistItems.list → video IDs + basic snippet
            resp = requests.get(
                f"{base}/playlistItems",
                params={
                    'part': 'snippet',
                    'playlistId': uploads_playlist_id,
                    'maxResults': min(max_items, 50),
                    'key': api_key,
                },
                headers=headers, timeout=timeout, proxies=proxies,
            )
            resp.raise_for_status()
            playlist_data = resp.json()
            playlist_items = playlist_data.get('items', [])
            if not playlist_items:
                warning(f"API v3: 播放列表 {uploads_playlist_id} 无视频", source="youtube_subscription")
                return []

            video_ids = [
                item['snippet']['resourceId']['videoId']
                for item in playlist_items
                if item.get('snippet', {}).get('resourceId', {}).get('videoId')
            ]
            if not video_ids:
                return []

            # Step 3: videos.list → full snippet + statistics + contentDetails
            resp = requests.get(
                f"{base}/videos",
                params={
                    'part': 'snippet,statistics,contentDetails',
                    'id': ','.join(video_ids),
                    'key': api_key,
                },
                headers=headers, timeout=timeout, proxies=proxies,
            )
            resp.raise_for_status()
            videos_data = resp.json()

            videos: List[Dict[str, Any]] = []
            for item in videos_data.get('items', []):
                snippet = item.get('snippet', {})
                statistics = item.get('statistics', {})
                content_details = item.get('contentDetails', {})
                video_id = item.get('id', '')

                videos.append({
                    'id': video_id,
                    'snippet': {
                        'title': snippet.get('title', ''),
                        'description': snippet.get('description', ''),
                        'publishedAt': snippet.get('publishedAt', ''),
                        'channelId': snippet.get('channelId', channel_id),
                        'channelTitle': snippet.get('channelTitle', ''),
                        'thumbnails': snippet.get('thumbnails', {}),
                    },
                    'statistics': {
                        'viewCount': statistics.get('viewCount', '0'),
                        'likeCount': statistics.get('likeCount', '0'),
                        'commentCount': statistics.get('commentCount', '0'),
                    },
                    'contentDetails': {
                        'duration': content_details.get('duration', ''),
                    },
                })

            info(f"API v3 fallback 成功获取 {len(videos)} 个视频 (channel={channel_id})",
                 source="youtube_subscription")
            return videos

        except requests.exceptions.RequestException as e:
            error(f"API v3 fallback 请求失败: {e}", source="youtube_subscription")
            return []
        except (KeyError, IndexError, ValueError) as e:
            error(f"API v3 fallback 解析响应失败: {e}", source="youtube_subscription")
            return []

    @staticmethod
    def fetch_channel_videos(
        channel_identifier: str,
        max_items: int = 15
    ) -> List[Dict[str, Any]]:
        """
        获取 YouTube 频道的最新视频
        
        Args:
            channel_identifier: YouTube 用户名或频道ID
            max_items: 最大获取数量（RSS 默认最多15个）
            
        Returns:
            视频列表（原始格式，需要通过 YouTubeAdapter 标准化）
        """
        username = YouTubeSubscriptionService.clean_username(channel_identifier)
        is_channel_id = len(username) >= 20 and username.startswith('UC')

        info(f"获取 YouTube 频道视频: identifier={username}, max_items={max_items}",
             source="youtube_subscription")

        # ── 1. 尝试 RSS Feed ─────────────────────────────────────
        videos = YouTubeSubscriptionService._fetch_channel_videos_via_rss(
            username, max_items, is_channel_id,
        )

        if videos:
            return videos

        # ── 2. RSS 失败 / 空，尝试 YouTube Data API v3 fallback ──
        if is_channel_id:
            api_key = _get_youtube_api_key()
            if api_key:
                info(f"RSS 未获取到视频，尝试 YouTube Data API v3 fallback (channel={username})",
                     source="youtube_subscription")
                videos = YouTubeSubscriptionService._fetch_channel_videos_via_api(
                    channel_id=username, max_items=max_items, api_key=api_key,
                )
                if videos:
                    return videos

        return []

    @staticmethod
    def _fetch_channel_videos_via_rss(
        username: str,
        max_items: int,
        is_channel_id: bool,
    ) -> List[Dict[str, Any]]:
        """通过 RSS Feed 获取频道视频，失败时返回空列表。"""
        if is_channel_id:
            rss_url = YouTubeSubscriptionService.RSS_BY_CHANNEL_ID.format(channel_id=username)
        else:
            rss_url = YouTubeSubscriptionService.RSS_BY_USER.format(username=username)

        try:
            proxies = _get_proxies()
            try:
                resp = requests.get(
                    rss_url,
                    headers={'User-Agent': 'feedparser/6.0'},
                    timeout=YouTubeSubscriptionService.REQUEST_TIMEOUT,
                    proxies=proxies,
                )
                resp.raise_for_status()
                feed = feedparser.parse(resp.content)
            except requests.exceptions.RequestException as req_err:
                error(f"无法获取 RSS Feed: {req_err}", source="youtube_subscription")
                return []

            if not feed or not hasattr(feed, 'entries'):
                error(f"无法获取 RSS Feed", source="youtube_subscription")
                return []

            videos = []

            for entry in feed.entries[:max_items]:
                try:
                    video_id = None
                    if hasattr(entry, 'yt_videoid'):
                        video_id = entry.yt_videoid
                    elif 'link' in entry:
                        match = re.search(r'watch\?v=([a-zA-Z0-9_-]{11})', entry.link)
                        if match:
                            video_id = match.group(1)

                    if not video_id:
                        warning(f"无法提取视频 ID: {entry.get('link', 'unknown')}",
                               source="youtube_subscription")
                        continue

                    thumbnail_url = None
                    if hasattr(entry, 'media_thumbnail') and len(entry.media_thumbnail) > 0:
                        thumbnail_url = entry.media_thumbnail[0].get('url')
                    if not thumbnail_url:
                        thumbnail_url = f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"

                    view_count = 0
                    like_count = 0
                    if hasattr(entry, 'media_statistics'):
                        view_count = int(entry.media_statistics.get('views', 0))
                    if hasattr(entry, 'media_starrating'):
                        like_count = int(entry.media_starrating.get('count', 0))

                    channel_id = None
                    channel_name = entry.get('author', 'Unknown')

                    if hasattr(entry, 'yt_channelid'):
                        channel_id = entry.yt_channelid
                    elif hasattr(entry, 'author_detail') and 'href' in entry.author_detail:
                        author_url = entry.author_detail['href']
                        match = re.search(r'/channel/([^/]+)', author_url)
                        if match:
                            channel_id = match.group(1)
                    channel_id = normalize_youtube_channel_id(channel_id) if channel_id else None

                    video_data = {
                        'id': video_id,
                        'snippet': {
                            'title': entry.get('title', 'Untitled'),
                            'description': entry.get('summary', ''),
                            'publishedAt': entry.get('published', ''),
                            'channelId': channel_id or username,
                            'channelTitle': channel_name,
                            'thumbnails': {
                                'high': {'url': thumbnail_url}
                            }
                        },
                        'statistics': {
                            'viewCount': str(view_count),
                            'likeCount': str(like_count),
                            'commentCount': '0'
                        },
                        'contentDetails': {
                            'duration': ''
                        }
                    }

                    videos.append(video_data)

                except Exception as e:
                    warning(f"解析视频条目失败: {str(e)}", source="youtube_subscription")
                    continue

            if videos:
                info(f"RSS 成功获取 {len(videos)} 个视频", source="youtube_subscription")
            return videos

        except Exception as e:
            error(f"获取 YouTube 视频失败: {str(e)}", source="youtube_subscription")
            return []
    
    @staticmethod
    def get_all_youtube_subscriptions(enabled_only: bool = True) -> List[Dict[str, Any]]:
        """
        获取所有 YouTube 订阅列表
        
        Args:
            enabled_only: 是否只获取启用的订阅
            
        Returns:
            订阅列表
        """
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_conn_params=db_config)
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT 
                    id, user_id, channel_type, channel_identifier,
                    channel_name, max_items_per_fetch, enabled
                FROM subscription_list
                WHERE channel_type = 'youtube'
            """
            
            if enabled_only:
                query += " AND enabled = true"
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            subscriptions = []
            for row in rows:
                subscriptions.append({
                    'id': row[0],
                    'user_id': row[1],
                    'channel_type': row[2],
                    'channel_identifier': row[3],
                    'channel_name': row[4],
                    'max_items_per_fetch': row[5],
                    'enabled': row[6]
                })
            
            info(f"成功获取YouTube订阅: {len(subscriptions)} 个", source="youtube_subscription")
            return subscriptions
            
        except Exception as e:
            error(f"获取YouTube订阅失败: {str(e)}", source="youtube_subscription")
            return []
        finally:
            cursor.close()
            conn.close()
    
    @staticmethod
    def _fetch_and_store_transcript(
        content_db_id: int,
        video_id: str,
        video_url: str,
        content_service
    ) -> bool:
        """
        为一条已保存的 YouTube 内容拉取字幕并写回数据库。
        
        Returns:
            True 表示成功，False 表示失败（失败不抛出，由调用方记录日志）
        """
        from backend.services.youtube_service import YouTubeService

        try:
            result = YouTubeService.fetch_transcript(video_url)
            full_text = YouTubeService.get_full_text(result)
            content_service.update_transcript(content_db_id, full_text, result)
            info(
                f"Transcript fetched: content_db_id={content_db_id}, video_id={video_id}, "
                f"segments={len(result['transcript'])}, language={result['language']}",
                source="youtube_subscription"
            )
            return True
        except ImportError as e:
            warning(f"youtube-transcript-api 未安装，跳过 transcript 拉取: {str(e)}", source="youtube_subscription")
            return False
        except Exception as e:
            warning(
                f"Transcript 拉取失败（不影响视频存储）: video_id={video_id}, error={str(e)}",
                source="youtube_subscription"
            )
            return False

    @staticmethod
    def fetch_and_store_videos(
        subscription_id: Optional[int] = None,
        user_id: Optional[int] = None,
        force_fetch: bool = False
    ) -> Dict[str, Any]:
        """
        获取并存储 YouTube 视频到数据库
        
        Args:
            subscription_id: 订阅ID
            user_id: 用户ID
            force_fetch: 是否强制获取
            
        Returns:
            执行结果统计
        """
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        from backend.services.subscription_content_service import SubscriptionContentService
        from backend.services.content_adapters import get_adapter
        
        start_time = time.time()
        
        try:
            db_config = get_database_config()
            db_manager = DatabaseManager(db_conn_params=db_config)
            conn = db_manager.get_connection()
            cursor = conn.cursor()
            
            # 获取订阅信息
            if subscription_id:
                cursor.execute("""
                    SELECT 
                        id, user_id, channel_type, channel_identifier,
                        channel_name, max_items_per_fetch
                    FROM subscription_list
                    WHERE id = %s
                """, (subscription_id,))
                subscription = cursor.fetchone()
                
                if not subscription:
                    error(f"订阅不存在: subscription_id={subscription_id}", source="youtube_subscription")
                    return {
                        'success': False,
                        'message': 'Subscription not found',
                        'videos_new': 0,
                        'videos_existing': 0
                    }
                
                sub_id, uid, channel_type, channel_identifier, channel_name, max_items = subscription
                user_id = uid
            else:
                error("必须提供 subscription_id", source="youtube_subscription")
                return {
                    'success': False,
                    'message': 'subscription_id is required',
                    'videos_new': 0,
                    'videos_existing': 0
                }
            
            cursor.close()
            conn.close()
            
            info(f"开始获取 YouTube 视频: subscription_id={subscription_id}, "
                 f"channel={channel_identifier}, max_items={max_items}",
                 source="youtube_subscription")
            
            # 获取视频列表
            raw_videos = YouTubeSubscriptionService.fetch_channel_videos(
                channel_identifier=channel_identifier,
                max_items=max_items or 15
            )
            
            if not raw_videos:
                warning(f"未获取到任何视频", source="youtube_subscription")
                return {
                    'success': True,
                    'message': 'No videos found',
                    'videos_new': 0,
                    'videos_existing': 0,
                    'execution_duration_ms': int((time.time() - start_time) * 1000)
                }
            
            # 保存到数据库（save_content 会内部调用 adapter 标准化）
            content_service = SubscriptionContentService()
            videos_new = 0
            videos_existing = 0
            videos_failed = 0
            
            new_video_items = []

            for raw_video in raw_videos:
                try:
                    content_db_id, is_new = content_service.save_content(
                        channel_type='youtube',
                        raw_data=raw_video,
                        user_ids=[user_id]
                    )

                    if is_new:
                        videos_new += 1
                        video_id_str = raw_video.get('id', '')
                        video_url = f"https://www.youtube.com/watch?v={video_id_str}"
                        new_video_items.append({
                            "content_db_id": content_db_id,
                            "video_id": video_id_str,
                            "video_url": video_url,
                        })
                    else:
                        videos_existing += 1

                except Exception as e:
                    video_id_str = raw_video.get('id', 'unknown')
                    warning(f"保存视频失败: video_id={video_id_str}, error={str(e)}",
                           source="youtube_subscription")
                    videos_failed += 1
                    continue

            if new_video_items:
                try:
                    from backend.tasks.subscription_tasks import fetch_youtube_transcripts_batch_task
                    fetch_youtube_transcripts_batch_task.apply_async(
                        args=[new_video_items],
                        queue='youtube_fetching',
                    )
                    info(f"Transcript 拉取任务已派发到家用服务器: {len(new_video_items)} 个视频",
                         source="youtube_subscription")
                except Exception as e:
                    warning(f"派发 transcript 任务失败: {str(e)}", source="youtube_subscription")

            info(f"保存完成: new={videos_new}, existing={videos_existing}, failed={videos_failed}",
                 source="youtube_subscription")
            
            execution_time = int((time.time() - start_time) * 1000)
            
            info(f"YouTube 视频获取完成: subscription_id={subscription_id}, "
                 f"new={videos_new}, existing={videos_existing}, failed={videos_failed}, "
                 f"duration={execution_time}ms",
                 source="youtube_subscription")
            
            # 更新订阅统计
            try:
                conn = db_manager.get_connection()
                cursor = conn.cursor()
                
                cursor.execute("""
                    UPDATE subscription_list
                    SET 
                        last_fetched_at = CURRENT_TIMESTAMP,
                        total_items_fetched = total_items_fetched + %s,
                        last_fetch_items_count = %s,
                        consecutive_failures = 0
                    WHERE id = %s
                """, (videos_new, videos_new, subscription_id))
                
                conn.commit()
                cursor.close()
                conn.close()
                
            except Exception as e:
                error(f"更新订阅统计失败: {str(e)}", source="youtube_subscription")
            
            return {
                'success': True,
                'message': f"Successfully fetched {videos_new} new videos",
                'videos_new': videos_new,
                'videos_existing': videos_existing,
                'videos_failed': videos_failed,
                'execution_duration_ms': execution_time
            }
            
        except Exception as e:
            error(f"获取并存储 YouTube 视频失败: {str(e)}", source="youtube_subscription")
            return {
                'success': False,
                'message': f'Failed to fetch videos: {str(e)}',
                'videos_new': 0,
                'videos_existing': 0,
                'execution_duration_ms': int((time.time() - start_time) * 1000)
            }
    
    @staticmethod
    def fetch_all_subscriptions() -> Dict[str, Any]:
        """
        批量获取所有YouTube订阅的最新视频（供Scheduler调用）
        
        Returns:
            执行结果统计
        """
        start_time = time.time()
        
        info("开始批量获取YouTube订阅视频", source="youtube_subscription")
        
        try:
            # 获取所有启用的订阅
            subscriptions = YouTubeSubscriptionService.get_all_youtube_subscriptions(enabled_only=True)
            
            if not subscriptions:
                info("未找到启用的YouTube订阅", source="youtube_subscription")
                return {
                    'success': True,
                    'message': '未找到启用的YouTube订阅',
                    'subscriptions_count': 0,
                    'channels_count': 0,
                    'videos_fetched': 0,
                    'videos_new': 0,
                    'videos_existing': 0,
                    'execution_duration_ms': int((time.time() - start_time) * 1000)
                }
            
            # 按频道聚合订阅（多个用户可能订阅同一频道）
            channels_map = {}  # {channel_id: {'subscription_ids': [...], 'user_ids': [...]}}
            
            for sub in subscriptions:
                channel_id = sub['channel_identifier']
                if channel_id not in channels_map:
                    channels_map[channel_id] = {
                        'channel_id': channel_id,
                        'channel_name': sub.get('channel_name', 'Unknown'),
                        'max_items': sub.get('max_items_per_fetch', 15),
                        'subscription_ids': [],
                        'user_ids': []
                    }
                
                channels_map[channel_id]['subscription_ids'].append(sub['id'])
                channels_map[channel_id]['user_ids'].append(sub['user_id'])
            
            info(f"找到 {len(subscriptions)} 个订阅，聚合为 {len(channels_map)} 个频道", 
                 source="youtube_subscription")
            
            # 逐个频道获取视频
            total_videos_fetched = 0
            total_videos_new = 0
            total_videos_existing = 0
            channels_success = 0
            channels_failed = 0
            
            from backend.services.subscription_content_service import SubscriptionContentService
            content_service = SubscriptionContentService()
            
            for channel_id, channel_info in channels_map.items():
                try:
                    info(f"获取频道视频: {channel_info['channel_name']} ({channel_id})", 
                         source="youtube_subscription")
                    
                    # 获取视频列表
                    raw_videos = YouTubeSubscriptionService.fetch_channel_videos(
                        channel_identifier=channel_id,
                        max_items=channel_info['max_items']
                    )
                    
                    if not raw_videos:
                        warning(f"频道 {channel_id} 未获取到视频", source="youtube_subscription")
                        continue
                    
                    # 保存视频
                    videos_new = 0
                    videos_existing = 0
                    user_ids = channel_info['user_ids']
                    new_video_items = []
                    
                    for raw_video in raw_videos:
                        try:
                            content_db_id, is_new = content_service.save_content(
                                channel_type='youtube',
                                raw_data=raw_video,
                                user_ids=user_ids
                            )

                            if is_new:
                                videos_new += 1
                                video_id_str = raw_video.get('id', '')
                                video_url = f"https://www.youtube.com/watch?v={video_id_str}"
                                new_video_items.append({
                                    "content_db_id": content_db_id,
                                    "video_id": video_id_str,
                                    "video_url": video_url,
                                })
                            else:
                                videos_existing += 1

                        except Exception as e:
                            video_id_str = raw_video.get('id', 'unknown')
                            warning(f"保存视频失败: video_id={video_id_str}, error={str(e)}",
                                   source="youtube_subscription")
                            continue

                    if new_video_items:
                        try:
                            from backend.tasks.subscription_tasks import fetch_youtube_transcripts_batch_task
                            fetch_youtube_transcripts_batch_task.apply_async(
                                args=[new_video_items],
                                queue='youtube_fetching',
                            )
                            info(f"Transcript 拉取任务已派发到家用服务器: {len(new_video_items)} 个视频",
                                 source="youtube_subscription")
                        except Exception as e:
                            warning(f"派发 transcript 任务失败: {str(e)}", source="youtube_subscription")
                    
                    total_videos_fetched += len(raw_videos)
                    total_videos_new += videos_new
                    total_videos_existing += videos_existing
                    channels_success += 1
                    
                    info(f"频道 {channel_info['channel_name']} 完成: new={videos_new}, existing={videos_existing}", 
                         source="youtube_subscription")
                    
                    # 更新订阅统计
                    from backend.database.db_manager import DatabaseManager
                    from backend.utils.database_config import get_database_config
                    
                    db_config = get_database_config()
                    db_manager = DatabaseManager(db_conn_params=db_config)
                    conn = db_manager.get_connection()
                    cursor = conn.cursor()
                    
                    for sub_id in channel_info['subscription_ids']:
                        try:
                            cursor.execute("""
                                UPDATE subscription_list
                                SET 
                                    last_fetched_at = CURRENT_TIMESTAMP,
                                    total_items_fetched = total_items_fetched + %s,
                                    last_fetch_items_count = %s,
                                    consecutive_failures = 0
                                WHERE id = %s
                            """, (videos_new, videos_new, sub_id))
                            
                        except Exception as e:
                            error(f"更新订阅统计失败: subscription_id={sub_id}, error={str(e)}", 
                                 source="youtube_subscription")
                    
                    conn.commit()
                    cursor.close()
                    conn.close()
                    
                except Exception as e:
                    error(f"获取频道 {channel_id} 视频失败: {str(e)}", source="youtube_subscription")
                    channels_failed += 1
                    continue
            
            execution_time = int((time.time() - start_time) * 1000)
            
            info(f"YouTube订阅批量获取完成: channels_success={channels_success}, "
                 f"channels_failed={channels_failed}, videos_new={total_videos_new}, "
                 f"videos_existing={total_videos_existing}, duration={execution_time}ms",
                 source="youtube_subscription")
            
            return {
                'success': True,
                'message': f"成功获取 {channels_success} 个频道的视频",
                'subscriptions_count': len(subscriptions),
                'channels_count': len(channels_map),
                'channels_success': channels_success,
                'channels_failed': channels_failed,
                'videos_fetched': total_videos_fetched,
                'videos_new': total_videos_new,
                'videos_existing': total_videos_existing,
                'execution_duration_ms': execution_time
            }
            
        except Exception as e:
            error(f"批量获取YouTube订阅失败: {str(e)}", source="youtube_subscription")
            return {
                'success': False,
                'message': f'Failed to fetch subscriptions: {str(e)}',
                'subscriptions_count': 0,
                'channels_count': 0,
                'videos_fetched': 0,
                'videos_new': 0,
                'videos_existing': 0,
                'execution_duration_ms': int((time.time() - start_time) * 1000)
            }
