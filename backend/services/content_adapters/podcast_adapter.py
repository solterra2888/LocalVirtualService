# -*- coding: utf-8 -*-
"""
Podcast 内容适配器

处理 feedparser 解析后的 Podcast RSS 条目，将其标准化为统一格式。
RSS Feed 数据由 PodcastSubscriptionService 通过 feedparser.parse() 获取，
每个 entry 会被注入额外的 _feed_url 和 _feed_title 字段用于标识来源。

Podcast RSS Feed entry 结构示例 (feedparser 解析后):
{
    "id": "https://cnbc.simplecastaudio.com/...",
    "title": "Closing Bell Overtime: Stocks Sell Off Into Close As Chip Weakness Continues 6/10/26",
    "summary": "Episode description...",
    "link": "https://podcasts.apple.com/...",
    "published": "Wed, 10 Jun 2026 21:17:10 +0000",
    "enclosures": [{"href": "https://dts.podtrac.com/redirect.mp3/...", "type": "audio/mpeg", "length": "41810496"}],
    "itunes_duration": "00:43:31",
    "tags": [{"term": "Investing"}, {"term": "Business"}],
    "_feed_url": "https://feeds.simplecast.com/Nh1wIaXT",
    "_feed_title": "Closing Bell",
}
"""

import re
from typing import Dict, Any, List, Optional
from backend.utils.datetime_utils import utcnow
from .base_adapter import BaseContentAdapter


def _parse_itunes_duration(duration_str: Optional[str]) -> Optional[int]:
    """
    解析 itunes:duration 字段为秒数。

    支持的格式:
    - HH:MM:SS  → "1:23:45" = 5025
    - MM:SS     → "45:30"   = 2730
    - 纯秒数     → "3600"    = 3600
    - ISO 8601  → "PT1H23M45S" = 5025
    """
    if not duration_str:
        return None

    duration_str = duration_str.strip()

    # ISO 8601 duration: PT1H23M45S
    iso_match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration_str)
    if iso_match:
        hours = int(iso_match.group(1) or 0)
        minutes = int(iso_match.group(2) or 0)
        seconds = int(iso_match.group(3) or 0)
        return hours * 3600 + minutes * 60 + seconds

    # HH:MM:SS or MM:SS
    colon_match = re.match(r'^(\d+):(\d+)(?::(\d+))?$', duration_str)
    if colon_match:
        parts = [int(g) for g in colon_match.groups() if g is not None]
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        elif len(parts) == 2:
            return parts[0] * 60 + parts[1]

    # 纯秒数
    try:
        val = int(duration_str)
        return val
    except ValueError:
        pass

    return None


class PodcastAdapter(BaseContentAdapter):
    """Podcast RSS Feed 适配器"""

    @property
    def channel_type(self) -> str:
        return 'podcast'

    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        将 Podcast RSS entry 数据标准化。

        raw_data 是 feedparser 解析后的单个 entry，由 PodcastSubscriptionService
        注入了 _feed_url 和 _feed_title 字段。
        """
        # 提取基本信息
        content_id = raw_data.get('id') or raw_data.get('guid', '')
        title = raw_data.get('title', '')

        # 内容: summary > description > title fallback
        summary = raw_data.get('summary', '')
        description = raw_data.get('description', '')
        content_list = raw_data.get('content', [])
        full_content = content_list[0].get('value', '') if content_list else ''
        content = full_content or summary or description or title

        # 作者信息：由 service 层注入的 feed 级别信息
        feed_url = raw_data.get('_feed_url', '')
        feed_title = raw_data.get('_feed_title', '')
        author_name = feed_title or raw_data.get('author', '')
        author_identifier = feed_url  # 使用 RSS Feed URL 作为唯一标识

        # 链接
        url = raw_data.get('link', '')

        # 提取音频 enclosure URL
        media_urls: List[str] = []
        audio_url = ''
        audio_size_bytes = 0
        audio_content_type = ''
        enclosures = raw_data.get('enclosures', [])
        if enclosures:
            enc = enclosures[0]
            audio_url = enc.get('href', '')
            audio_content_type = enc.get('type', '')
            try:
                audio_size_bytes = int(enc.get('length', 0) or 0)
            except (ValueError, TypeError):
                audio_size_bytes = 0
            if audio_url and audio_url.startswith('http'):
                media_urls.append(audio_url)

        # 解析发布时间
        published_str = raw_data.get('published') or raw_data.get('pubDate') or raw_data.get('updated', '')
        try:
            published_at = self.parse_datetime(published_str) if published_str else utcnow()
        except Exception:
            published_at = utcnow()

        # 解析 itunes:duration → duration_seconds
        duration_raw = raw_data.get('itunes_duration', '')
        duration_seconds = _parse_itunes_duration(duration_raw)

        # Podcast 通常没有互动数据
        engagement_count = 0
        view_count = 0

        # 提取分类/标签
        categories = []
        for tag in raw_data.get('tags', []):
            if isinstance(tag, dict):
                term = tag.get('term', '')
                if term:
                    categories.append(term)
            elif isinstance(tag, str) and tag:
                categories.append(tag)

        # 构建 metadata
        metadata = {
            'audio_url': audio_url,
            'audio_size_bytes': audio_size_bytes,
            'audio_content_type': audio_content_type,
            'itunes_duration_raw': duration_raw,
            'guid': raw_data.get('id') or raw_data.get('guid', ''),
        }

        # itunes:episodeType 用于区分 full/trailer/bonus
        episode_type = raw_data.get('itunes_episodetype', 'full')
        if episode_type and episode_type != 'full':
            metadata['episode_type'] = episode_type

        # itunes:episode / itunes:season
        episode_number = raw_data.get('itunes_episode')
        if episode_number:
            metadata['episode_number'] = int(episode_number)
        season_number = raw_data.get('itunes_season')
        if season_number:
            metadata['season_number'] = int(season_number)

        # 返回标准化数据
        normalized = {
            'channel_type': self.channel_type,
            'content_id': content_id,
            'title': title,
            'content': content,
            'author_name': author_name,
            'author_identifier': author_identifier,
            'url': url,
            'media_urls': media_urls,
            'published_at': published_at,
            'engagement_count': engagement_count,
            'view_count': view_count,
            'metadata': metadata,
            # 复用已有列
            'duration_seconds': duration_seconds,
            'feed_url': feed_url,
            'categories': categories,
        }

        # 验证数据完整性
        if not self.validate(normalized):
            raise ValueError(f"Invalid Podcast data: missing required fields")

        return normalized
