# -*- coding: utf-8 -*-
"""
YouTube 内容适配器
"""

import re
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
from backend.utils.datetime_utils import utcnow
from .base_adapter import BaseContentAdapter


# ── Show-name extraction ──────────────────────────────────────────────────────

# Trailing date patterns:  "5/8/2026"  "05/07/2026"  "5/07"  "05/07"
# Also catches typo years with wrong digit count (3-6 digits) to handle editorial mistakes.
_DATE_SUFFIX_RE = re.compile(r'\s+\d{1,2}/\d{1,2}(?:/\d{3,6})?\s*$')

# After date stripping, reject candidates that still contain a date-like fragment
# (e.g. "Open Interest 3/25/20226" where the 5-digit year survives above regex edge cases)
_RESIDUAL_DATE_RE = re.compile(r'\d+/\d+')


def _strip_date(text: str) -> str:
    return _DATE_SUFFIX_RE.sub('', text).strip()


def _looks_like_show_name(text: str) -> bool:
    """Return False if the candidate still contains a date-like fragment after stripping."""
    return not _RESIDUAL_DATE_RE.search(text)


# ── Transcripta alignment (dashboard.cntlabs.io/transcripta.html → podcast_data.js) ──
# Maps raw suffix/prefix extraction → canonical podcast labels used on the Transcripta dashboard.
# Keys must match what _extract_show_name_uncanon returns before normalization.
_TRANSCRIPTA_CANONICAL: Dict[str, Dict[str, str]] = {
    # Bloomberg Television (@markets / UCIALMKvObZNtJ6AmdCLP7Lg)
    'UCIALMKvObZNtJ6AmdCLP7Lg': {
        'The Close': 'Bloomberg The Close',
        'ETF IQ': 'Bloomberg ETF IQ',
        'Balance of Power': 'Bloomberg Balance of Power',
        'Balance of Power: Early Edition': 'Bloomberg Balance of Power',
        'Open Interest': 'Bloomberg Open Interest',
        'Real Yield': 'Bloomberg Real Yield',
        'The Pulse': 'Bloomberg The Pulse',
        'The China Show': 'Bloomberg The China Show',
        'Bloomberg: The China Show': 'Bloomberg The China Show',
        # Regional Horizons feeds roll up to the umbrella podcast on Transcripta
        'Horizons Middle East & Africa': 'Bloomberg Horizons',
        'Bloomberg Tech': 'Bloomberg Technology',
        'Bloomberg Technology': 'Bloomberg Technology',
    },
    # Bloomberg Podcasts
    'UChF5O40UBqAc82I7-i5ig6A': {
        'Odd Lots': 'OddLots',
        'Bloomberg Tech': 'Bloomberg Technology',
    },
    # Goldman Sachs — prefix extraction; map to Transcripta short names where applicable
    'UCyz6-taovlaOkPsPtK4KNEg': {
        'Goldman Sachs Exchanges': 'Exchanges',
        'Talks at GS': 'Talks at GS',
        'The Markets': 'The Markets',
    },
}


def _normalize_show_name_transcripta(channel_id: str, raw: Optional[str]) -> Optional[str]:
    """Apply Transcripta-aligned canonical labels when a mapping exists."""
    if raw is None:
        return None
    table = _TRANSCRIPTA_CANONICAL.get(channel_id)
    if not table:
        return raw
    return table.get(raw, raw)


# Per-channel extraction configuration.
# method values:
#   'suffix'             – show name follows the last " | " separator
#   'suffix_or_prefix'   – try suffix first; fall back to prefix before first " | "
#   'known_prefix'       – title starts with a known show name followed by ": " or " | "
#   None / absent        – no extraction for this channel
_CHANNEL_CONFIGS: Dict[str, Dict] = {
    # Bloomberg Television – dominant format: "Topic | Show [Date]"
    # minority format: "Show | Long topic list"  or  "Show [Date]" (no pipe)
    'UCIALMKvObZNtJ6AmdCLP7Lg': {
        'method': 'suffix_or_prefix',
        # Sponsorship labels and other non-show suffixes to discard:
        'ignore': {'Presented by CME Group'},
        'max_suffix_len': 52,
        'max_prefix_len': 46,
        'standalone_shows': {
            'Bloomberg Surveillance', 'Bloomberg Brief', 'Bloomberg Businessweek Daily',
            'Bloomberg Deals', 'Bloomberg Crypto', 'ETF IQ',
            'Bloomberg Technology', 'Bloomberg Tech',
            'The Close',
        },
    },
    # Bloomberg Podcasts – format: "Topic | Show [Date]"
    'UChF5O40UBqAc82I7-i5ig6A': {
        'method': 'suffix',
        'ignore': set(),
        'max_suffix_len': 62,
    },
    # Bloomberg Originals – format: "Topic | Bloomberg Primer / Bloomberg Investigates / The Deal"
    'UCUMZ7gohGI9HcU9VNsr2FJQ': {
        'method': 'suffix',
        'ignore': {'Bloomberg Originals'},
        'max_suffix_len': 52,
    },
    # Morgan Stanley – format: "Show: Topic" or "Show | Series"
    'UCz6RzD6KG_hH_oHb2kyW5jQ': {
        'method': 'known_prefix',
        'known_shows': {
            'Thoughts on the Market',
            'Hard Lessons',
            'The Morgan Stanley Blueprint',
            'Economic Roundtable',
            'European Financials Conference',
            'Morgan Stanley TMT Conference',
            'Morgan Stanley Innovation Awards',
            'Morgan Stanley Inclusive & Sustainable Ventures',
        },
    },
    # Norges Bank Investment Management – format: "Topic | In Good Company / Investment Conference..."
    'UCRhQsN8AVIfZuBNeRV1A37w': {
        'method': 'suffix',
        'ignore': {'Norges Bank Investment Management', 'NBIM'},
        'max_suffix_len': 52,
    },
    # WSJ News – format: "Topic | WSJ Leadership Institute" (ignore generic "WSJ News" / "WSJ")
    'UCMliswJ7oukCeW35GSayhRA': {
        'method': 'suffix',
        'ignore': {'WSJ News', 'WSJ'},
        'max_suffix_len': 52,
    },
    # The Economist – titles end with "| The Economist" which is just the channel name; skip
    'UC0p5jTq6Xx_DosDFxVXnWaQ': {
        'method': 'suffix',
        'ignore': {'The Economist'},
        'max_suffix_len': 52,
    },
    # Goldman Sachs — Transcripta: "Exchanges", "Talks at GS", "The Markets"
    # Typical titles: "Goldman Sachs Exchanges: ...", "Talks at GS: ..."
    'UCyz6-taovlaOkPsPtK4KNEg': {
        'method': 'known_prefix',
        'known_shows': {
            'Goldman Sachs Exchanges',
            'Talks at GS',
            'The Markets',
        },
    },
}


def _extract_show_name(title: str, channel_id: str) -> Optional[str]:
    raw = _extract_show_name_raw(title, channel_id)
    return _normalize_show_name_transcripta(channel_id, raw)


def _extract_show_name_raw(title: str, channel_id: str) -> Optional[str]:
    """
    Extract a programme / show name from a YouTube video title (raw string, before Transcripta normalization).

    Extraction is channel-aware because naming conventions differ:
    - Bloomberg Television:  "Topic | Show [Date]"  or  "Show | Long Topics"
    - Bloomberg Podcasts:    "Topic | Show [Date]"
    - Bloomberg Originals:   "Topic | Bloomberg Primer"
    - Morgan Stanley:        "Thoughts on the Market: ..."
    - Norges Bank:           "Topic | In Good Company"
    - WSJ News:              "Topic | WSJ Leadership Institute"
    - The Economist:         "| The Economist" suffix → ignored (is the channel name)
    """
    cfg = _CHANNEL_CONFIGS.get(channel_id)
    if cfg is None:
        return None

    method = cfg.get('method')
    ignore: Set[str] = cfg.get('ignore', set())

    # ── suffix ────────────────────────────────────────────────────────────────
    if method in ('suffix', 'suffix_or_prefix'):
        max_suffix = cfg.get('max_suffix_len', 52)

        suffix_rejected_by_ignore = False
        if ' | ' in title:
            # Take text after the LAST " | ", strip trailing date
            suffix = _strip_date(title.rpartition(' | ')[2])
            if suffix in ignore:
                # Suffix is an explicit exclusion (e.g. sponsorship label).
                # Do NOT fall back to prefix — the topic text before "|" is not a show name.
                suffix_rejected_by_ignore = True
            elif (suffix and len(suffix) <= max_suffix and _looks_like_show_name(suffix)):
                return suffix

        if method == 'suffix_or_prefix' and ' | ' in title and not suffix_rejected_by_ignore:
            # Fallback: text before the FIRST " | " (e.g. "Wall Street Week | Long Topics")
            # Only attempt when suffix failed due to length — not when it was explicitly ignored.
            max_prefix = cfg.get('max_prefix_len', 46)
            prefix = _strip_date(title.partition(' | ')[0])
            if (prefix and prefix not in ignore
                    and len(prefix) <= max_prefix
                    and _looks_like_show_name(prefix)):
                return prefix

        # Standalone titles with no pipe – match against a known list
        if method == 'suffix_or_prefix':
            for show in cfg.get('standalone_shows', set()):
                # Title is exactly "Show Name [Date]"
                candidate = _strip_date(title)
                if candidate == show:
                    return show

        return None

    # ── known_prefix ──────────────────────────────────────────────────────────
    if method == 'known_prefix':
        for show in cfg.get('known_shows', set()):
            # "Show: ..."  or  "Show | ..."  or  title == show exactly
            if (
                title.startswith(show + ': ')
                or title.startswith(show + ' | ')
                or title == show
            ):
                return show
        return None

    return None


class YouTubeAdapter(BaseContentAdapter):
    """YouTube 视频适配器"""

    @property
    def channel_type(self) -> str:
        return 'youtube'

    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        将 YouTube API 返回的视频数据标准化
        
        YouTube API 数据结构示例:
        {
            "id": "dQw4w9WgXcQ",
            "snippet": {
                "title": "Video Title",
                "description": "Video description",
                "publishedAt": "2024-01-01T12:00:00Z",
                "channelId": "UCxxxxx",
                "channelTitle": "Channel Name",
                "thumbnails": {"high": {"url": "https://..."}}
            },
            "statistics": {
                "viewCount": "1000000",
                "likeCount": "50000",
                "commentCount": "5000"
            },
            "contentDetails": {
                "duration": "PT3M33S"
            }
        }
        """
        # 提取基本信息
        video_id = raw_data.get('id', '')
        snippet = raw_data.get('snippet', {})
        statistics = raw_data.get('statistics', {})
        content_details = raw_data.get('contentDetails', {})
        
        # 构建标准化数据
        title = snippet.get('title', '')
        description = snippet.get('description', '')
        channel_id = snippet.get('channelId', '')
        channel_title = snippet.get('channelTitle', '')
        
        # 构建视频URL
        url = f"https://www.youtube.com/watch?v={video_id}"
        
        # 提取缩略图URL
        thumbnails = snippet.get('thumbnails', {})
        thumbnail_url = (
            thumbnails.get('maxres', {}).get('url') or
            thumbnails.get('high', {}).get('url') or
            thumbnails.get('medium', {}).get('url') or
            thumbnails.get('default', {}).get('url')
        )
        media_urls = [thumbnail_url] if thumbnail_url else []
        
        # 解析发布时间
        published_at_str = snippet.get('publishedAt', '')
        # 服务器在 HK (UTC+8); fallback 必须用 UTC, 否则与正常路径 (YouTube 返回 UTC) 不一致.
        published_at = self.parse_datetime(published_at_str) if published_at_str else utcnow()
        
        # 统计数据
        view_count = int(statistics.get('viewCount', 0) or 0)
        like_count = int(statistics.get('likeCount', 0) or 0)
        comment_count = int(statistics.get('commentCount', 0) or 0)
        engagement_count = like_count + comment_count
        
        # 构建 metadata
        metadata = {
            'video_id': video_id,
            'channel_id': channel_id,
            'show_name': _extract_show_name(title, channel_id),
            'duration': content_details.get('duration', ''),
            'view_count': view_count,
            'like_count': like_count,
            'comment_count': comment_count,
            'definition': content_details.get('definition', 'sd'),
            'caption': content_details.get('caption', 'false'),
            'category_id': snippet.get('categoryId', ''),
            'tags': snippet.get('tags', []),
            'live_broadcast_content': snippet.get('liveBroadcastContent', 'none'),
        }
        
        # 返回标准化数据
        normalized = {
            'channel_type': self.channel_type,
            'content_id': video_id,
            'title': title,
            'content': description,
            'author_name': channel_title,
            'author_identifier': channel_id,
            'url': url,
            'media_urls': media_urls,
            'published_at': published_at,
            'engagement_count': engagement_count,
            'view_count': view_count,
            'metadata': metadata,
        }
        
        # 验证数据完整性
        if not self.validate(normalized):
            raise ValueError(f"Invalid YouTube data: missing required fields")
        
        return normalized
