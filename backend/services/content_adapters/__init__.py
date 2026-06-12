# -*- coding: utf-8 -*-
"""
内容适配器模块 - 将不同渠道的数据格式标准化
"""

from .base_adapter import BaseContentAdapter
from .twitter_adapter import TwitterAdapter
from .youtube_adapter import YouTubeAdapter
from .rss_adapter import RSSAdapter
from .news_adapter import NewsAdapter
from .zsxq_adapter import ZsxqAdapter
from .podcast_adapter import PodcastAdapter

__all__ = [
    'BaseContentAdapter',
    'TwitterAdapter',
    'YouTubeAdapter',
    'RSSAdapter',
    'NewsAdapter',
    'ZsxqAdapter',
    'PodcastAdapter',
]


def get_adapter(channel_type: str) -> BaseContentAdapter:
    """
    获取指定渠道类型的适配器实例

    Args:
        channel_type: 渠道类型 (twitter, youtube, rss, news, zhishixingqiu, podcast)

    Returns:
        对应的适配器实例

    Raises:
        ValueError: 如果渠道类型不支持
    """
    adapters = {
        'twitter': TwitterAdapter(),
        'youtube': YouTubeAdapter(),
        'rss': RSSAdapter(),
        'news': NewsAdapter(),
        'zhishixingqiu': ZsxqAdapter(),
        'podcast': PodcastAdapter(),
    }
    
    adapter = adapters.get(channel_type)
    if not adapter:
        raise ValueError(f"Unsupported channel type: {channel_type}")
    
    return adapter
