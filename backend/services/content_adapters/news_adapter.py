# -*- coding: utf-8 -*-
"""
News 内容适配器 - 将 EODHD API 新闻数据标准化为 subscription_content 格式
"""

from typing import Dict, Any, List
from datetime import datetime
from urllib.parse import urlparse
from backend.utils.datetime_utils import utcnow
from .base_adapter import BaseContentAdapter


# EODHD 已知来源：domain 片段 → 显示名称
SOURCE_MAPPING = {
    'finance.yahoo.com': 'Yahoo Finance',
    'seekingalpha.com': 'Seeking Alpha',
    'bloomberg.com': 'Bloomberg',
    'reuters.com': 'Reuters',
    'cnbc.com': 'CNBC',
    'marketwatch.com': 'MarketWatch',
    'wsj.com': 'Wall Street Journal',
    'ft.com': 'Financial Times',
    'forbes.com': 'Forbes',
    'businessinsider.com': 'Business Insider',
    'fxstreet.com': 'Fxstreet',
    'nasdaq.com': 'Nasdaq',
    'globenewswire.com': 'Globenewswire',
    'u.today': 'U.Today',
    'actionforex.com': 'Actionforex',
    'dailyhodl.com': 'Dailyhodl',
    'thecurrencyanalytics.com': 'Thecurrencyanalytics',
}


def extract_source_from_url(url: str) -> str:
    """从 URL 中推断来源名称，与 EODHDNewsService._extract_source_from_url 逻辑一致"""
    if not url:
        return 'Unknown'
    try:
        domain = urlparse(url).netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        for domain_key, source_name in SOURCE_MAPPING.items():
            if domain_key in domain:
                return source_name
        return domain.replace('.com', '').replace('.org', '').replace('.net', '').title()
    except Exception:
        return 'Unknown'


def determine_source(article: Dict[str, Any]) -> str:
    """从 EODHD API 文章对象中确定来源名称"""
    source = article.get('source')
    if not source or source == 'EODHD':
        return extract_source_from_url(article.get('link', ''))
    return source


class NewsAdapter(BaseContentAdapter):
    """EODHD News 适配器"""

    @property
    def channel_type(self) -> str:
        return 'news'

    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        将 EODHD API 返回的新闻文章标准化。

        raw_data 结构示例（来自 EODHD /api/news）:
        {
            "date": "2026-03-14T11:06:49+00:00",
            "title": "...",
            "content": "...",
            "link": "https://finance.yahoo.com/news/...",
            "symbols": ["NXT.US"],
            "tags": ["SOLAR-ENERGY", "ESG"],
            "source": "Yahoo Finance",
            "sentiment": {"polarity": 0.99, "neg": 0.02, "neu": 0.88, "pos": 0.09}
        }
        """
        source_name = determine_source(raw_data)
        link = raw_data.get('link', '')
        content_id = link  # link 是天然唯一键

        title = raw_data.get('title', '')
        content = raw_data.get('content', '')

        # 解析发布时间
        date_str = raw_data.get('date', '')
        # 服务器在 HK (UTC+8); fallback 必须用 UTC 与系统契约对齐.
        try:
            published_at = self.parse_datetime(date_str) if date_str else utcnow()
        except Exception:
            published_at = utcnow()

        # 情感数据
        sentiment = raw_data.get('sentiment') or {}

        # symbols 和 tags
        symbols = raw_data.get('symbols', [])
        api_tags = raw_data.get('tags', [])

        metadata = {
            'news_source': 'eodhd',
            'sentiment': {
                'polarity': sentiment.get('polarity'),
                'neg': sentiment.get('neg'),
                'neu': sentiment.get('neu'),
                'pos': sentiment.get('pos'),
            } if sentiment else None,
            'symbols': symbols if symbols else [],
            'api_tags': api_tags if api_tags else [],
        }

        normalized = {
            'channel_type': self.channel_type,
            'content_id': content_id,
            'title': title,
            'content': content,
            'author_name': source_name,
            'author_identifier': source_name,
            'url': link,
            'media_urls': [],
            'published_at': published_at,
            'engagement_count': 0,
            'view_count': 0,
            'metadata': metadata,
            'categories': api_tags if api_tags else [],
        }

        if not self.validate(normalized):
            raise ValueError(f"Invalid news data: missing required fields")

        return normalized
