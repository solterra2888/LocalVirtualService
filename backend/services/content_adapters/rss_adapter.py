# -*- coding: utf-8 -*-
"""
RSS 内容适配器
"""

from typing import Dict, Any, List
from datetime import datetime
from backend.utils.datetime_utils import utcnow
from .base_adapter import BaseContentAdapter


class RSSAdapter(BaseContentAdapter):
    """RSS Feed 适配器"""
    
    @property
    def channel_type(self) -> str:
        return 'rss'
    
    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        将 RSS Feed 条目数据标准化
        
        RSS 数据结构示例 (使用 feedparser 解析后):
        {
            "id": "https://example.com/post/123",
            "title": "Post Title",
            "summary": "Post summary or description",
            "link": "https://example.com/post/123",
            "author": "Author Name",
            "published": "Mon, 01 Jan 2024 12:00:00 GMT",
            "tags": [{"term": "Technology"}, {"term": "AI"}],
            "enclosures": [{"href": "https://example.com/audio.mp3", "type": "audio/mpeg"}],
            "content": [{"value": "Full content HTML"}]
        }
        """
        # 提取基本信息
        content_id = raw_data.get('id') or raw_data.get('guid') or raw_data.get('link', '')
        title = raw_data.get('title', '')
        
        # 内容可能在 summary, description, 或 content 中
        summary = raw_data.get('summary', '')
        description = raw_data.get('description', '')
        content_list = raw_data.get('content', [])
        full_content = content_list[0].get('value', '') if content_list else ''
        content = full_content or summary or description or ''
        
        # 作者信息
        author = raw_data.get('author', '') or raw_data.get('author_detail', {}).get('name', '')
        # RSS 通常没有唯一的作者标识，使用名称作为标识
        author_identifier = author or 'unknown'
        
        # 链接
        url = raw_data.get('link', '')
        
        # 提取媒体URL（enclosures, media:content, etc.）
        media_urls = self._extract_rss_media_urls(raw_data)
        
        # 解析发布时间
        published_str = raw_data.get('published') or raw_data.get('pubDate') or raw_data.get('updated', '')
        # 服务器在 HK (UTC+8); fallback 必须用 UTC 与系统契约对齐.
        try:
            published_at = self.parse_datetime(published_str) if published_str else utcnow()
        except Exception:
            published_at = utcnow()
        
        # RSS 通常没有互动数据
        engagement_count = 0
        view_count = 0
        
        # 提取分类/标签
        tags = []
        for tag in raw_data.get('tags', []):
            if isinstance(tag, dict):
                tags.append(tag.get('term', ''))
            elif isinstance(tag, str):
                tags.append(tag)
        
        # 构建 metadata
        metadata = {
            'guid': raw_data.get('id') or raw_data.get('guid', ''),
            'feed_url': raw_data.get('feed_url', ''),
            'feed_title': raw_data.get('feed_title', ''),
            'categories': tags,
            'enclosures': [
                {
                    'url': enc.get('href', ''),
                    'type': enc.get('type', ''),
                    'length': enc.get('length', 0)
                }
                for enc in raw_data.get('enclosures', [])
            ],
            'comments': raw_data.get('comments', ''),
        }
        
        # 返回标准化数据
        normalized = {
            'channel_type': self.channel_type,
            'content_id': content_id,
            'title': title,
            'content': content,
            'author_name': author,
            'author_identifier': author_identifier,
            'url': url,
            'media_urls': media_urls,
            'published_at': published_at,
            'engagement_count': engagement_count,
            'view_count': view_count,
            'metadata': metadata,
        }
        
        # 验证数据完整性
        if not self.validate(normalized):
            raise ValueError(f"Invalid RSS data: missing required fields")
        
        return normalized
    
    def _extract_rss_media_urls(self, feed_entry: Dict[str, Any]) -> List[str]:
        """从 RSS 条目中提取媒体URL"""
        urls = []
        
        # 从 enclosures 提取
        for enclosure in feed_entry.get('enclosures', []):
            if isinstance(enclosure, dict):
                href = enclosure.get('href', '')
                if href and href.startswith('http'):
                    urls.append(href)
        
        # 从 media:content 提取（如果使用 feedparser）
        media_content = feed_entry.get('media_content', [])
        for media in media_content:
            if isinstance(media, dict):
                url = media.get('url', '')
                if url and url.startswith('http'):
                    urls.append(url)
        
        # 从 media:thumbnail 提取
        media_thumbnail = feed_entry.get('media_thumbnail', [])
        for thumbnail in media_thumbnail:
            if isinstance(thumbnail, dict):
                url = thumbnail.get('url', '')
                if url and url.startswith('http'):
                    urls.append(url)
        
        # 去重
        return list(set(urls))
