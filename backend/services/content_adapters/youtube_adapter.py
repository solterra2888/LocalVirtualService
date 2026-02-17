# -*- coding: utf-8 -*-
"""
YouTube 内容适配器
"""

from typing import Dict, Any, List
from datetime import datetime
from .base_adapter import BaseContentAdapter


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
        published_at = self.parse_datetime(published_at_str) if published_at_str else datetime.now()
        
        # 统计数据
        view_count = int(statistics.get('viewCount', 0) or 0)
        like_count = int(statistics.get('likeCount', 0) or 0)
        comment_count = int(statistics.get('commentCount', 0) or 0)
        engagement_count = like_count + comment_count
        
        # 构建 metadata
        metadata = {
            'video_id': video_id,
            'channel_id': channel_id,
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
