# -*- coding: utf-8 -*-
"""
Twitter 内容适配器
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from backend.utils.datetime_utils import utcnow, ensure_utc
from backend.utils.logger import get_logger
from .base_adapter import BaseContentAdapter

logger = get_logger("twitter_adapter")

# Twitter Snowflake epoch: Nov 4, 2010 00:00:00 UTC (in milliseconds)
_TWITTER_EPOCH_MS = 1288834974657

def _snowflake_to_utc(tweet_id_str: str) -> Optional[datetime]:
    """
    从 Twitter Snowflake ID 中提取精确的创建时间（UTC aware）。
    Snowflake ID 高 41 位编码了距 Twitter Epoch (2010-11-04 UTC) 的毫秒数，
    无论 API 返回的 createdAt 时区标签是否正确，ID 本身不会撒谎。
    """
    try:
        ms = (int(tweet_id_str) >> 22) + _TWITTER_EPOCH_MS
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    except (ValueError, OverflowError, TypeError):
        return None


class TwitterAdapter(BaseContentAdapter):
    """Twitter 推文适配器"""
    
    @property
    def channel_type(self) -> str:
        return 'twitter'
    
    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        将 Twitter API 返回的推文数据标准化
        
        Twitter API 数据结构示例:
        {
            "id": "1234567890",
            "text": "This is a tweet",
            "author": {"name": "User Name", "username": "username"},
            "createdAt": "2024-01-01T12:00:00.000Z",
            "url": "https://twitter.com/username/status/1234567890",
            "likeCount": 100,
            "retweetCount": 50,
            "replyCount": 25,
            "extendedEntities": {
                "media": [{"media_url_https": "https://..."}]
            }
        }
        """
        # 提取基本信息
        content_id = raw_data.get('id', '')
        text = raw_data.get('text', '')
        
        # 提取作者信息
        author = raw_data.get('author', {})
        author_name = author.get('name', '')
        author_identifier = author.get('username', author.get('screen_name', ''))
        
        # 提取URL
        url = raw_data.get('url', '')
        
        # 如果 author_identifier 为空，尝试从 URL 中提取
        if not author_identifier and url:
            # URL 格式: https://twitter.com/username/status/id 或 https://x.com/username/status/id
            if 'twitter.com/' in url or 'x.com/' in url:
                parts = url.split('/')
                if len(parts) >= 4:
                    author_identifier = parts[3]  # username 在第4部分
        
        # 如果还是没有 URL，尝试构建
        if not url and author_identifier and content_id:
            url = f"https://twitter.com/{author_identifier}/status/{content_id}"
        
        # 提取媒体URL
        media_urls = self._extract_twitter_media_urls(raw_data)
        
        # 解析发布时间。
        # 防御性兜底1：twitterapi.io 返回的 createdAt 偶发遇到无 tz 标记的字符串，
        # 此时 dateutil.parser 会返回 naive datetime；后续 psycopg2 全局适配器会把
        # naive 视为 HK 本地，再转 UTC 入库 → 比真实时间早 8h。
        # 用 ensure_utc 强制规整为 aware UTC，规避这个坑。fallback 直接走 utcnow()。
        #
        # 防御性兜底2（twitterapi.io +0000 mislabel bug）：
        # twitterapi.io 偶发将推文的 HK 本地时间（UTC+8）错误标记为 +0000 UTC，
        # 导致 createdAt 比真实 UTC 多 8h。
        # 修复方法：用 Tweet Snowflake ID 提取真实创建时间并与 createdAt 比对。
        # Snowflake ID 的高 41 位编码了距 Twitter Epoch 的毫秒数，
        # 无论 API 返回的时区标签是否正确，ID 本身不会撒谎。
        # 若偏差超过 60 秒，以 Snowflake 时间为准。
        _SNOWFLAKE_MAX_DIFF_SECONDS = 60
        created_at_str = raw_data.get('createdAt', '')
        if created_at_str:
            published_at = ensure_utc(self.parse_datetime(created_at_str))
            snowflake_time = _snowflake_to_utc(content_id)
            if snowflake_time is not None:
                diff_seconds = (published_at - snowflake_time).total_seconds()
                if abs(diff_seconds) > _SNOWFLAKE_MAX_DIFF_SECONDS:
                    logger.warning(
                        "twitterapi.io createdAt/Snowflake mismatch: using Snowflake time",
                        content_id=content_id,
                        created_at=published_at.isoformat(),
                        snowflake_time=snowflake_time.isoformat(),
                        diff_seconds=round(diff_seconds),
                    )
                    published_at = snowflake_time
        else:
            published_at = utcnow()
        
        # 计算互动数
        like_count = raw_data.get('likeCount', 0) or 0
        retweet_count = raw_data.get('retweetCount', 0) or 0
        reply_count = raw_data.get('replyCount', 0) or 0
        quote_count = raw_data.get('quoteCount', 0) or 0
        engagement_count = like_count + retweet_count + reply_count + quote_count
        
        # 提取转发信息（重要业务字段，需要直接查询）
        retweeted_tweet = raw_data.get('retweeted_tweet')
        is_retweet = bool(retweeted_tweet)
        retweeted_author = None
        retweeted_content = None
        retweeted_url = None
        
        if retweeted_tweet:
            retweeted_author_obj = retweeted_tweet.get('author', {})
            retweeted_author = retweeted_author_obj.get('name') or retweeted_author_obj.get('username', '')
            retweeted_content = retweeted_tweet.get('text', '')
            retweeted_url = retweeted_tweet.get('url', '')
        
        # 提取引用信息（重要业务字段，需要直接查询）
        quoted_tweet = raw_data.get('quoted_tweet')
        is_quote = bool(quoted_tweet)
        quoted_author = None
        quoted_content = None
        quoted_url = None
        
        if quoted_tweet:
            quoted_author_obj = quoted_tweet.get('author', {})
            quoted_author = quoted_author_obj.get('name') or quoted_author_obj.get('username', '')
            quoted_content = quoted_tweet.get('text', '')
            quoted_url = quoted_tweet.get('url', '')
        
        # 提取回复信息（重要业务字段，用于显示对话上下文）
        is_reply = raw_data.get('isReply', False)
        in_reply_to_id = raw_data.get('inReplyToId') or None
        in_reply_to_username = raw_data.get('inReplyToUsername') or None
        in_reply_to_user_id = raw_data.get('inReplyToUserId') or None
        
        # 🔧 修复：如果 API 没有返回 inReplyToUsername，尝试从推文文本中提取
        # Twitter 回复推文通常以 @username 开头
        if is_reply and not in_reply_to_username and text:
            import re
            # 匹配推文开头的 @username
            match = re.match(r'^@(\w+)', text)
            if match:
                in_reply_to_username = match.group(1)
                # 记录日志（可选）
                # logger.info(f"从推文文本提取 in_reply_to_username: {in_reply_to_username}")
        
        # 构建 metadata（存储不常用的扩展字段）
        metadata = {
            'tweet_id': content_id,
            'like_count': like_count,
            'retweet_count': retweet_count,
            'reply_count': reply_count,
            'quote_count': quote_count,
            'retweeted_tweet_id': retweeted_tweet.get('id') if retweeted_tweet else None,
            'quoted_tweet_id': quoted_tweet.get('id') if quoted_tweet else None,
            'conversation_id': raw_data.get('conversationId', ''),
            'lang': raw_data.get('lang', ''),
            'source': raw_data.get('source', ''),
        }
        
        # 返回标准化数据
        normalized = {
            'channel_type': self.channel_type,
            'content_id': content_id,
            'title': None,  # Twitter 无标题
            'content': text,
            'author_name': author_name,
            'author_identifier': author_identifier,
            'url': url,
            'media_urls': media_urls,
            'published_at': published_at,
            'engagement_count': engagement_count,
            'view_count': 0,  # Twitter API 不提供浏览数
            'metadata': metadata,
            # Twitter 特定字段
            'is_retweet': is_retweet,
            'retweeted_author': retweeted_author,
            'retweeted_content': retweeted_content,
            'retweeted_url': retweeted_url,
            'is_quote': is_quote,
            'quoted_author': quoted_author,
            'quoted_content': quoted_content,
            'quoted_url': quoted_url,
            # Reply 特定字段
            'is_reply': is_reply,
            'in_reply_to_id': in_reply_to_id,
            'in_reply_to_username': in_reply_to_username,
            'in_reply_to_user_id': in_reply_to_user_id,
        }
        
        # 验证数据完整性
        if not self.validate(normalized):
            raise ValueError(f"Invalid Twitter data: missing required fields")
        
        return normalized
    
    def _extract_twitter_media_urls(self, tweet_data: Dict[str, Any]) -> List[str]:
        """
        从推文数据中提取所有媒体URL
        
        处理 extendedEntities, entities, retweeted_tweet, quoted_tweet 等结构
        """
        urls = []
        
        def extract_from_entities(entities):
            """从 entities 或 extendedEntities 中提取媒体URL"""
            if not entities:
                return
            
            media_list = entities.get('media', [])
            for media in media_list:
                if isinstance(media, dict):
                    # 优先使用高清图
                    media_url = media.get('media_url_https') or media.get('media_url')
                    if media_url:
                        urls.append(media_url)
        
        # 主推文的媒体
        extract_from_entities(tweet_data.get('extendedEntities'))
        extract_from_entities(tweet_data.get('entities'))
        
        # 转推推文的媒体
        retweeted = tweet_data.get('retweeted_tweet')
        if retweeted:
            extract_from_entities(retweeted.get('extendedEntities'))
            extract_from_entities(retweeted.get('entities'))
        
        # 引用推文的媒体
        quoted = tweet_data.get('quoted_tweet')
        if quoted:
            extract_from_entities(quoted.get('extendedEntities'))
            extract_from_entities(quoted.get('entities'))
        
        # 去重
        return list(set(urls))
