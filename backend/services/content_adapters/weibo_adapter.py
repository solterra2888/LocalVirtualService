# -*- coding: utf-8 -*-
"""
微博内容适配器

将 crawl4weibo 抓取的微博博文（经 WeiboSubscriptionService._extract_post 转为 dict）
标准化为 subscription_content 统一格式。

字段映射规则：
  content_id        = str(post_id)              （Post.id 已是 str）
  channel_type      = 'weibo'
  author_identifier = uid                        （必须等于 subscription_list.channel_identifier）
  author_name       = screen_name
  content           = text（最小清洗，去残留 HTML 标签）
  url               = https://weibo.com/{uid}/{bid}
  media_urls        = pic_urls
  published_at      = created_at（crawl4weibo 已解析为 datetime，统一转 UTC）
  engagement_count  = reposts_count + comments_count + attitudes_count
  metadata          = { post_id, bid, uid, source, video_url, is_original, *_count }
  转发（is_original=False）→ 复用 Twitter 的 is_retweet / retweeted_author /
                             retweeted_content / retweeted_url 列（无需改表）
"""

import re
from datetime import datetime, timezone
from typing import Any, Dict, List

from .base_adapter import BaseContentAdapter
from backend.utils.logger import get_logger

logger = get_logger("weibo_adapter")


def _strip_html(text: str) -> str:
    """微博正文偶含 <br>/<a> 残留标签，做最小清洗。"""
    if not text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def _to_utc(dt: Any) -> datetime:
    """确保 datetime 带时区。crawl4weibo 返回的 created_at 可能 naive（按本地时间）。"""
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return datetime.now(tz=timezone.utc)


class WeiboAdapter(BaseContentAdapter):
    """微博博文适配器。

    ``raw_data`` 由 ``WeiboSubscriptionService._extract_post`` 产出：
        {
            "post_id": str,            # Post.id
            "bid": str,                # Post.bid
            "uid": str,                # 博主 UID（== channel_identifier）
            "screen_name": str,
            "avatar": str,             # 博主头像（来自 weibo_accounts）
            "text": str,
            "created_at": datetime | None,
            "source": str,             # 发博客户端（iPhone/安卓/...）
            "pic_urls": [url, ...],
            "video_url": str,
            "is_original": bool,
            "reposts_count": int, "comments_count": int, "attitudes_count": int,
            # 仅转发帖：
            "retweet_uid": str, "retweet_bid": str, "retweet_text": str,
        }
    """

    @property
    def channel_type(self) -> str:
        return "weibo"

    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        uid = str(raw_data.get("uid") or "")
        screen_name = str(raw_data.get("screen_name") or "")
        post_id = raw_data.get("post_id")
        content_id = str(post_id) if post_id is not None else ""
        bid = str(raw_data.get("bid") or "")

        content = _strip_html(raw_data.get("text") or "")
        media_urls: List[str] = list(raw_data.get("pic_urls") or [])

        reposts = int(raw_data.get("reposts_count") or 0)
        comments = int(raw_data.get("comments_count") or 0)
        attitudes = int(raw_data.get("attitudes_count") or 0)

        published_at = _to_utc(raw_data.get("created_at"))

        if uid and bid:
            url = f"https://weibo.com/{uid}/{bid}"
        elif uid:
            url = f"https://weibo.com/u/{uid}"
        else:
            url = ""

        is_original = bool(raw_data.get("is_original", True))
        retweet_text = ""
        retweet_uid = ""
        retweet_bid = ""
        if not is_original:
            retweet_text = _strip_html(raw_data.get("retweet_text") or "")
            retweet_uid = str(raw_data.get("retweet_uid") or "")
            retweet_bid = str(raw_data.get("retweet_bid") or "")

        retweeted_url = (
            f"https://weibo.com/{retweet_uid}/{retweet_bid}"
            if retweet_uid and retweet_bid
            else None
        )

        return {
            "channel_type": "weibo",
            "content_id": content_id,
            "title": "",  # 微博无标题
            "content": content,
            "author_name": screen_name,
            "author_identifier": uid,
            "author_avatar": raw_data.get("avatar") or None,
            "url": url,
            "media_urls": media_urls,
            "published_at": published_at,
            "engagement_count": reposts + comments + attitudes,
            "view_count": 0,
            "metadata": {
                "post_id": content_id,
                "bid": bid,
                "uid": uid,
                "source": raw_data.get("source") or "",
                "video_url": raw_data.get("video_url") or "",
                "is_original": is_original,
                "reposts_count": reposts,
                "comments_count": comments,
                "attitudes_count": attitudes,
            },
            # 转发映射到 Twitter 的转推列（subscription_content 已有这些列）
            "is_retweet": (not is_original),
            "retweeted_author": retweet_uid or None,
            "retweeted_content": retweet_text or None,
            "retweeted_url": retweeted_url,
            "is_quote": False,
            "quoted_author": None,
            "quoted_content": None,
            "quoted_url": None,
            "is_reply": False,
            "in_reply_to_id": None,
            "in_reply_to_username": None,
            "in_reply_to_user_id": None,
        }
