# -*- coding: utf-8 -*-
"""
知识星球内容适配器

将抓取的知识星球话题（topic）标准化为 subscription_content 统一格式。

字段映射规则：
  content_id        = str(topic_id)
  channel_type      = 'zhishixingqiu'
  author_identifier = group_id       （必须等于 subscription_list.channel_identifier）
  author_name       = group_name     （展示名为星球名称）
  title             = topic.title 或空
  content           = 清洗后的正文文本
  url               = https://wx.zsxq.com/dweb2/index/topic_detail/{topic_id}
  media_urls        = 图片 URL 列表
  published_at      = create_time
  metadata          = { group_id, group_name, post_author, files, type }
"""

import re
from datetime import datetime, timezone
from typing import Any, Dict, List

from .base_adapter import BaseContentAdapter
from backend.utils.logger import get_logger

logger = get_logger("zsxq_adapter")


def _strip_html_tags(text: str) -> str:
    """清洗知识星球特有的 <e> 标签（表情/话题标签等）。"""
    if not text:
        return ""
    text = re.sub(r"<e[^>]*>", "", text)
    text = re.sub(r"</e>", "", text)
    return text.strip()


class ZsxqAdapter(BaseContentAdapter):
    """知识星球话题适配器。

    ``raw_data`` 格式为 ``extract_content()`` 函数的输出（已半标准化）：
    {
        "topic_id": int,
        "create_time": "2026-06-01T10:00:00.000+0800",
        "type": "talk" | "question" | "task",
        "title": str,
        "author": str,           # 帖子作者（非星球名）
        "text": str,             # 已去除 <e> 标签的正文
        "images": [url, ...],
        "files": ["filename.pdf", ...],
        # 以下两字段由 ZsxqSubscriptionService 补充
        "group_id": str,
        "group_name": str,
    }
    """

    @property
    def channel_type(self) -> str:
        return "zhishixingqiu"

    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        group_id = str(raw_data.get("group_id") or "")
        group_name = str(raw_data.get("group_name") or "")
        topic_id = raw_data.get("topic_id")
        content_id = str(topic_id) if topic_id is not None else ""

        title = _strip_html_tags(raw_data.get("title") or "")
        content = _strip_html_tags(raw_data.get("text") or "")
        post_author = raw_data.get("author") or ""
        media_urls: List[str] = list(raw_data.get("images") or [])
        files: List[str] = list(raw_data.get("files") or [])

        # 发布时间解析（知识星球时间带 +0800）
        create_time_raw = raw_data.get("create_time") or ""
        published_at = self._parse_zsxq_time(create_time_raw)

        url = f"https://wx.zsxq.com/dweb2/index/topic_detail/{content_id}" if content_id else ""

        return {
            "channel_type": "zhishixingqiu",
            "content_id": content_id,
            "title": title,
            "content": content,
            "author_name": group_name,
            "author_identifier": group_id,
            "author_avatar": (
                str(raw_data.get("avatar") or raw_data.get("author_avatar") or "").strip()
                or None
            ),
            "url": url,
            "media_urls": media_urls,
            "published_at": published_at,
            "engagement_count": 0,
            "view_count": 0,
            "metadata": {
                "group_id": group_id,
                "group_name": group_name,
                "post_author": post_author,
                "files": files,
                "topic_type": raw_data.get("type") or "talk",
            },
        }

    @staticmethod
    def _parse_zsxq_time(value: str) -> datetime:
        if not value:
            return datetime.now(tz=timezone.utc)
        try:
            return datetime.fromisoformat(value.replace("+0800", "+08:00"))
        except (ValueError, TypeError):
            return datetime.now(tz=timezone.utc)
