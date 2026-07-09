# -*- coding: utf-8 -*-
"""
Substack 内容适配器（文章 post + 动态 note）

将抓取的 Substack 内容（半标准化 dict）映射为 subscription_content 统一格式。
raw_data["_content_kind"] ∈ {'post', 'note'} 决定映射分支：

post（文章）:
  content_id        = str(post_id)
  channel_type      = 'substack'
  author_identifier = publication_url   （必须等于 subscription_list.channel_identifier）
  author_name       = publication_name
  title             = meta.title
  content           = body_html 全文（付费全文依赖有效 Cookie）
  url               = canonical_url
  published_at      = post_date
  metadata          = {content_kind, publication_url, publication_name, author, audience, is_paywalled, subtitle}

note（动态）:
  content_id        = f"note_{note_id}"   （前缀避免与 post_id 数值冲突）
  author_identifier = publication_url（作者主刊；无主刊为 https://substack.com）
  author_name       = author_name
  title             = body_text[:60]
  content           = body_text
  url               = note_url
  published_at      = context.timestamp
  metadata          = {content_kind, publication_url, author_user_id, author_handle, note_type}
"""

from datetime import datetime, timezone
from typing import Any, Dict, List

from .base_adapter import BaseContentAdapter
from backend.utils.logger import get_logger

logger = get_logger("substack_adapter")


def _parse_dt(value: Any) -> datetime:
    if value is None:
        return datetime.now(tz=timezone.utc)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    if not s:
        return datetime.now(tz=timezone.utc)
    if s.isdigit() and len(s) >= 12:
        try:
            return datetime.fromtimestamp(int(s) / 1000.0, tz=timezone.utc)
        except (ValueError, OSError):
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return datetime.now(tz=timezone.utc)


class SubstackAdapter(BaseContentAdapter):
    """Substack 适配器（post / note 双分支）。"""

    @property
    def channel_type(self) -> str:
        return "substack"

    def normalize(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        kind = (raw_data.get("_content_kind") or "post").lower()
        if kind == "note":
            return self._normalize_note(raw_data)
        return self._normalize_post(raw_data)

    def _normalize_post(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        pub_url = str(raw_data.get("publication_url") or "")
        pub_name = str(raw_data.get("publication_name") or "")
        post_id = raw_data.get("post_id")
        content_id = str(post_id) if post_id is not None else ""

        title = (raw_data.get("title") or "").strip()
        content = raw_data.get("content") or ""

        return {
            "channel_type": "substack",
            "content_id": content_id,
            "title": title,
            "content": content,
            "author_name": pub_name,
            "author_identifier": pub_url,
            "url": raw_data.get("canonical_url") or "",
            "media_urls": [],
            "published_at": _parse_dt(raw_data.get("post_date")),
            "engagement_count": 0,
            "view_count": 0,
            "metadata": {
                "content_kind": "post",
                "publication_url": pub_url,
                "publication_name": pub_name,
                "author": raw_data.get("author") or "",
                "audience": raw_data.get("audience") or "",
                "is_paywalled": bool(raw_data.get("is_paywalled")),
                "subtitle": raw_data.get("subtitle") or "",
            },
        }

    def _normalize_note(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        note_id = raw_data.get("note_id")
        content_id = f"note_{note_id}" if note_id is not None else ""

        pub_url = str(raw_data.get("publication_url") or "https://substack.com")
        body_text = (raw_data.get("body_text") or "").strip()
        title = body_text[:60] if body_text else ""

        return {
            "channel_type": "substack",
            "content_id": content_id,
            "title": title,
            "content": body_text,
            "author_name": raw_data.get("author_name") or "",
            "author_identifier": pub_url,
            "url": raw_data.get("note_url") or "",
            "media_urls": [],
            "published_at": _parse_dt(raw_data.get("published_at")),
            "engagement_count": 0,
            "view_count": 0,
            "metadata": {
                "content_kind": "note",
                "publication_url": pub_url,
                "author_user_id": raw_data.get("author_user_id"),
                "author_handle": raw_data.get("author_handle") or "",
                "note_type": raw_data.get("note_type") or "",
            },
        }
