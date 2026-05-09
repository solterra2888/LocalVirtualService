# -*- coding: utf-8 -*-
"""
订阅 Digest 服务 — 把指定订阅 N 天内的主要内容拼接成纯文本，
供用户复制给 Chrome Gemini / LLM 插件做二次分析。

主体内容映射：
  twitter → content (+ retweet/quote/reply 结构化前缀)
  youtube → transcript_cleaned → transcript → content (描述) 三级回退
             若三者均为空则跳过该条 (计入 skipped_count)
  news    → content → url 兜底

拼接格式（每条）：
  ─── [{idx}] {date} · {show_name_or_channel_name} — {title} ───
  {main_body}

  (空行分隔)
"""

from __future__ import annotations

import textwrap
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

MAX_CHARS_PER_ITEM_DEFAULT = 12_000
MAX_TOTAL_CHARS_DEFAULT = 400_000
QUERY_ROW_LIMIT = 500  # 安全上限，避免一次拉太多行


def build_digest(
    subscription: Dict[str, Any],
    rows: List[Dict[str, Any]],
    *,
    max_chars_per_item: int = MAX_CHARS_PER_ITEM_DEFAULT,
    max_total_chars: int = MAX_TOTAL_CHARS_DEFAULT,
    days: int = 1,
    since: datetime,
) -> Dict[str, Any]:
    """
    将数据库行列表拼接为 Digest 文本。

    Parameters
    ----------
    subscription : dict
        subscription_list 行（须含 channel_name / channel_identifier / channel_type）
    rows : list[dict]
        subscription_content 行列表（见 _fetch_rows 中的列名）
    max_chars_per_item : int
        单条内容超出时截断
    max_total_chars : int
        全量文本超出时停止追加（truncated=True）
    days : int
        时间窗天数（仅用于 header 描述文字）
    since : datetime
        起始 UTC 时间（仅用于 header 描述文字）

    Returns
    -------
    dict  包含 text / items_count / skipped_count / total_chars / truncated
    """
    channel_name = (
        subscription.get("channel_name")
        or f'@{subscription.get("channel_identifier", "")}'
    ).strip()
    channel_type = subscription.get("channel_type", "")

    segments: List[str] = []
    items_count = 0
    skipped_count = 0
    total_chars = 0
    truncated = False

    header = (
        f"===== {channel_name} · Last {days} day{'s' if days > 1 else ''}"
        f" · {len(rows)} items fetched ====="
    )
    since_str = since.strftime("%Y-%m-%d %H:%M UTC")
    subheader = f"Range: {since_str} → now  |  channel_type: {channel_type}"

    for idx, row in enumerate(rows, start=1):
        body = _build_item_body(row)
        if not body:
            skipped_count += 1
            continue

        # 单条截断
        if len(body) > max_chars_per_item:
            trimmed = len(body) - max_chars_per_item
            body = body[:max_chars_per_item] + f"\n…[truncated {trimmed:,} chars]"

        # 构造每条标题行
        pub_at = row.get("published_at")
        date_str = ""
        if pub_at:
            if isinstance(pub_at, datetime):
                date_str = pub_at.strftime("%Y-%m-%d")
            else:
                date_str = str(pub_at)[:10]

        # 节目名（YouTube）或频道名
        show_name = None
        meta = row.get("metadata") or {}
        if channel_type == "youtube" and meta.get("show_name"):
            show_name = meta["show_name"]

        source_label = show_name or channel_name
        title = (row.get("title") or "").strip()
        item_header_parts = [f"[{items_count + 1}]"]
        if date_str:
            item_header_parts.append(date_str)
        item_header_parts.append(f"{source_label} — {title}" if title else source_label)
        item_header = " · ".join(item_header_parts)

        segment = f"{item_header}\n{body}"
        seg_chars = len(segment)

        # 全量截断检查
        if total_chars + seg_chars > max_total_chars:
            truncated = True
            break

        segments.append(segment)
        total_chars += seg_chars
        items_count += 1

    divider = "─" * 60
    body_text = f"\n\n{divider}\n\n".join(segments)
    full_text = f"{header}\n{subheader}\n\n{divider}\n\n{body_text}"

    return {
        "text": full_text,
        "items_count": items_count,
        "skipped_count": skipped_count,
        "total_chars": len(full_text),
        "truncated": truncated,
    }


# ──────────────────────────────────────────────
# 每条内容的主体提取
# ──────────────────────────────────────────────

def _build_item_body(row: Dict[str, Any]) -> Optional[str]:
    ct = row.get("channel_type", "")
    if ct == "youtube":
        return _youtube_body(row)
    if ct == "twitter":
        return _twitter_body(row)
    # news / rss / 其余渠道
    return _generic_body(row)


def _youtube_body(row: Dict[str, Any]) -> Optional[str]:
    """transcript_cleaned → transcript → content (描述) 三级回退；全空则返回 None（跳过）。"""
    cleaned = (row.get("transcript_cleaned") or "").strip()
    if cleaned:
        return cleaned
    raw = (row.get("transcript") or "").strip()
    if raw:
        return raw
    desc = (row.get("content") or "").strip()
    if desc:
        return f"[No transcript — video description]\n{desc}"
    return None


def _twitter_body(row: Dict[str, Any]) -> Optional[str]:
    """与 FeedTweetContent.tsx 渲染逻辑保持一致。"""
    content = (row.get("content") or "").strip()

    if row.get("is_reply") and row.get("in_reply_to_username"):
        prefix = f"[Replying to @{row['in_reply_to_username']}]"
        return f"{prefix}\n{content}" if content else prefix

    parts: List[str] = []
    if content:
        parts.append(content)

    if row.get("is_retweet") and row.get("retweeted_content"):
        rt_author = row.get("retweeted_author") or ""
        rt_body = (row["retweeted_content"] or "").strip()
        parts.append(f"[Retweeted from @{rt_author}]\n{rt_body}")

    if row.get("is_quote") and row.get("quoted_content"):
        q_author = row.get("quoted_author") or ""
        q_body = (row["quoted_content"] or "").strip()
        parts.append(f"[Quoted from @{q_author}]\n{q_body}")

    result = "\n\n".join(parts).strip()
    return result or None


def _generic_body(row: Dict[str, Any]) -> Optional[str]:
    """News / RSS：取 content，若为空则用 URL 兜底。"""
    content = (row.get("content") or "").strip()
    if content:
        return content
    url = (row.get("url") or "").strip()
    if url:
        return f"[Content not stored — read original]\n{url}"
    return None
