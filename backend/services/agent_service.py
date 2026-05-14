# -*- coding: utf-8 -*-
"""Agent-facing read service: Daily feed, item detail, Twitter/YouTube reports."""

from __future__ import annotations

import base64
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException

from backend.api import daily as daily_api
from backend.database.db_manager import DatabaseManager
from backend.schemas.agent import (
    AgentDailyFeedResponse,
    AgentDailyItem,
    AgentDailyItemDetail,
    AgentReportBodyResponse,
    AgentTwitterReportListResponse,
    AgentTwitterReportSummary,
)
from backend.schemas.daily import DailyFeedItem, DailyItemDetail
from backend.schemas.user import TokenData
from backend.services.twitter_daily_report_service import TwitterDailyReportService
from backend.services.youtube_daily_report_service import YouTubeDailyReportService
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("agent_service")

_MD_LIGHT = re.compile(r"[*_#`]+")

# Field-level truncation caps for Agent v1 detail. Kept generous enough to give an LLM
# real working material (~8K tokens per field) but bounded so a single drill-down can't
# blow up a 32K/128K context window when several items are fetched in parallel.
_MAX_CONTENT_CHARS = 32000
_MAX_TRANSCRIPT_CHARS = 32000
_MAX_EXCERPT_CHARS = 4000

# Upload file_type values that may carry a flattenable ASR transcript.
_UPLOAD_TRANSCRIPT_TYPES = {"youtube", "audio", "podcast"}


def _strip_md_light(text: str) -> str:
    if not text:
        return ""
    return _MD_LIGHT.sub("", text).strip()


def _encode_cursor(offset: int) -> str:
    raw = str(max(0, offset)).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(cursor: Optional[str]) -> int:
    if not cursor:
        return 0
    try:
        pad = "=" * ((4 - len(cursor) % 4) % 4)
        return int(base64.urlsafe_b64decode(cursor + pad).decode())
    except Exception:
        return 0


def _parse_since(since: Optional[str]) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Return (date_from, date_to) window; date_to is None (open upper bound)."""
    if not since or since.strip() == "":
        df = datetime.now(timezone.utc) - timedelta(days=7)
        return df, None
    s = since.strip().lower()
    now = datetime.now(timezone.utc)
    if s == "24h" or s == "1d":
        return now - timedelta(hours=24), None
    if s == "7d":
        return now - timedelta(days=7), None
    if s == "30d":
        return now - timedelta(days=30), None
    try:
        # ISO8601
        dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt, None
    except ValueError:
        return now - timedelta(days=7), None


def _expand_agent_sources(parts: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if p == "youtube":
            for x in ("youtube", "youtube_feed"):
                if x not in seen:
                    seen.add(x)
                    out.append(x)
            continue
        if p in daily_api.ALL_SOURCE_SUBTYPES and p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _parse_sources_param(sources: Optional[str]) -> Optional[List[str]]:
    if not sources:
        return None
    parts = [x.strip() for x in sources.split(",") if x.strip()]
    if not parts:
        return None
    expanded = _expand_agent_sources(parts)
    return daily_api._normalize_subtypes(expanded)


def _load_preset_subtypes(db: DatabaseManager, user_id: int) -> Optional[List[str]]:
    conn = db.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT source_subtypes FROM user_daily_preferences WHERE user_id = %s",
            (user_id,),
        )
        row = cursor.fetchone()
        raw = list(row[0]) if row and row[0] else []
        cleaned = [s for s in raw if s in daily_api.ALL_SOURCE_SUBTYPES]
        expanded = daily_api._expand_preset_for_legacy_full_upload(cleaned)
        if not expanded:
            return None
        return daily_api._normalize_subtypes(expanded)
    finally:
        conn.close()


def _map_agent_source(source: str, source_subtype: str) -> str:
    if source == "feed":
        if source_subtype == "twitter":
            return "twitter"
        if source_subtype in ("youtube_feed", "youtube"):
            return "youtube"
        if source_subtype in ("news", "rss"):
            return "news"
        return "news"
    # upload
    st = source_subtype
    if st == "document":
        return "pdf"
    if st in ("image", "audio", "youtube", "web", "text", "podcast"):
        return st
    return "text"


def _item_url(d: DailyFeedItem) -> Optional[str]:
    if d.navigate.external_url:
        return d.navigate.external_url
    base = os.getenv("PUBLIC_WEB_URL", "").rstrip("/")
    if not base:
        return None
    if d.navigate.type == "execution" and d.navigate.execution_id:
        return f"{base}/files?execution={d.navigate.execution_id}"
    if (
        d.navigate.type == "feed_article"
        and d.navigate.subscription_id
        and d.navigate.content_id
    ):
        return f"{base}/feeds/article/{d.navigate.subscription_id}/{d.navigate.content_id}"
    return None


def _daily_feed_item_to_agent(d: DailyFeedItem) -> AgentDailyItem:
    raw_sum = d.summary or d.snippet or ""
    summary = _strip_md_light(raw_sum)[:2800]
    return AgentDailyItem(
        id=d.id,
        title=d.title or "Untitled",
        summary=summary or "",
        url=_item_url(d),
        source=_map_agent_source(d.source, d.source_subtype),  # type: ignore[arg-type]
        author=d.author_name or d.author_identifier,
        published_at=d.published_at,
        tickers=list(d.tickers or [])[:50],
        tags=list(d.concepts or [])[:50],
    )


def _truncate_field(text: Optional[str], limit: int) -> Tuple[Optional[str], bool]:
    """Cut `text` to `limit` chars; return (value_or_none, was_truncated)."""
    if not text:
        return None, False
    if len(text) > limit:
        return text[:limit], True
    return text, False


def _flatten_asr_text(asr: Any) -> Optional[str]:
    """Flatten `file_processing_details.asr_with_diarization` JSONB to plain text.

    Mirrors the frontend's `flattenAsr` (web-frontend/src/components/daily/dailyPreviewUtils.ts):
    prefer `sentences[].text`, fall back to `transcript[]` (string or {text}).
    Returned text joins one line per sentence.
    """
    if not isinstance(asr, dict):
        return None
    sentences = asr.get("sentences")
    if isinstance(sentences, list):
        lines = [
            str(s.get("text") or "").strip()
            for s in sentences
            if isinstance(s, dict)
        ]
        lines = [t for t in lines if t]
        if lines:
            return "\n".join(lines)
    transcript_arr = asr.get("transcript")
    if isinstance(transcript_arr, list):
        lines = []
        for s in transcript_arr:
            if isinstance(s, str):
                t = s.strip()
            elif isinstance(s, dict):
                t = str(s.get("text") or "").strip()
            else:
                t = ""
            if t:
                lines.append(t)
        if lines:
            return "\n".join(lines)
    return None


def _fetch_upload_asr_text(cursor, file_id: int) -> Optional[str]:
    """Pull `asr_with_diarization` for an upload file and flatten to plain text."""
    cursor.execute(
        "SELECT asr_with_diarization FROM file_processing_details WHERE id = %s",
        (file_id,),
    )
    row = cursor.fetchone()
    if not row or row[0] is None:
        return None
    raw = row[0]
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None
    return _flatten_asr_text(raw)


def _fetch_feed_transcript_text(
    cursor, content_db_id: int, user_id: int
) -> Optional[str]:
    """Pull YouTube transcript for a subscription_content row.

    Same precedence as `GET /api/subscriptions/contents/{id}/transcript`:
    `transcript_cleaned` first, fall back to raw `transcript`.
    """
    cursor.execute(
        """
        SELECT transcript_cleaned, transcript
        FROM subscription_content
        WHERE id = %s AND %s = ANY(user_ids)
        """,
        (content_db_id, user_id),
    )
    row = cursor.fetchone()
    if not row:
        return None
    cleaned = (row[0] or "").strip()
    raw = (row[1] or "").strip()
    text = cleaned or raw
    return text or None


def _detail_to_agent_detail(
    d: DailyItemDetail, transcript_text: Optional[str] = None
) -> AgentDailyItemDetail:
    base = _daily_feed_item_to_agent(d)

    raw_body: Optional[str] = None
    if d.upload and d.upload.content:
        raw_body = str(d.upload.content)
    elif d.feed and d.feed.content:
        raw_body = str(d.feed.content)

    content_field, content_truncated = _truncate_field(raw_body, _MAX_CONTENT_CHARS)
    transcript_field, transcript_truncated = _truncate_field(
        transcript_text, _MAX_TRANSCRIPT_CHARS
    )

    excerpt: Optional[str] = None
    if raw_body:
        excerpt = _strip_md_light(raw_body[:_MAX_EXCERPT_CHARS])

    return AgentDailyItemDetail(
        **base.model_dump(),
        body_excerpt=excerpt,
        content=content_field,
        content_truncated=content_truncated,
        transcript=transcript_field,
        transcript_truncated=transcript_truncated,
    )


class AgentService:
    def __init__(self, db: Optional[DatabaseManager] = None) -> None:
        self._db = db or DatabaseManager(get_database_config())

    def get_daily_feed(
        self,
        user: TokenData,
        *,
        since: Optional[str] = None,
        q: Optional[str] = None,
        sources: Optional[str] = None,
        ticker: Optional[str] = None,
        limit: int = 20,
        cursor: Optional[str] = None,
    ) -> AgentDailyFeedResponse:
        if user.user_id is None or not user.username:
            raise HTTPException(status_code=401, detail="未授权")
        date_from, date_to = _parse_since(since)
        explicit = _parse_sources_param(sources)
        if explicit is not None:
            source_subtypes = explicit
        else:
            source_subtypes = _load_preset_subtypes(self._db, user.user_id)

        tickers = [ticker.strip()] if ticker and ticker.strip() else None

        upload_extra, feed_extra, extra_params = daily_api._build_filter_clauses(
            date_from=date_from,
            date_to=date_to,
            source_subtypes=source_subtypes,
            q=q,
            tickers=tickers,
        )
        base_params: Dict[str, Any] = {
            "username": user.username,
            "user_id": user.user_id,
        }
        base_params.update(extra_params)

        offset = _decode_cursor(cursor)
        lim = max(1, min(limit, 100))

        union_sql = f"""
            WITH upload_items AS (
                {daily_api._UPLOAD_SQL}
                {upload_extra}
            ),
            feed_items AS (
                {daily_api._FEED_SQL}
                {feed_extra}
            ),
            merged AS (
                SELECT * FROM upload_items
                UNION ALL
                SELECT * FROM feed_items
            )
            SELECT * FROM merged
            ORDER BY published_at DESC NULLS LAST
            LIMIT %(limit)s OFFSET %(offset)s
        """
        count_sql = f"""
            WITH upload_items AS (
                {daily_api._UPLOAD_SQL}
                {upload_extra}
            ),
            feed_items AS (
                {daily_api._FEED_SQL}
                {feed_extra}
            )
            SELECT
                (SELECT COUNT(*) FROM upload_items)
              + (SELECT COUNT(*) FROM feed_items) AS total
        """

        conn = self._db.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        try:
            cur = conn.cursor()
            cur.execute(count_sql, base_params)
            total = int(cur.fetchone()[0] or 0)
            page_params = dict(base_params, limit=lim, offset=offset)
            cur.execute(union_sql, page_params)
            rows = cur.fetchall()
            feed_chans = list({r[5] for r in rows if r[0] == "feed" and r[5]})
            sub_id_map = daily_api._resolve_subscription_ids(cur, user.user_id, feed_chans)
            items_dj = [daily_api._row_to_item(r, sub_id_map) for r in rows]
            items = [_daily_feed_item_to_agent(x) for x in items_dj]
            has_more = (offset + len(items)) < total
            next_c = _encode_cursor(offset + len(items)) if has_more and items else None
            return AgentDailyFeedResponse(
                items=items,
                next_cursor=next_c,
                has_more=has_more,
                total=total,
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"[AgentService.get_daily_feed] {exc}")
            raise HTTPException(status_code=500, detail=f"查询失败: {exc}")
        finally:
            conn.close()

    def get_daily_item(self, user: TokenData, item_id: str) -> AgentDailyItemDetail:
        if user.user_id is None or not user.username:
            raise HTTPException(status_code=401, detail="未授权")
        try:
            src, numeric_id = daily_api._parse_item_id(item_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid item id")
        conn = self._db.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        try:
            cur = conn.cursor()
            transcript_text: Optional[str] = None
            if src == "upload":
                detail = daily_api._fetch_upload_detail(cur, numeric_id, user)
                if (
                    detail.upload
                    and (detail.upload.file_type_raw or "").lower()
                    in _UPLOAD_TRANSCRIPT_TYPES
                ):
                    transcript_text = _fetch_upload_asr_text(
                        cur, detail.upload.file_id
                    )
            else:
                detail = daily_api._fetch_feed_detail(cur, numeric_id, user)
                if (
                    detail.feed
                    and detail.feed.channel_type == "youtube"
                    and detail.feed.has_transcript
                ):
                    transcript_text = _fetch_feed_transcript_text(
                        cur, detail.feed.content_db_id, user.user_id
                    )
            return _detail_to_agent_detail(detail, transcript_text)
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"[AgentService.get_daily_item] {item_id}: {exc}")
            raise HTTPException(status_code=500, detail=f"查询失败: {exc}")
        finally:
            conn.close()

    def search(
        self,
        user: TokenData,
        *,
        q: str,
        since: Optional[str] = None,
        sources: Optional[str] = None,
        ticker: Optional[str] = None,
        limit: int = 20,
        cursor: Optional[str] = None,
    ) -> AgentDailyFeedResponse:
        if not q or not q.strip():
            raise HTTPException(status_code=400, detail="q is required")
        return self.get_daily_feed(
            user,
            since=since,
            q=q.strip(),
            sources=sources,
            ticker=ticker,
            limit=limit,
            cursor=cursor,
        )

    # --- Twitter reports ---

    def twitter_list(self, user: TokenData, limit: int = 30, offset: int = 0) -> AgentTwitterReportListResponse:
        svc = TwitterDailyReportService()
        reports, total = svc.get_user_reports(user.user_id, limit, offset)
        summaries = [
            AgentTwitterReportSummary(
                report_date=str(r.get("report_date") or r.get("date") or ""),
                status=r.get("status"),
                title=r.get("title"),
            )
            for r in (reports or [])
        ]
        return AgentTwitterReportListResponse(reports=summaries, total=total)

    def twitter_latest(self, user: TokenData) -> Optional[AgentReportBodyResponse]:
        svc = TwitterDailyReportService()
        r = svc.get_latest_report(user.user_id)
        if not r:
            return None
        return self._report_dict_to_agent(r)

    def twitter_by_date(self, user: TokenData, report_date: str) -> Optional[AgentReportBodyResponse]:
        try:
            d = datetime.strptime(report_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Use YYYY-MM-DD")
        svc = TwitterDailyReportService()
        r = svc.get_report_by_date(user.user_id, d)
        if not r:
            return None
        return self._report_dict_to_agent(r)

    # --- YouTube reports ---

    def youtube_list(self, user: TokenData, limit: int = 30, offset: int = 0) -> AgentTwitterReportListResponse:
        svc = YouTubeDailyReportService()
        reports, total = svc.get_user_reports(user.user_id, limit, offset)
        summaries = [
            AgentTwitterReportSummary(
                report_date=str(r.get("report_date") or r.get("date") or ""),
                status=r.get("status"),
                title=r.get("title"),
            )
            for r in (reports or [])
        ]
        return AgentTwitterReportListResponse(reports=summaries, total=total)

    def youtube_latest(self, user: TokenData) -> Optional[AgentReportBodyResponse]:
        svc = YouTubeDailyReportService()
        r = svc.get_latest_report(user.user_id)
        if not r:
            return None
        return self._report_dict_to_agent(r)

    def youtube_by_date(self, user: TokenData, report_date: str) -> Optional[AgentReportBodyResponse]:
        try:
            d = datetime.strptime(report_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Use YYYY-MM-DD")
        svc = YouTubeDailyReportService()
        r = svc.get_report_by_date(user.user_id, d)
        if not r:
            return None
        return self._report_dict_to_agent(r)

    @staticmethod
    def _report_dict_to_agent(r: Dict[str, Any]) -> AgentReportBodyResponse:
        rd = r.get("report_date") or r.get("date")
        if hasattr(rd, "isoformat"):
            rd_str = rd.isoformat()
        else:
            rd_str = str(rd or "")
        return AgentReportBodyResponse(
            report_date=rd_str[:10],
            status=r.get("status"),
            content_markdown=r.get("content") or r.get("report_content") or r.get("body"),
            metadata=r.get("metadata"),
        )
