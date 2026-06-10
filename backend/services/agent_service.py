# -*- coding: utf-8 -*-
"""Agent-facing read service v0.2: Daily feed, item detail, multi-search, reports.

Changes from v0.1:
- _parse_since: accepts Nd / Nh / Nw patterns; raises 400 on bad values
- get_daily_feed: tickers is now List[str]; new tags / author / dedup params
- search() removed — use get_daily_feed(q=...) directly
- get_daily_items_batch: bulk item detail up to 10 ids
- multi_search: parallel groups + server-side dedup
- list_subscriptions / list_sources: introspection tools
- report helpers consolidated under _resolve_report_service(kind)
- diagnostics block on total==0 with q active
"""

from __future__ import annotations

import base64
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException

from backend.api import daily as daily_api
from backend.database.db_manager import DatabaseManager
from backend.schemas.agent import (
    AgentDailyBatchResponse,
    AgentDailyFeedResponse,
    AgentDailyItem,
    AgentDailyItemDetail,
    AgentDailyMultiResponse,
    AgentMultiGroupStats,
    AgentMultiSearchGroup,
    AgentMultiSearchRequest,
    AgentReportBodyResponse,
    AgentSearchDiagnostics,
    AgentSourceInfo,
    AgentSourcesInfoResponse,
    AgentSubscriptionChannel,
    AgentSubscriptionListResponse,
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

_MAX_CONTENT_CHARS = 32000
_MAX_TRANSCRIPT_CHARS = 32000
_MAX_EXCERPT_CHARS = 4000

_UPLOAD_TRANSCRIPT_TYPES = {"youtube", "audio", "podcast"}

# Diagnostics: max terms to probe and max wall-time budget in seconds
_DIAG_MAX_TERMS = 8
_DIAG_BUDGET_S = 0.2


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
    """Return (date_from, date_to) window; date_to is None (open upper bound).

    Accepted formats:
      Nd  — N days ago (e.g. 7d, 14d)
      Nh  — N hours ago (e.g. 24h, 48h)
      Nw  — N weeks ago (e.g. 2w)
      ISO 8601 datetime — exact lower bound
      None / empty — default 7d

    Raises HTTPException 400 on unrecognised non-empty input.
    """
    if not since or since.strip() == "":
        return datetime.now(timezone.utc) - timedelta(days=7), None

    s = since.strip().lower()
    now = datetime.now(timezone.utc)

    m = re.fullmatch(r"(\d+)([hdw])", s)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit == "h":
            return now - timedelta(hours=n), None
        if unit == "d":
            return now - timedelta(days=n), None
        if unit == "w":
            return now - timedelta(weeks=n), None

    try:
        dt = datetime.fromisoformat(since.strip().replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt, None
    except ValueError:
        pass

    raise HTTPException(
        status_code=400,
        detail=f"Invalid 'since' value '{since}'. "
        "Use Nd (e.g. 7d), Nh (e.g. 24h), Nw (e.g. 2w), or an ISO 8601 datetime.",
    )


# Mapping from user/agent-facing source names to internal ALL_SOURCE_SUBTYPES values.
# _map_agent_source() surfaces "pdf" to callers for all document uploads, but the
# internal subtype (and DB file_type group) is "document", which covers pdf/word/
# excel/csv files.  Agents may also pass common file-type names directly, so we
# map all reasonable variants to "document".
_AGENT_SOURCE_ALIASES: Dict[str, str] = {
    # PDF-specific filter (DB file_type in: pdf, document-legacy)
    "pdf":   "pdf",
    # Word-specific filter
    "word":  "word",
    "doc":   "word",
    "docx":  "word",
    # Excel/CSV-specific filter
    "excel": "excel",
    "xls":   "excel",
    "xlsx":  "excel",
    "csv":   "excel",
    # image aliases
    "img":   "image",
    "photo": "image",
    "pic":   "image",
}


def _expand_agent_sources(parts: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for p in parts:
        p = p.strip()
        if not p:
            continue
        # Resolve user-facing aliases before checking against ALL_SOURCE_SUBTYPES
        p = _AGENT_SOURCE_ALIASES.get(p, p)
        if p == "youtube":
            for x in ("youtube", "youtube_feed"):
                if x not in seen:
                    seen.add(x)
                    out.append(x)
            continue
        # "document" expands to cover both the document subtype AND image uploads,
        # because users consider all uploaded files (docs + images) as one group.
        if p == "document":
            for x in ("document", "image"):
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
        return None  # not provided → caller falls back to user preset
    parts = [x.strip() for x in sources.split(",") if x.strip()]
    if not parts:
        return None  # whitespace-only string → treat as not provided
    expanded = _expand_agent_sources(parts)
    normalized = daily_api._normalize_subtypes(expanded)
    # When the caller explicitly provided sources= but none matched a known type,
    # return [] (not None). None would silently fall back to the user's preset;
    # [] signals "explicit filter with zero valid types" to get_daily_feed so it
    # can return 0 results or a clear diagnostic instead of unrelated content.
    return normalized if normalized is not None else []


def _parse_tickers_param(tickers: Optional[str]) -> Optional[List[str]]:
    if not tickers:
        return None
    result = [t.strip() for t in tickers.split(",") if t.strip()]
    return result or None


def _parse_tags_param(tags: Optional[str]) -> Optional[List[str]]:
    if not tags:
        return None
    result = [t.strip() for t in tags.split(",") if t.strip()]
    return result or None


def _parse_people_param(people: Optional[str]) -> Optional[List[str]]:
    if not people:
        return None
    result = [t.strip() for t in people.split(",") if t.strip()]
    return result or None


def _parse_countries_param(countries: Optional[str]) -> Optional[List[str]]:
    if not countries:
        return None
    result = [t.strip() for t in countries.split(",") if t.strip()]
    return result or None


def _people_names_from_daily(d: DailyFeedItem) -> List[str]:
    names: List[str] = []
    for p in d.people or []:
        if isinstance(p, dict):
            name = (p.get("name") or "").strip()
            if name:
                names.append(name)
        elif isinstance(p, str) and p.strip():
            names.append(p.strip())
    return names


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


# Maps raw DB file_type values (from file_processing_details.file_type) to the
# specific AgentSource values exposed in API responses.
# This lets agents distinguish Excel from PDF instead of seeing "pdf" for both.
_RAW_FILE_TYPE_TO_AGENT_SOURCE: Dict[str, str] = {
    "pdf":      "pdf",
    "word":     "word",
    "excel":    "excel",
    "csv":      "excel",    # CSV is a spreadsheet; group with excel
    "document": "pdf",      # legacy DB value → generic pdf
    "image":    "image",
    "audio":    "audio",
    "youtube":  "youtube",
    "web":      "web",
    "text":     "text",
    "podcast":  "podcast",
}


def _map_agent_source(
    source: str,
    source_subtype: str,
    raw_file_type: Optional[str] = None,
) -> str:
    if source == "feed":
        if source_subtype == "twitter":
            return "twitter"
        if source_subtype in ("youtube_feed", "youtube"):
            return "youtube"
        if source_subtype in ("news", "rss"):
            return "news"
        if source_subtype == "zhishixingqiu":
            return "zhishixingqiu"
        return "news"
    # For uploads: use the raw DB file_type for a specific format label when available.
    if raw_file_type:
        mapped = _RAW_FILE_TYPE_TO_AGENT_SOURCE.get(raw_file_type)
        if mapped:
            return mapped
    # Fallback: derive from normalised source_subtype
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
        source=_map_agent_source(d.source, d.source_subtype, d.raw_file_type),  # type: ignore[arg-type]
        author=d.author_name or d.author_identifier,
        published_at=d.published_at,
        tickers=list(d.tickers or [])[:50],
        tags=list(d.concepts or [])[:50],
        people=_people_names_from_daily(d)[:30],
        countries=list(d.countries or [])[:30],
        matched_themes=[{
            "theme_id": t.get("theme_id"),
            "theme_name": t.get("theme_name"),
            "category": t.get("category"),
            "match_mode": t.get("match_mode"),
        } for t in (d.matched_themes or [])[:10]],
    )


def _truncate_field(text: Optional[str], limit: int) -> Tuple[Optional[str], bool]:
    if not text:
        return None, False
    if len(text) > limit:
        return text[:limit], True
    return text, False


def _flatten_asr_text(asr: Any) -> Optional[str]:
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


def _fetch_feed_transcript_text(cursor, content_db_id: int, user_id: int) -> Optional[str]:
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


def _tags_people_detail(d: DailyItemDetail) -> List[Dict[str, Any]]:
    tags = None
    if d.upload and d.upload.tags:
        tags = d.upload.tags
    elif d.feed and d.feed.tags:
        tags = d.feed.tags
    if not tags:
        return list(d.people or [])
    people = tags.get("people")
    if isinstance(people, list):
        return [p for p in people if isinstance(p, dict)]
    return list(d.people or [])


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
        people_detail=_tags_people_detail(d)[:50],
    )


def _resolve_report_service(kind: str):
    """Return the correct report service for the given kind."""
    if kind == "youtube":
        return YouTubeDailyReportService()
    if kind == "twitter":
        return TwitterDailyReportService()
    raise HTTPException(
        status_code=400,
        detail=f"Unknown report kind '{kind}'. Use 'twitter' or 'youtube'.",
    )


class AgentService:
    def __init__(self, db: Optional[DatabaseManager] = None) -> None:
        self._db = db or DatabaseManager(get_database_config())

    # ------------------------------------------------------------------
    # Core: daily feed / search (merged in v0.2)
    # ------------------------------------------------------------------

    def get_daily_feed(
        self,
        user: TokenData,
        *,
        since: Optional[str] = None,
        q: Optional[str] = None,
        sources: Optional[str] = None,
        tickers: Optional[str] = None,
        tags: Optional[str] = None,
        author: Optional[str] = None,
        people: Optional[str] = None,
        countries: Optional[str] = None,
        limit: int = 20,
        cursor: Optional[str] = None,
        dedup: bool = True,
    ) -> AgentDailyFeedResponse:
        if user.user_id is None or not user.username:
            raise HTTPException(status_code=401, detail="未授权")

        date_from, date_to = _parse_since(since)
        explicit = _parse_sources_param(sources)
        source_subtypes: Optional[List[str]]
        if explicit is not None:
            source_subtypes = explicit
        else:
            source_subtypes = _load_preset_subtypes(self._db, user.user_id)

        tickers_list = _parse_tickers_param(tickers)
        tags_list = _parse_tags_param(tags)
        people_list = _parse_people_param(people)
        countries_list = _parse_countries_param(countries)

        upload_extra, feed_extra, extra_params = daily_api._build_filter_clauses(
            date_from=date_from,
            date_to=date_to,
            source_subtypes=source_subtypes,
            q=q,
            tickers=tickers_list,
            tags=tags_list,
            author=author or None,
            people=people_list,
            countries=countries_list,
        )
        base_params: Dict[str, Any] = {
            "username": user.username,
            "user_id": user.user_id,
        }
        base_params.update(extra_params)

        offset = _decode_cursor(cursor)
        lim = max(1, min(limit, 100))

        # Dedup subquery wraps the union and collapses duplicate external_url rows.
        # Rows with NULL external_url are never collapsed (each stays distinct).
        if dedup:
            dedup_cte = """
            deduped AS (
                SELECT DISTINCT ON (
                    COALESCE(external_url, 'NULL_URL_' || source || '_' || item_id::text)
                ) *
                FROM merged
                ORDER BY
                    COALESCE(external_url, 'NULL_URL_' || source || '_' || item_id::text),
                    published_at DESC NULLS LAST
            )
            """
            final_table = "deduped"
        else:
            dedup_cte = ""
            final_table = "merged"

        dedup_cte_part = (", " + dedup_cte) if dedup_cte else ""

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
            ){dedup_cte_part}
            SELECT * FROM {final_table}
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
            ),
            merged AS (
                SELECT * FROM upload_items
                UNION ALL
                SELECT * FROM feed_items
            ){dedup_cte_part}
            SELECT COUNT(*) FROM {final_table}
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

            # Build diagnostics block when 0 results
            diagnostics: Optional[AgentSearchDiagnostics] = None
            if total == 0 and q:
                diagnostics = self._build_diagnostics(
                    cur=cur,
                    q=q,
                    date_from=date_from,
                    source_subtypes=source_subtypes,
                    base_params=base_params,
                )
            elif total == 0 and sources and explicit == []:
                # sources= was explicitly provided but all values were unrecognised.
                # Return a plain diagnostics block so the caller knows why they got 0 results.
                since_used = date_from.isoformat() if date_from else None
                diagnostics = AgentSearchDiagnostics(
                    parsed_q_groups=[],
                    terms_in_corpus={},
                    since_used=since_used,
                    sources_used=[],
                    hint=(
                        f"None of the requested source types {sources!r} matched known values. "
                        "Valid values: twitter, youtube, document, image, audio, text, podcast, news. "
                        "Tip: uploaded PDF/Word/Excel files are source type 'document' (or 'pdf'). "
                        "Use list_sources to see which source types are active in your account."
                    ),
                )

            return AgentDailyFeedResponse(
                items=items,
                next_cursor=next_c,
                has_more=has_more,
                total=total,
                diagnostics=diagnostics,
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"[AgentService.get_daily_feed] {exc}")
            raise HTTPException(status_code=500, detail=f"查询失败: {exc}")
        finally:
            conn.close()

    def _build_diagnostics(
        self,
        cur,
        q: str,
        date_from: Optional[datetime],
        source_subtypes: Optional[List[str]],
        base_params: Dict[str, Any],
    ) -> AgentSearchDiagnostics:
        """Probe each q term individually to surface which term is over-restrictive."""
        try:
            groups = daily_api._parse_q(q)
        except Exception:
            groups = []

        all_terms: List[str] = []
        for g in groups:
            all_terms.extend(g.get("terms", []))
        all_terms = list(dict.fromkeys(all_terms))[:_DIAG_MAX_TERMS]

        terms_in_corpus: Dict[str, int] = {}
        budget_start = time.monotonic()

        for term in all_terms:
            if time.monotonic() - budget_start > _DIAG_BUDGET_S:
                break
            like_val = f"%{term}%"
            try:
                cur.execute(
                    """
                    SELECT (
                        SELECT COUNT(*) FROM file_processing_details fpd
                        JOIN processing_stats ps ON ps.id = fpd.processing_stats_id
                        WHERE ps.user = %(username)s
                          AND (
                              COALESCE(fpd.display_name, '') ILIKE %(like)s
                              OR COALESCE(fpd.content, '') ILIKE %(like)s
                          )
                    ) + (
                        SELECT COUNT(*) FROM subscription_content sc
                        WHERE %(user_id)s = ANY(sc.user_ids)
                          AND (
                              COALESCE(sc.title, '') ILIKE %(like)s
                              OR COALESCE(sc.content, '') ILIKE %(like)s
                          )
                    ) AS cnt
                    """,
                    {"username": base_params["username"], "user_id": base_params["user_id"], "like": like_val},
                )
                row = cur.fetchone()
                terms_in_corpus[term] = int(row[0]) if row else 0
            except Exception:
                pass

        # Produce a hint based on findings
        hint: Optional[str] = None
        if terms_in_corpus:
            very_common = [t for t, c in terms_in_corpus.items() if c > 5000]
            zero_terms = [t for t, c in terms_in_corpus.items() if c == 0]
            if zero_terms:
                hint = (
                    f"Term(s) {zero_terms} have 0 matches anywhere in your content — "
                    "consider removing them or using OR instead of AND."
                )
            elif very_common and len(all_terms) > 1:
                hint = (
                    f"Term(s) {very_common} are very common (>{min(terms_in_corpus[t] for t in very_common)} hits). "
                    "The AND combination may still be very restrictive — try dropping some terms."
                )

        since_used = date_from.isoformat() if date_from else None
        sources_used = list(source_subtypes) if source_subtypes else []

        return AgentSearchDiagnostics(
            parsed_q_groups=groups,
            terms_in_corpus=terms_in_corpus,
            since_used=since_used,
            sources_used=sources_used,
            hint=hint,
        )

    # ------------------------------------------------------------------
    # Batch item detail
    # ------------------------------------------------------------------

    def get_daily_items_batch(
        self, user: TokenData, ids: List[str]
    ) -> AgentDailyBatchResponse:
        if user.user_id is None or not user.username:
            raise HTTPException(status_code=401, detail="未授权")
        if not ids:
            raise HTTPException(status_code=400, detail="ids must be non-empty")
        if len(ids) > 10:
            raise HTTPException(status_code=400, detail="ids max length is 10")

        conn = self._db.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")

        results: List[AgentDailyItemDetail] = []
        errors: Dict[str, str] = {}

        try:
            cur = conn.cursor()
            for item_id in ids:
                try:
                    src, numeric_id = daily_api._parse_item_id(item_id)
                except Exception:
                    errors[item_id] = "Invalid item id format"
                    continue
                try:
                    transcript_text: Optional[str] = None
                    if src == "upload":
                        detail = daily_api._fetch_upload_detail(cur, numeric_id, user)
                        if (
                            detail.upload
                            and (detail.upload.file_type_raw or "").lower()
                            in _UPLOAD_TRANSCRIPT_TYPES
                        ):
                            transcript_text = _fetch_upload_asr_text(cur, detail.upload.file_id)
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
                    results.append(_detail_to_agent_detail(detail, transcript_text))
                except HTTPException as e:
                    errors[item_id] = e.detail
                except Exception as e:
                    errors[item_id] = str(e)
        finally:
            conn.close()

        return AgentDailyBatchResponse(items=results, errors=errors)

    # ------------------------------------------------------------------
    # Multi-group search
    # ------------------------------------------------------------------

    def multi_search(
        self, user: TokenData, req: AgentMultiSearchRequest
    ) -> AgentDailyMultiResponse:
        if user.user_id is None:
            raise HTTPException(status_code=401, detail="未授权")
        if not req.groups:
            raise HTTPException(status_code=400, detail="groups must be non-empty")
        if len(req.groups) > 5:
            raise HTTPException(status_code=400, detail="groups max length is 5")

        per_group_stats: List[AgentMultiGroupStats] = []
        all_items_by_group: List[List[AgentDailyItem]] = []

        for idx, group in enumerate(req.groups):
            resp = self.get_daily_feed(
                user,
                since=group.since,
                q=group.q,
                sources=group.sources,
                tickers=group.tickers,
                tags=group.tags,
                author=group.author,
                people=group.people,
                countries=group.countries,
                limit=req.limit_per_group,
                dedup=False,  # dedup happens after merge
            )
            per_group_stats.append(
                AgentMultiGroupStats(
                    group_index=idx,
                    total=resp.total,
                    has_more=resp.has_more,
                )
            )
            all_items_by_group.append(resp.items)

        # Merge + dedup
        seen: set = set()
        merged: List[AgentDailyItem] = []
        for group_items in all_items_by_group:
            for item in group_items:
                if req.dedup_by == "url":
                    key = item.url or f"__nurl__{item.id}"
                else:
                    key = item.id
                if key in seen:
                    continue
                seen.add(key)
                merged.append(item)

        # Sort merged by published_at descending
        merged.sort(key=lambda x: x.published_at, reverse=True)

        return AgentDailyMultiResponse(
            items=merged,
            total_merged=len(merged),
            per_group=per_group_stats,
            dedup_by=req.dedup_by,
        )

    # ------------------------------------------------------------------
    # Single item detail
    # ------------------------------------------------------------------

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
                    transcript_text = _fetch_upload_asr_text(cur, detail.upload.file_id)
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

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def list_subscriptions(self, user: TokenData) -> AgentSubscriptionListResponse:
        if user.user_id is None:
            raise HTTPException(status_code=401, detail="未授权")
        conn = self._db.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    sl.channel_type,
                    sl.channel_identifier,
                    COALESCE(
                        NULLIF(TRIM(sl.channel_name), ''),
                        sl.channel_identifier
                    ) AS channel_title,
                    COALESCE(
                        (
                            SELECT COUNT(*)
                            FROM subscription_content sc
                            WHERE sc.channel_type = sl.channel_type
                              AND sc.author_identifier = sl.channel_identifier
                              AND %(user_id)s = ANY(sc.user_ids)
                              AND sc.published_at >= NOW() - INTERVAL '30 days'
                        ), 0
                    ) AS item_count_30d
                FROM subscription_list sl
                WHERE sl.user_id = %(user_id)s
                  AND sl.enabled IS TRUE
                ORDER BY sl.channel_type, sl.channel_identifier
                """,
                {"user_id": user.user_id},
            )
            rows = cur.fetchall()
            channels = [
                AgentSubscriptionChannel(
                    channel_type=r[0],
                    channel_identifier=r[1],
                    channel_title=r[2],
                    item_count_30d=int(r[3] or 0),
                )
                for r in rows
            ]
            by_source: Dict[str, int] = {}
            for ch in channels:
                by_source[ch.channel_type] = by_source.get(ch.channel_type, 0) + 1
            return AgentSubscriptionListResponse(
                channels=channels,
                total=len(channels),
                by_source=by_source,
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"[AgentService.list_subscriptions] {exc}")
            raise HTTPException(status_code=500, detail=f"查询失败: {exc}")
        finally:
            conn.close()

    def list_sources(self, user: TokenData) -> AgentSourcesInfoResponse:
        if user.user_id is None or not user.username:
            raise HTTPException(status_code=401, detail="未授权")

        preset = _load_preset_subtypes(self._db, user.user_id) or []
        preset_set = set(preset)

        conn = self._db.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        try:
            cur = conn.cursor()
            # Count recent items per source subtype
            cur.execute(
                """
                SELECT lower(fpd.file_type::text) AS subtype, COUNT(*) AS cnt
                FROM file_processing_details fpd
                JOIN processing_stats ps ON ps.id = fpd.processing_stats_id
                WHERE ps.user = %(username)s
                  AND fpd.created_at >= NOW() - INTERVAL '30 days'
                GROUP BY lower(fpd.file_type::text)

                UNION ALL

                SELECT sc.channel_type AS subtype, COUNT(*) AS cnt
                FROM subscription_content sc
                WHERE %(user_id)s = ANY(sc.user_ids)
                  AND sc.published_at >= NOW() - INTERVAL '30 days'
                GROUP BY sc.channel_type
                """,
                {"username": user.username, "user_id": user.user_id},
            )
            raw_counts: Dict[str, int] = {}
            for row in cur.fetchall():
                subtype = row[0]
                count = int(row[1] or 0)
                raw_counts[subtype] = raw_counts.get(subtype, 0) + count

            # Normalise raw DB file_type / channel_type counts to AgentSource labels.
            # For upload items, use _map_agent_source with raw_file_type to get specific
            # labels (pdf / word / excel / image …) rather than the generic "pdf" fallback.
            # For feed items, raw_subtype IS the channel_type, so pass it as source_subtype.
            #
            # We also accumulate counts into ALL_SOURCE_SUBTYPES entries so that both the
            # specific (e.g. "excel") and the broad "document" entry show correct totals.
            normalised: Dict[str, int] = {}

            # DB file_type values known to belong to uploads
            _upload_raw_types = set(daily_api._UPLOAD_SUBTYPE_TO_DB_TYPES.keys()) | {
                "pdf", "word", "excel", "csv", "document",
                "image", "audio", "youtube", "web", "text", "podcast",
            }

            for subtype, count in raw_counts.items():
                if subtype in ("twitter", "youtube", "news", "rss", "zhishixingqiu"):
                    # Feed channel_type — map via feed path
                    mapped = _map_agent_source("feed", subtype)
                    normalised[mapped] = normalised.get(mapped, 0) + count
                    if subtype == "youtube":
                        normalised["youtube_feed"] = normalised.get("youtube_feed", 0) + count
                else:
                    # Upload file_type — use raw_file_type for precise label
                    specific = _map_agent_source("upload", "document", subtype)
                    normalised[specific] = normalised.get(specific, 0) + count
                    # Also roll up into the broad "document" group
                    if specific in ("pdf", "word", "excel", "image"):
                        normalised["document"] = normalised.get("document", 0) + count

            source_infos = [
                AgentSourceInfo(
                    source=src,
                    in_preset=src in preset_set or any(
                        _map_agent_source("upload", p) == src
                        or _map_agent_source("feed", p) == src
                        for p in preset_set
                    ),
                    item_count_30d=normalised.get(src, 0),
                )
                for src in daily_api.ALL_SOURCE_SUBTYPES
                if src not in ("youtube_feed",)  # deduped into 'youtube' in output
            ]

            return AgentSourcesInfoResponse(
                all_sources=source_infos,
                preset_sources=list(preset_set),
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"[AgentService.list_sources] {exc}")
            raise HTTPException(status_code=500, detail=f"查询失败: {exc}")
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Reports (consolidated)
    # ------------------------------------------------------------------

    def report_list(
        self, user: TokenData, kind: str, limit: int = 30, offset: int = 0
    ) -> AgentTwitterReportListResponse:
        svc = _resolve_report_service(kind)
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

    def report_latest(self, user: TokenData, kind: str) -> Optional[AgentReportBodyResponse]:
        svc = _resolve_report_service(kind)
        r = svc.get_latest_report(user.user_id)
        if not r:
            return None
        return self._report_dict_to_agent(r, kind=kind)

    def report_by_date(
        self, user: TokenData, kind: str, report_date: str
    ) -> Optional[AgentReportBodyResponse]:
        try:
            d = datetime.strptime(report_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Use YYYY-MM-DD")
        svc = _resolve_report_service(kind)
        r = svc.get_report_by_date(user.user_id, d)
        if not r:
            return None
        return self._report_dict_to_agent(r, kind=kind)

    @staticmethod
    def _report_dict_to_agent(r: Dict[str, Any], kind: Optional[str] = None) -> AgentReportBodyResponse:
        rd = r.get("report_date") or r.get("date")
        if hasattr(rd, "isoformat"):
            rd_str = rd.isoformat()
        else:
            rd_str = str(rd or "")
        return AgentReportBodyResponse(
            report_date=rd_str[:10],
            kind=kind,
            status=r.get("status"),
            content_markdown=r.get("content") or r.get("report_content") or r.get("body"),
            metadata=r.get("metadata"),
        )
