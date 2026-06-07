# -*- coding: utf-8 -*-
"""
Feeds 内容自动打标签 + 摘要服务

对标 backend/services/content_tagging_service.py (ContentTaggingService),
但绑定到 subscription_content (Twitter / YouTube / News feed 单条内容).

Twitter 智能路由:
  - 长推 (>= TWEET_SIGNAL_HEAVY_THRESHOLD_CHARS) 或含 $cashtag → content_tag_feeds_v1
  - 其余短推/速报 (TWEET_SIGNAL_V1_ENABLED=true) → tweet_signal_v1
  - YouTube → content_tag_feeds_v1
  - News (白名单来源, 如 CCTV) → content_tag_feeds_v1 (LLM 打标)
  - News (EODHD 等有 API tags 来源) → 轻量映射 map_api_tags() (零 LLM)

设计文档: docs/instructions/FEEDS_AUTO_TAGGING_PLAN.md
"""
import json
import os
import re
import time
from typing import Dict, Any, Optional, Callable, Tuple

from psycopg2.extras import Json

from backend.services.local_llm_analysis_service import LocalLLMAnalysisService
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger

logger = get_logger("feed_content_tagging_service")

# LLM 打标支持的渠道 (rss 暂不做)
_SUPPORTED_CHANNELS = {"twitter", "youtube", "news", "zhishixingqiu"}

FEEDS_TEMPLATE_CODE = "content_tag_feeds_v1"
TWEET_SIGNAL_TEMPLATE_CODE = "tweet_signal_v1"
TAG_SCHEMA_FEEDS_V1 = "feeds_v1"
TAG_SCHEMA_TWEET_SIGNAL_V1 = "tweet_signal_v1"
TAG_SCHEMA_NEWS_EODHD_V1 = "news_eodhd_v1"

_CASHTAG_RE = re.compile(r"\$[A-Z]{1,5}\b")


class FeedContentTaggingService:
    """Feeds (subscription_content) 内容自动打标 + 摘要服务"""

    DEFAULT_TEMPLATE_CODE = FEEDS_TEMPLATE_CODE

    def __init__(
        self,
        db_manager: DatabaseManager,
        log_callback: Optional[Callable] = None,
        template_code: Optional[str] = None,
    ):
        self.db_manager = db_manager
        self.log_callback = log_callback
        # 允许通过 AUTO_FEED_TAG_TEMPLATE_CODE 环境变量灰度切换模板 (便于 v2 迭代)
        self.template_code = (
            template_code
            or os.getenv("AUTO_FEED_TAG_TEMPLATE_CODE")
            or self.DEFAULT_TEMPLATE_CODE
        )
        self.llm_service = LocalLLMAnalysisService(
            template_code=self.template_code,
            log_callback=log_callback,
        )

    def log(self, message: str, level: str = "info", **kwargs):
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(message, **kwargs)
        if self.log_callback:
            try:
                self.log_callback(log_message=message)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # 读 subscription_content
    # ------------------------------------------------------------------

    def get_content(self, content_id: int) -> Optional[Dict[str, Any]]:
        """
        读 subscription_content 单行 (只拉打标需要的字段).

        Args:
            content_id: subscription_content.id

        Returns:
            字典或 None. 包含 channel_type / content / title / transcript /
            is_retweet / retweeted_* / is_quote / quoted_* 等字段.
        """
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, channel_type, content_id, title, content,
                       author_identifier, author_name,
                       is_retweet, retweeted_author, retweeted_content,
                       is_quote, quoted_author, quoted_content,
                       transcript, metadata, categories
                FROM subscription_content
                WHERE id = %s
                """,
                (content_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "channel_type": row[1],
                "content_id_ext": row[2],
                "title": row[3],
                "content": row[4],
                "author_identifier": row[5],
                "author_name": row[6],
                "is_retweet": row[7],
                "retweeted_author": row[8],
                "retweeted_content": row[9],
                "is_quote": row[10],
                "quoted_author": row[11],
                "quoted_content": row[12],
                "transcript": row[13],
                "metadata": row[14] or {},
                "categories": row[15] or [],
            }
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 打标文本组装 (Channel Adapter)
    # ------------------------------------------------------------------

    def _build_tagging_text(
        self, row: Dict[str, Any]
    ) -> Optional[Tuple[str, str]]:
        """
        根据 channel_type 把 subscription_content 行组装成 LLM 输入文本.

        Returns:
            (text, channel_type) 或 None (跳过)
        """
        channel_type = (row.get("channel_type") or "").strip()
        if channel_type not in _SUPPORTED_CHANNELS:
            return None

        allowed = {
            c.strip()
            for c in os.getenv(
                "AUTO_FEED_TAG_CHANNELS", "twitter,youtube,news,zhishixingqiu"
            ).split(",")
            if c.strip()
        }
        if channel_type not in allowed:
            return None

        max_chars = int(os.getenv("AUTO_FEED_TAG_MAX_CHARS", "6000"))

        if channel_type == "news":
            min_len = int(os.getenv("AUTO_FEED_TAG_MIN_LENGTH_NEWS", "200"))
            title = (row.get("title") or "").strip()
            content = (row.get("content") or "").strip()
            text = f"{title}\n\n{content}" if (title and content) else (title or content)
            if len(text) < min_len:
                return None
            if len(text) > max_chars:
                text = text[:max_chars]
            return text, "news"

        if channel_type == "twitter":
            min_len = int(os.getenv("AUTO_FEED_TAG_MIN_LENGTH_TWITTER", "50"))
            parts = []
            main = (row.get("content") or "").strip()
            if main:
                parts.append(main)
            if row.get("is_quote") and row.get("quoted_content"):
                quoted_author = row.get("quoted_author") or ""
                parts.append(
                    f"\n[引用 @{quoted_author}]\n{row['quoted_content']}"
                )
            if row.get("is_retweet") and row.get("retweeted_content"):
                rt_author = row.get("retweeted_author") or ""
                parts.append(
                    f"\n[转推 @{rt_author}]\n{row['retweeted_content']}"
                )
            text = "\n".join(parts).strip()
            if len(text) < min_len:
                return None
            if len(text) > max_chars:
                text = text[:max_chars]
            return text, "twitter"

        if channel_type == "youtube":
            min_len = int(os.getenv("AUTO_FEED_TAG_MIN_LENGTH_YOUTUBE", "200"))
            transcript = (row.get("transcript") or "").strip()
            if not transcript or len(transcript) < min_len:
                return None
            title = (row.get("title") or "").strip()
            text = f"{title}\n\n{transcript}" if title else transcript
            if len(text) > max_chars:
                text = text[:max_chars]
            return text, "youtube"

        return None

    # ------------------------------------------------------------------
    # 模板路由 (Twitter 短推 → tweet_signal_v1)
    # ------------------------------------------------------------------

    def _pick_template_code(self, channel_type: str, text: str) -> str:
        """按渠道与内容特征选择打标模板."""
        if channel_type in ("youtube", "news"):
            return FEEDS_TEMPLATE_CODE
        if channel_type != "twitter":
            return self.template_code or FEEDS_TEMPLATE_CODE
        if os.getenv("TWEET_SIGNAL_V1_ENABLED", "false").lower() != "true":
            return FEEDS_TEMPLATE_CODE
        threshold = int(os.getenv("TWEET_SIGNAL_HEAVY_THRESHOLD_CHARS", "250"))
        if len(text) >= threshold or _CASHTAG_RE.search(text):
            return FEEDS_TEMPLATE_CODE
        return TWEET_SIGNAL_TEMPLATE_CODE

    @staticmethod
    def _schema_version_for_template(template_code: str) -> str:
        if template_code == TWEET_SIGNAL_TEMPLATE_CODE:
            return TAG_SCHEMA_TWEET_SIGNAL_V1
        return TAG_SCHEMA_FEEDS_V1

    def _normalize_tag_data(
        self, tag_data: Dict[str, Any], schema_version: str
    ) -> Dict[str, Any]:
        """校验并补全 LLM 输出字段."""
        if schema_version == TAG_SCHEMA_TWEET_SIGNAL_V1:
            required = [
                "tickers",
                "concepts",
                "people",
                "countries",
                "stance",
                "signal_type",
                "importance",
                "summary",
                "confidence",
            ]
        else:
            required = ["sector", "industry", "tickers", "people", "countries"]

        missing = [k for k in required if k not in tag_data]
        if missing:
            raise ValueError(
                f"Feeds 打标 LLM 输出缺少必需字段: {missing}, "
                f"got_keys={list(tag_data.keys())}"
            )

        tag_data.setdefault("sector_en", tag_data.get("sector_en") or "")
        tag_data.setdefault("industry_en", tag_data.get("industry_en") or "")
        tag_data.setdefault("concepts", [])
        tag_data.setdefault("people", [])
        tag_data.setdefault("countries", [])
        tag_data.setdefault("confidence", None)
        tag_data.setdefault("reasoning", None)
        tag_data.setdefault("stance", None)
        tag_data.setdefault("signal_type", None)
        tag_data.setdefault("importance", None)

        if schema_version == TAG_SCHEMA_TWEET_SIGNAL_V1:
            for key in ("sector", "industry", "sector_en", "industry_en"):
                if tag_data.get(key) in (None, "null", "None"):
                    tag_data[key] = None
        else:
            tag_data.setdefault("sector", "未分类")
            tag_data.setdefault("industry", "未分类")

        if not tag_data.get("summary"):
            tag_data["summary"] = None

        if not isinstance(tag_data.get("tickers"), list):
            tag_data["tickers"] = []
        if not isinstance(tag_data.get("people"), list):
            tag_data["people"] = []
        if not isinstance(tag_data.get("countries"), list):
            tag_data["countries"] = []

        return tag_data

    # ------------------------------------------------------------------
    # 现有标签 (幂等保护)
    # ------------------------------------------------------------------

    def get_existing_tags(self, content_id: int) -> Optional[Dict[str, Any]]:
        conn = self.db_manager.get_connection()
        if not conn:
            return None
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, subscription_content_id, sector, sector_en,
                       industry, industry_en, ticker, concepts,
                       additional_tags, confidence, reasoning, summary,
                       channel_type, tagged_by, tagged_at,
                       created_at, updated_at,
                       people, countries, tag_schema_version
                FROM subscription_content_tags
                WHERE subscription_content_id = %s
                """,
                (content_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            tickers = row[6] if isinstance(row[6], list) else (row[6] or [])
            concepts = row[7] if isinstance(row[7], list) else (row[7] or [])
            additional = row[8] if row[8] else {}
            people = row[17] if isinstance(row[17], list) else (row[17] or [])
            countries = row[18] if isinstance(row[18], list) else (row[18] or [])
            return {
                "id": row[0],
                "subscription_content_id": row[1],
                "sector": row[2],
                "sector_en": row[3],
                "industry": row[4],
                "industry_en": row[5],
                "tickers": tickers,
                "concepts": concepts,
                "additional_tags": additional,
                "confidence": row[9],
                "reasoning": row[10],
                "summary": row[11],
                "channel_type": row[12],
                "tagged_by": row[13],
                "tagged_at": row[14],
                "created_at": row[15],
                "updated_at": row[16],
                "people": people,
                "countries": countries,
                "tag_schema_version": row[19] or TAG_SCHEMA_FEEDS_V1,
            }
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 轻量映射 (EODHD News: 直接把 API tags 写入 subscription_content_tags, 零 LLM)
    # ------------------------------------------------------------------

    def map_api_tags(self, content_id: int) -> Dict[str, Any]:
        """
        把 subscription_content.metadata.symbols / categories 直接映射到
        subscription_content_tags, 无需调用 LLM.

        适用场景: EODHD 等已有 API tags 的 news 来源.
        tag_schema_version = 'news_eodhd_v1' 便于下游区分映射产物与 LLM 产物.

        Returns:
            {'success': bool, 'skipped': bool, 'tag_id': int, ...}
        """
        row = self.get_content(content_id)
        if not row:
            raise ValueError(f"subscription_content 不存在: id={content_id}")

        # 幂等: 已有标签则跳过
        existing = self.get_existing_tags(content_id)
        if existing:
            self.log(
                f"map_api_tags 跳过 (已有标签): content_id={content_id}",
                level="info",
            )
            return {
                "success": True,
                "skipped": True,
                "content_id": content_id,
                "message": "已有标签",
                "existing_tags": existing,
            }

        metadata = row.get("metadata") or {}
        symbols = metadata.get("symbols") or []
        categories = row.get("categories") or []

        if not symbols and not categories:
            self.log(
                f"map_api_tags 跳过 (无 symbols/categories): content_id={content_id}",
                level="info",
            )
            return {
                "success": True,
                "skipped": True,
                "content_id": content_id,
                "message": "无 API tags 可映射",
            }

        # 标准化 tickers: EODHD 格式 "NVDA.US" → 保留原样 (与 file_tags 一致)
        tickers = [str(s) for s in symbols if s] if symbols else []

        tag_data = {
            "sector": "未分类",
            "sector_en": "Uncategorized",
            "industry": "未分类",
            "industry_en": "Uncategorized",
            "tickers": tickers,
            "concepts": [str(c) for c in categories if c],
            "people": [],
            "countries": [],
            "summary": None,
            "confidence": None,
            "reasoning": None,
        }

        save_result = self.save_tags(
            content_id=content_id,
            channel_type="news",
            tag_data=tag_data,
            tag_schema_version=TAG_SCHEMA_NEWS_EODHD_V1,
            ai_metadata=None,
            tagged_by=None,
        )

        self.log(
            f"map_api_tags 完成: content_id={content_id} "
            f"tickers={len(tickers)} concepts={len(tag_data['concepts'])}",
            level="info",
        )

        return {
            "success": True,
            "skipped": False,
            "content_id": content_id,
            "tag_id": save_result["tag_id"],
            "tag_schema_version": TAG_SCHEMA_NEWS_EODHD_V1,
            "tickers": tickers,
            "concepts": tag_data["concepts"],
        }

    # ------------------------------------------------------------------
    # 主入口
    # ------------------------------------------------------------------

    async def tag_content(
        self,
        content_id: int,
        additional_context: str = "",
        force: bool = False,
        tagged_by: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        为一条 subscription_content 生成 AI 标签 + 摘要.

        返回:
            {
                'success': True,
                'skipped': bool,
                'content_id': ...,
                'channel_type': ...,
                'tag_id': ... (新/更新的 subscription_content_tags.id),
                'tags': {...},
                'processing_time': float,
                'ai_metadata': {...}
            }
        """
        start_time = time.time()

        self.log(f"正在获取 feed 内容: content_id={content_id}")
        row = self.get_content(content_id)
        if not row:
            raise ValueError(f"subscription_content 不存在: id={content_id}")

        channel_type = row.get("channel_type")

        built = self._build_tagging_text(row)
        if built is None:
            self.log(
                f"跳过打标: content_id={content_id} channel_type={channel_type} "
                "(不在支持列表 / 长度不足 / 无字幕)",
                level="info",
            )
            return {
                "success": True,
                "skipped": True,
                "content_id": content_id,
                "channel_type": channel_type,
                "message": "内容不满足打标条件",
            }

        text, resolved_channel = built

        picked_template = self._pick_template_code(resolved_channel, text)
        schema_version = self._schema_version_for_template(picked_template)

        if picked_template != getattr(self.llm_service, "template_code", None):
            self.llm_service = LocalLLMAnalysisService(
                template_code=picked_template,
                log_callback=self.log_callback,
            )

        # 幂等: 已有标签且非强制 -> 跳过
        if not force:
            existing = self.get_existing_tags(content_id)
            if existing:
                self.log(
                    f"已有标签, 跳过: content_id={content_id}", level="info"
                )
                return {
                    "success": True,
                    "skipped": True,
                    "content_id": content_id,
                    "channel_type": channel_type,
                    "message": "已有标签",
                    "existing_tags": existing,
                }

        self.log(
            f"调用 LLM 打标: content_id={content_id} channel={resolved_channel} "
            f"template={picked_template} schema={schema_version} text_len={len(text)}"
        )

        llm_model = None
        if picked_template == TWEET_SIGNAL_TEMPLATE_CODE:
            override = os.getenv("TWEET_SIGNAL_MODEL_OVERRIDE", "").strip()
            if override:
                llm_model = override

        analysis_result = await self.llm_service.analyze_file_content(
            file_content=text,
            file_name=f"feed:{resolved_channel}:{content_id}",
            custom_variables={
                "channel_type": resolved_channel,
                "additional_context": additional_context or "",
            },
            user_id=tagged_by,
            model=llm_model,
        )

        report_content = analysis_result.get("report_content", "") or ""

        try:
            cleaned = report_content.strip()
            if "```json" in cleaned:
                cleaned = cleaned.split("```json", 1)[1].split("```", 1)[0].strip()
            elif "```" in cleaned:
                parts = cleaned.split("```")
                if len(parts) >= 3:
                    cleaned = parts[1].strip()
            tag_data = json.loads(cleaned)
        except json.JSONDecodeError as e:
            self.log(f"LLM 返回 JSON 解析失败: {e}", level="error")
            raise ValueError(
                f"Feeds 打标 LLM 返回格式错误, 无法解析 JSON: {e}"
            )

        tag_data = self._normalize_tag_data(tag_data, schema_version)

        save_result = self.save_tags(
            content_id=content_id,
            channel_type=resolved_channel,
            tag_data=tag_data,
            tag_schema_version=schema_version,
            ai_metadata={
                "total_tokens": analysis_result.get("total_tokens", 0),
                "total_price": analysis_result.get("total_price", 0),
                "response_time": analysis_result.get("response_time", 0),
                "model": analysis_result.get("model"),
                "provider": analysis_result.get("provider"),
            },
            tagged_by=tagged_by,
        )

        processing_time = time.time() - start_time

        self.log(
            f"Feeds 打标完成: content_id={content_id} channel={resolved_channel} "
            f"schema={schema_version} sector={tag_data.get('sector')} "
            f"tickers={len(tag_data.get('tickers') or [])} "
            f"people={len(tag_data.get('people') or [])} "
            f"countries={len(tag_data.get('countries') or [])} "
            f"summary_len={len(tag_data.get('summary') or '')} "
            f"耗时={processing_time:.2f}s"
        )

        return {
            "success": True,
            "skipped": False,
            "content_id": content_id,
            "channel_type": resolved_channel,
            "tag_schema_version": schema_version,
            "template_code": picked_template,
            "tag_id": save_result["tag_id"],
            "tags": tag_data,
            "processing_time": processing_time,
            "ai_metadata": {
                "total_tokens": analysis_result.get("total_tokens", 0),
                "total_price": analysis_result.get("total_price", 0),
                "model": analysis_result.get("model"),
                "provider": analysis_result.get("provider"),
            },
        }

    # ------------------------------------------------------------------
    # UPSERT
    # ------------------------------------------------------------------

    def save_tags(
        self,
        content_id: int,
        channel_type: str,
        tag_data: Dict[str, Any],
        tag_schema_version: str = TAG_SCHEMA_FEEDS_V1,
        ai_metadata: Optional[Dict[str, Any]] = None,
        tagged_by: Optional[int] = None,
    ) -> Dict[str, Any]:
        """UPSERT subscription_content_tags"""
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        try:
            cursor = conn.cursor()

            if tag_schema_version == TAG_SCHEMA_TWEET_SIGNAL_V1:
                sector = tag_data.get("sector") or ""
                industry = tag_data.get("industry") or ""
            else:
                sector = tag_data.get("sector") or "未分类"
                industry = tag_data.get("industry") or "未分类"

            sector_en = tag_data.get("sector_en") or ""
            industry_en = tag_data.get("industry_en") or ""
            tickers = tag_data.get("tickers") or []
            concepts = tag_data.get("concepts") or []
            people = tag_data.get("people") or []
            countries = tag_data.get("countries") or []
            confidence = tag_data.get("confidence")
            reasoning = tag_data.get("reasoning")
            summary = tag_data.get("summary")

            ticker_jsonb = Json(tickers) if tickers else None
            concepts_jsonb = Json(concepts)
            people_jsonb = Json(people)
            countries_jsonb = Json(countries)

            additional_tags: Dict[str, Any] = {"tickers_full": tickers}
            if tag_schema_version == TAG_SCHEMA_TWEET_SIGNAL_V1:
                additional_tags["tweet_signal"] = {
                    "stance": tag_data.get("stance"),
                    "signal_type": tag_data.get("signal_type"),
                    "importance": tag_data.get("importance"),
                }
            if ai_metadata:
                additional_tags["ai_metadata"] = {
                    "total_tokens": ai_metadata.get("total_tokens", 0),
                    "total_price": ai_metadata.get("total_price", 0),
                    "response_time": ai_metadata.get("response_time", 0),
                    "model": ai_metadata.get("model"),
                    "provider": ai_metadata.get("provider"),
                }

            cursor.execute(
                """
                INSERT INTO subscription_content_tags (
                    subscription_content_id,
                    sector, sector_en, industry, industry_en,
                    ticker, concepts, people, countries,
                    additional_tags,
                    confidence, reasoning, summary,
                    channel_type, tag_schema_version,
                    tagged_by, tagged_at
                ) VALUES (
                    %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, CURRENT_TIMESTAMP
                )
                ON CONFLICT (subscription_content_id) DO UPDATE SET
                    sector = EXCLUDED.sector,
                    sector_en = EXCLUDED.sector_en,
                    industry = EXCLUDED.industry,
                    industry_en = EXCLUDED.industry_en,
                    ticker = EXCLUDED.ticker,
                    concepts = EXCLUDED.concepts,
                    people = EXCLUDED.people,
                    countries = EXCLUDED.countries,
                    additional_tags = EXCLUDED.additional_tags,
                    confidence = EXCLUDED.confidence,
                    reasoning = EXCLUDED.reasoning,
                    summary = EXCLUDED.summary,
                    channel_type = EXCLUDED.channel_type,
                    tag_schema_version = EXCLUDED.tag_schema_version,
                    tagged_by = EXCLUDED.tagged_by,
                    tagged_at = EXCLUDED.tagged_at,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
                """,
                (
                    content_id,
                    sector, sector_en, industry, industry_en,
                    ticker_jsonb, concepts_jsonb, people_jsonb, countries_jsonb,
                    Json(additional_tags),
                    confidence, reasoning, summary,
                    channel_type, tag_schema_version,
                    tagged_by,
                ),
            )

            tag_id = cursor.fetchone()[0]
            conn.commit()
            return {
                "success": True,
                "tag_id": tag_id,
                "subscription_content_id": content_id,
            }
        except Exception as e:
            conn.rollback()
            self.log(f"保存 feeds 标签失败: {e}", level="error")
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 手动编辑 / 删除
    # ------------------------------------------------------------------

    def update_tags(
        self,
        content_id: int,
        updates: Dict[str, Any],
        tagged_by: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        更新指定字段. 允许的字段: sector, sector_en, industry, industry_en,
        tickers, concepts, people, countries, confidence, reasoning, summary,
        additional_tags (merge).
        """
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        try:
            cursor = conn.cursor()

            update_fields = []
            params: list = []

            simple_fields = {
                "sector": "sector",
                "sector_en": "sector_en",
                "industry": "industry",
                "industry_en": "industry_en",
                "confidence": "confidence",
                "reasoning": "reasoning",
                "summary": "summary",
            }
            for key, col in simple_fields.items():
                if key in updates:
                    update_fields.append(f"{col} = %s")
                    params.append(updates[key])

            if "tickers" in updates:
                tickers = updates["tickers"] or []
                update_fields.append("ticker = %s")
                params.append(Json(tickers) if tickers else None)
                update_fields.append(
                    "additional_tags = "
                    "jsonb_set(COALESCE(additional_tags, '{}'::jsonb), "
                    "'{tickers_full}', %s::jsonb)"
                )
                params.append(json.dumps(tickers, ensure_ascii=False))

            if "concepts" in updates:
                concepts = updates["concepts"] or []
                update_fields.append("concepts = %s::jsonb")
                params.append(json.dumps(concepts, ensure_ascii=False))

            if "people" in updates:
                people = updates["people"] or []
                update_fields.append("people = %s::jsonb")
                params.append(json.dumps(people, ensure_ascii=False))

            if "countries" in updates:
                countries = updates["countries"] or []
                update_fields.append("countries = %s::jsonb")
                params.append(json.dumps(countries, ensure_ascii=False))

            if "additional_tags" in updates:
                update_fields.append(
                    "additional_tags = COALESCE(additional_tags, '{}'::jsonb) || %s::jsonb"
                )
                params.append(
                    json.dumps(updates["additional_tags"], ensure_ascii=False)
                )

            if not update_fields:
                return {
                    "success": True,
                    "message": "没有需要更新的字段",
                    "subscription_content_id": content_id,
                }

            if tagged_by is not None:
                update_fields.append("tagged_by = %s")
                params.append(tagged_by)

            update_fields.append("updated_at = CURRENT_TIMESTAMP")
            params.append(content_id)

            sql = f"""
                UPDATE subscription_content_tags
                SET {', '.join(update_fields)}
                WHERE subscription_content_id = %s
                RETURNING id
            """
            cursor.execute(sql, params)
            result = cursor.fetchone()
            if not result:
                raise ValueError(
                    f"subscription_content_tags 不存在: content_id={content_id}"
                )
            conn.commit()
            self.log(
                f"Feeds 标签更新成功: content_id={content_id} tag_id={result[0]}"
            )
            return {
                "success": True,
                "tag_id": result[0],
                "subscription_content_id": content_id,
            }
        except Exception as e:
            conn.rollback()
            self.log(f"更新 feeds 标签失败: {e}", level="error")
            raise
        finally:
            conn.close()

    def delete_tags(self, content_id: int) -> Dict[str, Any]:
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM subscription_content_tags
                WHERE subscription_content_id = %s
                RETURNING id
                """,
                (content_id,),
            )
            result = cursor.fetchone()
            conn.commit()
            if result:
                self.log(f"Feeds 标签删除: content_id={content_id}")
                return {"success": True, "deleted": True}
            return {
                "success": True,
                "deleted": False,
                "message": "标签不存在",
            }
        except Exception as e:
            conn.rollback()
            self.log(f"删除 feeds 标签失败: {e}", level="error")
            raise
        finally:
            conn.close()
