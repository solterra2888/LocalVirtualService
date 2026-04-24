# -*- coding: utf-8 -*-
"""
Feeds 内容自动打标签 + 摘要服务

对标 backend/services/content_tagging_service.py (ContentTaggingService),
但绑定到 subscription_content (Twitter / YouTube feed 单条内容),
使用 content_tag_feeds_v1 prompt 模板.

设计文档: docs/instructions/FEEDS_AUTO_TAGGING_PLAN.md
"""
import json
import os
import time
from typing import Dict, Any, Optional, Callable, Tuple

from psycopg2.extras import Json

from backend.services.local_llm_analysis_service import LocalLLMAnalysisService
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger

logger = get_logger("feed_content_tagging_service")

# 支持的渠道 (news/rss 本轮不做, 直接短路)
_SUPPORTED_CHANNELS = {"twitter", "youtube"}


class FeedContentTaggingService:
    """Feeds (subscription_content) 内容自动打标 + 摘要服务"""

    DEFAULT_TEMPLATE_CODE = "content_tag_feeds_v1"

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
                       transcript
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
                "AUTO_FEED_TAG_CHANNELS", "twitter,youtube"
            ).split(",")
            if c.strip()
        }
        if channel_type not in allowed:
            return None

        max_chars = int(os.getenv("AUTO_FEED_TAG_MAX_CHARS", "6000"))

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
                       created_at, updated_at
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
            }
        finally:
            conn.close()

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
            f"text_len={len(text)}"
        )

        # 调用 LLM.
        #
        # LocalLLMAnalysisService.analyze_file_content 会把 file_content 参数
        # 强制注入到 variables['file_content'] (覆盖 custom_variables 中的同名键),
        # 所以此处把 channel_type / additional_context 放 custom_variables 即可.
        analysis_result = await self.llm_service.analyze_file_content(
            file_content=text,
            file_name=f"feed:{resolved_channel}:{content_id}",
            custom_variables={
                "channel_type": resolved_channel,
                "additional_context": additional_context or "",
            },
        )

        report_content = analysis_result.get("report_content", "") or ""

        # 解析 JSON (与 ContentTaggingService 同样的剥壳策略)
        try:
            cleaned = report_content.strip()
            if "```json" in cleaned:
                cleaned = cleaned.split("```json", 1)[1].split("```", 1)[0].strip()
            elif "```" in cleaned:
                # 兼容没写语言的代码块
                parts = cleaned.split("```")
                if len(parts) >= 3:
                    cleaned = parts[1].strip()
            tag_data = json.loads(cleaned)
        except json.JSONDecodeError as e:
            self.log(f"LLM 返回 JSON 解析失败: {e}", level="error")
            raise ValueError(
                f"Feeds 打标 LLM 返回格式错误, 无法解析 JSON: {e}"
            )

        # 校验必填字段
        required = ["sector", "industry", "tickers"]
        missing = [k for k in required if k not in tag_data]
        if missing:
            raise ValueError(
                f"Feeds 打标 LLM 输出缺少必需字段: {missing}, got_keys={list(tag_data.keys())}"
            )

        # 补默认值 (容忍旧模板)
        tag_data.setdefault("sector_en", "")
        tag_data.setdefault("industry_en", "")
        tag_data.setdefault("concepts", [])
        tag_data.setdefault("confidence", None)
        # summary 是 Feeds 模板的 required, 但防御性兜底
        if "summary" not in tag_data or not tag_data.get("summary"):
            self.log(
                f"⚠ LLM 未返回 summary (content_id={content_id}), 置空",
                level="warning",
            )
            tag_data["summary"] = None
        # reasoning 可选, 老字段兼容
        tag_data.setdefault("reasoning", None)

        save_result = self.save_tags(
            content_id=content_id,
            channel_type=resolved_channel,
            tag_data=tag_data,
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
            f"sector={tag_data.get('sector')} "
            f"tickers={len(tag_data.get('tickers') or [])} "
            f"concepts={len(tag_data.get('concepts') or [])} "
            f"summary_len={len(tag_data.get('summary') or '')} "
            f"耗时={processing_time:.2f}s"
        )

        return {
            "success": True,
            "skipped": False,
            "content_id": content_id,
            "channel_type": resolved_channel,
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
        ai_metadata: Optional[Dict[str, Any]] = None,
        tagged_by: Optional[int] = None,
    ) -> Dict[str, Any]:
        """UPSERT subscription_content_tags"""
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        try:
            cursor = conn.cursor()

            sector = tag_data.get("sector") or "未分类"
            sector_en = tag_data.get("sector_en") or ""
            industry = tag_data.get("industry") or "未分类"
            industry_en = tag_data.get("industry_en") or ""
            tickers = tag_data.get("tickers") or []
            concepts = tag_data.get("concepts") or []
            confidence = tag_data.get("confidence")
            reasoning = tag_data.get("reasoning")
            summary = tag_data.get("summary")

            ticker_jsonb = Json(tickers) if tickers else None
            concepts_jsonb = Json(concepts)

            additional_tags: Dict[str, Any] = {"tickers_full": tickers}
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
                    ticker, concepts, additional_tags,
                    confidence, reasoning, summary,
                    channel_type, tagged_by, tagged_at
                ) VALUES (
                    %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, CURRENT_TIMESTAMP
                )
                ON CONFLICT (subscription_content_id) DO UPDATE SET
                    sector = EXCLUDED.sector,
                    sector_en = EXCLUDED.sector_en,
                    industry = EXCLUDED.industry,
                    industry_en = EXCLUDED.industry_en,
                    ticker = EXCLUDED.ticker,
                    concepts = EXCLUDED.concepts,
                    additional_tags = EXCLUDED.additional_tags,
                    confidence = EXCLUDED.confidence,
                    reasoning = EXCLUDED.reasoning,
                    summary = EXCLUDED.summary,
                    channel_type = EXCLUDED.channel_type,
                    tagged_by = EXCLUDED.tagged_by,
                    tagged_at = EXCLUDED.tagged_at,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
                """,
                (
                    content_id,
                    sector, sector_en, industry, industry_en,
                    ticker_jsonb, concepts_jsonb, Json(additional_tags),
                    confidence, reasoning, summary,
                    channel_type, tagged_by,
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
        tickers, concepts, confidence, reasoning, summary, additional_tags (merge).
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
