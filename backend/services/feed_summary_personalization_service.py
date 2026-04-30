# -*- coding: utf-8 -*-
"""
Feed Summary Personalization Service — Feeds per-user 个性化摘要服务

设计目标 (LLM_REFACTOR_PLAN §5.2, PR-6):
    与 FeedContentTaggingService 互补:
      - FeedContentTaggingService → subscription_content_tags (全局共享, 客观分类 + baseline summary)
      - FeedSummaryPersonalizationService → subscription_content_user_summaries (per-user 主观覆盖)

    保护 Feeds 跨用户内容复用机制不被破坏: 客观字段 (sector/industry/ticker) 仍走全局,
    主观字段 (summary/reasoning) per-user 个性化, 互不影响.

调用链:
    用户主动 "Re-summarize" / 加星收藏 (PR-7)
        ↓
    POST /api/subscriptions/contents/{id}/personalize  (PR-7)
        ↓
    personalize_feed_summary_task  (PR-6 ③ Celery)
        ↓
    FeedSummaryPersonalizationService.generate_for_user()
        ├─ LLMTemplateResolver.resolve(module='feed_summary', user_id, runtime_template_id)
        ├─ FeedContentTaggingService._build_tagging_text() (复用文本组装)
        └─ LocalLLMAnalysisService.analyze_file_content() → llm_prompt_usage_logs

模板 (PR-3 注册):
    SYSTEM_DEFAULTS['feed_summary'] = None (无系统默认)
    用户必须自己创建 user-scope 模板, 或在 runtime 显式传 template_id.
    无可用模板时抛 ResolveError.
"""
import json
import time
from typing import Any, Callable, Dict, Optional

from psycopg2.extras import Json

from backend.database.db_manager import DatabaseManager
from backend.services.feed_content_tagging_service import FeedContentTaggingService
from backend.services.llm_template_resolver import (
    LLMTemplateResolver,
    ResolveError,
)
from backend.services.local_llm_analysis_service import LocalLLMAnalysisService
from backend.utils.logger import get_logger

logger = get_logger("feed_summary_personalization_service")


class FeedSummaryPersonalizationService:
    """Feeds per-user 个性化 summary 服务.

    PR-E (2026-04-27): 按 channel_type 自动路由到对应 module:
      - twitter → feed_summary  (Twitter feed item per-user 摘要)
      - youtube → youtube_summary (YouTube video per-user 摘要, PR-A 新增)
    通过 _CHANNEL_TO_MODULE 决定调用 LLMTemplateResolver 时用哪个 module_code,
    实现"一份代码 + 一张存储表 (subscription_content_user_summaries) 服务两个模块".
    """

    # 默认 (channel_type 缺失或未知时退回 feed_summary, 兼容旧调用)
    DEFAULT_MODULE_CODE = "feed_summary"

    # PR-E: channel → module 路由表
    _CHANNEL_TO_MODULE: Dict[str, str] = {
        "twitter": "feed_summary",
        "youtube": "youtube_summary",
    }

    def __init__(
        self,
        db_manager: DatabaseManager,
        log_callback: Optional[Callable] = None,
    ):
        self.db_manager = db_manager
        self.log_callback = log_callback
        # 复用 FeedContentTaggingService 的文本组装能力 (channel adapter)
        # 避免在两个服务里重复 "Twitter 引用/转推树拼接" 与 "YouTube title+transcript 拼接"
        self._text_builder = FeedContentTaggingService(
            db_manager=db_manager,
            log_callback=log_callback,
        )

    # ──────────────────────────────────────────────────────────────────
    # Logger
    # ──────────────────────────────────────────────────────────────────

    def log(self, message: str, level: str = "info"):
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(message)
        if self.log_callback:
            try:
                self.log_callback(log_message=message)
            except Exception:
                pass

    # ──────────────────────────────────────────────────────────────────
    # 主入口
    # ──────────────────────────────────────────────────────────────────

    async def generate_for_user(
        self,
        content_id: int,
        user_id: int,
        runtime_template_id: Optional[int] = None,
        template_variables: Optional[Dict[str, Any]] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        为 (content_id, user_id) 生成个性化 summary.

        Args:
            content_id: subscription_content.id
            user_id:    用户 ID (per-user 视图归属; 同时进入 llm_prompt_usage_logs.user_id)
            runtime_template_id: 显式指定模板 ID (从 API / 用户主动 "Re-summarize" 而来);
                                  优先级最高
            template_variables: 运行时变量 (合并到 LLM 调用 custom_variables)
            force: True 时即使已有 personalize 摘要也强制重生成
                   (默认 False: 模板没变就直接复用历史)

        Returns:
            {
                'success': True,
                'skipped': bool,
                'content_id': ...,
                'user_id': ...,
                'summary_id': ...,        # subscription_content_user_summaries.id
                'template_id': ...,       # 实际使用的模板 ID
                'template_source': ...,   # 'runtime' | 'preference' | 'default'
                'summary': '...',
                'reasoning': '...' | None,
                'processing_time': float,
                'ai_metadata': {...}
            }
        """
        start_time = time.time()

        if not content_id:
            raise ValueError("content_id is required")
        if not user_id:
            raise ValueError("user_id is required (FeedSummaryPersonalizationService is per-user)")

        # 1. 读 subscription_content 行
        self.log(f"读取 feed 内容: content_id={content_id}")
        row = self._text_builder.get_content(content_id)
        if not row:
            raise ValueError(f"subscription_content.id={content_id} 不存在")

        channel_type = row.get("channel_type") or ""

        # 2. 文本组装 (复用 FeedContentTaggingService 的 channel adapter)
        built = self._text_builder._build_tagging_text(row)
        if built is None:
            self.log(
                f"跳过个性化: content_id={content_id} channel_type={channel_type} "
                "(渠道不支持 / 长度不足 / YouTube 无字幕)",
                level="info",
            )
            return {
                "success": True,
                "skipped": True,
                "content_id": content_id,
                "user_id": user_id,
                "channel_type": channel_type,
                "message": "内容不满足个性化条件",
            }
        text, resolved_channel = built

        # 3. PR-E: 按 channel_type 选模块, 然后走 resolver
        #    (runtime_template_id > 用户私有模板 > system default)
        target_module = self._CHANNEL_TO_MODULE.get(
            resolved_channel, self.DEFAULT_MODULE_CODE
        )
        try:
            resolver = LLMTemplateResolver()
            resolved = resolver.resolve(
                module_code=target_module,
                user_id=user_id,
                runtime_template_id=runtime_template_id,
            )
        except ResolveError as e:
            # PR-A/B 后两个模块都已 seed user_owned + system default,
            # 走到这里通常是数据异常 (该用户私有模板被人为删除且 system default 被 archive).
            self.log(f"模板解析失败: module={target_module} err={e}", level="error")
            raise ValueError(
                f"无法解析 {target_module} 模板. 请检查 Prompt Templates 页面, "
                f"或调用 API 时显式传 template_id."
            ) from e

        self.log(
            f"模板解析: source={resolved.source} template_id={resolved.template_id} "
            f"code={resolved.template_code}"
        )

        # 4. 幂等保护: 若已有相同 (user, content, template) 的 summary 且非 force, 直接返回
        if not force:
            existing = self._get_existing(content_id, user_id)
            if existing and existing.get("template_id") == resolved.template_id:
                self.log(
                    f"个性化 summary 已存在 (相同模板), 跳过: "
                    f"content_id={content_id} user_id={user_id}"
                )
                return {
                    "success": True,
                    "skipped": True,
                    "content_id": content_id,
                    "user_id": user_id,
                    "summary_id": existing["id"],
                    "template_id": existing["template_id"],
                    "template_source": resolved.source,
                    "summary": existing["summary"],
                    "reasoning": existing.get("reasoning"),
                    "message": "已有同模板个性化摘要",
                }

        # 5. 调 LLM (走中央入口 → 自动写 usage_log)
        self.log(
            f"调用 LLM 生成个性化 summary: content_id={content_id} user_id={user_id} "
            f"channel={resolved_channel} text_len={len(text)}"
        )

        llm_service = LocalLLMAnalysisService(
            template_id=resolved.template_id,
            log_callback=self.log_callback,
        )

        # 方案 B: 按渠道把 row 的细颗粒字段注入模板变量,
        # 让用户在 PromptTemplatesPage 里可以自由用 {{author}}/{{title}}/{{text}}/
        # {{channel}}/{{description}}/{{transcript}} 等占位符.
        # 调用方传入的 template_variables 优先级最高 (覆盖默认推导).
        per_channel_vars = self._build_template_variables(
            row=row,
            channel=resolved_channel,
            text=text,
        )
        merged_vars: Dict[str, Any] = {
            "channel_type": resolved_channel,
            **per_channel_vars,
            **(template_variables or {}),
        }

        analysis = await llm_service.analyze_file_content(
            file_content=text,
            file_name=f"feed_personalize:{resolved_channel}:{content_id}:user{user_id}",
            custom_variables=merged_vars,
            user_id=user_id,
        )

        # 6. 解析输出
        # parsed_data 优先 (chat_completion_with_template 已根据 template.output_format 解析);
        # 缺失则尝试从 report_content 直接拿 plain text 作为 summary
        parsed = analysis.get("parsed_data")
        report_content = analysis.get("report_content", "") or ""

        if isinstance(parsed, dict) and parsed.get("summary"):
            summary_text = parsed["summary"]
            reasoning = parsed.get("reasoning")
            custom_concepts = parsed.get("concepts") or parsed.get("custom_concepts") or []
        else:
            # 尝试 JSON 解析 (兼容用户使用同款 content_tag_feeds_v1 风格模板)
            summary_text, reasoning, custom_concepts = self._parse_legacy(report_content)

        if not summary_text:
            raise ValueError(
                f"LLM 返回内容无法解析为 summary (content_id={content_id} user_id={user_id})"
            )

        # 7. UPSERT subscription_content_user_summaries
        save_result = self._upsert(
            content_id=content_id,
            user_id=user_id,
            template_id=resolved.template_id,
            summary=summary_text,
            reasoning=reasoning,
            custom_concepts=custom_concepts,
            ai_metadata={
                "total_tokens": analysis.get("total_tokens", 0),
                "total_price": analysis.get("total_price", 0),
                "model": analysis.get("model"),
                "provider": analysis.get("provider"),
                "response_time": analysis.get("response_time", 0),
                "currency": analysis.get("currency"),
            },
        )

        elapsed = time.time() - start_time
        self.log(
            f"个性化 summary 完成: content_id={content_id} user_id={user_id} "
            f"summary_len={len(summary_text)} 耗时={elapsed:.2f}s"
        )

        return {
            "success": True,
            "skipped": False,
            "content_id": content_id,
            "user_id": user_id,
            "channel_type": resolved_channel,
            "summary_id": save_result["summary_id"],
            "template_id": resolved.template_id,
            "template_source": resolved.source,
            "summary": summary_text,
            "reasoning": reasoning,
            "custom_concepts": custom_concepts,
            "processing_time": elapsed,
            "ai_metadata": {
                "total_tokens": analysis.get("total_tokens", 0),
                "total_price": analysis.get("total_price", 0),
                "model": analysis.get("model"),
                "provider": analysis.get("provider"),
            },
        }

    # ──────────────────────────────────────────────────────────────────
    # 辅助: 模板变量映射 (PR-A 方案 B)
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _build_template_variables(
        row: Dict[str, Any],
        channel: str,
        text: str,
    ) -> Dict[str, Any]:
        """
        把 subscription_content 行按渠道映射到 prompt 模板占位符.

        feed_summary_v1 (Twitter / 通用): {{author}} {{title}} {{text}}
            - text 是 _build_tagging_text 已合并的正文 (含转推/引用)
            - title 多数 Twitter 行为空, 兜底空字符串
        youtube_summary_v1 (v3 投研派): {{channel}} {{title}} {{transcript}}
            - channel = author_name (频道名)
            - transcript = 仅字幕, 不含 title (与 _build_tagging_text 合并的 text 区分)
            - description (RSS 抓的视频简介) v3 起不再注入: 内容多为长广告/标签/链接,
              信噪比低, 让 LLM 看反而干扰投研侧分析; 见 migration
              update_youtube_summary_bloomberg_v3_investor_lens.sql
        """
        if channel == "youtube":
            return {
                "channel": (row.get("author_name") or "").strip(),
                "title": (row.get("title") or "").strip(),
                "transcript": (row.get("transcript") or "").strip(),
            }

        # twitter / 默认: feed_summary_v1
        return {
            "author": (
                row.get("author_name") or row.get("author_identifier") or ""
            ).strip(),
            "title": (row.get("title") or "").strip(),
            "text": text,
        }

    # ──────────────────────────────────────────────────────────────────
    # 辅助: 已有 personalize 摘要查询
    # ──────────────────────────────────────────────────────────────────

    def _get_existing(self, content_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        conn = self.db_manager.get_connection()
        if not conn:
            return None
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, template_id, summary, reasoning, custom_concepts,
                       ai_metadata, tagged_at, updated_at
                FROM subscription_content_user_summaries
                WHERE subscription_content_id = %s AND user_id = %s
                """,
                (content_id, user_id),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "template_id": row[1],
                "summary": row[2],
                "reasoning": row[3],
                "custom_concepts": row[4] if isinstance(row[4], list) else (row[4] or []),
                "ai_metadata": row[5] if row[5] else {},
                "tagged_at": row[6],
                "updated_at": row[7],
            }
        finally:
            conn.close()

    # ──────────────────────────────────────────────────────────────────
    # 兼容性 JSON 输出解析 (容忍 report_content 是 JSON 字符串的旧模板)
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_legacy(report_content: str):
        """从 LLM 返回的 plain text / JSON-in-markdown 中尽力提取 summary."""
        cleaned = (report_content or "").strip()
        if not cleaned:
            return None, None, []

        # 剥 markdown json 代码块
        if "```json" in cleaned:
            cleaned = cleaned.split("```json", 1)[1].split("```", 1)[0].strip()
        elif "```" in cleaned:
            parts = cleaned.split("```")
            if len(parts) >= 3:
                cleaned = parts[1].strip()

        # 尝试 JSON 解析
        try:
            data = json.loads(cleaned)
            if isinstance(data, dict) and data.get("summary"):
                return (
                    data["summary"],
                    data.get("reasoning"),
                    data.get("concepts") or data.get("custom_concepts") or [],
                )
        except json.JSONDecodeError:
            pass

        # 当作纯文本 summary 处理 (完整保留 LLM 输出, 不截断)
        return cleaned, None, []

    # ──────────────────────────────────────────────────────────────────
    # UPSERT
    # ──────────────────────────────────────────────────────────────────

    def _upsert(
        self,
        content_id: int,
        user_id: int,
        template_id: Optional[int],
        summary: str,
        reasoning: Optional[str],
        custom_concepts,
        ai_metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        conn = self.db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO subscription_content_user_summaries (
                    subscription_content_id, user_id, template_id,
                    summary, reasoning, custom_concepts, ai_metadata,
                    tagged_at
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT (subscription_content_id, user_id) DO UPDATE SET
                    template_id     = EXCLUDED.template_id,
                    summary         = EXCLUDED.summary,
                    reasoning       = EXCLUDED.reasoning,
                    custom_concepts = EXCLUDED.custom_concepts,
                    ai_metadata     = EXCLUDED.ai_metadata,
                    tagged_at       = EXCLUDED.tagged_at,
                    updated_at      = CURRENT_TIMESTAMP
                RETURNING id
                """,
                (
                    content_id, user_id, template_id,
                    summary, reasoning,
                    Json(custom_concepts or []),
                    Json(ai_metadata or {}),
                ),
            )
            summary_id = cur.fetchone()[0]
            conn.commit()
            return {"summary_id": summary_id}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ──────────────────────────────────────────────────────────────────
    # 删除 (用户取消个性化, 回退到全局 summary)
    # ──────────────────────────────────────────────────────────────────

    def delete_for_user(self, content_id: int, user_id: int) -> bool:
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        try:
            cur = conn.cursor()
            cur.execute(
                """
                DELETE FROM subscription_content_user_summaries
                WHERE subscription_content_id = %s AND user_id = %s
                """,
                (content_id, user_id),
            )
            deleted = cur.rowcount > 0
            conn.commit()
            return deleted
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
