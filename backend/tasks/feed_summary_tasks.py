# -*- coding: utf-8 -*-
"""
Feed Summary Personalization Celery Tasks (PR-6 ③)

异步执行 per-user 个性化 summary 生成. 与 tag_feed_content_task (全局打标) 并存,
但不互相替代:
    tag_feed_content_task                 → subscription_content_tags (全局共享)
    personalize_feed_summary_task (本文件) → subscription_content_user_summaries (per-user)

调用入口:
    PR-7 POST /api/subscriptions/contents/{id}/personalize  → 用户主动 "Re-summarize"
    (可选未来) 加星/收藏 feed 时自动派发

队列: file_processing (与其他 LLM 任务复用)
重试: 3 次指数退避, 与 tag_feed_content_task 一致
"""
import asyncio
import os
from datetime import datetime
from typing import Any, Dict, Optional

from celery import current_task

from backend.celery_app import app
from backend.database.db_manager import DatabaseManager
from backend.services.feed_summary_personalization_service import (
    FeedSummaryPersonalizationService,
)
from backend.utils.database_config import get_database_config
from backend.utils.logger import error, info


@app.task(
    bind=True,
    name="personalize_feed_summary",
    queue="file_processing",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 30},
    retry_backoff=True,
    retry_backoff_max=120,
    retry_jitter=True,
)
def personalize_feed_summary_task(
    self,
    content_id: int,
    user_id: int,
    template_id: Optional[int] = None,
    template_variables: Optional[Dict[str, Any]] = None,
    force: bool = False,
) -> Dict[str, Any]:
    """
    为 (content_id, user_id) 异步生成个性化 summary.

    Args:
        content_id: subscription_content.id
        user_id:    用户 ID (per-user 视图归属 + LLM usage_log 归属)
        template_id: 显式指定模板 (优先级最高); None 时走 user_template_preferences[feed_summary]
                     或抛 ResolveError (feed_summary 无系统默认)
        template_variables: 运行时模板变量
        force: True 时强制重生成 (忽略已有同模板摘要的幂等保护)

    Returns:
        统一 schema (status in completed / skipped / failed):
            { 'status', 'content_id', 'user_id', 'summary_id', 'template_id',
              'template_source', 'summary', 'reasoning', 'processing_time',
              'ai_metadata', 'logs' }
    """
    _progress = {"status": "processing", "progress": 0.0, "logs": []}

    def update_progress(
        status: Optional[str] = None,
        progress: Optional[float] = None,
        log_message: Optional[str] = None,
        **_kwargs,
    ):
        try:
            if progress is not None:
                _progress["progress"] = progress
            if status is not None:
                _progress["status"] = status
            if log_message:
                _progress["logs"].append(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "level": "info",
                        "message": log_message,
                        "source": "celery_feed_summary",
                    }
                )
                if len(_progress["logs"]) > 30:
                    _progress["logs"] = _progress["logs"][-30:]
            current_task.update_state(
                state="PROGRESS",
                meta={
                    "status": _progress["status"],
                    "progress": _progress["progress"],
                    "message": log_message or "",
                    "content_id": content_id,
                    "user_id": user_id,
                    "logs": _progress["logs"],
                },
            )
            if log_message:
                info(
                    f"FeedSummary {self.request.id}: {log_message}",
                    source="celery_feed_summary",
                )
        except Exception as e:
            error(
                f"更新 personalize_feed_summary 进度失败: {e}",
                source="celery_feed_summary",
            )

    try:
        # 总开关 (与 AUTO_FEED_TAG_ENABLED 风格一致)
        feature_enabled = (
            os.getenv("FEED_SUMMARY_PERSONALIZATION_ENABLED", "true").lower() == "true"
        )
        if not feature_enabled and not force:
            info(
                f"Feed summary 个性化功能已禁用 (FEED_SUMMARY_PERSONALIZATION_ENABLED=false), "
                f"跳过 content_id={content_id} user_id={user_id}",
                source="celery_feed_summary",
            )
            return {
                "status": "skipped",
                "message": "Feed summary personalization disabled by feature flag",
                "content_id": content_id,
                "user_id": user_id,
            }

        info(
            f"开始 feed summary 个性化任务 {self.request.id}: "
            f"content_id={content_id} user_id={user_id} "
            f"template_id={template_id} force={force}",
            source="celery_feed_summary",
        )

        update_progress(
            status="initializing",
            progress=5,
            log_message=(
                f"初始化个性化任务 (content_id={content_id} user_id={user_id})"
            ),
        )

        db_manager = DatabaseManager(get_database_config())
        service = FeedSummaryPersonalizationService(
            db_manager=db_manager,
            log_callback=update_progress,
        )

        update_progress(progress=15, log_message="读取 feed 内容并解析模板...")

        # 跑核心异步逻辑
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                service.generate_for_user(
                    content_id=content_id,
                    user_id=user_id,
                    runtime_template_id=template_id,
                    template_variables=template_variables,
                    force=force,
                )
            )
        finally:
            loop.close()

        if result.get("skipped"):
            update_progress(
                status="completed",
                progress=100,
                log_message=result.get("message", "已跳过"),
            )
            return {
                "status": "skipped",
                "content_id": content_id,
                "user_id": user_id,
                "summary_id": result.get("summary_id"),
                "template_id": result.get("template_id"),
                "template_source": result.get("template_source"),
                "summary": result.get("summary"),
                "reasoning": result.get("reasoning"),
                "message": result.get("message"),
                "logs": _progress["logs"],
            }

        update_progress(
            status="completed",
            progress=100,
            log_message=(
                f"个性化完成: source={result.get('template_source')} "
                f"summary_len={len(result.get('summary') or '')}"
            ),
        )

        return {
            "status": "completed",
            "content_id": content_id,
            "user_id": user_id,
            "summary_id": result["summary_id"],
            "template_id": result["template_id"],
            "template_source": result["template_source"],
            "summary": result["summary"],
            "reasoning": result.get("reasoning"),
            "custom_concepts": result.get("custom_concepts"),
            "processing_time": result["processing_time"],
            "ai_metadata": result.get("ai_metadata", {}),
            "logs": _progress["logs"],
        }

    except Exception as e:
        error_msg = f"feed summary 个性化失败: {e}"
        error(
            f"FeedSummary {self.request.id}: {error_msg}",
            source="celery_feed_summary",
        )
        # autoretry_for=(Exception,) 会拿到这个 raise 自动重试; 用尽重试后向调用方暴露
        raise
