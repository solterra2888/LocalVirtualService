# -*- coding: utf-8 -*-
"""
Feeds 内容自动打标 + 摘要 的 Celery 任务

对标 backend/tasks/tagging_tasks.py (tag_file_content_task / batch_tag_files_task),
但绑定到 subscription_content (Twitter / YouTube / News feed 单条内容).

任务列表:
  tag_feed_content        — LLM 打标 (twitter/youtube/cctv news)
  batch_tag_feed_content  — 批量派发 tag_feed_content
  map_news_tags           — 轻量映射 (EODHD news: 零 LLM, API tags → subscription_content_tags)

设计文档: docs/instructions/FEEDS_AUTO_TAGGING_PLAN.md
"""
import asyncio
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from celery import current_task

from backend.celery_app import app
from backend.database.db_manager import DatabaseManager
from backend.services.feed_content_tagging_service import (
    FeedContentTaggingService,
)
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import utcnow
from backend.utils.logger import error, info, warning


@app.task(
    bind=True,
    name="tag_feed_content",
    queue="file_processing",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 30},
    retry_backoff=True,
    retry_backoff_max=120,
    retry_jitter=True,
)
def tag_feed_content_task(
    self,
    content_id: int,
    username: Optional[str] = None,
    additional_context: str = "",
    force: bool = False,
) -> Dict[str, Any]:
    """
    为 subscription_content 单条内容自动打标 + 生成摘要.

    结构完全对标 tag_file_content_task:
      - autoretry_for + max_retries=3, 指数退避, jitter
      - 启用性检查 (AUTO_FEED_TAG_ENABLED)
      - 长度 / channel 白名单门槛在 FeedContentTaggingService._build_tagging_text 内处理
      - 已有标签且非 force -> skipped
      - 失败返回 status='failed' 而不 raise (避免 Celery 序列化栈),
        同时 autoretry_for 会在真实异常时兜底重试

    Args:
        content_id: subscription_content.id
        username: 触发用户 (仅用于日志)
        additional_context: 传给 prompt 的额外上下文
        force: 忽略幂等与启用开关, 强制重跑

    Returns:
        统一 schema 的 dict (status in completed/skipped/failed).
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
                        "timestamp": utcnow().isoformat(),
                        "level": "info",
                        "message": log_message,
                        "source": "celery_feed_tagging",
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
                    "username": username,
                    "logs": _progress["logs"],
                },
            )
            if log_message:
                info(
                    f"FeedTagging {self.request.id}: {log_message}",
                    source="celery_feed_tagging",
                )
        except Exception as e:
            error(
                f"更新 feed 打标任务状态失败: {e}",
                source="celery_feed_tagging",
            )

    try:
        auto_enabled = (
            os.getenv("AUTO_FEED_TAG_ENABLED", "true").lower() == "true"
        )
        if not auto_enabled and not force:
            info(
                f"Feeds 自动打标已禁用, 跳过 content_id={content_id}",
                source="celery_feed_tagging",
            )
            return {
                "status": "skipped",
                "message": "Feeds 自动打标功能已禁用",
                "content_id": content_id,
            }

        info(
            f"开始 feed 打标任务 {self.request.id}, content_id={content_id}, "
            f"user={username}, force={force}",
            source="celery_feed_tagging",
        )
        update_progress(
            status="initializing",
            progress=5,
            log_message=f"初始化 feed 打标... (content_id={content_id})",
        )

        db_manager = DatabaseManager(get_database_config())
        tagging_service = FeedContentTaggingService(
            db_manager=db_manager,
            log_callback=update_progress,
        )

        update_progress(progress=15, log_message="读取 feed 内容...")

        # PR-4 修复: username → user_id, 让 LLM usage_log 能正确归属用户
        from backend.utils.user_resolver import get_user_id_by_username
        user_id = get_user_id_by_username(username)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                tagging_service.tag_content(
                    content_id=content_id,
                    additional_context=additional_context,
                    force=force,
                    tagged_by=user_id,
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
                "channel_type": result.get("channel_type"),
                "message": result.get("message", "已跳过"),
                "existing_tags": result.get("existing_tags"),
            }

        update_progress(
            status="completed",
            progress=100,
            log_message=(
                f"打标完成: sector={result['tags'].get('sector')} "
                f"tickers={len(result['tags'].get('tickers') or [])} "
                f"summary_len={len(result['tags'].get('summary') or '')}"
            ),
        )

        # PR-N2: 全局打标成功后链式派发 fan-out.
        # FEED_SUMMARY_AUTO_FANOUT_ENABLED (默认 false):
        #   - false → 个性化摘要改为纯手动触发, 不自动派发 (节省 tokens/成本).
        #   - true  → 恢复旧行为: fan-out 根据 user_feed_summary_settings 为每个用户跑一次 LLM.
        # 手动触发入口 (POST /api/subscriptions/contents/{id}/personalize) 不受此开关影响.
        auto_fanout_enabled = (
            os.getenv("FEED_SUMMARY_AUTO_FANOUT_ENABLED", "false").lower() == "true"
        )
        if not auto_fanout_enabled:
            info(
                f"FeedTagging {self.request.id}: 跳过自动 fan-out "
                f"(FEED_SUMMARY_AUTO_FANOUT_ENABLED=false) content_id={content_id}",
                source="celery_feed_tagging",
            )
        else:
            try:
                from backend.tasks.feed_summary_tasks import (
                    fan_out_personalize_summary_task,
                )
                fan_out_personalize_summary_task.apply_async(
                    args=(content_id,),
                    countdown=2,
                )
                info(
                    f"FeedTagging {self.request.id}: 已链式派发 fan_out_personalize_summary "
                    f"content_id={content_id}",
                    source="celery_feed_tagging",
                )
            except Exception as fan_out_err:
                # 即使 fan-out 派发本身失败, tag 主链路也算成功 (用户仍能看到 baseline summary)
                error(
                    f"FeedTagging {self.request.id}: 派发 fan_out_personalize_summary 失败 "
                    f"content_id={content_id} err={fan_out_err}",
                    source="celery_feed_tagging",
                )

        # ── Theme Matching Chain: 打标完成后自动关联投资主题（向量匹配） ──
        try:
            theme_matching_enabled = os.getenv('THEME_VECTOR_MATCHING_ENABLED', 'false').lower() == 'true'
            if theme_matching_enabled:
                from backend.tasks.theme_matching_tasks import match_content_themes_vector_task
                match_content_themes_vector_task.apply_async(
                    args=('feed', content_id),
                    countdown=2,
                )
                info(
                    f"FeedTagging {self.request.id}: 已链式派发 theme vector matching "
                    f"content_id={content_id}",
                    source="celery_feed_tagging",
                )
        except Exception as theme_err:
            error(
                f"FeedTagging {self.request.id}: Theme matching dispatch 失败 "
                f"content_id={content_id} err={theme_err}",
                source="celery_feed_tagging",
            )

        return {
            "status": "completed",
            "content_id": content_id,
            "channel_type": result.get("channel_type"),
            "tag_id": result["tag_id"],
            "tags": result["tags"],
            "processing_time": result["processing_time"],
            "ai_metadata": result.get("ai_metadata", {}),
            "logs": _progress["logs"],
        }

    except Exception as e:
        error_msg = f"feed 打标任务失败: {e}"
        error(
            f"FeedTagging {self.request.id}: {error_msg}",
            source="celery_feed_tagging",
        )
        # autoretry_for=(Exception,) 会在 retries < max_retries 时自动重试;
        # 这里返回 failed 结果仅在用尽重试后生效 (Celery 会把最终异常吞掉交给 autoretry).
        # 注意: 由于 autoretry_for 仅对真实异常生效, 要触发重试必须 raise.
        # 所以正确做法是 raise, 让 Celery 走 retry 链.
        raise


@app.task(
    bind=True,
    name="batch_tag_feed_content",
    queue="file_processing",
)
def batch_tag_feed_content_task(
    self,
    content_ids: List[int],
    username: Optional[str] = None,
    force: bool = False,
    per_item_countdown: int = 1,
) -> Dict[str, Any]:
    """
    批量为多条 feed content 打标. 不直接跑 LLM, 而是把 tag_feed_content_task
    以 apply_async + countdown 梯度派发出去, 避免单 task 长占用 + 雪崩.

    适用场景:
      - 前端"批量重打标"按钮
      - 运维 / 回填脚本一次提交 N 条

    Args:
        content_ids: subscription_content.id 列表
        username: 触发用户 (仅日志)
        force: 是否忽略幂等
        per_item_countdown: 相邻任务的最小延迟 (秒), 防止瞬时 QPS 峰值

    Returns:
        {'total', 'dispatched', 'errors', 'details'}
    """
    info(
        f"开始批量 feed 打标任务 {self.request.id}, "
        f"total={len(content_ids)}, user={username}, force={force}",
        source="celery_feed_tagging",
    )

    results: Dict[str, Any] = {
        "total": len(content_ids),
        "dispatched": 0,
        "errors": 0,
        "details": [],
    }

    for idx, cid in enumerate(content_ids):
        try:
            countdown = per_item_countdown * idx
            tag_feed_content_task.apply_async(
                args=(cid, username, "", force),
                countdown=countdown,
            )
            results["dispatched"] += 1
            results["details"].append(
                {
                    "content_id": cid,
                    "status": "dispatched",
                    "countdown": countdown,
                }
            )
        except Exception as e:
            results["errors"] += 1
            warning(
                f"批量 feed 打标派发失败: content_id={cid} error={e}",
                source="celery_feed_tagging",
            )
            results["details"].append(
                {
                    "content_id": cid,
                    "status": "error",
                    "error": str(e),
                }
            )

    info(
        f"批量 feed 打标派发完成 {self.request.id}: "
        f"dispatched={results['dispatched']}, errors={results['errors']}",
        source="celery_feed_tagging",
    )
    return results


@app.task(
    bind=True,
    name="map_news_tags",
    queue="file_processing",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 2, "countdown": 10},
    retry_backoff=True,
)
def map_news_tags_task(
    self,
    content_id: int,
) -> Dict[str, Any]:
    """
    轻量映射任务: 把 EODHD news 的 metadata.symbols / categories 直接写入
    subscription_content_tags, 不调用 LLM.

    幂等: 已有 tag 行时直接跳过.

    Args:
        content_id: subscription_content.id

    Returns:
        {'status': 'completed'|'skipped'|'failed', 'content_id': int, ...}
    """
    info(
        f"map_news_tags 开始: content_id={content_id}",
        source="celery_feed_tagging",
    )
    try:
        db_manager = DatabaseManager(get_database_config())
        service = FeedContentTaggingService(db_manager=db_manager)
        result = service.map_api_tags(content_id)

        status = "skipped" if result.get("skipped") else "completed"
        info(
            f"map_news_tags {status}: content_id={content_id} "
            f"tickers={len(result.get('tickers') or [])} "
            f"concepts={len(result.get('concepts') or [])}",
            source="celery_feed_tagging",
        )

        # ── Theme Matching Chain: EODHD news 映射后也关联投资主题（向量匹配） ──
        if status == "completed":
            try:
                theme_matching_enabled = os.getenv('THEME_VECTOR_MATCHING_ENABLED', 'false').lower() == 'true'
                if theme_matching_enabled:
                    from backend.tasks.theme_matching_tasks import match_content_themes_vector_task
                    match_content_themes_vector_task.apply_async(
                        args=('feed', content_id),
                        countdown=2,
                    )
                    info(
                        f"map_news_tags: 已链式派发 theme vector matching content_id={content_id}",
                        source="celery_feed_tagging",
                    )
            except Exception as theme_err:
                error(
                    f"map_news_tags: Theme matching dispatch 失败 content_id={content_id} err={theme_err}",
                    source="celery_feed_tagging",
                )

        return {
            "status": status,
            "content_id": content_id,
            "tag_id": result.get("tag_id"),
            "message": result.get("message", ""),
        }

    except Exception as e:
        error(
            f"map_news_tags 失败: content_id={content_id} error={e}",
            source="celery_feed_tagging",
        )
        raise
