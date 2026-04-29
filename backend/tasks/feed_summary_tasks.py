# -*- coding: utf-8 -*-
"""
Feed Summary Personalization Celery Tasks (PR-6 ③ / PR-N2)

异步执行 per-user 个性化 summary 生成. 与 tag_feed_content_task (全局打标) 并存,
但不互相替代:
    tag_feed_content_task                 → subscription_content_tags (全局共享)
    personalize_feed_summary_task (本文件) → subscription_content_user_summaries (per-user)

调用入口:
    1. PR-7  POST /api/subscriptions/contents/{id}/personalize  → 用户主动 "Re-summarize" (兜底)
    2. PR-N2 fan_out_personalize_summary_task                   → tag 完成后自动扇出
                                                                  (基于 user_feed_summary_settings 开关)

队列: file_processing (与其他 LLM 任务复用)
重试: 3 次指数退避, 与 tag_feed_content_task 一致
"""
import asyncio
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from celery import current_task

from backend.celery_app import app
from backend.database.db_manager import DatabaseManager
from backend.services.feed_summary_personalization_service import (
    FeedSummaryPersonalizationService,
)
from backend.services.user_feed_summary_settings_service import (
    UserFeedSummarySettingsService,
)
from backend.utils.database_config import get_database_config
from backend.utils.logger import error, info, warning


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


# ──────────────────────────────────────────────────────────────────────
# PR-N2: Fan-out 任务
#
# 在 tag_feed_content_task 成功完成后被链式派发. 职责:
#   1. 读 subscription_content (拿 channel_type / user_ids / 内容长度)
#   2. 长度门槛过滤 (max(env_min, user.min_length))
#   3. 查 user_feed_summary_settings 找出该渠道下 enabled=true 的用户
#      AND user_id ∈ subscription_content.user_ids
#   4. for each eligible user:
#        personalize_feed_summary_task.apply_async(content_id, user_id, ...)
#        countdown 阶梯 (避免雪崩)
#
# 不直接跑 LLM, 只做调度. 失败也不阻塞 tag 主链路.
# ──────────────────────────────────────────────────────────────────────


def _measure_content_length(row: Dict[str, Any]) -> int:
    """估算一条 subscription_content 的"打标可用长度", 用于 min_length 门槛.

    与 FeedContentTaggingService._build_tagging_text 的语义对齐:
        - twitter: content + retweeted_content + quoted_content 总长
        - youtube: transcript 长度
    """
    ch = (row.get("channel_type") or "").strip()
    if ch == "twitter":
        parts = [
            row.get("content") or "",
            row.get("retweeted_content") or "" if row.get("is_retweet") else "",
            row.get("quoted_content") or "" if row.get("is_quote") else "",
        ]
        return sum(len(p) for p in parts if p)
    if ch == "youtube":
        return len(row.get("transcript") or "")
    return 0


def _get_default_min_length(channel_type: str) -> int:
    """渠道默认最小长度 (与 FeedContentTaggingService 用同一组 env)."""
    if channel_type == "twitter":
        return int(os.getenv("AUTO_FEED_TAG_MIN_LENGTH_TWITTER", "50"))
    if channel_type == "youtube":
        return int(os.getenv("AUTO_FEED_TAG_MIN_LENGTH_YOUTUBE", "200"))
    return 0


@app.task(
    bind=True,
    name="fan_out_personalize_summary",
    queue="file_processing",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 2, "countdown": 10},
    retry_backoff=True,
    retry_jitter=True,
)
def fan_out_personalize_summary_task(self, content_id: int) -> Dict[str, Any]:
    """
    扇出 per-user 个性化 summary 派发器.

    Args:
        content_id: subscription_content.id (tag 完成后由 tag_feed_content_task 链式触发)

    Returns:
        {
            'status': 'completed' | 'skipped',
            'content_id': ...,
            'channel_type': ...,
            'eligible_users': int,
            'dispatched': int,
            'skipped_short_content': int,
        }
    """
    try:
        # 总开关 (与 personalize_feed_summary_task 共享同一 env, 关则整个链路停)
        feature_enabled = (
            os.getenv("FEED_SUMMARY_PERSONALIZATION_ENABLED", "true").lower() == "true"
        )
        if not feature_enabled:
            info(
                f"FanOut[{self.request.id}] 跳过: "
                "FEED_SUMMARY_PERSONALIZATION_ENABLED=false",
                source="celery_feed_summary",
            )
            return {
                "status": "skipped",
                "content_id": content_id,
                "message": "feature disabled by env",
            }

        db_manager = DatabaseManager(get_database_config())

        # 1. 读 subscription_content (channel_type / user_ids / 文本长度字段)
        conn = db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, channel_type, user_ids,
                       content, transcript,
                       is_retweet, retweeted_content,
                       is_quote, quoted_content
                FROM   subscription_content
                WHERE  id = %s
                """,
                (content_id,),
            )
            row = cur.fetchone()
        finally:
            conn.close()

        if not row:
            warning(
                f"FanOut[{self.request.id}] subscription_content.id={content_id} 不存在, 跳过",
                source="celery_feed_summary",
            )
            return {
                "status": "skipped",
                "content_id": content_id,
                "message": "content not found",
            }

        content_dict = {
            "channel_type": row[1],
            "user_ids": row[2] or [],
            "content": row[3],
            "transcript": row[4],
            "is_retweet": row[5],
            "retweeted_content": row[6],
            "is_quote": row[7],
            "quoted_content": row[8],
        }
        channel_type = content_dict["channel_type"]
        user_ids: List[int] = content_dict["user_ids"]

        if channel_type not in ("twitter", "youtube"):
            info(
                f"FanOut[{self.request.id}] channel_type={channel_type!r} 不在白名单, 跳过",
                source="celery_feed_summary",
            )
            return {
                "status": "skipped",
                "content_id": content_id,
                "channel_type": channel_type,
                "message": "channel not supported",
            }

        if not user_ids:
            info(
                f"FanOut[{self.request.id}] content_id={content_id} 无订阅用户, 跳过",
                source="celery_feed_summary",
            )
            return {
                "status": "skipped",
                "content_id": content_id,
                "channel_type": channel_type,
                "message": "no subscribers",
            }

        # 2. 内容长度 (用于 min_length 门槛)
        content_length = _measure_content_length(content_dict)
        env_min_length = _get_default_min_length(channel_type)

        # 3. 找 eligible 用户 (开关开启 + 用户在订阅列表)
        ufss_service = UserFeedSummarySettingsService(db_manager)
        eligible = ufss_service.find_eligible_users(
            channel_type=channel_type,
            user_id_pool=user_ids,
        )

        if not eligible:
            info(
                f"FanOut[{self.request.id}] content_id={content_id} channel={channel_type} "
                f"无开启个性化的订阅用户 (订阅总数={len(user_ids)}), 跳过",
                source="celery_feed_summary",
            )
            return {
                "status": "completed",
                "content_id": content_id,
                "channel_type": channel_type,
                "eligible_users": 0,
                "dispatched": 0,
                "skipped_short_content": 0,
            }

        # 4. 逐个派发, 阶梯 countdown 避免同一秒打满 LLM 限速
        dispatched = 0
        skipped_short = 0
        for idx, user_pref in enumerate(eligible):
            user_min_length = user_pref.get("min_length") or 0
            effective_min = max(env_min_length, user_min_length)
            if content_length < effective_min:
                skipped_short += 1
                continue

            try:
                personalize_feed_summary_task.apply_async(
                    args=(content_id, user_pref["user_id"]),
                    kwargs={
                        "template_id": user_pref.get("template_id"),
                        "force": False,
                    },
                    countdown=2 + idx * 2,  # 阶梯派发: 2s, 4s, 6s...
                )
                dispatched += 1
            except Exception as e:
                error(
                    f"FanOut[{self.request.id}] 派发 user_id={user_pref['user_id']} 失败: {e}",
                    source="celery_feed_summary",
                )

        info(
            f"FanOut[{self.request.id}] content_id={content_id} channel={channel_type} "
            f"subscribers={len(user_ids)} eligible={len(eligible)} "
            f"dispatched={dispatched} skipped_short={skipped_short} "
            f"content_len={content_length}",
            source="celery_feed_summary",
        )

        return {
            "status": "completed",
            "content_id": content_id,
            "channel_type": channel_type,
            "eligible_users": len(eligible),
            "dispatched": dispatched,
            "skipped_short_content": skipped_short,
        }

    except Exception as e:
        error(
            f"FanOut[{self.request.id}] 失败 content_id={content_id}: {e}",
            source="celery_feed_summary",
        )
        # 父任务 (tag_feed_content_task) 通过 apply_async 解耦, 不会被这个异常影响.
        # autoretry_for=(Exception,) 让 fan-out 自身重试 2 次后认输.
        raise
