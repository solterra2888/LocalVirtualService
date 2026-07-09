# -*- coding: utf-8 -*-
"""
订阅相关的Celery任务
"""
import logging
from typing import Optional, Dict, Any

from backend.celery_app import app
from backend.utils.datetime_utils import utcnow

logger = logging.getLogger(__name__)


@app.task(
    name='fetch_twitter_subscription',
    bind=True,
    max_retries=3,
    default_retry_delay=60,  # 失败后60秒重试
    time_limit=900,   # 15分钟硬限制（实测拉取全量需 ~266s，预留充足裕量）
    soft_time_limit=780,  # 13分钟软限制
)
def fetch_twitter_subscription_task(
    self,
    subscription_id: Optional[int] = None,
    user_id: Optional[int] = None,
    force_fetch: bool = False
) -> Dict[str, Any]:
    """
    异步获取Twitter订阅内容（Celery任务）
    
    Args:
        subscription_id: 订阅ID
        user_id: 用户ID
        force_fetch: 是否强制获取
    
    Returns:
        执行结果统计
    """
    try:
        logger.info(f"[Celery Task] Starting Twitter fetch for subscription_id={subscription_id}, "
                   f"user_id={user_id}, force_fetch={force_fetch}")
        
        from backend.services.twitter_subscription_service import TwitterSubscriptionService
        
        twitter_service = TwitterSubscriptionService()
        result = twitter_service.fetch_and_store_tweets(
            subscription_id=subscription_id,
            user_id=user_id,
            force_fetch=force_fetch
        )
        
        logger.info(f"[Celery Task] Twitter fetch completed: {result.get('tweets_new', 0)} new tweets, "
                   f"{result.get('tweets_existing', 0)} existing tweets")
        
        return result
        
    except Exception as exc:
        logger.error(f"[Celery Task] Twitter fetch failed: {str(exc)}", exc_info=True)
        
        # 如果不是最后一次重试，则继续重试
        if self.request.retries < self.max_retries:
            logger.warning(f"[Celery Task] Retrying... (attempt {self.request.retries + 1}/{self.max_retries})")
            raise self.retry(exc=exc)
        else:
            logger.error(f"[Celery Task] Max retries reached, task failed permanently")
            return {
                'success': False,
                'message': f'Failed after {self.max_retries} retries: {str(exc)}',
                'tweets_new': 0,
                'tweets_existing': 0,
                'authors_count': 0,
            }


@app.task(
    name='add_user_to_existing_twitter_content',
    bind=True,
    max_retries=2,
    default_retry_delay=5,
    time_limit=60,
    soft_time_limit=45,
)
def add_user_to_existing_content_task(
    self,
    user_id: int,
    channel_type: str,
    channel_identifier: str,
    subscription_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    将用户添加到现有 subscription_content 的 user_ids（Celery 任务）

    用于复用：Twitter / YouTube / News 等渠道在库里已有该作者内容时，
    新订阅用户需写入 user_ids，否则列表接口（user_id = ANY(user_ids)）查不到数据。

    Args:
        user_id: 新用户 ID
        channel_type: 渠道类型
        channel_identifier: 作者标识符
        subscription_id: 若提供，在合并 user_ids 后刷新该订阅的 unread_count

    Returns:
        更新结果
    """
    try:
        logger.info(f"[Celery Task] Adding user {user_id} to existing content for {channel_identifier}")
        
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 使用 DISTINCT UNNEST 确保不重复
        cursor.execute("""
            UPDATE subscription_content
            SET user_ids = ARRAY(SELECT DISTINCT UNNEST(user_ids || %s::integer))
            WHERE channel_type = %s 
            AND author_identifier = %s
            AND NOT (%s = ANY(user_ids))
        """, (user_id, channel_type, channel_identifier, user_id))
        
        updated_count = cursor.rowcount

        if subscription_id is not None and updated_count >= 0:
            try:
                cursor.execute(
                    "SELECT update_subscription_unread_count(%s, %s)",
                    (subscription_id, user_id),
                )
                cursor.fetchone()
            except Exception as unread_exc:
                logger.warning(
                    f"[Celery Task] update_subscription_unread_count failed: {unread_exc}",
                    exc_info=True,
                )

        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(
            f"[Celery Task] Added user {user_id} to {updated_count} existing rows "
            f"from {channel_identifier} ({channel_type})"
        )
        
        return {
            'success': True,
            'updated_count': updated_count,
            'user_id': user_id,
            'channel_identifier': channel_identifier,
        }
        
    except Exception as exc:
        logger.error(f"[Celery Task] Failed to add user to existing content: {str(exc)}", exc_info=True)
        
        if self.request.retries < self.max_retries:
            logger.warning(f"[Celery Task] Retrying... (attempt {self.request.retries + 1}/{self.max_retries})")
            raise self.retry(exc=exc)
        else:
            return {
                'success': False,
                'message': str(exc),
                'updated_count': 0,
            }


@app.task(
    name='smart_wait_and_add_user',
    bind=True,
    max_retries=1,
    time_limit=120,  # 2分钟硬限制
    soft_time_limit=90,
)
def smart_wait_and_add_user_task(
    self,
    user_id: int,
    channel_type: str,
    channel_identifier: str,
    subscription_id: int,
    max_wait_seconds: int = 60
) -> Dict[str, Any]:
    """
    智能等待其他用户的获取完成，然后添加当前用户（Celery任务）
    
    用于处理race condition：两个用户几乎同时订阅同一作者
    
    Args:
        user_id: 当前用户ID
        channel_type: 渠道类型
        channel_identifier: 作者标识符
        subscription_id: 当前订阅ID
        max_wait_seconds: 最大等待时间（秒）
    
    Returns:
        操作结果
    """
    import time
    
    try:
        logger.info(f"[Celery Task] Smart waiting for {channel_identifier}, user={user_id}")
        
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        check_interval = 5  # 每5秒检查一次
        max_checks = max_wait_seconds // check_interval
        
        for i in range(max_checks):
            time.sleep(check_interval)
            
            conn = db_manager.get_connection()
            cursor = conn.cursor()
            
            # 检查是否有内容了
            cursor.execute("""
                SELECT COUNT(*) FROM subscription_content 
                WHERE channel_type = %s 
                AND author_identifier = %s
            """, (channel_type, channel_identifier))
            
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            if count > 0:
                logger.info(f"[Celery Task] Content for {channel_identifier} is now available ({count} tweets). "
                           f"Adding user {user_id} to existing content.")
                
                # 添加用户到现有内容
                conn = db_manager.get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE subscription_content
                    SET user_ids = ARRAY(SELECT DISTINCT UNNEST(user_ids || %s::integer))
                    WHERE channel_type = %s 
                    AND author_identifier = %s
                    AND NOT (%s = ANY(user_ids))
                """, (user_id, channel_type, channel_identifier, user_id))
                
                updated_count = cursor.rowcount
                conn.commit()
                cursor.close()
                conn.close()
                
                logger.info(f"[Celery Task] Added user {user_id} to {updated_count} existing tweets from {channel_identifier}")
                
                return {
                    'success': True,
                    'strategy': 'waited_and_reused',
                    'updated_count': updated_count,
                    'wait_seconds': (i + 1) * check_interval,
                }
            else:
                logger.info(f"[Celery Task] Waiting for content... ({i+1}/{max_checks} checks)")
        
        # 等待超时，没有内容，触发API获取
        logger.warning(f"[Celery Task] Waited {max_wait_seconds}s but no content found for {channel_identifier}. "
                      f"Triggering API fetch.")
        
        from backend.services.twitter_subscription_service import TwitterSubscriptionService
        
        twitter_service = TwitterSubscriptionService()
        result = twitter_service.fetch_and_store_tweets(
            subscription_id=subscription_id,
            user_id=None,
            force_fetch=True
        )
        
        logger.info(f"[Celery Task] Fetch completed after wait: {result.get('tweets_new', 0)} new tweets")
        
        return {
            'success': True,
            'strategy': 'waited_then_fetched',
            'tweets_new': result.get('tweets_new', 0),
            'wait_seconds': max_wait_seconds,
        }
        
    except Exception as exc:
        logger.error(f"[Celery Task] Smart wait failed: {str(exc)}", exc_info=True)
        return {
            'success': False,
            'message': str(exc),
        }


# NOTE (2026-04 → 2026-05 更新):
# 此任务现在直接使用 YouTubeSubscriptionService 执行实际抓取, 不再做二次派发.
# 路由到 youtube_fetching 队列 (celery_config.py), 由能访问 YouTube 的 Worker 消费:
#   - 本地开发: data_fetching_worker 同时监听 youtube_fetching 队列
#   - 生产/HK: local_virtual_service 的家用 Worker 监听 youtube_fetching 队列
@app.task(
    name='fetch_youtube_subscription',
    bind=True,
    max_retries=2,
    default_retry_delay=60,
    time_limit=600,      # 10 分钟硬超时（RSS + 字幕抓取）
    soft_time_limit=540,
)
def fetch_youtube_subscription_task(
    self,
    subscription_id: Optional[int] = None,
    user_id: Optional[int] = None,
    force_fetch: bool = False
) -> Dict[str, Any]:
    """
    获取单个 YouTube 订阅的最新视频并存入数据库。

    由 subscriptions API 在新建订阅时触发（首次抓取），也可由调度器调用。
    路由到 youtube_fetching 队列，需要能访问 YouTube 的 Worker 执行。
    """
    logger.info(
        f"[Celery Task] fetch_youtube_subscription 开始: "
        f"subscription_id={subscription_id} user_id={user_id} force_fetch={force_fetch}"
    )
    try:
        from backend.services.youtube_subscription_service import YouTubeSubscriptionService

        result = YouTubeSubscriptionService.fetch_and_store_videos(
            subscription_id=subscription_id,
            user_id=user_id,
            force_fetch=force_fetch,
        )

        logger.info(
            f"[Celery Task] fetch_youtube_subscription 完成: "
            f"subscription_id={subscription_id}, "
            f"new={result.get('videos_new', 0)}, "
            f"existing={result.get('videos_existing', 0)}"
        )
        return result

    except Exception as exc:
        logger.error(
            f"[Celery Task] fetch_youtube_subscription 失败: "
            f"subscription_id={subscription_id}, error={exc}",
            exc_info=True,
        )
        if self.request.retries < self.max_retries:
            logger.warning(
                f"[Celery Task] 重试中... "
                f"(attempt {self.request.retries + 1}/{self.max_retries})"
            )
            raise self.retry(exc=exc)
        return {
            'success': False,
            'message': str(exc),
            'videos_new': 0,
            'videos_existing': 0,
        }


@app.task(
    name='fetch_all_youtube_subscriptions',
    bind=True,
    max_retries=2,
    default_retry_delay=120,
    # 本任务实际在 HK 家用 Worker 上执行（concurrency=1 + Webshare 住宅代理），
    # 单次批量要串行跑 ~20 个频道 × ~15 个视频，每次 transcript-api 经过 Webshare
    # 的延迟在 5–30s 之间，叠加偶发 ASR fallback，整批耗时可达 30–60 min。
    # Celery 会把 @task 装饰器上的 time_limit 写进消息头并压过远端 worker 的默认值，
    # 旧值 600/540 在 concurrency=2 的直连时代刚够用，现在必爆（worker 被 SIGKILL
    # 强杀 → 剩余频道全部丢失）。放宽到 70min/60min，给串行 + 代理留足裕量。
    time_limit=4200,     # 70 min 硬超时
    soft_time_limit=3600,  # 60 min 软超时
)
def fetch_all_youtube_subscriptions_task(self) -> Dict[str, Any]:
    """
    批量获取所有启用的 YouTube 订阅内容（Celery 任务）。

    供 Scheduler 的 youtube_subscription 定时任务调用，
    路由到 youtube_fetching 队列，由家用电脑的远程 Worker 执行，
    以使用住宅 IP 绕过 YouTube 对云厂商 IP 的封锁。
    """
    try:
        logger.info("[Celery Task] Starting batch YouTube fetch for all subscriptions")

        from backend.services.youtube_subscription_service import YouTubeSubscriptionService

        result = YouTubeSubscriptionService.fetch_all_subscriptions()

        logger.info(
            f"[Celery Task] Batch YouTube fetch completed: "
            f"channels={result.get('channels_count', 0)}, "
            f"new={result.get('videos_new', 0)}, "
            f"existing={result.get('videos_existing', 0)}"
        )
        return result

    except Exception as exc:
        logger.error(f"[Celery Task] Batch YouTube fetch failed: {str(exc)}", exc_info=True)

        if self.request.retries < self.max_retries:
            logger.warning(f"[Celery Task] Retrying... (attempt {self.request.retries + 1}/{self.max_retries})")
            raise self.retry(exc=exc)
        else:
            logger.error("[Celery Task] Max retries reached, batch YouTube fetch failed permanently")
            return {
                'success': False,
                'message': f'Failed after {self.max_retries} retries: {str(exc)}',
                'channels_count': 0,
                'videos_new': 0,
                'videos_existing': 0,
            }


@app.task(
    name='fetch_youtube_transcripts_batch',
    bind=True,
    max_retries=2,
    default_retry_delay=30,
    # 同 fetch_all_youtube_subscriptions_task：在 HK 家用 Worker 上按 concurrency=1
    # 串行处理 video_items，每项经过 Webshare 会放大到 5–30s。把 time_limit 拉到
    # 40min，让一次能消化 ~150 个视频的批量补抓请求。
    time_limit=2400,     # 40 min 硬超时
    soft_time_limit=2100,  # 35 min 软超时
)
def fetch_youtube_transcripts_batch_task(
    self,
    video_items: list,
) -> Dict[str, Any]:
    """
    批量为 YouTube 视频拉取 transcript（Celery 任务）。

    路由到 youtube_fetching 队列，由家用服务器的远程 Worker 执行，
    以使用住宅 IP 绕过 YouTube 对云厂商 IP 的封锁。

    Args:
        video_items: [{"content_db_id": int, "video_id": str, "video_url": str}, ...]
    """
    try:
        logger.info(
            f"[Celery Task] Starting transcript batch fetch for {len(video_items)} videos"
        )

        from backend.services.youtube_service import YouTubeService
        from backend.services.subscription_content_service import SubscriptionContentService

        content_service = SubscriptionContentService()
        success_count = 0
        fail_count = 0

        for item in video_items:
            content_db_id = item["content_db_id"]
            video_id = item["video_id"]
            video_url = item["video_url"]
            try:
                result = YouTubeService.fetch_transcript(video_url)
                full_text = YouTubeService.get_full_text(result)
                content_service.update_transcript(content_db_id, full_text, result)
                success_count += 1
                logger.info(
                    f"[Celery Task] Transcript OK: content_db_id={content_db_id}, "
                    f"video_id={video_id}, language={result.get('language')}"
                )
            except Exception as e:
                fail_count += 1
                logger.warning(
                    f"[Celery Task] Transcript failed for video_id={video_id}: {e}"
                )

        logger.info(
            f"[Celery Task] Transcript batch done: success={success_count}, "
            f"failed={fail_count}"
        )
        return {
            "success": True,
            "total": len(video_items),
            "transcripts_ok": success_count,
            "transcripts_failed": fail_count,
        }

    except Exception as exc:
        logger.error(
            f"[Celery Task] Transcript batch failed: {str(exc)}", exc_info=True
        )
        if self.request.retries < self.max_retries:
            raise self.retry(exc=exc)
        return {
            "success": False,
            "message": str(exc),
            "total": len(video_items),
            "transcripts_ok": 0,
            "transcripts_failed": len(video_items),
        }


@app.task(
    name='finalize_youtube_feed_task',
    bind=True,
    max_retries=2,
    default_retry_delay=30,
    time_limit=120,
    soft_time_limit=90,
)
def finalize_youtube_feed_task(
    self,
    subscription_content_id: int,
) -> Dict[str, Any]:
    """
    YouTube Feed 字幕落库后的"终态回调"任务.

    与 Upload YouTube Link 的 finalize_youtube_file_task 架构对称:
      - 家用 Worker 在写完 subscription_content.transcript 后,
        通过 Celery send_task 把本任务派发到远程 file_processing 队列.
      - 本任务作为统一入口, 仅负责触发 auto-feed-tagging; 未来可扩展
        auto-name / auto-classify / notify-user.

    设计文档: docs/instructions/FEEDS_AUTO_TAGGING_PLAN.md §3.3 §6.5

    Args:
        subscription_content_id: subscription_content.id

    Returns:
        {'ok': bool, 'content_id': int}
    """
    try:
        logger.info(
            f"[finalize_feed] content_id={subscription_content_id} "
            f"triggering auto-feed-tagging"
        )
        # 延迟导入避免在 worker 启动阶段触发循环引用
        from backend.tasks.file_tasks import trigger_auto_feed_tagging

        trigger_auto_feed_tagging([subscription_content_id], delay=2)
        return {'ok': True, 'content_id': subscription_content_id}

    except Exception as exc:
        logger.error(
            f"[finalize_feed] content_id={subscription_content_id} failed: {exc}",
            exc_info=True,
        )
        if self.request.retries < self.max_retries:
            raise self.retry(exc=exc)
        # 打标失败不影响字幕已落库的事实, 静默返回
        return {'ok': False, 'content_id': subscription_content_id, 'error': str(exc)}


@app.task(
    name='fetch_zhishixingqiu_subscription',
    bind=True,
    max_retries=2,
    default_retry_delay=120,
    # backfill 可能跨多星球翻多页，给足够大的上限
    time_limit=3600,
    soft_time_limit=3300,
)
def fetch_zhishixingqiu_subscription_task(
    self,
    subscription_id: Optional[int] = None,
    user_id: Optional[int] = None,
    force_fetch: bool = False,
) -> Dict[str, Any]:
    """
    抓取用户订阅的知识星球话题，写入 subscription_content。

    路由到 data_fetching 队列（rc-celery-data worker）。
    由 subscriptions API 在新建订阅时触发首抓（backfill），
    也由 Scheduler 定时触发增量抓取。
    """
    is_scheduler_run = subscription_id is None and user_id is None
    logger.info(
        "[Celery Task] fetch_zhishixingqiu_subscription 开始: "
        "subscription_id=%s user_id=%s force_fetch=%s scheduler=%s",
        subscription_id, user_id, force_fetch, is_scheduler_run,
    )
    try:
        from backend.services.zsxq_subscription_service import ZsxqSubscriptionService
        from backend.utils.lark_notifier import send_zsxq_scheduler_report

        result = ZsxqSubscriptionService().fetch_and_store_topics(
            subscription_id=subscription_id,
            user_id=user_id,
            force_fetch=force_fetch,
            collect_report=is_scheduler_run,
        )
        logger.info(
            "[Celery Task] fetch_zhishixingqiu_subscription 完成: "
            "groups=%s new=%s updated=%s failed=%s",
            result.get("groups_processed", 0),
            result.get("topics_new", 0),
            result.get("topics_updated", 0),
            result.get("failed_groups", []),
        )
        if is_scheduler_run:
            send_zsxq_scheduler_report(result)
        return result

    except Exception as exc:
        logger.error(
            "[Celery Task] fetch_zhishixingqiu_subscription 失败: %s",
            exc,
            exc_info=True,
        )
        if self.request.retries < self.max_retries:
            logger.warning(
                "[Celery Task] 重试中... (attempt %d/%d)",
                self.request.retries + 1, self.max_retries,
            )
            raise self.retry(exc=exc)
        failure_result = {
            "success": False,
            "message": str(exc),
            "groups_processed": 0,
            "topics_new": 0,
            "topics_updated": 0,
            "failed_groups": [],
            "report": {
                "errors": [str(exc)],
                "finished_at": utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            },
        }
        if is_scheduler_run:
            from backend.utils.lark_notifier import send_zsxq_scheduler_report
            send_zsxq_scheduler_report(failure_result)
        return failure_result


# ── Substack 订阅任务 ───────────────────────────────────────────────────────


@app.task(
    name='fetch_substack_subscription',
    bind=True,
    max_retries=2,
    default_retry_delay=120,
    time_limit=3600,
    soft_time_limit=3300,
)
def fetch_substack_subscription_task(
    self,
    subscription_id: Optional[int] = None,
    user_id: Optional[int] = None,
    force_fetch: bool = False,
) -> Dict[str, Any]:
    """
    抓取用户订阅的 Substack 文章 + 关注作者动态，写入 subscription_content。

    路由到 data_fetching 队列。由 subscriptions API 新建订阅时触发首抓，
    也由 Scheduler 定时触发增量抓取。
    """
    is_scheduler_run = subscription_id is None and user_id is None
    logger.info(
        "[Celery Task] fetch_substack_subscription 开始: "
        "subscription_id=%s user_id=%s force_fetch=%s scheduler=%s",
        subscription_id, user_id, force_fetch, is_scheduler_run,
    )
    try:
        from backend.services.substack_subscription_service import SubstackSubscriptionService
        from backend.utils.lark_notifier import send_substack_scheduler_report

        result = SubstackSubscriptionService().fetch_and_store(
            subscription_id=subscription_id,
            user_id=user_id,
            force_fetch=force_fetch,
            collect_report=is_scheduler_run,
        )
        logger.info(
            "[Celery Task] fetch_substack_subscription 完成: "
            "publications=%s posts_new=%s notes_new=%s failed_pubs=%s failed_authors=%s",
            result.get("publications_processed", 0),
            result.get("posts_new", 0),
            result.get("notes_new", 0),
            result.get("failed_publications", []),
            result.get("failed_authors", []),
        )
        if is_scheduler_run:
            send_substack_scheduler_report(result)
        return result

    except Exception as exc:
        logger.error(
            "[Celery Task] fetch_substack_subscription 失败: %s",
            exc,
            exc_info=True,
        )
        if self.request.retries < self.max_retries:
            logger.warning(
                "[Celery Task] 重试中... (attempt %d/%d)",
                self.request.retries + 1, self.max_retries,
            )
            raise self.retry(exc=exc)
        failure_result = {
            "success": False,
            "message": str(exc),
            "publications_processed": 0,
            "posts_new": 0,
            "notes_new": 0,
            "failed_publications": [],
            "failed_authors": [],
            "report": {
                "errors": [str(exc)],
                "finished_at": utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            },
        }
        if is_scheduler_run:
            from backend.utils.lark_notifier import send_substack_scheduler_report
            send_substack_scheduler_report(failure_result)
        return failure_result


# ── Weibo 订阅任务 ─────────────────────────────────────────────────────────


@app.task(
    name='fetch_weibo_subscription',
    bind=True,
    max_retries=2,
    default_retry_delay=120,
    # backfill 可能跨多博主翻多页，给足够大的上限
    time_limit=3600,
    soft_time_limit=3300,
)
def fetch_weibo_subscription_task(
    self,
    subscription_id: Optional[int] = None,
    user_id: Optional[int] = None,
    force_fetch: bool = False,
) -> Dict[str, Any]:
    """
    抓取用户订阅的微博博主博文，写入 subscription_content。

    路由到 data_fetching 队列（rc-celery-data worker）。
    由 subscriptions API 在新建订阅时触发首抓（backfill），
    也由 Scheduler 定时触发增量抓取。
    """
    is_scheduler_run = subscription_id is None and user_id is None
    logger.info(
        "[Celery Task] fetch_weibo_subscription 开始: "
        "subscription_id=%s user_id=%s force_fetch=%s scheduler=%s",
        subscription_id, user_id, force_fetch, is_scheduler_run,
    )
    try:
        from backend.services.weibo_subscription_service import WeiboSubscriptionService
        from backend.utils.lark_notifier import send_weibo_scheduler_report

        result = WeiboSubscriptionService().fetch_and_store_posts(
            subscription_id=subscription_id,
            user_id=user_id,
            force_fetch=force_fetch,
            collect_report=is_scheduler_run,
        )
        logger.info(
            "[Celery Task] fetch_weibo_subscription 完成: "
            "accounts=%s new=%s updated=%s failed=%s",
            result.get("accounts_processed", 0),
            result.get("posts_new", 0),
            result.get("posts_updated", 0),
            result.get("failed_accounts", []),
        )
        if is_scheduler_run:
            send_weibo_scheduler_report(result)
        return result

    except Exception as exc:
        logger.error(
            "[Celery Task] fetch_weibo_subscription 失败: %s",
            exc,
            exc_info=True,
        )
        if self.request.retries < self.max_retries:
            logger.warning(
                "[Celery Task] 重试中... (attempt %d/%d)",
                self.request.retries + 1, self.max_retries,
            )
            raise self.retry(exc=exc)
        failure_result = {
            "success": False,
            "message": str(exc),
            "accounts_processed": 0,
            "posts_new": 0,
            "posts_updated": 0,
            "failed_accounts": [],
            "report": {
                "errors": [str(exc)],
                "finished_at": utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            },
        }
        if is_scheduler_run:
            from backend.utils.lark_notifier import send_weibo_scheduler_report
            send_weibo_scheduler_report(failure_result)
        return failure_result


# ── Podcast 订阅任务 ────────────────────────────────────────────────────────


@app.task(
    name='fetch_podcast_subscription',
    bind=True,
    max_retries=2,
    default_retry_delay=60,
    time_limit=600,        # 10 分钟
    soft_time_limit=540,
)
def fetch_podcast_subscription_task(
    self,
    subscription_id: Optional[int] = None,
    user_id: Optional[int] = None,
    force_fetch: bool = False,
) -> Dict[str, Any]:
    """
    异步获取 Podcast 订阅的 episode（Celery 任务）。

    路由到 data_fetching 队列，Podcast RSS Feed 无需住宅 IP。
    """
    try:
        logger.info(
            "[Celery Task] fetch_podcast_subscription 开始: "
            "subscription_id=%s user_id=%s force_fetch=%s",
            subscription_id, user_id, force_fetch,
        )

        from backend.services.podcast_subscription_service import PodcastSubscriptionService

        result = PodcastSubscriptionService.fetch_and_store_episodes(
            subscription_id=subscription_id,
            user_id=user_id,
            force_fetch=force_fetch,
        )

        logger.info(
            "[Celery Task] fetch_podcast_subscription 完成: "
            "new=%s existing=%s failed=%s",
            result.get('episodes_new', 0),
            result.get('episodes_existing', 0),
            result.get('episodes_failed', 0),
        )
        return result

    except Exception as exc:
        logger.error(
            "[Celery Task] fetch_podcast_subscription 失败: %s",
            exc, exc_info=True,
        )
        if self.request.retries < self.max_retries:
            raise self.retry(exc=exc)
        return {
            'success': False,
            'message': str(exc),
            'episodes_new': 0,
            'episodes_existing': 0,
        }


@app.task(
    name='fetch_all_podcast_subscriptions',
    bind=True,
    max_retries=2,
    default_retry_delay=120,
    time_limit=1800,       # 30 分钟
    soft_time_limit=1500,
)
def fetch_all_podcast_subscriptions_task(self) -> Dict[str, Any]:
    """
    批量获取所有 Podcast 订阅的最新 episode（Scheduler 定时调用）。
    """
    try:
        logger.info("[Celery Task] fetch_all_podcast_subscriptions 开始")

        from backend.services.podcast_subscription_service import PodcastSubscriptionService

        result = PodcastSubscriptionService.fetch_all_subscriptions()

        logger.info(
            "[Celery Task] fetch_all_podcast_subscriptions 完成: "
            "feeds=%s new=%s existing=%s",
            result.get('feeds_count', 0),
            result.get('episodes_new', 0),
            result.get('episodes_existing', 0),
        )
        return result

    except Exception as exc:
        logger.error(
            "[Celery Task] fetch_all_podcast_subscriptions 失败: %s",
            exc, exc_info=True,
        )
        if self.request.retries < self.max_retries:
            raise self.retry(exc=exc)
        return {
            'success': False,
            'message': str(exc),
            'episodes_new': 0,
            'episodes_existing': 0,
        }


@app.task(
    name='podcast_transcribe_episode',
    bind=True,
    max_retries=2,
    default_retry_delay=120,
    time_limit=1200,       # 20 分钟（含下载 ~40MB + ASR 转写）
    soft_time_limit=1080,
)
def podcast_transcribe_episode_task(
    self,
    episode_item: Dict[str, Any],
) -> Dict[str, Any]:
    """
    单个 Podcast episode ASR 转写任务。

    Args:
        episode_item: {
            'content_db_id': int,
            'audio_url': str,
            'episode_title': str,
        }

    流程:
        1. HTTP 流式下载音频到临时文件（CDN 直连，无需 yt-dlp）
        2. 上传到 OSS（获取公网 URL 供 Fun-ASR 下载）
        3. 提交 Fun-ASR 转写（DashScope 异步轮询）
        4. 写回 subscription_content.transcript
        5. 触发 auto-feed-tagging（AI 自动打标 + 摘要）
        6. 清理临时文件
    """
    import os
    import tempfile

    import requests

    content_db_id = episode_item['content_db_id']
    audio_url = episode_item['audio_url']
    episode_title = episode_item.get('episode_title', 'unknown')

    temp_path = None

    try:
        logger.info(
            "── Podcast ASR 开始: content=%d title='%s' ──",
            content_db_id, episode_title,
        )

        # ── [1/5] 下载音频到临时文件 ──
        logger.info("[1/5] 下载音频: %s", audio_url[:100])
        temp_dir = tempfile.mkdtemp(prefix='podcast_asr_')
        safe_title = "".join(c for c in episode_title[:80] if c.isalnum() or c in ' _-').strip()
        temp_path = os.path.join(temp_dir, f"{safe_title or 'episode'}.mp3")

        try:
            with requests.get(audio_url, stream=True, timeout=600) as r:
                r.raise_for_status()
                with open(temp_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status in (403, 404, 410):
                # CDN 链接过期/失效，属于永久性错误，不重试
                logger.error(
                    "── Podcast ASR 永久失败[下载]: content=%d status=%d url=%s ──",
                    content_db_id, status, audio_url[:80],
                )
                return {
                    'success': False,
                    'content_db_id': content_db_id,
                    'episode_title': episode_title,
                    'error': f'audio_download_permanent_{status}',
                }
            raise  # 5xx 等瞬时错误，走重试

        file_size = os.path.getsize(temp_path)
        if file_size < 1024:  # < 1KB，下载异常
            logger.error(
                "── Podcast ASR 失败[下载]: content=%d file_too_small=%dB ──",
                content_db_id, file_size,
            )
            return {
                'success': False,
                'content_db_id': content_db_id,
                'episode_title': episode_title,
                'error': f'audio_file_too_small_{file_size}B',
            }

        logger.info("[1/5] 下载完成: %.1f MB", file_size / 1024 / 1024)

        # ── [2/5] 上传到 OSS ──
        from backend.services.oss_service import OSSService

        oss_service = OSSService()
        upload_filename = os.path.basename(temp_path)
        upload_result = oss_service.upload_audio_file(temp_path, upload_filename)
        signed_url = upload_result.get('signed_url') or upload_result.get('public_url')

        if not signed_url:
            raise RuntimeError(f"OSS 上传失败: 无法获取音频 URL, title='{episode_title}'")

        logger.info("[2/5] OSS 上传完成: %s", upload_result.get('object_name', ''))

        # ── [3/5] 提交 Fun-ASR 转写 ──
        from backend.services.asr_service import ASRService

        asr_service = ASRService()
        asr_result = asr_service.transcribe_audio(signed_url)

        if not asr_result or not asr_result.get("success"):
            err = (asr_result or {}).get("error") or "unknown"
            raise RuntimeError(f"ASR 转写失败, title='{episode_title}': {err}")

        logger.info("[3/5] ASR 转写完成: title='%s'", episode_title)

        # ── [4/5] 格式化结果并写回 DB ──
        formatted_texts = []
        for trans_data in asr_result.get("transcriptions") or []:
            formatted_texts.append(
                asr_service.format_transcription_result(
                    trans_data,
                    include_timestamps=True,
                    include_words=False,
                    include_speaker=True,
                    timestamped_only=True,
                )
            )
        formatted_text = "\n".join(formatted_texts) if formatted_texts else (asr_result.get("text") or "")
        word_count = len(formatted_text) if formatted_text else 0

        if word_count == 0:
            raise RuntimeError(
                f"ASR 转写结果为空, title='{episode_title}' "
                f"(task_id={asr_result.get('task_id', '')})"
            )

        from backend.services.subscription_content_service import SubscriptionContentService
        content_service = SubscriptionContentService()
        content_service.update_transcript(
            content_db_id=content_db_id,
            transcript=formatted_text,
            transcript_data={'source': 'podcast_asr', 'episode_title': episode_title},
        )

        logger.info(
            "[4/5] 转写结果已写入 DB: content=%d 字数=%d", content_db_id, word_count,
        )

        # ── [5/5] 触发 auto-feed-tagging（AI 自动打标 + 摘要）──
        try:
            from backend.tasks.subscription_tasks import finalize_youtube_feed_task
            finalize_youtube_feed_task.apply_async(
                args=[content_db_id],
                queue='file_processing',
            )
        except Exception as tag_err:
            # 打标是增值能力，不阻断 ASR 主流程
            logger.warning(
                "[5/5] signal_feed_transcript_ready 派发失败 (不阻断主流程): "
                "content=%d error=%s", content_db_id, tag_err,
            )

        logger.info(
            "── Podcast ASR 完成: content=%d title='%s' 字数=%d ──",
            content_db_id, episode_title, word_count,
        )

        return {
            'success': True,
            'content_db_id': content_db_id,
            'episode_title': episode_title,
            'word_count': word_count,
        }

    except Exception as exc:
        logger.error(
            "── Podcast ASR 失败[可重试]: content=%d title='%s' "
            "error=%s (attempt %d/%d) ──",
            content_db_id, episode_title, exc,
            self.request.retries + 1, self.max_retries + 1,
            exc_info=True,
        )
        if self.request.retries < self.max_retries:
            logger.info("→ 120s 后重试: content=%d", content_db_id)
            raise self.retry(exc=exc)
        return {
            'success': False,
            'content_db_id': content_db_id,
            'episode_title': episode_title,
            'error': str(exc),
        }

    finally:
        # 清理临时文件
        if temp_path and os.path.exists(temp_path):
            try:
                temp_dir = os.path.dirname(temp_path)
                os.remove(temp_path)
                os.rmdir(temp_dir)
            except OSError:
                pass
