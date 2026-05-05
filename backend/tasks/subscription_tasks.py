# -*- coding: utf-8 -*-
"""
订阅相关的Celery任务
"""
import logging
from typing import Optional, Dict, Any

from backend.celery_app import app

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
