# -*- coding: utf-8 -*-
"""
Podcast 订阅服务

通过 Podcast RSS Feed 获取音频节目列表，存入 subscription_content 表，
并对新 episode 下载音频进行 ASR 转写。

数据流：
  Podcast RSS Feed URL (subscription_list.channel_identifier)
    → requests.get() → feedparser.parse()
    → PodcastAdapter.normalize() 标准化每个 episode
    → SubscriptionContentService.save_content('podcast', ...)
    → 对新 episode 且发布时间在 PODCAST_ASR_RECENT_HOURS 内者派发 ASR 转写

抓取策略（env 可配）：
  - PODCAST_FETCH_RECENT_DAYS：默认拉取最近 7 天 episode 入库
  - PODCAST_FETCH_MAX_ITEMS：单日过滤后的数量上限（默认 100）
  - PODCAST_ASR_RECENT_HOURS：仅对最近 48 小时内发布的 episode 自动 ASR

与 YouTube 订阅的关键区别：
  - 音频 URL 是公网 CDN 链接，不需要 yt-dlp 或住宅 IP
  - 所有任务路由到 data_fetching 队列
  - 转写使用 Fun-ASR（同 YouTube Feed ASR）
"""

import os
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

import feedparser
import requests
from dateutil import parser as date_parser

from backend.utils.datetime_utils import ensure_utc, utcnow
from backend.utils.logger import info, error, warning


class PodcastSubscriptionService:
    """Podcast 订阅抓取服务"""

    # RSS Feed 请求超时（秒）
    _FEED_TIMEOUT = 30
    # 兼容旧逻辑：subscription_list.max_items_per_fetch 为空时的条目上限兜底
    _DEFAULT_MAX_ITEMS = 50

    @staticmethod
    def _fetch_recent_days() -> int:
        return max(1, int(os.getenv("PODCAST_FETCH_RECENT_DAYS", "7")))

    @staticmethod
    def _fetch_max_items_cap(max_items: Optional[int] = None) -> int:
        env_cap = max(1, int(os.getenv("PODCAST_FETCH_MAX_ITEMS", "100")))
        if max_items:
            return min(max_items, env_cap)
        return env_cap

    @staticmethod
    def _asr_recent_hours() -> int:
        return max(1, int(os.getenv("PODCAST_ASR_RECENT_HOURS", "48")))

    @staticmethod
    def _parse_entry_published_at(entry: Dict[str, Any]) -> datetime:
        published_parsed = entry.get("published_parsed")
        if published_parsed:
            return ensure_utc(datetime(*published_parsed[:6], tzinfo=timezone.utc))
        for key in ("published", "pubDate", "updated"):
            raw = entry.get(key)
            if not raw:
                continue
            try:
                return ensure_utc(date_parser.parse(str(raw)))
            except (ValueError, TypeError, OverflowError):
                continue
        return utcnow()

    @staticmethod
    def _entry_within_asr_window(entry: Dict[str, Any]) -> bool:
        cutoff = utcnow() - timedelta(hours=PodcastSubscriptionService._asr_recent_hours())
        return PodcastSubscriptionService._parse_entry_published_at(entry) >= cutoff

    @staticmethod
    def _filter_entries_by_recency(
        entries: List[Dict[str, Any]],
        recent_days: int,
        max_items: int,
    ) -> List[Dict[str, Any]]:
        """保留最近 recent_days 天内的 episode，最多 max_items 条（RSS 通常倒序）。"""
        cutoff = utcnow() - timedelta(days=recent_days)
        filtered: List[Dict[str, Any]] = []
        for entry in entries:
            if len(filtered) >= max_items:
                break
            published_at = PodcastSubscriptionService._parse_entry_published_at(entry)
            if published_at >= cutoff:
                filtered.append(entry)
            elif filtered:
                # 常见 Podcast RSS 按时间倒序；遇到更旧条目可提前结束
                break
        return filtered

    @staticmethod
    def _dispatch_asr_tasks(new_episode_items: List[Dict[str, Any]]) -> None:
        if not new_episode_items:
            return
        try:
            from backend.celery_app import app as celery_app

            stagger_seconds = 60
            for i, item in enumerate(new_episode_items):
                celery_app.send_task(
                    "podcast_transcribe_episode",
                    args=[item],
                    queue="data_fetching",
                    countdown=i * stagger_seconds,
                )
            info(
                f"Podcast 转写任务已派发: {len(new_episode_items)} 个 episode, "
                f"stagger={stagger_seconds}s, asr_window={PodcastSubscriptionService._asr_recent_hours()}h",
                source="podcast_subscription",
            )
        except Exception as e:
            warning(f"派发转写任务失败: {str(e)}", source="podcast_subscription")

    # ── Feed 验证 ──────────────────────────────────────────────────────────

    @staticmethod
    def validate_podcast_feed(feed_url: str) -> Dict[str, Any]:
        """
        验证 Podcast RSS Feed URL 是否有效。

        Args:
            feed_url: RSS Feed URL

        Returns:
            验证结果，包含:
            - valid: bool
            - channel_name: str (Feed 标题)
            - description: str
            - episode_count: int
            - error: str (失败时的错误信息)
        """
        try:
            resp = requests.get(feed_url, timeout=PodcastSubscriptionService._FEED_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            return {
                'valid': False,
                'error': f'无法访问 RSS Feed: {str(e)}',
            }

        try:
            feed = feedparser.parse(resp.content)
        except Exception as e:
            return {
                'valid': False,
                'error': f'RSS Feed 解析失败: {str(e)}',
            }

        # 检查 feed 级别错误（bozo 检测）
        if feed.bozo and not feed.entries:
            return {
                'valid': False,
                'error': f'无效的 RSS Feed: {getattr(feed, "bozo_exception", "unknown error")}',
            }

        channel_name = feed.feed.get('title', '')
        description = feed.feed.get('description', '') or feed.feed.get('subtitle', '')
        episode_count = len(feed.entries)

        if not channel_name:
            return {
                'valid': False,
                'error': 'RSS Feed 缺少标题，不是有效的 Podcast Feed',
            }

        # 检查是否有至少一个带音频 enclosure 的 entry
        has_audio = False
        for entry in feed.entries[:5]:
            enclosures = entry.get('enclosures', [])
            if enclosures and any(
                enc.get('href', '').startswith('http')
                for enc in enclosures
            ):
                has_audio = True
                break

        if not has_audio and episode_count > 0:
            warning(
                f"Feed '{channel_name}' 有 {episode_count} 个 episode 但前 5 个均无音频 enclosure",
                source="podcast_subscription",
            )

        info(
            f"Podcast Feed 验证通过: '{channel_name}', {episode_count} episodes",
            source="podcast_subscription",
        )

        return {
            'valid': True,
            'channel_name': channel_name,
            'description': description[:500] if description else '',
            'episode_count': episode_count,
            'has_audio': has_audio,
        }

    # ── Episode 获取 ──────────────────────────────────────────────────────

    @staticmethod
    def fetch_channel_episodes(
        feed_url: str,
        max_items: Optional[int] = None,
        recent_days: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        从 Podcast RSS Feed 获取 episode 列表。

        Args:
            feed_url: RSS Feed URL
            max_items: 条目上限（默认读 PODCAST_FETCH_MAX_ITEMS，并与订阅配置取较小值）
            recent_days: 仅保留最近 N 天（默认读 PODCAST_FETCH_RECENT_DAYS=7）

        Returns:
            原始 episode 列表（feedparser 解析后的 entry dict，注入了 _feed_url 和 _feed_title）
        """
        days = recent_days if recent_days is not None else PodcastSubscriptionService._fetch_recent_days()
        cap = PodcastSubscriptionService._fetch_max_items_cap(max_items)
        info(
            f"获取 Podcast Feed: {feed_url}, recent_days={days}, max_items={cap}",
            source="podcast_subscription",
        )

        try:
            resp = requests.get(feed_url, timeout=PodcastSubscriptionService._FEED_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            error(f"获取 Podcast Feed 失败: {str(e)}", source="podcast_subscription")
            return []

        feed = feedparser.parse(resp.content)

        if feed.bozo and not feed.entries:
            error(f"Podcast Feed 解析失败: {getattr(feed, 'bozo_exception', 'unknown')}",
                  source="podcast_subscription")
            return []

        feed_title = feed.feed.get('title', '')
        entries = PodcastSubscriptionService._filter_entries_by_recency(
            list(feed.entries),
            recent_days=days,
            max_items=cap,
        )

        # 注入 feed 级别信息到每个 entry，供 PodcastAdapter 使用
        for entry in entries:
            entry['_feed_url'] = feed_url
            entry['_feed_title'] = feed_title

        info(
            f"获取到 {len(entries)} 个 episode (feed: '{feed_title}', "
            f"window={days}d, cap={cap})",
            source="podcast_subscription",
        )

        return entries

    # ── 单订阅抓取 + 存储 ─────────────────────────────────────────────────

    @staticmethod
    def fetch_and_store_episodes(
        subscription_id: Optional[int] = None,
        user_id: Optional[int] = None,
        force_fetch: bool = False,
    ) -> Dict[str, Any]:
        """
        获取并存储单个 Podcast 订阅的 episode 到数据库。

        Args:
            subscription_id: 订阅 ID（必须提供）
            user_id: 用户 ID
            force_fetch: 是否强制获取

        Returns:
            执行结果统计
        """
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        from backend.services.subscription_content_service import SubscriptionContentService

        start_time = time.time()

        try:
            db_config = get_database_config()
            db_manager = DatabaseManager(db_conn_params=db_config)
            conn = db_manager.get_connection()
            cursor = conn.cursor()

            # 获取订阅信息
            if subscription_id:
                cursor.execute("""
                    SELECT
                        id, user_id, channel_type, channel_identifier,
                        channel_name, max_items_per_fetch
                    FROM subscription_list
                    WHERE id = %s
                """, (subscription_id,))
                subscription = cursor.fetchone()

                if not subscription:
                    error(f"订阅不存在: subscription_id={subscription_id}",
                          source="podcast_subscription")
                    return {
                        'success': False,
                        'message': 'Subscription not found',
                        'episodes_new': 0,
                        'episodes_existing': 0,
                    }

                sub_id, uid, channel_type, channel_identifier, channel_name, max_items = subscription
                user_id = uid
            else:
                error("必须提供 subscription_id", source="podcast_subscription")
                return {
                    'success': False,
                    'message': 'subscription_id is required',
                    'episodes_new': 0,
                    'episodes_existing': 0,
                }

            cursor.close()
            conn.close()

            recent_days = PodcastSubscriptionService._fetch_recent_days()
            max_cap = PodcastSubscriptionService._fetch_max_items_cap(
                max_items or PodcastSubscriptionService._DEFAULT_MAX_ITEMS
            )
            info(
                f"开始获取 Podcast episode: subscription_id={subscription_id}, "
                f"feed={channel_identifier}, recent_days={recent_days}, max_items={max_cap}",
                source="podcast_subscription",
            )

            # 获取 episode 列表（默认最近 7 天，条数上限见 PODCAST_FETCH_MAX_ITEMS）
            raw_episodes = PodcastSubscriptionService.fetch_channel_episodes(
                feed_url=channel_identifier,
                max_items=max_cap,
                recent_days=recent_days,
            )

            if not raw_episodes:
                warning("未获取到任何 episode", source="podcast_subscription")
                return {
                    'success': True,
                    'message': 'No episodes found',
                    'episodes_new': 0,
                    'episodes_existing': 0,
                    'execution_duration_ms': int((time.time() - start_time) * 1000),
                }

            # 保存到数据库
            content_service = SubscriptionContentService()
            episodes_new = 0
            episodes_existing = 0
            episodes_failed = 0

            new_episode_items = []
            asr_skipped_old = 0

            for raw_episode in raw_episodes:
                try:
                    content_db_id, is_new = content_service.save_content(
                        channel_type='podcast',
                        raw_data=raw_episode,
                        user_ids=[user_id],
                    )

                    if is_new:
                        episodes_new += 1
                        # 提取音频 URL；仅最近 PODCAST_ASR_RECENT_HOURS 内发布者派发 ASR
                        audio_url = ''
                        enclosures = raw_episode.get('enclosures', [])
                        if enclosures:
                            audio_url = enclosures[0].get('href', '')

                        episode_title = raw_episode.get('title', 'unknown')
                        if not audio_url:
                            warning(
                                f"Episode 无音频 URL，跳过转写: {episode_title}",
                                source="podcast_subscription",
                            )
                        elif PodcastSubscriptionService._entry_within_asr_window(raw_episode):
                            new_episode_items.append({
                                'content_db_id': content_db_id,
                                'audio_url': audio_url,
                                'episode_title': episode_title,
                            })
                        else:
                            asr_skipped_old += 1
                            info(
                                f"Episode 超出 ASR 时间窗，仅入库不转写: {episode_title}",
                                source="podcast_subscription",
                            )
                    else:
                        episodes_existing += 1

                except Exception as e:
                    episode_title = raw_episode.get('title', 'unknown')
                    warning(f"保存 episode 失败: {episode_title}, error={str(e)}",
                            source="podcast_subscription")
                    episodes_failed += 1
                    continue

            PodcastSubscriptionService._dispatch_asr_tasks(new_episode_items)
            if asr_skipped_old:
                info(
                    f"ASR 跳过（超出 {PodcastSubscriptionService._asr_recent_hours()}h 窗口）: "
                    f"{asr_skipped_old} 个 episode",
                    source="podcast_subscription",
                )

            execution_time = int((time.time() - start_time) * 1000)

            info(
                f"Podcast episode 获取完成: subscription_id={subscription_id}, "
                f"new={episodes_new}, existing={episodes_existing}, failed={episodes_failed}, "
                f"asr_dispatched={len(new_episode_items)}, "
                f"duration={execution_time}ms",
                source="podcast_subscription",
            )

            # 更新订阅统计 + 写抓取历史 (统一 helper: utcnow + 失败自增 + history)
            from backend.services.subscription_status_service import record_fetch_outcome_auto
            record_fetch_outcome_auto(
                subscription_id,
                status='success',
                items_fetched=episodes_new + episodes_existing,
                items_new=episodes_new,
                execution_duration_ms=execution_time,
            )

            return {
                'success': True,
                'message': f"Successfully fetched {episodes_new} new episodes",
                'episodes_new': episodes_new,
                'episodes_existing': episodes_existing,
                'episodes_failed': episodes_failed,
                'execution_duration_ms': execution_time,
            }

        except Exception as e:
            error(f"获取并存储 Podcast episode 失败: {str(e)}", source="podcast_subscription")
            # 失败也回写状态 (consecutive_failures+1) + history, 与 Twitter 对齐 (P1-2)
            from backend.services.subscription_status_service import record_fetch_outcome_auto
            record_fetch_outcome_auto(
                subscription_id,
                status='failed',
                error=str(e),
                execution_duration_ms=int((time.time() - start_time) * 1000),
            )
            return {
                'success': False,
                'message': f'Failed to fetch episodes: {str(e)}',
                'episodes_new': 0,
                'episodes_existing': 0,
                'execution_duration_ms': int((time.time() - start_time) * 1000),
            }

    # ── 批量抓取（Scheduler 调用）───────────────────────────────────────────

    @staticmethod
    def get_all_podcast_subscriptions(enabled_only: bool = True) -> List[Dict[str, Any]]:
        """
        获取所有 Podcast 订阅。

        Args:
            enabled_only: 是否只获取启用的订阅

        Returns:
            订阅列表
        """
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config

        db_config = get_database_config()
        db_manager = DatabaseManager(db_conn_params=db_config)
        conn = db_manager.get_connection()
        cursor = conn.cursor()

        query = """
            SELECT
                id, user_id, channel_type, channel_identifier,
                channel_name, max_items_per_fetch, enabled
            FROM subscription_list
            WHERE channel_type = 'podcast'
        """
        if enabled_only:
            query += " AND enabled = true"

        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        subscriptions = []
        for row in rows:
            subscriptions.append({
                'id': row[0],
                'user_id': row[1],
                'channel_type': row[2],
                'channel_identifier': row[3],
                'channel_name': row[4],
                'max_items_per_fetch': row[5],
                'enabled': row[6],
            })

        return subscriptions

    @staticmethod
    def fetch_all_subscriptions() -> Dict[str, Any]:
        """
        批量获取所有 Podcast 订阅的最新 episode（供 Scheduler 调用）。

        Returns:
            执行结果统计
        """
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        from backend.services.subscription_content_service import SubscriptionContentService

        start_time = time.time()

        info("开始批量获取 Podcast 订阅 episode", source="podcast_subscription")

        try:
            subscriptions = PodcastSubscriptionService.get_all_podcast_subscriptions(enabled_only=True)

            if not subscriptions:
                info("没有启用的 Podcast 订阅", source="podcast_subscription")
                return {
                    'success': True,
                    'subscriptions_count': 0,
                    'feeds_count': 0,
                    'episodes_new': 0,
                    'episodes_existing': 0,
                }

            # 按 feed_url 聚合（多个用户可能订阅同一个 Feed）
            feeds_map: Dict[str, Dict[str, Any]] = {}
            for sub in subscriptions:
                feed_url = sub['channel_identifier']
                if feed_url not in feeds_map:
                    feeds_map[feed_url] = {
                        'feed_url': feed_url,
                        'channel_name': sub['channel_name'],
                        'max_items': PodcastSubscriptionService._fetch_max_items_cap(
                            sub['max_items_per_fetch'] or PodcastSubscriptionService._DEFAULT_MAX_ITEMS
                        ),
                        'user_ids': [],
                        'subscription_ids': [],
                    }
                feeds_map[feed_url]['user_ids'].append(sub['user_id'])
                feeds_map[feed_url]['subscription_ids'].append(sub['id'])

            content_service = SubscriptionContentService()
            total_episodes_new = 0
            total_episodes_existing = 0
            feeds_success = 0
            feeds_failed = 0

            for feed_url, feed_info in feeds_map.items():
                try:
                    raw_episodes = PodcastSubscriptionService.fetch_channel_episodes(
                        feed_url=feed_url,
                        max_items=feed_info['max_items'],
                        recent_days=PodcastSubscriptionService._fetch_recent_days(),
                    )

                    if not raw_episodes:
                        feeds_success += 1
                        continue

                    new_episode_items = []
                    asr_skipped_old = 0
                    user_ids = feed_info['user_ids']

                    for raw_episode in raw_episodes:
                        try:
                            content_db_id, is_new = content_service.save_content(
                                channel_type='podcast',
                                raw_data=raw_episode,
                                user_ids=user_ids,
                            )

                            if is_new:
                                total_episodes_new += 1
                                audio_url = ''
                                enclosures = raw_episode.get('enclosures', [])
                                if enclosures:
                                    audio_url = enclosures[0].get('href', '')
                                episode_title = raw_episode.get('title', 'unknown')
                                if audio_url and PodcastSubscriptionService._entry_within_asr_window(raw_episode):
                                    new_episode_items.append({
                                        'content_db_id': content_db_id,
                                        'audio_url': audio_url,
                                        'episode_title': episode_title,
                                    })
                                elif audio_url:
                                    asr_skipped_old += 1
                            else:
                                total_episodes_existing += 1

                        except Exception as e:
                            warning(f"保存 episode 失败: {str(e)}", source="podcast_subscription")
                            continue

                    PodcastSubscriptionService._dispatch_asr_tasks(new_episode_items)
                    if new_episode_items or asr_skipped_old:
                        info(
                            f"Feed '{feed_info['channel_name']}' ASR: dispatched={len(new_episode_items)}, "
                            f"skipped_old={asr_skipped_old}, window={PodcastSubscriptionService._asr_recent_hours()}h",
                            source="podcast_subscription",
                        )

                    # 更新该 Feed 下所有订阅的统计
                    try:
                        db_config = get_database_config()
                        db_manager = DatabaseManager(db_conn_params=db_config)
                        conn = db_manager.get_connection()
                        cursor = conn.cursor()

                        new_count = len(new_episode_items)
                        from backend.services.subscription_status_service import record_fetch_outcome
                        for sub_id in feed_info['subscription_ids']:
                            record_fetch_outcome(
                                cursor, sub_id,
                                status='success',
                                items_new=new_count,
                                items_fetched=new_count,
                            )

                        conn.commit()
                        cursor.close()
                        conn.close()
                    except Exception as e:
                        error(f"更新订阅统计失败: {str(e)}", source="podcast_subscription")

                    feeds_success += 1

                except Exception as e:
                    error(f"获取 Feed 失败: {feed_url}, error={str(e)}",
                          source="podcast_subscription")
                    feeds_failed += 1
                    # 失败回写 (consecutive_failures+1 + history) for each affected sub (P1-2)
                    try:
                        from backend.services.subscription_status_service import record_fetch_outcome_auto
                        for sub_id in feed_info.get('subscription_ids', []):
                            record_fetch_outcome_auto(sub_id, status='failed', error=str(e))
                    except Exception:
                        pass
                    continue

            execution_time = int((time.time() - start_time) * 1000)

            info(
                f"Podcast 批量获取完成: feeds={len(feeds_map)}, "
                f"success={feeds_success}, failed={feeds_failed}, "
                f"new={total_episodes_new}, existing={total_episodes_existing}, "
                f"duration={execution_time}ms",
                source="podcast_subscription",
            )

            return {
                'success': True,
                'subscriptions_count': len(subscriptions),
                'feeds_count': len(feeds_map),
                'feeds_success': feeds_success,
                'feeds_failed': feeds_failed,
                'episodes_new': total_episodes_new,
                'episodes_existing': total_episodes_existing,
                'execution_duration_ms': execution_time,
            }

        except Exception as e:
            error(f"批量获取 Podcast 订阅失败: {str(e)}", source="podcast_subscription")
            return {
                'success': False,
                'message': f'Failed to fetch podcast subscriptions: {str(e)}',
                'episodes_new': 0,
                'episodes_existing': 0,
                'execution_duration_ms': int((time.time() - start_time) * 1000),
            }
