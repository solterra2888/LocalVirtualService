# -*- coding: utf-8 -*-
"""
订阅内容服务 - 统一管理多渠道订阅内容的存储和查询
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import Json

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger
from backend.services.content_adapters import get_adapter

logger = get_logger("subscription_content_service")


class SubscriptionContentService:
    """
    订阅内容服务
    
    功能:
    - 保存多渠道内容（Twitter, YouTube, RSS等）
    - 查询用户订阅的内容
    - 管理内容的阅读状态
    - 内容统计和分析
    """
    
    def __init__(self):
        """初始化服务"""
        db_params = get_database_config()
        self.db_manager = DatabaseManager(db_params)
    
    @staticmethod
    def calculate_heat_level(engagement_count: int) -> int:
        """
        根据互动数计算热度等级
        
        热度分级（基于Twitter行业标准）：
        - 灰色（level 1）: 低热度 - engagement < 100
        - 蓝色（level 2）: 中等热度 - 100 <= engagement < 1000
        - 橙色（level 3）: 高热度 - 1000 <= engagement < 10000
        - 红色（level 4）: 超高热度 - engagement >= 10000
        
        Args:
            engagement_count: 互动总数（点赞+转发+评论+引用）
            
        Returns:
            热度等级 (1-4)
        """
        if engagement_count >= 10000:
            return 4  # 红色 - 超高热度
        elif engagement_count >= 1000:
            return 3  # 橙色 - 高热度
        elif engagement_count >= 100:
            return 2  # 蓝色 - 中等热度
        else:
            return 1  # 灰色 - 低热度
    
    def save_content(
        self,
        channel_type: str,
        raw_data: Dict[str, Any],
        user_ids: List[int]
    ) -> tuple[int, bool]:
        """
        保存内容（使用适配器标准化数据）
        
        Args:
            channel_type: 渠道类型 (twitter, youtube, rss, etc.)
            raw_data: 原始数据字典
            user_ids: 订阅该内容的用户ID列表
            
        Returns:
            (content_id, is_new): 内容ID和是否为新内容
            
        Raises:
            ValueError: 如果渠道类型不支持或数据无效
        """
        try:
            # 获取对应的适配器
            adapter = get_adapter(channel_type)
            
            # 标准化数据
            normalized_data = adapter.normalize(raw_data)
            
            # 保存到数据库
            return self._save_normalized_content(
                normalized_data=normalized_data,
                user_ids=user_ids
            )
            
        except Exception as e:
            logger.error(f"Failed to save {channel_type} content: {str(e)}")
            raise
    
    def _save_normalized_content(
        self,
        normalized_data: Dict[str, Any],
        user_ids: List[int]
    ) -> tuple[int, bool]:
        """
        保存标准化后的内容到数据库
        
        Returns:
            (content_id, is_new): 内容ID和是否为新内容
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        
        try:
            # 计算热度等级
            engagement_count = normalized_data.get('engagement_count', 0)
            heat_level = self.calculate_heat_level(engagement_count)
            
            query = """
                INSERT INTO subscription_content (
                    channel_type,
                    content_id,
                    title,
                    content,
                    author_name,
                    author_identifier,
                    url,
                    media_urls,
                    published_at,
                    engagement_count,
                    view_count,
                    metadata,
                    user_ids,
                    fetched_at,
                    is_retweet,
                    retweeted_author,
                    retweeted_content,
                    retweeted_url,
                    is_quote,
                    quoted_author,
                    quoted_content,
                    quoted_url,
                    duration_seconds,
                    channel_id,
                    feed_url,
                    categories,
                    is_reply,
                    in_reply_to_id,
                    in_reply_to_username,
                    in_reply_to_user_id,
                    heat_level
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (channel_type, content_id) DO UPDATE SET
                    user_ids = ARRAY(SELECT DISTINCT UNNEST(subscription_content.user_ids || EXCLUDED.user_ids)),
                    engagement_count = EXCLUDED.engagement_count,
                    view_count = EXCLUDED.view_count,
                    metadata = EXCLUDED.metadata,
                    is_retweet = EXCLUDED.is_retweet,
                    retweeted_author = EXCLUDED.retweeted_author,
                    retweeted_content = EXCLUDED.retweeted_content,
                    retweeted_url = EXCLUDED.retweeted_url,
                    is_quote = EXCLUDED.is_quote,
                    quoted_author = EXCLUDED.quoted_author,
                    quoted_content = EXCLUDED.quoted_content,
                    quoted_url = EXCLUDED.quoted_url,
                    duration_seconds = EXCLUDED.duration_seconds,
                    channel_id = EXCLUDED.channel_id,
                    heat_level = EXCLUDED.heat_level,
                    fetched_at = CURRENT_TIMESTAMP
                RETURNING id, (xmax = 0) AS inserted
            """
            
            cursor.execute(query, (
                normalized_data['channel_type'],
                normalized_data['content_id'],
                normalized_data.get('title'),
                normalized_data['content'],
                normalized_data.get('author_name'),
                normalized_data['author_identifier'],
                normalized_data.get('url'),
                normalized_data.get('media_urls', []),
                normalized_data['published_at'],
                engagement_count,
                normalized_data.get('view_count', 0),
                Json(normalized_data.get('metadata', {})),
                user_ids,
                # Twitter 特定字段
                normalized_data.get('is_retweet', False),
                normalized_data.get('retweeted_author'),
                normalized_data.get('retweeted_content'),
                normalized_data.get('retweeted_url'),
                normalized_data.get('is_quote', False),
                normalized_data.get('quoted_author'),
                normalized_data.get('quoted_content'),
                normalized_data.get('quoted_url'),
                # YouTube 特定字段
                normalized_data.get('duration_seconds'),
                normalized_data.get('channel_id'),
                # RSS 特定字段
                normalized_data.get('feed_url'),
                normalized_data.get('categories', []),
                # Reply 特定字段
                normalized_data.get('is_reply', False),
                normalized_data.get('in_reply_to_id'),
                normalized_data.get('in_reply_to_username'),
                normalized_data.get('in_reply_to_user_id'),
                # 热度等级
                heat_level,
            ))
            
            result = cursor.fetchone()
            content_id = result[0]
            is_new = result[1]  # True if inserted, False if updated
            
            conn.commit()
            
            logger.debug(
                f"{'Inserted' if is_new else 'Updated'} {normalized_data['channel_type']} content: "
                f"id={content_id}, content_id={normalized_data['content_id']}"
            )
            
            return content_id, is_new
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error saving content: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def get_user_content(
        self,
        user_id: int,
        channel_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
        since: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        获取用户订阅的内容
        
        Args:
            user_id: 用户ID
            channel_type: 渠道类型筛选（可选）
            limit: 返回数量限制
            offset: 偏移量
            since: 起始时间（可选）
            
        Returns:
            内容列表
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        
        try:
            # 构建查询条件
            conditions = [f"{user_id} = ANY(user_ids)"]
            params = []
            
            if channel_type:
                conditions.append("channel_type = %s")
                params.append(channel_type)
            
            if since:
                conditions.append("published_at >= %s")
                params.append(since)
            
            where_clause = " AND ".join(conditions)
            params.extend([limit, offset])
            
            query = f"""
                SELECT 
                    id,
                    channel_type,
                    content_id,
                    title,
                    content,
                    author_name,
                    author_identifier,
                    url,
                    media_urls,
                    published_at,
                    fetched_at,
                    engagement_count,
                    view_count,
                    metadata,
                    user_ids,
                    read_status,
                    is_retweet,
                    retweeted_author,
                    retweeted_content,
                    retweeted_url,
                    is_quote,
                    quoted_author,
                    quoted_content,
                    quoted_url,
                    duration_seconds,
                    channel_id,
                    feed_url,
                    categories,
                    is_reply,
                    in_reply_to_id,
                    in_reply_to_username,
                    in_reply_to_user_id,
                    heat_level,
                    ((transcript IS NOT NULL AND transcript != '') OR transcript_data IS NOT NULL) AS has_transcript
                FROM subscription_content
                WHERE {where_clause}
                ORDER BY published_at DESC
                LIMIT %s OFFSET %s
            """
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            contents = []
            for row in rows:
                contents.append({
                    'id': row[0],
                    'channel_type': row[1],
                    'content_id': row[2],
                    'title': row[3],
                    'content': row[4],
                    'author_name': row[5],
                    'author_identifier': row[6],
                    'url': row[7],
                    'media_urls': row[8],
                    'published_at': row[9],
                    'fetched_at': row[10],
                    'engagement_count': row[11],
                    'view_count': row[12],
                    'metadata': row[13],
                    'user_ids': row[14],
                    'read_status': row[15],
                    # Twitter 特定字段
                    'is_retweet': row[16],
                    'retweeted_author': row[17],
                    'retweeted_content': row[18],
                    'retweeted_url': row[19],
                    'is_quote': row[20],
                    'quoted_author': row[21],
                    'quoted_content': row[22],
                    'quoted_url': row[23],
                    # YouTube 特定字段
                    'duration_seconds': row[24],
                    'channel_id': row[25],
                    # RSS 特定字段
                    'feed_url': row[26],
                    'categories': row[27],
                    # Reply 特定字段
                    'is_reply': row[28],
                    'in_reply_to_id': row[29],
                    'in_reply_to_username': row[30],
                    'in_reply_to_user_id': row[31],
                    # 热度等级
                    'heat_level': row[32],
                    # YouTube: 仅返回是否有 transcript（不返回正文）
                    'has_transcript': bool(row[33]) if row[33] is not None else False,
                })

            # PR-7: 叠加 per-user 个性化 summary 覆盖
            # 每条 content 的字段:
            #   summary_source : 'personalized' | 'global' | 'none'
            #   personalized_* : 当前用户的个性化数据 (仅当 summary_source='personalized')
            self._attach_personalized_summaries(cursor, user_id, contents)

            return contents

        except Exception as e:
            logger.error(f"Failed to get user content: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    def _attach_personalized_summaries(self, cursor, user_id: int, contents: List[Dict[str, Any]]) -> None:
        """
        PR-7: 批量附加 per-user 个性化 summary 到 contents 列表 (in-place 修改).

        - 对每条 content, 如果 (content_id, user_id) 在 subscription_content_user_summaries 有行,
          标记 summary_source='personalized' 并附上 personalized_summary / reasoning / template_id.
        - 否则 summary_source='global' (前端可后续用 LEFT JOIN subscription_content_tags 拿全局 summary).
        - 不查询 subscription_content_tags (避免重复 JOIN 增大 SELECT 列数), 全局 summary 在
          /tags 端点单独获取或 daily 聚合查询里另行处理.

        本方法 swallow 任何查询异常: 最差情况下所有 contents 退化为 summary_source='global',
        不阻断 feed 列表加载.
        """
        if not contents:
            return
        try:
            content_ids = [c['id'] for c in contents if c.get('id')]
            if not content_ids:
                for c in contents:
                    c['summary_source'] = 'global'
                return

            cursor.execute(
                """
                SELECT subscription_content_id, id, template_id, summary, reasoning,
                       custom_concepts, ai_metadata, tagged_at, updated_at
                FROM subscription_content_user_summaries
                WHERE subscription_content_id = ANY(%s) AND user_id = %s
                """,
                (content_ids, user_id),
            )
            rows = cursor.fetchall()
            override_map: Dict[int, Dict[str, Any]] = {}
            for r in rows:
                override_map[r[0]] = {
                    'summary_id': r[1],
                    'template_id': r[2],
                    'summary': r[3],
                    'reasoning': r[4],
                    'custom_concepts': r[5] if isinstance(r[5], list) else (r[5] or []),
                    'ai_metadata': r[6] if r[6] else {},
                    'tagged_at': r[7].isoformat() if r[7] else None,
                    'updated_at': r[8].isoformat() if r[8] else None,
                }

            for c in contents:
                ovr = override_map.get(c['id'])
                if ovr:
                    c['summary_source'] = 'personalized'
                    c['personalized_summary'] = ovr['summary']
                    c['personalized_reasoning'] = ovr['reasoning']
                    c['personalized_template_id'] = ovr['template_id']
                    c['personalized_summary_id'] = ovr['summary_id']
                    c['personalized_tagged_at'] = ovr['tagged_at']
                else:
                    c['summary_source'] = 'global'
        except Exception as e:
            logger.warning(
                f"Failed to attach personalized summaries (will fallback to global): {e}"
            )
            for c in contents:
                c.setdefault('summary_source', 'global')
    
    def update_transcript(
        self,
        content_db_id: int,
        transcript: str,
        transcript_data: Dict[str, Any]
    ) -> None:
        """
        写入或更新某条 YouTube 内容的 transcript
        
        Args:
            content_db_id: subscription_content 的主键 id
            transcript: 拼接好的纯文本字幕
            transcript_data: 带时间戳的分段结构 (YouTubeService.fetch_transcript 的返回值)
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE subscription_content
                SET transcript = %s, transcript_data = %s
                WHERE id = %s
                """,
                (transcript, Json(transcript_data), content_db_id)
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update transcript for content id={content_db_id}: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    def mark_as_read(
        self,
        content_id: int,
        user_id: int,
        is_read: bool = True
    ) -> bool:
        """
        标记内容为已读/未读
        
        Args:
            content_id: 内容ID
            user_id: 用户ID
            is_read: 是否已读（默认为 True）
            
        Returns:
            是否成功
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        
        try:
            query = """
                UPDATE subscription_content
                SET read_status = jsonb_set(
                    COALESCE(read_status, '{}'::jsonb),
                    ARRAY[%s::text],
                    %s::jsonb
                )
                WHERE id = %s
            """
            
            read_info = Json({
                'is_read': is_read,
                'read_at': datetime.now().isoformat() if is_read else None
            })
            
            cursor.execute(query, (str(user_id), read_info, content_id))
            conn.commit()
            
            return cursor.rowcount > 0
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to mark content as {'read' if is_read else 'unread'}: {str(e)}")
            return False
        finally:
            cursor.close()
            conn.close()
    
    def get_content_stats(
        self,
        user_id: int,
        channel_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取用户的内容统计
        
        Args:
            user_id: 用户ID
            channel_type: 渠道类型筛选（可选）
            
        Returns:
            统计信息字典
        """
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        
        try:
            conditions = [f"{user_id} = ANY(user_ids)"]
            params = [user_id]
            
            if channel_type:
                conditions.append("channel_type = %s")
                params.append(channel_type)
            
            where_clause = " AND ".join(conditions)
            
            query = f"""
                SELECT 
                    COUNT(*) as total_count,
                    COUNT(CASE WHEN read_status->%s->>'is_read' = 'true' THEN 1 END) as read_count,
                    COUNT(DISTINCT channel_type) as channel_count,
                    COUNT(DISTINCT author_identifier) as author_count
                FROM subscription_content
                WHERE {where_clause}
            """
            
            cursor.execute(query, [str(user_id)] + params[1:])
            row = cursor.fetchone()
            
            return {
                'total_content': row[0],
                'read_content': row[1],
                'unread_content': row[0] - row[1],
                'channel_types': row[2],
                'unique_authors': row[3],
            }
            
        except Exception as e:
            logger.error(f"Failed to get content stats: {str(e)}")
            return {}
        finally:
            cursor.close()
            conn.close()
