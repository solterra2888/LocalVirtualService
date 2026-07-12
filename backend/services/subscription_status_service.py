"""订阅抓取状态/历史的统一写入 helper (P1 一致性统一).

7 个 Feeds 抓取源 (twitter/youtube/podcast/zsxq/substack/weibo/news) 历史上对
`subscription_list` 状态字段与 `subscription_fetch_history` 的写法各不相同 (Twitter 完整、
YouTube/Podcast 部分、其余完全不写), 本模块提供单一入口 `record_fetch_outcome`, 所有源
在抓取成功/失败时统一调用, 保证:

  1. `last_fetched_at` 用 Python `utcnow()` (aware UTC, 经 psycopg2 全局 UTC adapter 规整进
     naive 列), **不用 SQL `CURRENT_TIMESTAMP`** —— 后者在 worker session 时区非 UTC 时会
     写成服务器本地时间 (详见 backend/utils/datetime_utils.py 的 install_psycopg2_utc_adapter
     及 twitter_subscription_service._update_subscription_status 的 docstring).
  2. `consecutive_failures` 失败自增、成功清零 (旧: 仅 Twitter 正确, YouTube/Podcast 只清零).
  3. 每次抓取写一行 `subscription_fetch_history` (旧: 仅 Twitter 写).

实现直接搬迁自 twitter_subscription_service 的 `_update_subscription_status` +
`_record_fetch_history` (含订阅已删除则跳过、SAVEPOINT 隔离单条 FK 失败).
"""

import logging
import uuid
from typing import Optional

from backend.utils.datetime_utils import utcnow

logger = logging.getLogger(__name__)


def record_fetch_outcome(
    cursor,
    subscription_id: int,
    *,
    status: str,
    items_fetched: int = 0,
    items_new: int = 0,
    items_duplicated: int = 0,
    error: Optional[str] = None,
    execution_duration_ms: Optional[int] = None,
) -> None:
    """统一写 subscription_list 状态 + subscription_fetch_history 一行.

    必须在调用方已持有的 cursor/事务上执行 (与 twitter_subscription_service 一致).
    任何子步骤失败都仅记日志、不抛 —— 状态/历史写入不应中断抓取主链路.

    Args:
        cursor: 调用方事务中的 DB cursor.
        subscription_id: subscription_list.id.
        status: 'success' 或 'failed' (其他值按 failed 处理 consecutive_failures, 但 history 原样存).
        items_fetched: 本次抓取拉到的总条数 (history 用).
        items_new: 本次新增条数 (用于 total_items_fetched 自增 + last_fetch_items_count).
        items_duplicated: 命中已有内容的条数 (history 用).
        error: 失败时的错误信息 (history.error_message).
        execution_duration_ms: 本次抓取耗时毫秒 (history 用).
    """
    _update_subscription_status(cursor, subscription_id, items_new, status)
    _record_fetch_history(
        cursor,
        subscription_id,
        items_fetched=items_fetched,
        items_new=items_new,
        items_duplicated=items_duplicated,
        status=status,
        error_message=error,
        execution_duration_ms=execution_duration_ms,
    )


def record_fetch_outcome_for_channel(
    channel_type: str,
    *,
    status: str,
    items_new: int = 0,
    items_fetched: int = 0,
    error: Optional[str] = None,
    execution_duration_ms: Optional[int] = None,
) -> None:
    """Group3 批量源用: 对该 channel_type 下所有 enabled 订阅写状态 + history.

    zsxq/substack/weibo/news 这类源"一次 tick 抓全部源实体 → user_ids fan-out", 无单一
    subscription_id; 整批结束时为每个受影响订阅写一行 (last_fetched_at / consecutive_failures
    / history). 原本这些源只写 per-source 表 (zsxq_groups 等), subscription_list 状态恒为 NULL.
    """
    try:
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        db = DatabaseManager(get_database_config())
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM subscription_list WHERE channel_type = %s AND enabled = true",
                (channel_type,),
            )
            sub_ids = [r[0] for r in cur.fetchall()]
            for sid in sub_ids:
                record_fetch_outcome(
                    cur, sid,
                    status=status,
                    items_new=items_new,
                    items_fetched=items_fetched,
                    error=error,
                    execution_duration_ms=execution_duration_ms,
                )
    except Exception as e:
        logger.error(
            "record_fetch_outcome_for_channel 失败: channel_type=%s exception=%s",
            channel_type, e,
        )


def record_fetch_outcome_auto(
    subscription_id: int,
    *,
    status: str,
    items_fetched: int = 0,
    items_new: int = 0,
    items_duplicated: int = 0,
    error: Optional[str] = None,
    execution_duration_ms: Optional[int] = None,
) -> None:
    """便捷入口: 自开连接 (get_connection_context, 自动 commit/rollback + TimeZone=UTC)
    写状态+history. 用于无现成事务/游标的调用点 (失败回写、Group3 源等)."""
    try:
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        db = DatabaseManager(get_database_config())
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            record_fetch_outcome(
                cur, subscription_id,
                status=status,
                items_fetched=items_fetched,
                items_new=items_new,
                items_duplicated=items_duplicated,
                error=error,
                execution_duration_ms=execution_duration_ms,
            )
    except Exception as e:
        logger.error(
            "record_fetch_outcome_auto 失败: subscription_id=%s exception=%s",
            subscription_id, e,
        )


def _update_subscription_status(
    cursor, subscription_id: int, items_new: int, status: str
) -> None:
    """更新 subscription_list 状态. last_fetched_at 用 utcnow() (非 CURRENT_TIMESTAMP)."""
    now = utcnow()
    try:
        if status == 'success':
            cursor.execute(
                '''
                UPDATE subscription_list
                SET last_fetched_at = %s,
                    last_fetch_items_count = %s,
                    total_items_fetched = total_items_fetched + %s,
                    consecutive_failures = 0
                WHERE id = %s
                ''',
                (now, items_new, items_new, subscription_id),
            )
        else:
            cursor.execute(
                '''
                UPDATE subscription_list
                SET last_fetched_at = %s,
                    last_fetch_items_count = 0,
                    consecutive_failures = consecutive_failures + 1
                WHERE id = %s
                ''',
                (now, subscription_id),
            )
    except Exception as e:
        logger.error(
            "更新订阅状态失败: subscription_id=%s exception=%s",
            subscription_id, e,
        )


def _record_fetch_history(
    cursor,
    subscription_id: int,
    *,
    items_fetched: int = 0,
    items_new: int = 0,
    items_duplicated: int = 0,
    status: str = 'success',
    error_message: Optional[str] = None,
    execution_duration_ms: Optional[int] = None,
) -> None:
    """记录获取历史. 订阅已删除时跳过; INSERT 用 SAVEPOINT 隔离, 避免单条 FK 失败中止整批事务."""
    try:
        cursor.execute(
            "SELECT 1 FROM subscription_list WHERE id = %s",
            (subscription_id,),
        )
        if not cursor.fetchone():
            logger.warning(
                "跳过抓取历史记录：订阅不存在（可能已删除）: subscription_id=%s",
                subscription_id,
            )
            return
        sp = f"sp_rf_{uuid.uuid4().hex[:16]}"
        cursor.execute(f"SAVEPOINT {sp}")
        cursor.execute(
            '''
            INSERT INTO subscription_fetch_history
            (subscription_id, items_fetched, items_new, items_duplicated,
             status, error_message, execution_duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''',
            (subscription_id, items_fetched, items_new, items_duplicated,
             status, error_message, execution_duration_ms),
        )
        cursor.execute(f"RELEASE SAVEPOINT {sp}")
    except Exception as e:
        try:
            cursor.execute(f"ROLLBACK TO SAVEPOINT {sp}")
        except Exception:
            pass
        logger.error(
            "记录获取历史失败: subscription_id=%s exception=%s",
            subscription_id, e,
        )
