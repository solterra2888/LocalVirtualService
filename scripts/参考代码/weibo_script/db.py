"""微博帖子 PostgreSQL 持久化（对齐 zsxq-crawl 的 TopicRepository 模式）。"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from crawl4weibo.models.post import Post
from crawl4weibo.models.user import User

logger = logging.getLogger("weibo")

PIC_SEP = ","

SCHEMA_DDL = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")

UPSERT_SQL = """
INSERT INTO weibo_posts (
    post_id, user_id, screen_name, bid, created_at, source, text,
    pic_urls, video_url, is_original, create_at, updated_at
) VALUES (
    %(post_id)s, %(user_id)s, %(screen_name)s, %(bid)s, %(created_at)s,
    %(source)s, %(text)s, %(pic_urls)s, %(video_url)s, %(is_original)s,
    now(), now()
)
ON CONFLICT (post_id) DO UPDATE SET
    screen_name     = EXCLUDED.screen_name,
    bid             = EXCLUDED.bid,
    created_at      = EXCLUDED.created_at,
    source          = EXCLUDED.source,
    text            = EXCLUDED.text,
    pic_urls        = EXCLUDED.pic_urls,
    video_url       = EXCLUDED.video_url,
    is_original     = EXCLUDED.is_original,
    updated_at      = now()
"""


def post_to_row(post: Post, user: User) -> dict[str, Any]:
    return {
        "post_id": int(post.id),
        "user_id": int(user.id or post.user_id),
        "screen_name": user.screen_name or "",
        "bid": post.bid or "",
        "created_at": post.created_at,
        "source": post.source or "",
        "text": post.text or "",
        "pic_urls": PIC_SEP.join(post.pic_urls or []),
        "video_url": post.video_url or "",
        "is_original": post.is_original,
    }


class PostRepository:
    def __init__(self, database_url: str):
        self.database_url = database_url

    @staticmethod
    def ensure_schema(database_url: str) -> None:
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA_DDL)
            conn.commit()
        logger.debug("数据库表 weibo_posts 已就绪")

    def upsert_many(self, rows: list[dict[str, Any]]) -> tuple[int, int]:
        """返回 (新入库条数, 更新条数)。"""
        if not rows:
            return 0, 0

        post_ids = [r["post_id"] for r in rows]
        with psycopg2.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT post_id FROM weibo_posts WHERE post_id = ANY(%s)",
                    (post_ids,),
                )
                existing = {row[0] for row in cur.fetchall()}
                psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=100)
            conn.commit()

        inserted = sum(1 for pid in post_ids if pid not in existing)
        return inserted, len(post_ids) - inserted


def check_database_connection(database_url: str) -> None:
    try:
        with psycopg2.connect(database_url, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    except psycopg2.OperationalError as e:
        raise SystemExit(f"无法连接 PostgreSQL: {e}") from e
    except psycopg2.Error as e:
        raise SystemExit(f"PostgreSQL 连接异常: {e}") from e
