# -*- coding: utf-8 -*-
"""
轻量级数据库管理器 — 仅包含 Worker 所需的连接管理功能。
"""

import os
import time
import contextlib
import psycopg2
from psycopg2.extras import Json


def get_db_config() -> dict:
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": os.getenv("DB_NAME", ""),
        "user": os.getenv("DB_USER", ""),
        "password": os.getenv("DB_PASSWORD", ""),
    }


class DB:
    """极简 PostgreSQL 连接管理"""

    def __init__(self, params: dict | None = None):
        self._params = params or get_db_config()

    def get_connection(self, max_retries=5, retry_delay=2):
        last_err = None
        for attempt in range(max_retries):
            try:
                return psycopg2.connect(
                    **self._params,
                    connect_timeout=30,
                    options="-c statement_timeout=600000",
                    keepalives_idle=30,
                    keepalives_interval=5,
                    keepalives_count=6,
                )
            except Exception as e:
                last_err = e
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 1.5, 10)
        raise ConnectionError(f"无法连接数据库: {last_err}")

    @contextlib.contextmanager
    def connection(self):
        conn = self.get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
