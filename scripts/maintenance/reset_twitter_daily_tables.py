#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
临时脚本：清空 Twitter Daily Report 相关表并重置 id 序列（SERIAL）。

- 表：twitter_daily_reports、twitter_author_daily_summaries
- TRUNCATE ... RESTART IDENTITY：清空数据并将 id 序列重置为从 1 起算
- REINDEX TABLE：重建上述表的索引（数据清空后通常很快）

用法（在项目根目录）:
  python scripts/maintenance/reset_twitter_daily_tables.py
  python scripts/maintenance/reset_twitter_daily_tables.py --yes   # 跳过确认

依赖：与后端相同的 DB 环境变量（.env 中 DB_*）。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from config.settings import config  # noqa: F401 — 确保加载项目配置
from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config

TABLES = ("twitter_daily_reports", "twitter_author_daily_summaries")


def main() -> None:
    parser = argparse.ArgumentParser(description="清空 Twitter 日报相关表并重置序列")
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="不询问确认，直接执行",
    )
    args = parser.parse_args()

    db_cfg = get_database_config()
    if not db_cfg.get("db_name") or not db_cfg.get("db_user"):
        print("错误: 请设置 DB_NAME、DB_USER（及 DB_PASSWORD 等）环境变量或 .env。", file=sys.stderr)
        sys.exit(1)

    if not args.yes:
        print("将执行以下操作（不可恢复）：")
        for t in TABLES:
            print(f"  - TRUNCATE {t} RESTART IDENTITY CASCADE")
        for t in TABLES:
            print(f"  - REINDEX TABLE {t}")
        confirm = input("确认继续? [y/N]: ").strip().lower()
        if confirm not in ("y", "yes"):
            print("已取消。")
            sys.exit(0)

    dm = DatabaseManager(db_cfg)
    conn = dm.get_connection()
    if conn is None:
        print("错误: 无法连接数据库。", file=sys.stderr)
        sys.exit(1)

    try:
        with conn.cursor() as cur:
            # 单次 TRUNCATE 多表；RESTART IDENTITY 重置两表 id 序列
            cur.execute(
                """
                TRUNCATE TABLE twitter_daily_reports, twitter_author_daily_summaries
                RESTART IDENTITY CASCADE;
                """
            )
            for t in TABLES:
                cur.execute(f"REINDEX TABLE {t};")
        conn.commit()
        print("完成: 两表已清空，id 序列已重置，索引已 REINDEX。")
    except Exception as e:
        conn.rollback()
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
