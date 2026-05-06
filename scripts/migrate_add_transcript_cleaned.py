#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库迁移：为 subscription_content 表新增 transcript_cleaned 列

用法（在项目根目录执行）:
  python scripts/migrate_add_transcript_cleaned.py
  python scripts/migrate_add_transcript_cleaned.py --dry-run   # 只检查，不执行

幂等：若列已存在则直接跳过，不会报错。
"""

import os
import sys
import argparse

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)
except ImportError:
    pass

import psycopg2
from backend.utils.database_config import get_psycopg2_config


def column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
        """,
        (table, column),
    )
    return cur.fetchone() is not None


def run(dry_run: bool = False) -> None:
    config = get_psycopg2_config()
    conn = psycopg2.connect(**config)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            if column_exists(cur, "subscription_content", "transcript_cleaned"):
                print("✓ 列 transcript_cleaned 已存在，无需迁移。")
                return

            sql = (
                "ALTER TABLE subscription_content "
                "ADD COLUMN transcript_cleaned TEXT DEFAULT NULL;"
            )
            print(f"将执行：{sql}")

            if dry_run:
                print("[dry-run] 未实际执行。")
                return

            cur.execute(sql)
            conn.commit()
            print("✓ 迁移完成：transcript_cleaned 列已添加。")
    except Exception as exc:
        conn.rollback()
        print(f"✗ 迁移失败：{exc}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="添加 transcript_cleaned 列")
    parser.add_argument("--dry-run", action="store_true", help="只检查，不执行 DDL")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
