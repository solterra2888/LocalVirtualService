#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量清洗 YouTube transcript，将结果写入 transcript_cleaned 列

用法（在项目根目录执行）:
  python scripts/run_transcript_cleaning.py               # 处理全部未清洗条目
  python scripts/run_transcript_cleaning.py --limit 100   # 只处理前 100 条（测试用）
  python scripts/run_transcript_cleaning.py --force       # 强制重新处理已有 cleaned 的条目
  python scripts/run_transcript_cleaning.py --dry-run     # 只打印样本，不写库

依赖：
  先执行 python scripts/migrate_add_transcript_cleaned.py 确保列存在
"""

from __future__ import annotations

import os
import sys
import argparse
import time
from typing import Optional

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
import psycopg2.extras
from backend.utils.database_config import get_psycopg2_config
from backend.services.transcript_cleaner import clean_transcript_with_meta, TranscriptFormat


# ──────────────────────────────────────────────────────────────────────────────
# 内部工具
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_rows(cur, limit: Optional[int], force: bool) -> list:
    """
    获取待处理的 transcript 行。
    - force=False: 只取 transcript_cleaned IS NULL 的行
    - force=True : 取所有非空 transcript 行（重新处理）
    """
    condition = (
        "transcript IS NOT NULL AND btrim(transcript) <> ''"
        if force else
        "transcript IS NOT NULL AND btrim(transcript) <> '' AND transcript_cleaned IS NULL"
    )
    sql = f"""
        SELECT id, transcript
        FROM subscription_content
        WHERE channel_type = 'youtube' AND {condition}
        ORDER BY id
        {"LIMIT %s" if limit else ""}
    """
    params = (limit,) if limit else ()
    cur.execute(sql, params)
    return cur.fetchall()


def _batch_update(conn, updates: list[tuple]) -> None:
    """批量写回 transcript_cleaned。updates: [(cleaned_text, row_id), ...]"""
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            "UPDATE subscription_content SET transcript_cleaned = %s WHERE id = %s",
            updates,
            page_size=200,
        )
    conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# 主函数
# ──────────────────────────────────────────────────────────────────────────────

def run(limit: Optional[int], force: bool, dry_run: bool) -> None:
    config = get_psycopg2_config()
    conn = psycopg2.connect(**config)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            print("正在查询待处理条目…")
            rows = _fetch_rows(cur, limit, force)

        total = len(rows)
        print(f"共找到 {total} 条待处理 transcript。\n")

        if total == 0:
            print("无需处理，退出。")
            return

        # 统计用
        fmt_counter: dict[str, int] = {}
        skipped = 0
        processed = 0

        updates: list[tuple] = []
        batch_size = 200
        start_time = time.time()

        for i, (row_id, raw_text) in enumerate(rows, start=1):
            meta = clean_transcript_with_meta(raw_text)
            fmt = meta["format"]
            cleaned = meta["cleaned"]

            fmt_counter[fmt] = fmt_counter.get(fmt, 0) + 1

            if cleaned is None:
                skipped += 1
                continue

            if dry_run:
                # 打印前 3 条样本
                if processed < 3:
                    print(f"─── 样本 id={row_id}  格式={fmt} ───")
                    print(f"[原始 {meta['original_len']} 字符]\n{raw_text[:400]}")
                    print(f"\n[清洗后 {meta['cleaned_len']} 字符]\n{cleaned[:400]}\n")
                processed += 1
                continue

            updates.append((cleaned, row_id))
            processed += 1

            # 每 batch_size 条写一次库
            if len(updates) >= batch_size:
                _batch_update(conn, updates)
                updates.clear()
                elapsed = time.time() - start_time
                print(f"  已处理 {i}/{total}，用时 {elapsed:.1f}s…")

        # 写剩余
        if updates:
            _batch_update(conn, updates)

        elapsed = time.time() - start_time

        # 结果摘要
        print("\n" + "=" * 55)
        print(f"清洗完成！")
        print(f"  总条数     : {total}")
        print(f"  已处理     : {processed}")
        print(f"  跳过(空结果): {skipped}")
        print(f"  写库       : {'跳过（dry-run）' if dry_run else processed - skipped}")
        print(f"  用时       : {elapsed:.1f}s")
        print(f"\n  格式分布:")
        for fmt_name, cnt in sorted(fmt_counter.items(), key=lambda x: -x[1]):
            print(f"    {fmt_name:<12} {cnt:>6} 条")
        print("=" * 55)

    except Exception as exc:
        conn.rollback()
        print(f"✗ 处理失败：{exc}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="批量清洗 YouTube transcript")
    parser.add_argument("--limit",   type=int,  default=None, help="最多处理 N 条（不设则全量）")
    parser.add_argument("--force",   action="store_true",     help="重新处理已有 cleaned 的条目")
    parser.add_argument("--dry-run", action="store_true",     help="只打印样本，不写库")
    args = parser.parse_args()

    run(limit=args.limit, force=args.force, dry_run=args.dry_run)
