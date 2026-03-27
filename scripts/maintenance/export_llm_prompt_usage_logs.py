#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
临时脚本：导出 llm_prompt_usage_logs 表数据（默认 Excel .xlsx，可选 CSV / JSON）。

用法（在项目根目录执行）:
  python scripts/maintenance/export_llm_prompt_usage_logs.py
  python scripts/maintenance/export_llm_prompt_usage_logs.py -o ./exports/usage_logs.xlsx
  python scripts/maintenance/export_llm_prompt_usage_logs.py --format csv
  python scripts/maintenance/export_llm_prompt_usage_logs.py --format json --since-days 7

依赖：DB_* 环境变量（与后端一致，通常来自项目根目录 .env）
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path

# 项目根：scripts/maintenance/xxx.py -> parents[2]
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import psycopg2
import psycopg2.extras

from backend.utils.database_config import get_psycopg2_config

# Excel 单单元格最大字符数（超出会截断并加省略标记）
EXCEL_MAX_CELL = 32700


def _cell(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _truncate_for_excel(s: str) -> str:
    if len(s) <= EXCEL_MAX_CELL:
        return s
    return s[: EXCEL_MAX_CELL - 20] + "\n...[已截断，Excel 单格上限]"


def export_excel(rows: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "id", "template_id", "user_id", "provider", "model",
        "rendered_prompt", "variables_data", "response_content",
        "tokens_used", "cost", "response_time_ms", "success",
        "error_message", "user_feedback", "created_at",
    ]
    if not rows:
        df = pd.DataFrame(columns=cols)
    else:
        df = pd.DataFrame(rows)
        present = [c for c in cols if c in df.columns]
        df = df[present]

        def _variables_cell(x):
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return ""
            if isinstance(x, (dict, list)):
                return json.dumps(x, ensure_ascii=False)
            return str(x)

        for c in ("rendered_prompt", "response_content", "error_message"):
            if c in df.columns:
                df[c] = df[c].apply(
                    lambda x: _truncate_for_excel(str(x))
                    if x is not None and not (isinstance(x, float) and pd.isna(x))
                    else ""
                )
        if "variables_data" in df.columns:
            df["variables_data"] = df["variables_data"].apply(_variables_cell)
    df.to_excel(out_path, index=False, engine="openpyxl")


def export_csv(rows: list, columns: list[str], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for row in rows:
            w.writerow([_cell(row.get(c)) for c in columns])


def export_json(rows: list, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2, default=str)


def main() -> int:
    parser = argparse.ArgumentParser(description="导出 llm_prompt_usage_logs")
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=None,
        help="输出文件路径（默认：exports/llm_prompt_usage_logs_<timestamp>.xlsx）",
    )
    parser.add_argument(
        "--format",
        choices=("xlsx", "csv", "json"),
        default="xlsx",
        help="导出格式（默认 Excel xlsx）",
    )
    parser.add_argument(
        "--since-days",
        type=int,
        default=None,
        help="仅导出最近 N 天（按 created_at）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="最多导出行数（调试用）",
    )
    args = parser.parse_args()

    cfg = get_psycopg2_config()
    if not cfg.get("database") or not cfg.get("user"):
        print("错误: 请配置 .env 中的 DB_NAME / DB_USER / DB_PASSWORD / DB_HOST", file=sys.stderr)
        return 1

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.output is None:
        args.output = ROOT / "exports" / f"llm_prompt_usage_logs_{ts}.{args.format}"

    sql = """
        SELECT
            id, template_id, user_id, provider, model,
            rendered_prompt, variables_data, response_content,
            tokens_used, cost, response_time_ms, success, error_message,
            user_feedback, created_at
        FROM llm_prompt_usage_logs
    """
    params: list = []
    if args.since_days is not None:
        sql += " WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL %s"
        params.append(f"{args.since_days} days")
    sql += " ORDER BY id ASC"
    if args.limit is not None:
        sql += " LIMIT %s"
        params.append(args.limit)

    conn = psycopg2.connect(**cfg)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        conn.close()

    # RealDictRow -> plain dict（JSON 可序列化）
    plain = [dict(r) for r in rows]

    if args.format == "xlsx":
        export_excel(plain, args.output)
    elif args.format == "csv":
        if not plain:
            cols = [
                "id", "template_id", "user_id", "provider", "model",
                "rendered_prompt", "variables_data", "response_content",
                "tokens_used", "cost", "response_time_ms", "success",
                "error_message", "user_feedback", "created_at",
            ]
            export_csv([], cols, args.output)
        else:
            cols = list(plain[0].keys())
            export_csv(plain, cols, args.output)
    else:
        export_json(plain, args.output)

    print(f"已导出 {len(plain)} 行 -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
