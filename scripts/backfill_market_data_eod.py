#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用脚本：拉取 EODHD 历史日线并回填 market_data_eod 表（upsert 幂等，可重复执行）。

与 backend/services/market_data_service.py 同源:
- 数据接口: EODHD /api/eod/{code}
- 入库方式: execute_values + ON CONFLICT (code, date) DO UPDATE
- 资产注册（显示名/category）由 market_data_service 的 MAP 字典管理，
  本脚本只负责历史回填，asset_type 需与注册时一致。

用法（在项目根目录）:
  python scripts/backfill_market_data_eod.py --codes GSPC.INDX,NDX.INDX --asset-type index --dry-run
  python scripts/backfill_market_data_eod.py --codes GSPC.INDX,NDX.INDX --asset-type index            # 默认 3 年
  python scripts/backfill_market_data_eod.py --codes VIX.INDX --asset-type index --years 5           # 指定年数

Token 从环境变量 EODHD_API_TOKEN 读取，会尝试加载项目根目录 .env。
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import timedelta
from pathlib import Path

import psycopg2.extras
import requests

# 允许从项目根目录直接运行
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backend.database.db_manager import DatabaseManager          # noqa: E402
from backend.utils.database_config import get_database_config    # noqa: E402
from backend.utils.datetime_utils import utcnow                  # noqa: E402

BASE_URL = "https://eodhd.com/api"


def load_env_if_present() -> None:
    """简单加载项目根 .env 中的 EODHD_API_TOKEN / DB_*（不引入 python-dotenv）。"""
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if not env_file.exists():
        return
    wanted = {"EODHD_API_TOKEN", "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"}
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key = line.split("=", 1)[0].strip()
        if key in wanted and not os.getenv(key):
            os.environ[key] = line.split("=", 1)[1].strip().strip('"').strip("'")


def fetch_eod_history(token: str, code: str, from_date, to_date) -> list:
    """调用 EODHD 历史日线接口，返回按日期升序的记录列表。"""
    url = f"{BASE_URL}/eod/{code}"
    params = {
        "api_token": token,
        "fmt": "json",
        "period": "d",
        "from": from_date.strftime("%Y-%m-%d"),
        "to": to_date.strftime("%Y-%m-%d"),
        "order": "a",
    }
    resp = requests.get(url, params=params, timeout=60,
                        headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError(f"EODHD 返回异常: {data}")
    return data


# 与 market_data_service.parse_eod_data 一致的字段口径
def safe_float(v):
    if v is None or v == "" or str(v).upper() == "NA":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def safe_int(v):
    if v is None or v == "" or str(v).upper() == "NA":
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="回填 EODHD 日线到 market_data_eod")
    parser.add_argument("--codes", required=True,
                        help="EODHD 代码，逗号分隔（如 GSPC.INDX,NDX.INDX）")
    parser.add_argument("--asset-type", required=True,
                        help="asset_type，需与 MAP 注册一致（index/forex/commodity/bond/sector）")
    parser.add_argument("--years", type=int, default=3, help="回填年数（默认 3）")
    parser.add_argument("--dry-run", action="store_true", help="只拉取不入库")
    args = parser.parse_args()

    load_env_if_present()
    token = os.getenv("EODHD_API_TOKEN")
    if not token:
        print("未找到 EODHD_API_TOKEN", file=sys.stderr)
        sys.exit(1)

    codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    to_date = utcnow().date()
    from_date = to_date - timedelta(days=args.years * 365)

    all_records: list[tuple] = []
    for code in codes:
        print(f"拉取 {code} {from_date} ~ {to_date} 日线...")
        try:
            rows = fetch_eod_history(token, code, from_date, to_date)
        except Exception as e:
            print(f"❌ {code} 拉取失败: {e}", file=sys.stderr)
            if len(codes) == 1:
                sys.exit(1)
            continue  # 多代码时单个失败不阻断整体
        print(f"  共 {len(rows)} 个交易日, 首日: {rows[0]['date'] if rows else '-'}, "
              f"末日: {rows[-1]['date'] if rows else '-'}")

        for r in rows:
            if not r.get("date") or safe_float(r.get("close")) is None:
                continue
            all_records.append((
                code,
                r["date"],
                args.asset_type,
                safe_float(r.get("open")),
                safe_float(r.get("high")),
                safe_float(r.get("low")),
                safe_float(r.get("close")),
                safe_float(r.get("adjusted_close")),
                safe_int(r.get("volume")),
            ))

    if args.dry_run:
        print(f"[dry-run] 解析后待入库 {len(all_records)} 条，不入库，结束")
        return
    if not all_records:
        print("无数据可入库", file=sys.stderr)
        sys.exit(1)
    print(f"解析后待入库共 {len(all_records)} 条")

    db = DatabaseManager(db_conn_params=get_database_config())
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        upsert_sql = """
            INSERT INTO market_data_eod
            (code, date, asset_type, open, high, low, close, adjusted_close, volume,
             created_at, updated_at)
            VALUES %s
            ON CONFLICT (code, date)
            DO UPDATE SET
                asset_type = EXCLUDED.asset_type,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                adjusted_close = EXCLUDED.adjusted_close,
                volume = EXCLUDED.volume,
                updated_at = CURRENT_TIMESTAMP
        """
        psycopg2.extras.execute_values(
            cursor, upsert_sql, all_records,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            page_size=500,
        )
        conn.commit()
        codes_str = ", ".join(codes)
        print(f"✅ 已 upsert {len(all_records)} 条到 market_data_eod (code ∈ [{codes_str}])")
    except Exception as e:
        conn.rollback()
        print(f"❌ 入库失败: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
