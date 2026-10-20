# -*- coding: utf-8 -*-
"""
港股公司信息回填脚本(补 Logo / 名称缺口)

用途: earnings_calendar 中的港股(.HK)卡片需要 companies 表提供公司名和 Logo。
表里港股按"裸代码 + primaryticker 带后缀"存储, 大部分已有名称, 但 Logo 缺失。
本脚本只对「无 companies 记录」或「记录缺 logourl」的港股调 EODHD fundamentals
General 接口, 按 primaryticker upsert 补齐。幂等, 可重复执行。

用法:
    python scripts/maintenance/backfill_hk_companies.py             # 全量补缺口
    python scripts/maintenance/backfill_hk_companies.py --limit 5   # 试跑
    python scripts/maintenance/backfill_hk_companies.py --only 0700.HK,9988.HK
"""

import argparse
import os
import sys
import time

import psycopg2.extras
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("backfill_hk_companies")

# 按 primaryticker upsert: 已有行只补 name/logourl; 新行(表里完全没有)插入完整记录。
# 注意 code 用 General 返回的裸代码(与表内港股存储口径一致), 若与美股裸代码撞车
# 会触发 code 唯一约束, 该条按失败记日志, 不影响其余。
UPSERT_SQL = """
    INSERT INTO companies (
        code, name, exchange, primaryticker, type,
        currencycode, currencyname, currencysymbol, countryname, countryiso,
        sector, industry, description, weburl, logourl,
        data_source, ticker_active, created_at, updated_at
    ) VALUES %s
    ON CONFLICT (primaryticker) DO UPDATE SET
        logourl = EXCLUDED.logourl,
        updated_at = EXCLUDED.updated_at
"""


def process_logo_url(url):
    """与 markets.py 同口径: EODHD 相对路径补全为完整 URL"""
    if not url or str(url).strip() in ("", "N/A", "None"):
        return None
    url = str(url).strip()
    if url.startswith(("http://", "https://")):
        return url
    if url.startswith("/"):
        return f"https://eodhd.com{url}"
    return f"https://eodhd.com/{url}"


def get_pending_codes(db_manager, only=None):
    """返回需要处理的港股代码: 无 companies 记录, 或记录缺 logourl"""
    if only:
        return [c.strip() for c in only.split(",") if c.strip()]
    with db_manager.get_connection_context() as conn:
        with conn.cursor() as cursor:
            # 无绑定参数, psycopg2 不做 % 转义, LIKE 模式用单个 %
            cursor.execute(
                "SELECT DISTINCT ec.code FROM earnings_calendar ec "
                "LEFT JOIN companies c ON c.primaryticker = ec.code "
                "WHERE ec.code LIKE '%.HK' AND (c.id IS NULL OR c.logourl IS NULL) "
                "ORDER BY 1"
            )
            return [row[0] for row in cursor.fetchall()]


def fetch_general(session, code, api_token):
    resp = session.get(
        f"https://eodhd.com/api/fundamentals/{code}",
        params={"filter": "General", "api_token": api_token, "fmt": "json"},
        timeout=(10, 60),
    )
    resp.raise_for_status()
    return resp.json()


def upsert_batch(db_manager, rows):
    from datetime import datetime
    now = datetime.now()
    values = []
    for full_code, g in rows:
        values.append((
            g.get("Code") or full_code,
            g.get("Name") or full_code,
            "HK",
            full_code,
            g.get("Type"),
            g.get("CurrencyCode", "HKD"),
            g.get("CurrencyName"),
            g.get("CurrencySymbol"),
            g.get("CountryName"),
            g.get("CountryISO"),
            g.get("Sector"),
            g.get("Industry"),
            g.get("Description"),
            g.get("WebURL"),
            process_logo_url(g.get("LogoURL")),
            "EODHD",
            True,
            now,
            now,
        ))
    with db_manager.get_connection_context() as conn:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, UPSERT_SQL, values, page_size=100)
        conn.commit()


def main():
    parser = argparse.ArgumentParser(description="回填港股公司 Logo/名称到 companies 表")
    parser.add_argument("--limit", type=int, default=None, help="最多处理多少个代码(试跑用)")
    parser.add_argument("--only", type=str, default=None, help="只处理指定代码, 逗号分隔")
    args = parser.parse_args()

    api_token = os.getenv("EODHD_API_TOKEN")
    if not api_token:
        logger.error("EODHD_API_TOKEN 未设置")
        sys.exit(1)

    db_manager = DatabaseManager(get_database_config())
    codes = get_pending_codes(db_manager, only=args.only)
    if args.limit:
        codes = codes[: args.limit]

    logger.info(f"待回填港股代码: {len(codes)} 个")
    if not codes:
        logger.info("无可处理数据, 退出")
        return

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    batch, ok, fail, no_logo = 0, 0, 0, 0
    for i, code in enumerate(codes, 1):
        try:
            general = fetch_general(session, code, api_token)
            if not general or not general.get("Code"):
                logger.warning(f"[{i}/{len(codes)}] {code}: General 返回空, 跳过")
                fail += 1
            elif not process_logo_url(general.get("LogoURL")):
                # EODHD 本身没有该公司的 Logo, 不入库(避免空值覆盖)
                no_logo += 1
            else:
                batch_item = (code, general)
                try:
                    upsert_batch(db_manager, [batch_item])
                    ok += 1
                except Exception as e:
                    logger.warning(f"[{i}/{len(codes)}] {code}: 入库失败 {str(e)[:120]}")
                    fail += 1
        except Exception as e:
            logger.warning(f"[{i}/{len(codes)}] {code}: 获取失败 {str(e)[:120]}")
            fail += 1

        if i % 50 == 0:
            logger.info(f"[{i}/{len(codes)}] 进度: 成功 {ok}, 无Logo {no_logo}, 失败 {fail}")

        time.sleep(0.35)

    logger.info(f"回填完成: 成功 {ok}, EODHD无Logo {no_logo}, 失败 {fail}")


if __name__ == "__main__":
    main()
