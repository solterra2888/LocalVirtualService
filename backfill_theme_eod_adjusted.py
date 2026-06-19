#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回填所有 theme 成分股的 EODHD 日线历史价格，刷新 adjusted_close / split_coefficient /
dividend_amount 等字段（ON CONFLICT 覆盖旧值）。

用途：修复 Theme Report 的 1Y（及跨拆股/分拆/除息日的 1M/3M）收益率失真——
旧数据里历史段的 adjusted_close 没有正确前复权公司行为，重新从 EODHD 拉取即可覆盖。

设计：不依赖项目 import（避开 pandas 等依赖），直接 urllib + psycopg2。
ticker 来源：theme_company_associations WHERE is_verified=TRUE（与报告读取口径一致）。

用法（项目根目录）：
    # 1) 先 dry-run：只拉取不写库，确认 ticker 数量与 API 连通
    python3 backfill_theme_eod_adjusted.py --dry-run

    # 2) 正式回填（全量 theme ticker，400 天，delay 0.3s/req）
    python3 backfill_theme_eod_adjusted.py

    # 可选：限定结束日（与参考报告 6/17 同期对比时用，避免 price_as_of 漂移到 6/18）
    python3 backfill_theme_eod_adjusted.py --to-date 2026-06-17

    # 遇到 EODHD 限流（大量 429/失败）时加大间隔
    python3 backfill_theme_eod_adjusted.py --delay 0.8
"""
import os
import sys
import time
import json
import argparse
import urllib.request
import urllib.parse
from datetime import date, datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values

ROOT = os.path.dirname(os.path.abspath(__file__))

# ---- 读配置：优先 os.environ（命令行传入），fallback .env ----
ENV = {}
_env_path = os.path.join(ROOT, ".env")
if os.path.exists(_env_path):
    with open(_env_path) as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith("#") and "=" in s:
                k, v = s.split("=", 1)
                ENV[k.strip()] = v.strip().strip('"').strip("'")


def _conf(key, default=None):
    """命令行 env 优先，其次 .env 文件"""
    return os.environ.get(key) or ENV.get(key) or default


TOKEN = _conf("EODHD_API_TOKEN")

ap = argparse.ArgumentParser(description="Backfill theme EOD adjusted history from EODHD.")
ap.add_argument("--days-back", type=int, default=400, help="回溯天数（默认 400，覆盖 1Y）")
ap.add_argument("--delay", type=float, default=0.3, help="每个请求间隔秒数（防限流）")
ap.add_argument("--to-date", default=None, help="结束日期 YYYY-MM-DD，默认今天")
ap.add_argument("--retries", type=int, default=2, help="单 ticker 失败重试次数")
ap.add_argument("--dry-run", action="store_true", help="只拉取不写库")
args = ap.parse_args()

to_date = datetime.strptime(args.to_date, "%Y-%m-%d").date() if args.to_date else date.today()
from_date = to_date - timedelta(days=args.days_back)

# ---- 连库 ----
conn = psycopg2.connect(
    host=_conf("DB_HOST"), port=_conf("DB_PORT"), dbname=_conf("DB_NAME"),
    user=_conf("DB_USER"), password=_conf("DB_PASSWORD"), connect_timeout=15,
)
cur = conn.cursor()

# ---- 取所有 theme 成分股（与报告读取口径一致）----
cur.execute(
    "SELECT DISTINCT ticker FROM theme_company_associations "
    "WHERE is_verified = TRUE ORDER BY ticker"
)
tickers = [r[0] for r in cur.fetchall()]
print(
    f"theme ticker: {len(tickers)} 只 | 范围 {from_date} ~ {to_date} | "
    f"delay={args.delay}s | retries={args.retries} | dry_run={args.dry_run}"
)
if not tickers:
    print("没有 is_verified=TRUE 的 theme ticker，退出。")
    sys.exit(0)

UPSERT = """
INSERT INTO stock_prices_eod
  (code, date, open, high, low, close, volume, adjusted_close, split_coefficient, dividend_amount, period)
VALUES %s
ON CONFLICT (code, date, period) DO UPDATE SET
  open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
  volume=EXCLUDED.volume, adjusted_close=EXCLUDED.adjusted_close,
  split_coefficient=EXCLUDED.split_coefficient, dividend_amount=EXCLUDED.dividend_amount,
  updated_at=CURRENT_TIMESTAMP
"""


def fetch_eodhd(ticker):
    qs = urllib.parse.urlencode({
        "from": from_date.isoformat(), "to": to_date.isoformat(),
        "period": "d", "api_token": TOKEN, "fmt": "json",
    })
    url = f"https://eodhd.com/api/eod/{ticker}?{qs}"
    last_err = None
    for attempt in range(args.retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return json.load(r)
        except Exception as e:
            last_err = e
            if attempt < args.retries:
                time.sleep(1.5 * (attempt + 1))  # 指数退避
    raise last_err


ok = fail = 0
total = 0
failed = []
t0 = time.time()

for i, t in enumerate(tickers, 1):
    try:
        data = fetch_eodhd(t)
        if not isinstance(data, list) or not data:
            print(f"[{i}/{len(tickers)}] {t}: 无数据，跳过")
            continue
        rows = [
            (
                t, it.get("date"), it.get("open"), it.get("high"), it.get("low"),
                it.get("close"), it.get("volume"), it.get("adjusted_close"),
                it.get("split_coefficient", 1.0), it.get("dividend_amount", 0.0),
            )
            for it in data
        ]
        if not args.dry_run:
            execute_values(cur, UPSERT, rows, template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'d')")
            conn.commit()
        ok += 1
        total += len(rows)
        if i % 25 == 0 or i == len(tickers):
            tag = "(dry-run)" if args.dry_run else "✓"
            print(f"[{i}/{len(tickers)}] {t} {tag} 本批 {len(rows)} 条 | 累计 {total} 条 | {time.time()-t0:.0f}s")
    except Exception as e:
        fail += 1
        failed.append(t)
        print(f"[{i}/{len(tickers)}] {t}: ❌ {e}")
        try:
            conn.rollback()
        except Exception:
            pass
    time.sleep(args.delay)

print("\n==== 完成 ====")
print(
    f"成功 {ok}/{len(tickers)} | 失败 {fail} | 总记录 {total} | "
    f"耗时 {time.time()-t0:.0f}s | {'DRY-RUN 未写库' if args.dry_run else '已提交'}"
)
if failed:
    print(f"失败 ticker（{len(failed)}），可对它们重跑：{failed}")
conn.close()
