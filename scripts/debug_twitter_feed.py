#!/usr/bin/env python3
"""
Twitter Feed 排查脚本
测试 DeItaone (@DeItaone) 的推文获取情况：
1. 直接查询 twitterapi.io
2. 与数据库已有记录交叉比对
3. 分析时间戳存储是否存在时区偏差
"""
import os
import sys
import requests
import psycopg2
from datetime import datetime, timedelta, timezone

# ─── 配置 ───────────────────────────────────────────────────────────────
API_KEY     = "new1_958a42497f844e1e9dd1b866cb54a9d9"
API_URL     = "https://api.twitterapi.io/twitter/tweet/advanced_search"
AUTHOR      = "DeItaone"
SINCE_HOURS = 48   # 查最近 48 小时（保证覆盖足够范围）

DB_HOST     = "43.99.37.76"
DB_PORT     = 5432
DB_NAME     = "guanhe_database"
DB_USER     = "postgres"
DB_PASS     = "1q23l,Sc45+@!"

# ─── 颜色工具 ─────────────────────────────────────────────────────────────
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"

def banner(msg, color=CYAN):
    print(f"\n{color}{'='*60}\n{msg}\n{'='*60}{RESET}")

# ─── 1. 查询 twitterapi.io ────────────────────────────────────────────────
def fetch_from_api(since_hours: int = 48):
    now_utc   = datetime.now(timezone.utc)
    since_utc = now_utc - timedelta(hours=since_hours)
    since_str = since_utc.strftime("%Y-%m-%d_%H:%M:%S_UTC")
    until_str = now_utc.strftime("%Y-%m-%d_%H:%M:%S_UTC")

    query  = f"from:{AUTHOR} since:{since_str} until:{until_str} include:nativeretweets"
    params = {"query": query, "queryType": "Latest"}

    banner(f"[STEP 1] 查询 twitterapi.io\nauthor={AUTHOR}\nsince={since_str}\nuntil={until_str}")

    all_tweets = []
    page = 0
    cursor = None
    while page < 5:
        if cursor:
            params["cursor"] = cursor
        resp = requests.get(
            API_URL,
            headers={"X-API-Key": API_KEY},
            params=params,
            timeout=30,
        )
        print(f"  HTTP {resp.status_code}  page={page}")
        if resp.status_code != 200:
            print(f"  {RED}错误响应: {resp.text[:300]}{RESET}")
            break

        data = resp.json()
        tweets = data.get("tweets") or data.get("data") or (data if isinstance(data, list) else [])
        if not tweets:
            print(f"  无更多推文，停止分页")
            break
        all_tweets.extend(tweets)
        print(f"  本页 {len(tweets)} 条，累计 {len(all_tweets)} 条")

        has_next  = data.get("has_next_page") or data.get("hasNextPage")
        cursor    = data.get("next_cursor")   or data.get("nextCursor")
        if not has_next or not cursor:
            break
        page += 1

    print(f"\n{GREEN}twitterapi.io 共返回 {len(all_tweets)} 条推文{RESET}")
    return all_tweets

# ─── 2. 查询数据库 ────────────────────────────────────────────────────────
def fetch_from_db(since_hours: int = 48):
    banner("[STEP 2] 查询数据库 subscription_content")
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS,
        options="-c TimeZone=UTC",
    )
    cur = conn.cursor()

    # 取最近 since_hours 的数据（DB 时间以 UTC 存储）
    # 同时多取一些历史数据作为参考（两倍窗口）
    cur.execute("""
        SELECT content_id, published_at, fetched_at, content
        FROM subscription_content
        WHERE channel_type = 'twitter'
          AND author_identifier = %s
        ORDER BY published_at DESC
        LIMIT 50
    """, (AUTHOR,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"{GREEN}数据库最近 50 条记录（published_at DESC）：{RESET}")
    for r in rows[:20]:
        print(f"  content_id={r[0]}  published_at={r[1]}  fetched_at={r[2]}")
        print(f"    {r[3][:80]}")
    if len(rows) > 20:
        print(f"  ... 还有 {len(rows)-20} 条")

    return {str(r[0]): r for r in rows}

# ─── 3. 交叉比对 ──────────────────────────────────────────────────────────
def compare(api_tweets, db_map):
    banner("[STEP 3] 交叉比对 — API vs 数据库")

    missing = []
    found   = []

    for t in api_tweets:
        tid = str(t.get("id", ""))
        if not tid:
            continue
        if tid in db_map:
            found.append(t)
        else:
            missing.append(t)

    print(f"{GREEN}  API 返回: {len(api_tweets)} 条{RESET}")
    print(f"{GREEN}  DB  已有: {len(found)} 条{RESET}")
    print(f"{RED}  DB  缺失: {len(missing)} 条{RESET}")

    if missing:
        print(f"\n{YELLOW}=== 数据库缺失的推文 ==={RESET}")
        for t in sorted(missing, key=lambda x: x.get("createdAt",""), reverse=True):
            created = t.get("createdAt", "N/A")
            tid     = t.get("id", "N/A")
            text    = (t.get("text","") or "")[:100]
            print(f"  [{created}] id={tid}")
            print(f"    {text}")
    return missing

# ─── 4. 时间戳分析 ────────────────────────────────────────────────────────
def analyze_timestamps(api_tweets, db_map):
    banner("[STEP 4] 时间戳分析 — published_at 时区验证")
    now_utc = datetime.now(timezone.utc)
    print(f"  当前 UTC 时间: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  当前 HK  时间: {(now_utc + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S HKT')}")

    # 对比 API 时间戳和 DB 存储时间戳
    sample_count = min(5, len(api_tweets))
    print(f"\n  === 前 {sample_count} 条推文 API vs DB published_at 对比 ===")
    for t in api_tweets[:sample_count]:
        tid     = str(t.get("id", ""))
        created = t.get("createdAt", "")  # API 返回 UTC ISO
        db_row  = db_map.get(tid)

        if db_row:
            db_pub = db_row[1]  # published_at from DB
            # 如果 DB 存的是 UTC naive datetime，它应与 createdAt（UTC）吻合
            # 如果 DB 存的是 HK local (+8h)，它会比 UTC 大 8 小时
            try:
                from dateutil import parser
                api_dt = parser.parse(created)  # aware UTC
                if db_pub:
                    # db_pub 是从 DB 取到的 naive datetime（psql UTC连接下是 UTC）
                    diff_hours = (db_pub - api_dt.replace(tzinfo=None)).total_seconds() / 3600
                    flag = "✅ UTC" if abs(diff_hours) < 0.01 else f"{RED}⚠️  偏差 {diff_hours:+.1f}h (疑似HK本地时间){RESET}"
                else:
                    diff_hours = None
                    flag = "N/A"
                print(f"  API  createdAt   : {created}")
                print(f"  DB   published_at : {db_pub}  → {flag}")
            except Exception as e:
                print(f"  解析出错: {e}")
        else:
            print(f"  id={tid} [{created}] → {YELLOW}DB 无此记录{RESET}")
        print()

# ─── 5. 调度窗口分析 ─────────────────────────────────────────────────────
def analyze_scheduling():
    banner("[STEP 5] 调度窗口分析")
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS,
        options="-c TimeZone=UTC",
    )
    cur = conn.cursor()

    # task_scheduling
    cur.execute("""
        SELECT name, cron_expression, timezone, enabled, last_run, next_run, success_count, fail_count
        FROM task_scheduling
        WHERE id = 'twitter_subscription'
    """)
    row = cur.fetchone()
    if row:
        print(f"  任务名      : {row[0]}")
        print(f"  Cron        : {row[1]}  (时区: {row[2]})")
        print(f"  启用        : {row[3]}")
        print(f"  last_run    : {row[4]}")
        print(f"  next_run    : {row[5]}")
        print(f"  成功/失败   : {row[6]}/{row[7]}")

    # subscription_list for DeItaone
    cur.execute("""
        SELECT id, channel_identifier, last_fetched_at, fetch_interval_minutes
        FROM subscription_list
        WHERE channel_type='twitter' AND channel_identifier = %s
    """, (AUTHOR,))
    subs = cur.fetchall()
    print(f"\n  subscription_list for {AUTHOR}:")
    for s in subs:
        print(f"    id={s[0]}  last_fetched_at={s[2]}  interval={s[3]}min")

    # 最近 10 条 fetch history
    sub_ids = [s[0] for s in subs]
    if sub_ids:
        cur.execute("""
            SELECT subscription_id, fetch_time, items_fetched, items_new, items_duplicated, status, error_message
            FROM subscription_fetch_history
            WHERE subscription_id = ANY(%s)
            ORDER BY fetch_time DESC
            LIMIT 10
        """, (sub_ids,))
        rows = cur.fetchall()
        print(f"\n  最近 10 条 fetch history（UTC）:")
        for r in rows:
            err = f"  ERR:{r[6][:50]}" if r[6] else ""
            print(f"    {r[1]}  fetched={r[2]}  new={r[3]}  dup={r[4]}  status={r[5]}{err}")

    cur.close()
    conn.close()

# ─── Main ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    api_tweets = fetch_from_api(since_hours=SINCE_HOURS)
    db_map     = fetch_from_db(since_hours=SINCE_HOURS)
    missing    = compare(api_tweets, db_map)
    analyze_timestamps(api_tweets, db_map)
    analyze_scheduling()
    banner("排查完成", GREEN)
