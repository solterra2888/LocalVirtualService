#!/usr/bin/env python3
"""
twitterapi.io 诊断脚本
测试不同查询方式对 @DeItaone 的抓取结果，找出自动化任务漏抓推文的根因。

结论预期：
  - since:/until: 格式 + cursor 分页 → 文档明确不支持，预期遗漏推文
  - since_time:/until_time: unix 时间戳 + 时间窗口切片 → 文档推荐方式
"""

import time
import requests
from datetime import datetime, timedelta, timezone

API_KEY = "new1_958a42497f844e1e9dd1b866cb54a9d9"
BASE_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"
AUTHOR = "deitaone"

session = requests.Session()
session.headers.update({"X-API-Key": API_KEY})


def fetch_page(query: str, cursor: str = None) -> dict:
    params = {"query": query, "queryType": "Latest"}
    if cursor:
        params["cursor"] = cursor
    resp = session.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fmt(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M UTC")


def tweet_summary(t: dict) -> str:
    return f"  {t['createdAt'][:24]}  id={t['id']}  {repr(t['text'][:50])}"


# ─────────────────────────────────────────────
# 公共时间范围：过去 48h
# ─────────────────────────────────────────────
NOW_UTC = datetime.now(timezone.utc)
SINCE_48H = NOW_UTC - timedelta(hours=48)

since_unix = int(SINCE_48H.timestamp())
until_unix = int(NOW_UTC.timestamp())

print("=" * 70)
print(f"诊断时间: {fmt(NOW_UTC)} (HK {(NOW_UTC + timedelta(hours=8)).strftime('%H:%M')})")
print(f"查询范围: {fmt(SINCE_48H)} → {fmt(NOW_UTC)}")
print("=" * 70)


# ─────────────────────────────────────────────
# 测试 1：旧格式 since:/until: + cursor 分页（当前代码用法）
# ─────────────────────────────────────────────
print("\n【测试 1】旧格式 since:date + cursor 分页（当前代码行为）")
since_str = SINCE_48H.strftime("%Y-%m-%d_%H:%M:%S_UTC")
until_str = NOW_UTC.strftime("%Y-%m-%d_%H:%M:%S_UTC")
q_old = f"from:{AUTHOR} since:{since_str} until:{until_str} include:nativeretweets"
print(f"  query: {q_old}")

all_old, cursor, pages = [], None, 0
while pages < 3:
    data = fetch_page(q_old, cursor)
    tweets = data.get("tweets", [])
    all_old.extend(tweets)
    has_next = data.get("has_next_page", False)
    cursor = data.get("next_cursor")
    pages += 1
    print(f"  页 {pages}: {len(tweets)} 条, has_next={has_next}")
    if not has_next or not cursor:
        break
    time.sleep(0.5)

print(f"  总计: {len(all_old)} 条")
for t in sorted(all_old, key=lambda x: x["id"], reverse=True)[:5]:
    print(tweet_summary(t))


# ─────────────────────────────────────────────
# 测试 2：新格式 since_time:/until_time: unix 时间戳（文档推荐）
# ─────────────────────────────────────────────
print("\n【测试 2】新格式 since_time:/until_time: unix 时间戳（文档推荐）")
q_new = f"from:{AUTHOR} since_time:{since_unix} until_time:{until_unix} include:nativeretweets"
print(f"  query: {q_new}")

data2 = fetch_page(q_new)
tweets2 = data2.get("tweets", [])
has_next2 = data2.get("has_next_page", False)
print(f"  单页返回: {len(tweets2)} 条, has_next={has_next2}")
for t in tweets2[:5]:
    print(tweet_summary(t))


# ─────────────────────────────────────────────
# 测试 3：时间窗口切片（文档推荐方式：≤20条/窗口，不用分页）
# ─────────────────────────────────────────────
print("\n【测试 3】时间窗口切片（每 4h 一个窗口，不用 cursor）")
SLICE_HOURS = 4
all_sliced: dict[str, dict] = {}  # id → tweet，用于去重
window_end = NOW_UTC
slices_fetched = 0

while window_end > SINCE_48H:
    window_start = max(window_end - timedelta(hours=SLICE_HOURS), SINCE_48H)
    s_unix = int(window_start.timestamp())
    u_unix = int(window_end.timestamp())
    q_slice = f"from:{AUTHOR} since_time:{s_unix} until_time:{u_unix} include:nativeretweets"
    
    data_s = fetch_page(q_slice)
    tweets_s = data_s.get("tweets", [])
    new_in_slice = 0
    for t in tweets_s:
        if t["id"] not in all_sliced:
            all_sliced[t["id"]] = t
            new_in_slice += 1
    
    print(f"  [{fmt(window_start)} → {fmt(window_end)}]: "
          f"{len(tweets_s)} 条返回, {new_in_slice} 条新增, "
          f"has_next={data_s.get('has_next_page', False)}")
    
    window_end = window_start
    slices_fetched += 1
    time.sleep(0.3)

print(f"\n  切片查询总计唯一推文: {len(all_sliced)} 条 (共 {slices_fetched} 个窗口)")
for t in sorted(all_sliced.values(), key=lambda x: x["id"], reverse=True)[:10]:
    print(tweet_summary(t))


# ─────────────────────────────────────────────
# 对比摘要
# ─────────────────────────────────────────────
print("\n" + "=" * 70)
print("对比摘要")
print("=" * 70)
print(f"  测试1 (旧格式+分页):       {len(all_old)} 条推文")
print(f"  测试2 (新格式单页):        {len(tweets2)} 条推文")
print(f"  测试3 (新格式切片，无分页): {len(all_sliced)} 条推文")

# 测试1 vs 测试3 差异
ids_old = {t["id"] for t in all_old}
ids_sliced = set(all_sliced.keys())
missed = ids_sliced - ids_old
extra = ids_old - ids_sliced

if missed:
    print(f"\n  ⚠️  旧方式漏抓了 {len(missed)} 条（切片有但旧方式没有）:")
    for tid in sorted(missed, reverse=True):
        t = all_sliced[tid]
        print(f"    {t['createdAt'][:24]}  id={tid}  {repr(t['text'][:50])}")
else:
    print("\n  ✅ 旧方式和切片方式结果一致，无漏抓")
