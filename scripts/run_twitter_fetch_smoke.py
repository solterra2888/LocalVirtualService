#!/usr/bin/env python3
"""
冒烟测试：直接在本地用修复后的 TwitterSubscriptionService 拉取一次 DeItaone，
验证 1) 24h lookback 生效  2) fetched_at/last_fetched_at 写 UTC 正确
"""
import os
import sys

# 注入数据库环境变量（与 backend/utils/database_config.py 默认值对齐）
os.environ.setdefault("DB_HOST", "43.99.37.76")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_NAME", "guanhe_database")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "1q23l,Sc45+@!")

# 把项目根加入 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 关键：先初始化全局 psycopg2 UTC 适配器（与 celery_app.py 同步）
from backend.utils.datetime_utils import install_psycopg2_utc_adapter
install_psycopg2_utc_adapter()

from backend.services.twitter_subscription_service import TwitterSubscriptionService

print("=" * 60)
print("冒烟测试：DeItaone 强制拉取 (force_fetch=True)")
print("=" * 60)

svc = TwitterSubscriptionService()
print(f"  default_query_hours    = {svc.default_query_hours}")
print(f"  min_lookback_hours     = {svc.min_lookback_hours}")
print(f"  test_fetch_query_hours = {svc.test_fetch_query_hours}")

# 找到 DeItaone 的订阅 id
import psycopg2
conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ["DB_PORT"],
    database=os.environ["DB_NAME"], user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"], options="-c TimeZone=UTC",
)
cur = conn.cursor()
cur.execute("SELECT id FROM subscription_list WHERE channel_type='twitter' AND channel_identifier='DeItaone' LIMIT 1")
row = cur.fetchone()
sub_id = row[0]
cur.close()
conn.close()
print(f"  使用订阅 id = {sub_id}")

print("\n开始拉取……（force_fetch=True 会用 test_fetch_query_hours=12h）")
result = svc.fetch_and_store_tweets(subscription_id=sub_id, force_fetch=True)
print("\n结果:")
for k, v in result.items():
    if k != "author_results":
        print(f"  {k}: {v}")

# 验证写入的 last_fetched_at 是 UTC 时钟
print("\n" + "=" * 60)
print("验证：last_fetched_at 是 UTC 时钟（不应在未来）")
print("=" * 60)
conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ["DB_PORT"],
    database=os.environ["DB_NAME"], user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"], options="-c TimeZone=UTC",
)
cur = conn.cursor()
cur.execute("""
    SELECT id, last_fetched_at, NOW() AT TIME ZONE 'UTC',
           EXTRACT(EPOCH FROM ((NOW() AT TIME ZONE 'UTC') - last_fetched_at))/60 AS minutes_ago
    FROM subscription_list
    WHERE channel_type='twitter' AND channel_identifier='DeItaone'
""")
for r in cur.fetchall():
    flag = "✅ UTC" if -1 < r[3] < 60 else f"❌ 偏差 {r[3]:.1f} 分钟"
    print(f"  id={r[0]}  last_fetched_at={r[1]}  now_utc={r[2]}  minutes_ago={r[3]:.2f}  {flag}")

cur.execute("""
    SELECT content_id, published_at, fetched_at,
           EXTRACT(EPOCH FROM ((NOW() AT TIME ZONE 'UTC') - fetched_at))/60 AS f_min_ago
    FROM subscription_content
    WHERE channel_type='twitter' AND author_identifier='DeItaone'
    ORDER BY fetched_at DESC LIMIT 5
""")
print("\n最近 5 条 subscription_content (verifying fetched_at):")
for r in cur.fetchall():
    flag = "✅ UTC" if -1 < r[3] < 60 else f"❌ 偏差 {r[3]:.1f} 分钟"
    print(f"  id={r[0]}  published_at={r[1]}  fetched_at={r[2]}  fetched_min_ago={r[3]:.2f}  {flag}")

cur.close()
conn.close()
