#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一次性 / 可反复执行：补全 subscription_content 中仍以「...全文」结尾的微博长文。

背景见 docs/instructions/WEIBO_LONGTEXT_TRUNCATION.md：
  列表接口只返回截断正文；展开失败后截断被永久写入。
  本脚本按 bid 调 get_post_by_bid，校验更长且非截断后 UPDATE content。

用法:
  # 仅盘点
  python scripts/repair_weibo_truncated.py --dry-run

  # 先修 10 条
  python scripts/repair_weibo_truncated.py --limit 10

  # 全量清扫（可反复跑直到 truncated=0）
  python scripts/repair_weibo_truncated.py

环境:
  优先 weibo_credentials 登录态；否则 WEIBO_COOKIE；再无则访客模式（需 playwright）。
"""

from __future__ import annotations

import argparse
import os
import sys
import time

# 仓库根加入 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.utils.datetime_utils import install_psycopg2_utc_adapter  # noqa: E402

install_psycopg2_utc_adapter()

from backend.services.weibo_subscription_service import (  # noqa: E402
    LONG_TEXT_EXPAND_INTERVAL,
    WeiboClientWrapper,
    WeiboCookieExpiredError,
    WeiboSubscriptionService,
    _TRUNCATION_SUFFIX,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="补全微博长文截断（...全文）")
    parser.add_argument("--dry-run", action="store_true", help="只统计候选，不请求微博、不写库")
    parser.add_argument("--limit", type=int, default=0, help="最多处理条数（0=全部）")
    parser.add_argument(
        "--quota-per-batch",
        type=int,
        default=20,
        help="每批从库取出的条数（默认 20，避免一次加载过多）",
    )
    args = parser.parse_args()

    svc = WeiboSubscriptionService()
    batch_size = args.quota_per_batch if args.limit <= 0 else min(args.quota_per_batch, args.limit)
    candidates = svc._list_truncated_posts(batch_size if args.limit != 0 else max(batch_size, 5000))
    if args.limit > 0:
        candidates = candidates[: args.limit]

    print(f"截断候选: {len(candidates)} 条（探测后缀 {_TRUNCATION_SUFFIX!r}）")
    if args.dry_run:
        for row in candidates[:20]:
            print(
                f"  id={row['id']} author={row.get('author_name')} "
                f"len={row.get('content_len')} bid={row.get('bid')}"
            )
        if len(candidates) > 20:
            print(f"  ... 另有 {len(candidates) - 20} 条")
        return 0

    if not candidates:
        print("无需修复")
        return 0

    active = svc._get_active_credential()
    cookie = active[1] if active else (os.getenv("WEIBO_COOKIE", "").strip() or None)
    if cookie:
        print(f"使用登录态凭证 (credential_id={active[0] if active else 'WEIBO_COOKIE'})")
    else:
        print("警告: 无登录凭证，使用访客模式（风控更严，建议先录入 Cookie）")

    try:
        client = WeiboClientWrapper(
            cookie,
            candidates[0].get("uid") or "0",
            account_label="repair-script",
        )
    except Exception as e:
        print(f"客户端初始化失败: {e}")
        return 1

    repaired = failed = skipped = 0
    for idx, row in enumerate(candidates):
        bid = (row.get("bid") or "").strip()
        if not bid:
            skipped += 1
            print(f"  SKIP id={row['id']} 缺少 bid")
            continue
        if idx > 0:
            time.sleep(LONG_TEXT_EXPAND_INTERVAL)
        try:
            full_text = client.expand_post_by_bid(bid)
            if not full_text:
                failed += 1
                print(f"  FAIL id={row['id']} bid={bid} 展开失败")
                continue
            if svc._update_content_text(int(row["id"]), full_text):
                repaired += 1
                print(
                    f"  OK   id={row['id']} author={row.get('author_name')} "
                    f"{row.get('content_len')}→{len(full_text)} 字"
                )
            else:
                skipped += 1
                print(f"  SKIP id={row['id']} 正文未变长或仍截断")
        except WeiboCookieExpiredError as e:
            failed += 1
            print(f"  AUTH id={row['id']} Cookie 失效，中止: {e}")
            break
        except Exception as e:
            failed += 1
            print(f"  FAIL id={row['id']} bid={bid}: {e}")

    remaining = svc._list_truncated_posts(1)
    print(
        f"\n完成: repaired={repaired} failed={failed} skipped={skipped} "
        f"remaining_sample={'yes' if remaining else '0'}"
    )
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
