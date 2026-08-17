#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
按 publication 回填近 N 年 Substack 文章到 subscription_content。

默认目标: https://michaeljburry.substack.com （Cassandra Unchained）近 1 年。
复用 SubstackSubscriptionService 的 Cookie / archive 分页 / 正文补全 / 入库路径，
只跑文章轨，不扫 Notes。已入库的 post 默认跳过正文拉取。

用法:
  python scripts/maintenance/backfill_substack_publication.py --dry-run
  python scripts/maintenance/backfill_substack_publication.py
  python scripts/maintenance/backfill_substack_publication.py \\
      --url https://michaeljburry.substack.com --years 1
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Set

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from backend.utils.datetime_utils import install_psycopg2_utc_adapter, utcnow  # noqa: E402

install_psycopg2_utc_adapter()

from backend.services.substack_credential_service import (  # noqa: E402
    normalize_publication_url,
)
from backend.services.substack_subscription_service import (  # noqa: E402
    CookieExpiredError,
    SubstackClient,
    SubstackSubscriptionService,
    _get_conn,
    _meta_dict_to_row,
    _parse_dt,
    _save_publication_state,
    _store_batch,
    _trigger_tagging,
)
from backend.utils.logger import get_logger  # noqa: E402

logger = get_logger("backfill_substack_publication")

DEFAULT_URL = "https://michaeljburry.substack.com"
DEFAULT_YEARS = 1.0
DEFAULT_MAX_POSTS = 400
BODY_INTERVAL = 0.6


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="回填指定 Substack publication 近 N 年文章到 subscription_content",
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="publication 根 URL")
    parser.add_argument(
        "--years",
        type=float,
        default=DEFAULT_YEARS,
        help="回填窗口（年），默认 1",
    )
    parser.add_argument(
        "--max-posts",
        type=int,
        default=DEFAULT_MAX_POSTS,
        help="archive 最多拉取篇数（安全上限）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只列出将入库的文章，不写库、不拉正文",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="已入库的文章也重新拉正文并更新",
    )
    parser.add_argument(
        "--no-tag",
        action="store_true",
        help="跳过新文章的自动打标",
    )
    return parser.parse_args()


def _existing_post_ids(pub_url: str) -> Set[str]:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT content_id
                FROM subscription_content
                WHERE channel_type = 'substack'
                  AND author_identifier = %s
                  AND content_id ~ '^[0-9]+$'
                """,
                (pub_url,),
            )
            return {str(row[0]) for row in cur.fetchall()}


def _resolve_pub_name(meta: Dict[str, Any], items: List[Dict[str, Any]], pub_url: str) -> str:
    name = (meta.get("publication_name") or "").strip()
    if name and not name.lower().startswith("http"):
        return name
    for item in items:
        nested = item.get("publishedBylines") or []
        pub = item.get("publication") if isinstance(item.get("publication"), dict) else {}
        candidate = (pub.get("name") or "").strip()
        if candidate:
            return candidate
        if isinstance(nested, list):
            for author in nested:
                if isinstance(author, dict) and author.get("name"):
                    return str(author["name"]).strip()
    return pub_url


def _filter_by_cutoff(
    items: List[Dict[str, Any]],
    cutoff: datetime,
) -> List[Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    stopped_early = False
    for item in items:
        if not isinstance(item, dict):
            continue
        published = _parse_dt(item.get("post_date"))
        if published is not None and published < cutoff:
            stopped_early = True
            break
        kept.append(item)
    logger.info(
        f"日期过滤: archive={len(items)} 窗口内={len(kept)} "
        f"cutoff={cutoff.isoformat()} stopped_early={stopped_early}"
    )
    return kept


def backfill(
    pub_url: str,
    years: float,
    max_posts: int,
    dry_run: bool,
    force_refresh: bool,
    no_tag: bool,
) -> int:
    pub_url = normalize_publication_url(pub_url)
    cutoff = utcnow() - timedelta(days=int(round(years * 365.25)))
    logger.info(f"目标 {pub_url}  窗口 >= {cutoff.date()}  max_posts={max_posts}")

    svc = SubstackSubscriptionService()
    try:
        agg = [
            item
            for item in svc._aggregate_publications()
            if item["publication_url"] == pub_url
        ]
        if not agg:
            logger.error(f"subscription_list 中没有启用的订阅: {pub_url}")
            return 1
        pub = agg[0]
        user_ids: List[int] = list(pub.get("user_ids") or [])
        if not user_ids and not dry_run:
            logger.error("没有订阅用户，拒绝写库")
            return 1

        meta = svc._get_publication_meta(pub_url)
        cred_id, auth = svc._get_auth_for_credential(meta.get("credential_id"))
        logger.info(f"使用凭证 cred_id={cred_id}  user_ids={user_ids}")

        client = SubstackClient(auth, pub_url)
        archive_items = client._fetch_archive_items(max_posts)
        logger.info(f"archive 拉取 {len(archive_items)} 篇 (fetch_url={client.fetch_url})")
        if not archive_items:
            logger.warning("archive 为空，结束")
            return 0

        window_items = _filter_by_cutoff(archive_items, cutoff)
        pub_name = _resolve_pub_name(meta, archive_items, pub.get("publication_name") or pub_url)
        pub_avatar = str(meta.get("avatar") or "").strip()
        existing = _existing_post_ids(pub_url)
        logger.info(f"库中已有 {len(existing)} 篇 post")

        for idx, item in enumerate(window_items, 1):
            title = (item.get("title") or "").strip() or "(untitled)"
            published = item.get("post_date") or ""
            post_id = item.get("id")
            flag = "exists" if str(post_id) in existing else "new"
            logger.info(f"  [{idx:03d}/{len(window_items)}] {published}  {flag}  {title}")

        if dry_run:
            new_in_window = sum(1 for it in window_items if str(it.get("id")) not in existing)
            logger.info(f"dry-run：不写库。窗口内 {len(window_items)} 篇，其中新 {new_in_window} 篇")
            return 0

        to_fetch = [
            item for item in window_items
            if force_refresh or str(item.get("id")) not in existing
        ]
        logger.info(f"待拉正文 {len(to_fetch)} 篇（跳过已入库 {len(window_items) - len(to_fetch)} 篇）")

        rows: List[Dict[str, Any]] = []
        for idx, item in enumerate(to_fetch):
            if idx:
                time.sleep(BODY_INTERVAL)
            enriched = client._enrich_item_body(item)
            rows.append(_meta_dict_to_row(enriched, pub_url, pub_name, pub_avatar))
            if (idx + 1) % 10 == 0 or idx + 1 == len(to_fetch):
                logger.info(f"正文补全进度 {idx + 1}/{len(to_fetch)}")

        if not rows:
            logger.info("没有需要入库的新文章")
        else:
            new_count, updated_count, new_ids = _store_batch(
                svc.content_service, rows, user_ids, pub_name,
            )
            logger.info(f"入库完成: 新 {new_count}  更新 {updated_count}  合计 {len(rows)}")
            if not no_tag:
                _trigger_tagging(new_ids, pub_name, report=None)

        head_ids = [int(it["id"]) for it in archive_items if it.get("id") is not None][:20]
        state_rows = rows or [
            _meta_dict_to_row(item, pub_url, pub_name, pub_avatar) for item in window_items[:1]
        ]
        with _get_conn() as conn:
            _save_publication_state(
                conn,
                pub_url,
                state_rows,
                mode="backfill",
                head_ids=head_ids or None,
                backfill_complete=True,
            )

        dates = [_parse_dt(it.get("post_date")) for it in window_items]
        dates = [d for d in dates if d]
        if dates:
            logger.info(f"覆盖区间 {min(dates).date()} → {max(dates).date()}")
        logger.info(f"完成 {pub_url}: 窗口 {len(window_items)} 篇，新入库 {len(rows)} 篇")
        return 0
    except CookieExpiredError as exc:
        logger.error(f"Substack Cookie 已过期: {exc}")
        return 2
    except Exception as exc:
        logger.error(f"回填失败: {exc}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
    finally:
        svc._cleanup_auth_cache()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    args = _parse_args()
    return backfill(
        pub_url=args.url,
        years=args.years,
        max_posts=args.max_posts,
        dry_run=args.dry_run,
        force_refresh=args.force_refresh,
        no_tag=args.no_tag,
    )


if __name__ == "__main__":
    raise SystemExit(main())
