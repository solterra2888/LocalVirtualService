#!/usr/bin/env python
"""定期轮询博主更新，并 upsert 到 PostgreSQL（默认 5 分钟）。"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from crawl4weibo import WeiboClient
from dotenv import load_dotenv

from db import PostRepository, check_database_connection, post_to_row
from verify import build_client as create_client

load_dotenv()

logger = logging.getLogger("weibo")


def setup_logging() -> None:
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"weibo_{datetime.now():%Y%m%d}.log"

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
    root = logging.getLogger("weibo")
    root.handlers.clear()
    root.setLevel(logging.INFO)

    for handler in (
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(),
    ):
        handler.setFormatter(fmt)
        root.addHandler(handler)

    logging.getLogger("crawl4weibo").setLevel(logging.WARNING)


def load_json(path: Path, default):
    if not path.exists():
        return default
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def poll_once(
    client: WeiboClient,
    config: dict,
    state: dict,
    repo: PostRepository,
) -> int:
    user_ids = config.get("user_ids", [])
    limit = int(config.get("posts_per_user", 5))
    total_inserted = 0

    for uid in user_ids:
        key = str(uid)
        seen_ids = set(state.get(key, []))
        try:
            user = client.get_user_by_uid(uid)
            posts = client.get_user_posts(uid, page=1, expand=True)
        except Exception as exc:
            logger.warning("拉取失败 %s: %s", uid, exc)
            continue

        rows = [post_to_row(post, user) for post in posts[:limit]]
        inserted, _ = repo.upsert_many(rows)
        total_inserted += inserted

        fresh_ids: list[str] = []
        for post in posts[:limit]:
            post_id = str(post.id)
            fresh_ids.append(post_id)
            if post_id in seen_ids:
                continue
            preview = (post.text or "").replace("\n", " ")[:60]
            logger.info(
                "新微博 %s(%s) %s: %s",
                user.screen_name,
                uid,
                post_id,
                preview,
            )

        state[key] = fresh_ids[:50]

    return total_inserted


def validate_config(config: dict) -> str:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url or "your_db" in database_url:
        raise SystemExit("请在 .env 中配置 DATABASE_URL")
    if database_url.count("@") > 1:
        raise SystemExit(
            "DATABASE_URL 格式有误：密码中的 @ 须 URL 编码（@→%40）"
        )
    if not config.get("user_ids"):
        raise SystemExit("请在 config.json 中配置 user_ids。")
    return database_url


def main() -> int:
    parser = argparse.ArgumentParser(description="定期监控微博博主更新并写入数据库")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--once", action="store_true", help="只执行一轮后退出")
    args = parser.parse_args()

    setup_logging()

    config_path = Path(args.config)
    if not config_path.exists():
        logger.error("缺少配置文件: %s", config_path)
        return 1

    config = load_json(config_path, {})
    database_url = validate_config(config)

    check_database_connection(database_url)
    try:
        PostRepository.ensure_schema(database_url)
    except Exception as e:
        logger.error("自动建表失败: %s", e)
        return 1

    state_path = Path(config.get("state_file", "data/last_seen.json"))
    state = load_json(state_path, {})
    interval = int(config.get("poll_interval_seconds", 300))
    client = create_client(config, log_level="WARNING")
    repo = PostRepository(database_url)

    logger.info(
        "监控启动 | %d 个博主 | 间隔 %ds",
        len(config["user_ids"]),
        interval,
    )

    while True:
        try:
            inserted = poll_once(client, config, state, repo)
            save_json(state_path, state)
            if inserted:
                logger.info("本轮新入库 %d 条", inserted)
        except Exception:
            logger.exception("本轮采集异常")

        if args.once:
            break
        time.sleep(interval)

    return 0


if __name__ == "__main__":
    sys.exit(main())
