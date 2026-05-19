#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A/B 评估: content_tag_feeds_v1 vs tweet_signal_v1

从 sector='未分类' 且 confidence<0.3 的 Twitter 中抽样,
对同一条内容分别调用两个模板 (不写库), 输出对比 CSV.

用法:
    python scripts/eval_tweet_signal_v1.py --limit 50
    python scripts/eval_tweet_signal_v1.py --limit 10 --output /tmp/ab.csv
"""
import argparse
import asyncio
import csv
import json
import os
import sys
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database.db_manager import DatabaseManager  # noqa: E402
from backend.services.local_llm_analysis_service import LocalLLMAnalysisService  # noqa: E402
from backend.utils.database_config import get_database_config  # noqa: E402


def _fetch_samples(db: DatabaseManager, limit: int) -> List[Dict[str, Any]]:
    conn = db.get_connection()
    if not conn:
        raise RuntimeError("无法连接数据库")
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT sc.id, sc.author_name, sc.content,
                   sc.is_retweet, sc.retweeted_content, sc.retweeted_author,
                   sc.is_quote, sc.quoted_content, sc.quoted_author,
                   sct.sector, sct.confidence, sct.summary
            FROM subscription_content sc
            JOIN subscription_content_tags sct
              ON sct.subscription_content_id = sc.id
            WHERE sct.channel_type = 'twitter'
              AND sct.sector = '未分类'
              AND COALESCE(sct.confidence, 0) < 0.3
            ORDER BY RANDOM()
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
        out = []
        for r in rows:
            parts = [(r[2] or "").strip()]
            if r[5] and r[4]:
                parts.append(f"\n[转推 @{r[5] or ''}]\n{r[4]}")
            if r[8] and r[7]:
                parts.append(f"\n[引用 @{r[8] or ''}]\n{r[7]}")
            text = "\n".join(parts).strip()
            out.append({
                "id": r[0],
                "author_name": r[1],
                "text": text,
                "old_sector": r[9],
                "old_confidence": r[10],
                "old_summary": r[11],
            })
        return out
    finally:
        conn.close()


def _parse_json_report(report: str) -> Dict[str, Any]:
    cleaned = (report or "").strip()
    if "```json" in cleaned:
        cleaned = cleaned.split("```json", 1)[1].split("```", 1)[0].strip()
    elif "```" in cleaned:
        parts = cleaned.split("```")
        if len(parts) >= 3:
            cleaned = parts[1].strip()
    return json.loads(cleaned)


async def _run_template(
    template_code: str, text: str, model: str | None = None
) -> Dict[str, Any]:
    svc = LocalLLMAnalysisService(template_code=template_code)
    result = await svc.analyze_file_content(
        file_content=text,
        file_name="eval",
        custom_variables={"channel_type": "twitter", "additional_context": ""},
        model=model,
    )
    parsed = _parse_json_report(result.get("report_content", ""))
    return {
        "parsed": parsed,
        "tokens": result.get("total_tokens", 0),
        "price": result.get("total_price", 0),
        "model": result.get("model"),
    }


async def _eval_samples(samples: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows_out = []
    signal_model = os.getenv("TWEET_SIGNAL_MODEL_OVERRIDE", "").strip() or None

    for s in samples:
        text = s["text"]
        if not text:
            continue
        try:
            old_r = await _run_template("content_tag_feeds_v1", text)
            new_r = await _run_template("tweet_signal_v1", text, model=signal_model)
        except Exception as e:
            rows_out.append({**s, "error": str(e)})
            continue

        op, np = old_r["parsed"], new_r["parsed"]
        rows_out.append({
            **s,
            "old_sector": op.get("sector"),
            "old_tickers": op.get("tickers"),
            "old_people": op.get("people"),
            "old_countries": op.get("countries"),
            "old_summary": op.get("summary"),
            "old_confidence": op.get("confidence"),
            "old_tokens": old_r["tokens"],
            "old_price": old_r["price"],
            "new_sector": np.get("sector"),
            "new_tickers": np.get("tickers"),
            "new_people": np.get("people"),
            "new_countries": np.get("countries"),
            "new_stance": np.get("stance"),
            "new_signal_type": np.get("signal_type"),
            "new_importance": np.get("importance"),
            "new_summary": np.get("summary"),
            "new_confidence": np.get("confidence"),
            "new_tokens": new_r["tokens"],
            "new_price": new_r["price"],
        })
    return rows_out


def main():
    parser = argparse.ArgumentParser(description="tweet_signal_v1 A/B 评估")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--output", default="tweet_signal_v1_ab_eval.csv")
    args = parser.parse_args()

    db = DatabaseManager(get_database_config())
    samples = _fetch_samples(db, args.limit)
    print(f"抽样 {len(samples)} 条未分类 Twitter")

    results = asyncio.run(_eval_samples(samples))

    fieldnames = [
        "id", "author_name", "text",
        "old_sector", "old_confidence", "old_summary", "old_tickers",
        "old_people", "old_countries", "old_tokens", "old_price",
        "new_sector", "new_confidence", "new_summary", "new_tickers",
        "new_people", "new_countries", "new_stance", "new_signal_type",
        "new_importance", "new_tokens", "new_price", "error",
    ]
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in results:
            for k in ("old_tickers", "new_tickers", "old_people", "new_people",
                      "old_countries", "new_countries"):
                if k in r and isinstance(r[k], (list, dict)):
                    r[k] = json.dumps(r[k], ensure_ascii=False)
            w.writerow(r)

    print(f"已写入 {args.output} ({len(results)} 行)")


if __name__ == "__main__":
    main()
