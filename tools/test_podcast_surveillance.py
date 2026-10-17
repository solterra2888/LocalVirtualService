#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""测试：能否从 Bloomberg Surveillance 官方播客源获取节目转录（字幕）。

验证链路（与生产 podcast ASR 路径完全一致）:
  1. iTunes 目录 API 解析官方 RSS 源
  2. feedparser 解析近期剧集（标题 / 北京时间发布时刻 / 时长 / 音频 URL）
  3. 检查 RSS 是否自带 <podcast:transcript>（若自带则无需 ASR）
  4. 与 YouTube 正片发布时间对比（同期节目，读 subscription_content）
  5. 下载最新一集音频 →（可选 ffmpeg 截取前 N 分钟）→ OSS → DashScope
     Fun-ASR 转写 → 打印转录样本与耗时

用法（在仓库根目录运行）:
  python tools/test_podcast_surveillance.py             # 默认截取前 10 分钟做 ASR 验证
  python tools/test_podcast_surveillance.py --full      # 转写完整一集（2-3h 音频，ASR 约 10-20 分钟）
  python tools/test_podcast_surveillance.py --no-asr    # 只看 feed 可达性与时间对比
"""

import argparse
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import feedparser
import requests

CST = timezone(timedelta(hours=8))
ITUNES_SEARCH = (
    "https://itunes.apple.com/search"
    "?term=bloomberg+surveillance&media=podcast&limit=5"
)
FEED_FALLBACK = (
    "https://www.omnycontent.com/d/playlist/e73c998e-6e60-432f-8610-ae210140c5b1/"
    "8e704079-ca57-4eac-9741-ae27003e2b7f/9739700c-72c3-4176-ae55-ae27003e2b96/podcast.rss"
)


def bj(dt):
    return dt.astimezone(CST).strftime("%Y-%m-%d %H:%M:%S")


def resolve_feed_url():
    try:
        resp = requests.get(ITUNES_SEARCH, timeout=15)
        resp.raise_for_status()
        for r in resp.json().get("results", []):
            if "surveillance" in (r.get("collectionName") or "").lower():
                return r["feedUrl"], r["collectionName"]
    except Exception as exc:
        print(f"[warn] iTunes API 解析失败，使用兜底 URL: {exc}")
    return FEED_FALLBACK, "Bloomberg Surveillance (fallback)"


def fetch_episodes(feed_url, limit=10):
    resp = requests.get(feed_url, timeout=30)
    resp.raise_for_status()
    raw = resp.content.decode("utf-8", errors="replace")

    feed = feedparser.parse(resp.content)
    item_blocks = raw.split("<item>")[1:]
    episodes = []
    for idx, e in enumerate(feed.entries[:limit]):
        pub = None
        for key in ("published_parsed", "updated_parsed"):
            st = e.get(key)
            if st:
                pub = datetime(*st[:6], tzinfo=timezone.utc)
                break
        audio_url = ""
        size = 0
        for enc in e.get("enclosures", []):
            audio_url = enc.get("href") or enc.get("url") or ""
            size = int(enc.get("length") or 0)
            break
        block = item_blocks[idx] if idx < len(item_blocks) else ""
        m = re.search(
            r'<podcast:transcript url="([^"]*TextWithTimestamps[^"]*)"', block
        )
        episodes.append({
            "title": e.get("title", ""),
            "published": pub,
            "duration": e.get("itunes_duration", ""),
            "audio_url": audio_url,
            "audio_mb": round(size / 1024 / 1024, 1),
            "transcript_url": m.group(1).replace("&amp;", "&") if m else None,
        })
    return episodes


def fetch_official_transcript(ep):
    """下载 Omny 官方字幕（TextWithTimestamps，带说话人与时间戳）。"""
    if not ep.get("transcript_url"):
        return None
    resp = requests.get(ep["transcript_url"], timeout=30)
    if resp.status_code != 200:
        print(f"[warn] transcript 下载失败: HTTP {resp.status_code}")
        return None
    return resp.text


def duration_seconds(dur):
    try:
        parts = [int(p) for p in str(dur).split(":")]
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        if len(parts) == 2:
            return parts[0] * 60 + parts[1]
        return int(float(dur))
    except (ValueError, TypeError):
        return 0


def youtube_recent_episodes(days=4):
    """读 subscription_content 里最近几期 YouTube 正片的发布时间（北京时间）。"""
    import psycopg2

    env = {}
    with open("/opt/guanhetech/.env", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()

    conn = psycopg2.connect(
        host=env["DB_HOST"], port=env.get("DB_PORT", 5432),
        dbname=env["DB_NAME"], user=env["DB_USER"], password=env["DB_PASSWORD"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT title, published_at
                FROM subscription_content
                WHERE channel_type='youtube'
                  AND author_identifier='UCIALMKvObZNtJ6AmdCLP7Lg'
                  AND title ~ '^Bloomberg Surveillance [0-9]{1,2}/[0-9]{1,2}/[0-9]{4}'
                ORDER BY published_at DESC LIMIT %s
                """,
                (days,),
            )
            return [(t, p.replace(tzinfo=timezone.utc)) for t, p in cur.fetchall()]
    finally:
        conn.close()


def download_and_clip(audio_url, clip_minutes):
    resp = requests.get(audio_url, stream=True, timeout=600)
    resp.raise_for_status()
    tmp_dir = tempfile.mkdtemp(prefix="podcast_test_")
    full_path = os.path.join(tmp_dir, "episode.mp3")
    with open(full_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1 << 16):
            f.write(chunk)
    print(f"下载完成: {os.path.getsize(full_path) / 1024 / 1024:.1f} MB -> {full_path}")

    if clip_minutes <= 0:
        return full_path
    clip_path = os.path.join(tmp_dir, "clip.mp3")
    import subprocess
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error", "-i", full_path,
         "-t", str(clip_minutes * 60), "-c", "copy", clip_path],
        check=True,
    )
    print(f"已截取前 {clip_minutes} 分钟: {clip_path}")
    return clip_path


def run_asr(audio_path):
    from backend.services.asr_service import ASRService
    from backend.services.oss_service import OSSService

    upload = OSSService().upload_audio_file(
        audio_path, os.path.basename(audio_path)
    )
    url = upload.get("signed_url") or upload.get("public_url")
    if not url:
        raise RuntimeError(f"OSS 上传失败: {upload}")
    print(f"OSS 上传完成: {upload.get('object_name', '')}")

    asr = ASRService()
    if not asr.is_configured():
        raise RuntimeError("DashScope ASR 未配置 (DASHSCOPE_API_KEY)")

    t0 = time.time()
    result = asr.transcribe_audio(url)
    elapsed = time.time() - t0
    if not result or not result.get("success"):
        raise RuntimeError(f"ASR 转写失败: {(result or {}).get('error')}")

    texts = []
    for trans in result.get("transcriptions") or []:
        texts.append(
            asr.format_transcription_result(
                trans,
                include_timestamps=True,
                include_words=False,
                include_speaker=True,
                timestamped_only=True,
            )
        )
    full_text = "\n".join(texts) or (result.get("text") or "")
    return full_text, elapsed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="转写完整一集（默认只取前 10 分钟）")
    parser.add_argument("--no-asr", action="store_true", help="跳过 ASR，只看 feed 与时间对比")
    parser.add_argument("--clip-minutes", type=int, default=10)
    args = parser.parse_args()

    print("=" * 70)
    print("[1] 解析官方播客源")
    feed_url, name = resolve_feed_url()
    print(f"    {name}\n    {feed_url}")

    print("\n[2] 近期剧集（时间为北京时间）")
    episodes = fetch_episodes(feed_url)
    for ep in episodes:
        pub = bj(ep["published"]) if ep["published"] else "?"
        print(f"    {pub} | {duration_seconds(ep['duration'])//60:>3}min "
              f"| {ep['audio_mb']:>5}MB | {ep['title'][:60]}")

    print("\n[3] 对比 YouTube 正片发布时间（北京时间）")
    try:
        for title, pub in youtube_recent_episodes():
            print(f"    {bj(pub)} | YouTube: {title}")
    except Exception as exc:
        print(f"    [warn] 读取数据库失败: {exc}")

    daily = next(
        (e for e in episodes if "surveillance tv" in e["title"].lower()), None
    )
    if not daily:
        raise RuntimeError("feed 中未找到 'Surveillance TV' 每日剧集")

    print(f"\n[4] 官方字幕验证: {daily['title']}")
    print(f"    发布(北京): {bj(daily['published'])}")
    official = fetch_official_transcript(daily)
    if official:
        words = len(official.split())
        print(f"    ✅ 官方字幕可直接下载: {len(official)} 字符 / {words} 词")
        print("    --- 样本 ---")
        for line in official.splitlines()[:14]:
            print(f"    {line}")
        print(f"\n    结论: 无需 ASR，官方字幕即可用 ({words} 词)")
    else:
        print("    ❌ 官方字幕不可用，回退 ASR 路径")

    if args.no_asr or official:
        if args.no_asr:
            print("\n--no-asr 指定，跳过转写测试")
        return

    target = daily
    print(f"\n[5] ASR 验证对象: {target['title'][:70]}")
    print(f"    音频: {target['audio_url'][:100]}...")

    clip = 0 if args.full else args.clip_minutes
    audio_path = download_and_clip(target["audio_url"], clip)
    print(f"\n[6] 提交 DashScope Fun-ASR（{'完整一集' if args.full else f'前 {clip} 分钟'}）")
    text, elapsed = run_asr(audio_path)
    words = len(text.split())
    print(f"\n[7] 结果: 转写耗时 {elapsed:.0f}s, {len(text)} 字符 / {words} 词")
    print("-" * 70)
    print(text[:1200])
    print("-" * 70)
    print("结论: podcast 音频 -> Fun-ASR 转录链路可用" if words > 100
          else "结论: 转录结果过短，需人工检查")


if __name__ == "__main__":
    main()
