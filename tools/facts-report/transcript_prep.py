#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Transcript 预处理(直查取数 + Layer 1 清洗 + Layer 3 对账留档)。

取数(v5,不经过摘要表):按交易日 T 直查 subscription_content 定位节目转录
  - The Close(YouTube, Bloomberg Television 频道): T 晚间 21:00-23:00 UTC 窗口、
    转录 >=50k 字符、开场白自识别 "THIS IS THE CLOSE"(同窗口有 Surveillance/
    Businessweek 等同量级干扰项,长度+窗口不足以唯一定位)。
  - Closing Bell(播客): T 晚间 19:00-24:00 UTC 窗口、标题带 T 的 MM/DD/YY 日期
    后缀(28/28 样本覆盖;日间节目行无日期后缀,天然隔离)。主段与 Overtime 各
    一行(约各 1 小时),按 published_at 排序早=主段、晚=Overtime,并用标题
    'overtime' 或开场白 "closing bell overtime" 自识别校验。Overtime 标题有
    两种风格:"Closing Bell Overtime: …" 或纯副标题(无 "Closing Bell" 字样),
    不可按标题关键字过滤。

Layer 1 确定性清洗:原始转录取回后按格式分支清洗,产出 transcripts/{show}.clean.txt。
  - youtube 字幕式(The Close):按 ">>" 切说话轮次,轮次内合并硬换行,去连续复读行,
    裁掉首个 ">>" 之前的开场模板。保持原大小写(避免误伤专名)。
  - podcast ASR 式(Closing Bell 主段/Overtime):解析 "[x.xxs - y.yys]" 时间戳为
    [mm:ss] 锚,按段间隔 >3s 分段合段。说话人未标注,不猜名字。

Layer 3 确定性对账(留档未启用,严格性过强曾撤下,见 TEST-2026-10-28.md §12):
结构化 agent 产出的数字台账(transcripts/{show}.numbers.json)逐条对快照数据层,
标 matched / consistent / conflict / unknown,冲突清单写回 transcripts/{show}.ledger.json。

CLI(对既有快照补挂转录,测试/复用用):
    python transcript_prep.py --ws backend/reports/pipeline/2026-10-28
build_snapshot 正常构建时直接调用 prep_workspace(),不走 CLI。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

import requests  # noqa: E402  — Closing Bell 单集溯源(RSS)
from backend.database.db_manager import DatabaseManager  # noqa: E402
from backend.utils.database_config import get_database_config  # noqa: E402

SHOWS = ("the_close", "cnbc_closing_bell")
PODCAST_TS_RE = re.compile(
    r"^\[(\d+(?:\.\d+)?)s\s*-\s*(\d+(?:\.\d+)?)s\]\s*(.*)$", re.M)
NUM_TOKEN_RE = re.compile(
    r"\$?\d[\d,]*(?:\.\d+)?\s?(?:%|bp|bps|x|bn|b|mm|m|k| trillion| billion| million)?",
    re.I)
GAP_SEC_PARA = 3.0

# ── 直查取数:节目自识别指纹 ───────────────────────────────────────────
TC_SELF_ID_RE = re.compile(r"THIS\s+IS\s*[\"\u201c]?\s*THE\s+CLOSE", re.I)
try:  # 生产级识别器(2026-11-10 起):标题层+其他栏目排除+16000字口播窗,单一事实源
    from backend.services.the_close_summary_service import (
        is_bloomberg_the_close_video as _is_tc_prod,
    )
except Exception:  # backend 不可导入时退回本地正则(独立可跑)
    _is_tc_prod = None

TC_TITLE_DATE_RE = re.compile(r"the\s+close\s+0?\d{1,2}/0?\d{1,2}", re.I)
CB_OT_SELF_ID_RE = re.compile(r"CLOS\w*\s*(?:THE\s+)?BELL\s+OVERTIME", re.I)


def _conn() -> Any:
    db = DatabaseManager(get_database_config())
    return db.get_connection()


def _row(cur: Any, channel: str, t0: datetime, t1: datetime,
         min_len: int = 0) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT content_id, title, published_at,
               COALESCE(transcript_cleaned, transcript),
               url, feed_url
        FROM subscription_content
        WHERE channel_type = %(ch)s
          AND published_at >= %(t0)s AND published_at < %(t1)s
          AND COALESCE(transcript_cleaned, transcript) IS NOT NULL
        ORDER BY published_at
        """,
        {"ch": channel, "t0": t0, "t1": t1},
    )
    out = []
    for cid, title, pub, tr, url, feed in cur.fetchall():
        if len(tr or "") < min_len:
            continue
        out.append({"content_id": cid, "title": title or "",
                    "published_at": pub, "transcript": tr,
                    "url": url, "feed_url": feed})
    return out


def fetch_the_close(conn: Any, session_date: date) -> Optional[Dict[str, Any]]:
    """Bloomberg Television 频道当晚的 The Close 整期转录(自识别校验)。"""
    day0 = datetime(session_date.year, session_date.month, session_date.day,
                    tzinfo=timezone.utc)
    # 逐窗口取行→行内自识别;全部不中才放宽下一档。2026-11-03 实跑踩坑:
    # 主窗口(21-23 时)先命中中期选举特辑(56k)就 break,自识别失败直接
    # 放弃,当晚真正的 The Close 落在 23:00:27 UTC,兜底窗口从未被执行。
    with conn.cursor() as cur:
        for t0h, t1h in ((21, 23), (20.5, 24.0), (20.5, 25.0)):
            rows = _row(cur, "youtube", day0 + timedelta(hours=t0h),
                        day0 + timedelta(hours=t1h), min_len=50000)
            for r in rows:
                # 2026-11-10 修复:自标识句可能被长冷开场推后(11-09 首次出现在第
                # 7945 字),搜索窗 2500→12000;另加日期式标题兜底("The Close
                # 11/9/2026")。标题两种形态互补:11-06 整期标题是新闻句式,单靠
                # 标题会漏;单靠开头正则会漏 11-09 —— 取或。
                head = r["transcript"][:12000]
                if (_is_tc_prod(r["title"], transcript=r["transcript"]) if _is_tc_prod
                    else (TC_SELF_ID_RE.search(head) or TC_TITLE_DATE_RE.search(r["title"] or ""))):
                    return r
    return None


def _cb_title_has_date(title: str, session_date: date) -> bool:
    """标题带 T 的 M(M)/D(D)/YY(或 4 位年)后缀 —— 主段与 Overtime 行都
    满足,日间节目行(无日期后缀)天然被排除。

    CNBC 标题的月/日不补零("11/2/26"),strftime 的 %m/%d 会补零
    ("11/02/26")—— 个位日/月必须两种形式都匹配(2026-11-02 实跑踩坑)。
    """
    pat = re.compile(
        rf"0?{session_date.month}/0?{session_date.day}/"
        rf"(?:{session_date.strftime('%y')}|{session_date.year})")
    return bool(pat.search(title))



CB_FEED_FALLBACK = "https://feeds.simplecast.com/Nh1wIaXT"
_CB_FEED_CACHE: Dict[str, Dict[str, str]] = {}


def _cb_feed_items(feed: str) -> Dict[str, str]:
    """官方播客源 → {标题: 单集链接};失败返回空。"""
    if feed in _CB_FEED_CACHE:
        return _CB_FEED_CACHE[feed]
    items: Dict[str, str] = {}
    try:
        resp = requests.get(feed, timeout=25,
                            headers={"User-Agent": "Mozilla/5.0"})
        root = ET.fromstring(resp.content)
        for it in root.iter("item"):
            t = (it.findtext("title") or "").strip()
            # CNBC 官方 RSS 的 <link> 全是首页占位;单集最源头是
            # <enclosure>(节目 MP3 音频,转录的原始素材),无 enclosure 才退 link。
            enc = it.find("enclosure")
            link = (enc.get("url") if enc is not None else None) \
                or (it.findtext("link") or "").strip()
            if t and link and link != "https://www.cnbc.com":
                items[t] = link
    except Exception:  # noqa: BLE001 — 溯源增强,失败不阻断建快照
        pass
    _CB_FEED_CACHE[feed] = items
    return items


def _cb_episode_link(r: Dict[str, Any], rows: List[Dict[str, Any]]) -> str:
    feed = (r.get("feed_url") or CB_FEED_FALLBACK) or CB_FEED_FALLBACK
    items = _cb_feed_items(feed)
    title = (r.get("title") or "").strip()
    if title and title in items:
        return items[title]
    # 标题微差兜底:双方去空格小写后互相包含
    tl = re.sub(r"\s+", "", title.lower())
    for t, link in items.items():
        if tl and (tl in re.sub(r"\s+", "", t.lower())
                   or re.sub(r"\s+", "", t.lower()) in tl):
            return link
    return feed


def fetch_closing_bell(conn: Any,
                       session_date: date) -> Dict[str, Optional[Dict[str, Any]]]:
    """当日全部 Closing Bell 播客行 → {main, overtime}(各约 1 小时)。

    分类:published_at 早 = 主段、晚 = Overtime;Overtime 佐证 = 标题含
    'overtime' 或开场白自识别。>2 行时优先标题带 closing bell 的行。
    """
    day0 = datetime(session_date.year, session_date.month, session_date.day,
                    19, tzinfo=timezone.utc)
    with conn.cursor() as cur:
        rows = _row(cur, "podcast", day0, day0 + timedelta(hours=5))
    # 2026-11-12 起 CNBC 单集标题频繁退化为纯编辑标题(无日期后缀;11-16 两集
    # 全无日期,旧过滤直接全丢)。晚间窗口内本 feed 只有 Closing Bell 两集,
    # 日间节目挂在别的 feed —— feed 兜底安全,日期后缀仍是首选判据。
    rows = [r for r in rows
            if _cb_title_has_date(r["title"], session_date)
            or CB_FEED_FALLBACK in (r.get("feed_url") or "")]
    if len(rows) > 2:  # 罕见:同日多行时优先规范标题
        ranked = sorted(rows, key=lambda r: 0 if "closing bell" in r["title"].lower() else 1)
        rows = ranked[:2]
    main = ot = None
    # 溯源:官方播客 RSS 匹配单集链接(匹配失败回退官方源,绝不阻断)
    for r in rows:
        r["episode_url"] = _cb_episode_link(r, rows)
    if len(rows) == 1:
        only = rows[0]
        head = only["transcript"][:1500]
        if "overtime" in only["title"].lower() or CB_OT_SELF_ID_RE.search(head):
            ot = only
        else:
            main = only
    elif len(rows) >= 2:
        main, ot = rows[0], rows[1]
        # 早行自识别成 Overtime 则互换(窗口重叠日的兜底)
        if (CB_OT_SELF_ID_RE.search(main["transcript"][:1500])
                and not CB_OT_SELF_ID_RE.search(ot["transcript"][:1500])):
            main, ot = ot, main
    return {"main": main, "overtime": ot}


def window_probe(session_date: date) -> Dict[str, Dict[str, Any]]:
    """就绪门诊断(2026-11-17):区分"数据未入库"与"已入库但零匹配"。

    show_gate 等待期每轮调用:某节目窗口内已有候选行但快照仍缺 → 匹配器
    缺口,继续等不会自愈 → 门应立即 🔴 告警放行,而不是耗到截止。背景:
    2026-11-16 Closing Bell 两集 06:37 入库、因标题退化被过滤,门空等到
    09:40。自管连接供 show_gate 无 conn 调用;DB 抖动抛出由调用方容忍。
    """
    day0 = datetime(session_date.year, session_date.month, session_date.day,
                    tzinfo=timezone.utc)
    conn = _conn()
    try:
        with conn.cursor() as cur:
            # The Close:三档窗口内 ≥50k 的 youtube 行(≈整期体量;youtube 行
            # 无 feed 标识,只能按体量计;可能混入别频道的整期,标题一并带出)
            tc_rows: List[Dict[str, Any]] = []
            seen: set = set()
            for t0h, t1h in ((21, 23), (20.5, 24.0), (20.5, 25.0)):
                for r in _row(cur, "youtube",
                              day0 + timedelta(hours=t0h),
                              day0 + timedelta(hours=t1h), min_len=50000):
                    if r["content_id"] not in seen:
                        seen.add(r["content_id"])
                        tc_rows.append(r)
            # Closing Bell:晚间窗口 podcast 行,其中官方 feed 行是强信号
            # (日间节目挂另一 feed,不会误入)
            cb_rows = _row(cur, "podcast", day0 + timedelta(hours=19),
                           day0 + timedelta(hours=24))
    finally:
        conn.close()
    cb_feed = [r for r in cb_rows if CB_FEED_FALLBACK in (r.get("feed_url") or "")]
    return {
        "the_close": {
            "candidate_rows": len(tc_rows),
            "titles": [r["title"][:60] for r in tc_rows[:3]],
        },
        "cnbc_closing_bell": {
            "window_rows": len(cb_rows),
            "cb_feed_rows": len(cb_feed),
            "titles": [r["title"][:60] for r in cb_feed[:4]],
        },
    }


# ── Layer 1: 清洗 ──────────────────────────────────────────────────────

def detect_format(text: str) -> str:
    """>> 说话人标记 + 大写为主 → youtube;[x.xxs - y.yys] 时间戳 → podcast。"""
    if PODCAST_TS_RE.search(text[:2000]):
        return "podcast"
    return "youtube"


def _dedup_adjacent(lines: List[str]) -> List[str]:
    out: List[str] = []
    for ln in lines:
        if out and ln.strip() and ln.strip() == out[-1].strip():
            continue
        out.append(ln)
    return out


def clean_youtube(text: str) -> Tuple[str, Dict[str, Any]]:
    """字幕式:首 ">>" 前的开场模板裁掉;按 ">>" 切轮次;轮次内合并硬换行。"""
    lines = [ln.rstrip() for ln in text.splitlines()]
    lines = _dedup_adjacent(lines)
    first_turn = next((i for i, ln in enumerate(lines)
                       if ln.lstrip().startswith(">>")), 0)
    lines = lines[first_turn:]
    turns: List[str] = []
    cur: List[str] = []
    for ln in lines:
        if ln.lstrip().startswith(">>"):
            if cur:
                turns.append(" ".join(x.strip() for x in cur if x.strip()))
            cur = [ln.lstrip()[2:].strip()]
        else:
            cur.append(ln)
    if cur:
        turns.append(" ".join(x.strip() for x in cur if x.strip()))
    turns = [t for t in turns if t]
    out_lines = [
        f">> {t}" for t in turns
    ]
    clean = "\n".join(out_lines)
    meta = {"format": "youtube", "turns": len(turns),
            "raw_chars": len(text), "clean_chars": len(clean)}
    return clean, meta


def _mmss(sec: float) -> str:
    m, s = int(sec // 60), int(sec % 60)
    return f"{m:02d}:{s:02d}"


def clean_podcast(text: str) -> Tuple[str, Dict[str, Any]]:
    """ASR 式:时间戳转 [mm:ss] 锚;段间隔 > GAP_SEC_PARA 秒即分段合段。"""
    segs: List[Tuple[float, str]] = []
    for ln in text.splitlines():
        m = PODCAST_TS_RE.match(ln.strip())
        if m:
            segs.append((float(m.group(1)), m.group(3).strip()))
        elif segs and ln.strip():
            segs[-1] = (segs[-1][0], (segs[-1][1] + " " + ln.strip()).strip())
    paras: List[str] = []
    cur_start: Optional[float] = None
    cur_text: List[str] = []
    prev_end = 0.0
    for start, body in segs:
        if cur_start is None or start - prev_end > GAP_SEC_PARA:
            if cur_text:
                paras.append(f"[{_mmss(cur_start or 0)}] " + " ".join(cur_text))
            cur_start, cur_text = start, []
        cur_text.append(body)
        prev_end = start
    if cur_text:
        paras.append(f"[{_mmss(cur_start or 0)}] " + " ".join(cur_text))
    clean = "\n\n".join(paras)
    meta = {"format": "podcast", "paragraphs": len(paras),
            "raw_chars": len(text), "clean_chars": len(clean)}
    return clean, meta


def prep_transcript(text: str) -> Tuple[str, Dict[str, Any]]:
    fmt = detect_format(text)
    return clean_podcast(text) if fmt == "podcast" else clean_youtube(text)


def number_tokens(text: str) -> List[str]:
    return [m.group(0).strip() for m in NUM_TOKEN_RE.finditer(text)]


def _num_set(text: str) -> set:
    out = set()
    for m in NUM_TOKEN_RE.finditer(text):
        head = re.match(r"\$?\d[\d,]*(?:\.\d+)?", m.group(0))
        if not head:
            continue
        try:
            v = float(head.group(0).replace(",", "").lstrip("$"))
        except ValueError:
            continue
        if v == int(v) and 0 <= v <= 12:  # 序号/时刻类噪声
            continue
        out.add(v)
    return out


def number_coverage(clean_text: str, structured_text: str) -> float:
    """结构化输出的数字覆盖率(按数值集合;单位后缀/词形数字不参与)。"""
    src = _num_set(clean_text)
    if not src:
        return 1.0
    dst = _num_set(structured_text)
    return len(src & dst) / len(src)


# ── Layer 3: 对账(留档未启用) ─────────────────────────────────────────

def _num(s: Any) -> Optional[float]:
    try:
        return float(str(s).replace(",", "").replace("%", "").replace("$", ""))
    except (TypeError, ValueError):
        return None


def _near(a: float, b: float, tol: float = 0.051) -> bool:
    return abs(a - b) <= max(tol, abs(b) * 0.02)


def build_data_index(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """数据层索引:earnings(ticker→eps est/act)+ 全快照扁平数字集。"""
    src = snapshot.get("sources") or {}
    ec = src.get("earnings_calendar") or {}
    earnings: Dict[str, Dict[str, float]] = {}
    for bucket in ("today", "prior_after_close", "upcoming"):
        for r in ec.get(bucket) or []:
            code = (r.get("code") or "").upper()
            if not code:
                continue
            row = earnings.setdefault(code, {})
            for f in ("eps_estimate", "eps_actual"):
                v = _num(r.get(f))
                if v is not None:
                    row[f] = v
    flat: List[float] = []

    def _walk(o: Any) -> None:
        if isinstance(o, dict):
            for v in o.values():
                _walk(v)
        elif isinstance(o, list):
            for v in o:
                _walk(v)
        elif isinstance(o, bool):
            return
        elif isinstance(o, (int, float)):
            flat.append(float(o))
        elif isinstance(o, str):
            for m in NUM_TOKEN_RE.finditer(o):
                v = _num(m.group(0))
                if v is not None:
                    flat.append(v)

    _walk(snapshot.get("sources") or {})
    return {"earnings": earnings, "flat": flat}


def reconcile_numbers(entries: List[Dict[str, Any]],
                      snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    """逐条标注 matched / consistent / conflict / unknown。"""
    idx = build_data_index(snapshot)
    out: List[Dict[str, Any]] = []
    for e in entries:
        v = _num(e.get("value"))
        ent = dict(e)
        if v is None:
            ent["status"] = "unknown"
            out.append(ent)
            continue
        status = "unknown"
        ticker = (e.get("ticker") or "").upper()
        metric = (e.get("metric") or "").lower()
        row = idx["earnings"].get(ticker)
        if row and metric in ("eps", "eps_estimate", "eps_actual", "earnings per share"):
            ref = [row.get("eps_actual"), row.get("eps_estimate")]
            ref = [r for r in ref if r is not None]
            if any(_near(v, r) for r in ref):
                status = "matched"
            else:
                status = "conflict"
        if status == "unknown" and any(_near(v, f) for f in idx["flat"]):
            status = "consistent"
        ent["status"] = status
        out.append(ent)
    return out


def write_ledger(ws: Path, show: str,
                 annotated: List[Dict[str, Any]]) -> Dict[str, Any]:
    conflicts = [e for e in annotated if e.get("status") == "conflict"]
    payload = {"entries": annotated,
               "conflicts": conflicts,
               "counts": {s: sum(1 for e in annotated if e.get("status") == s)
                          for s in ("matched", "consistent", "conflict", "unknown")}}
    path = ws / "transcripts" / f"{show}.ledger.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    return payload


# ── 快照注入:节目转录直查 + 清洗内联(无摘要依赖) ───────────────────────

def _clean_and_write(ws: Path, name: str, raw: str,
                     src: Dict[str, Any], field: str = "clean_transcript") -> None:
    clean, meta = prep_transcript(raw)
    path = ws / "transcripts" / f"{name}.clean.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(clean, encoding="utf-8")
    src[field] = clean
    src[f"{field}_file"] = {"file": f"transcripts/{name}.clean.txt", **meta}


def _pub_iso(v: Any) -> Optional[str]:
    return v.isoformat() if hasattr(v, "isoformat") else str(v or "")


def prep_workspace(ws: Path) -> int:
    """按 snapshot.report_date 直查两档节目转录,清洗后内联进 sources。

    写入形状(无摘要字段):
      sources.the_close = {video_id, video_title, published_utc,
                           session_date, clean_transcript, transcript}
      sources.cnbc_closing_bell = {session_date, main{…}, overtime{…}|null,
                                   clean_transcript(主段), clean_transcript_ot(OT)}
    缺源记 snapshot.errors,不阻塞其余源。
    """
    snap_path = ws / "snapshot.json"
    snap = json.loads(snap_path.read_text(encoding="utf-8"))
    t = date.fromisoformat(snap["report_date"])
    conn = _conn()
    try:
        # ── The Close ──
        try:
            tc = fetch_the_close(conn, t)
        except Exception as e:  # noqa: BLE001
            tc = None
            snap.setdefault("errors", {})["the_close"] = f"fetch failed: {e}"
        if tc:
            src = {"video_id": tc["content_id"], "video_title": tc["title"],
                   "published_utc": _pub_iso(tc["published_at"]),
                   "session_date": t.isoformat()}
            _clean_and_write(ws, "the_close", tc["transcript"], src)
            src["transcript"] = src.pop("clean_transcript_file")
            snap["sources"]["the_close"] = src
            # 就绪门重试成功时清掉此前留下的缺源错误(show_gate 轮询依赖)
            snap.setdefault("errors", {}).pop("the_close", None)
            meta = src["transcript"]
            print(f"[the_close] {meta['format']}: raw {meta['raw_chars']} -> "
                  f"clean {meta['clean_chars']} chars "
                  f"({meta.get('turns', meta.get('paragraphs', 0))} units)")
        elif "the_close" not in (snap.get("errors") or {}):
            snap.setdefault("errors", {})["the_close"] = (
                f"no Bloomberg Television 'The Close' transcript self-identified "
                f"in {t.isoformat()} evening window")

        # ── Closing Bell(主段 + Overtime)──
        try:
            cb = fetch_closing_bell(conn, t)
        except Exception as e:  # noqa: BLE001
            cb = {"main": None, "overtime": None}
            snap.setdefault("errors", {})["cnbc_closing_bell"] = f"fetch failed: {e}"
        if cb["main"] or cb["overtime"]:
            src: Dict[str, Any] = {"session_date": t.isoformat()}
            for slot in ("main", "overtime"):
                r = cb[slot]
                if not r:
                    src[slot] = None
                    continue
                _clean_and_write(
                    ws, f"cnbc_closing_bell_{slot}" if slot == "main"
                    else "cnbc_closing_bell_ot",
                    r["transcript"], src,
                    field="clean_transcript" if slot == "main"
                    else "clean_transcript_ot")
                # 同上:重试补回后清陈旧错误键
                snap.setdefault("errors", {}).pop(
                    "cnbc_closing_bell" if slot == "main"
                    else "cnbc_closing_bell.overtime", None)
                src[slot] = {"video_id": r["content_id"],
                             "video_title": r["title"],
                             "episode_url": r.get("episode_url"),
                             "podcast_feed": r.get("feed_url") or CB_FEED_FALLBACK,
                             "published_utc": _pub_iso(r["published_at"]),
                             "transcript": src.pop(
                                 "clean_transcript_file" if slot == "main"
                                 else "clean_transcript_ot_file")}
            snap["sources"]["cnbc_closing_bell"] = src
            for slot, fld in (("main", "clean_transcript"),
                              ("overtime", "clean_transcript_ot")):
                meta = (src.get(slot) or {}).get("transcript")
                if meta:
                    print(f"[cnbc_closing_bell.{slot}] {meta['format']}: raw "
                          f"{meta['raw_chars']} -> clean {meta['clean_chars']} chars "
                          f"({meta.get('turns', meta.get('paragraphs', 0))} units)")
            if not cb["overtime"]:
                snap.setdefault("errors", {})["cnbc_closing_bell.overtime"] = (
                    "no Overtime-hour podcast row matched (title date suffix)")
        elif "cnbc_closing_bell" not in (snap.get("errors") or {}):
            snap.setdefault("errors", {})["cnbc_closing_bell"] = (
                f"no Closing Bell podcast rows with {t.strftime('%m/%d/%y')} "
                f"title date in evening window")
    finally:
        conn.close()
    snap_path.write_text(json.dumps(snap, ensure_ascii=False), encoding="utf-8")
    print(f"snapshot updated: {snap_path}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ws", required=True, help="pipeline 工作区目录(含 snapshot.json)")
    ap.add_argument("--show", action="append", choices=SHOWS,
                    help="兼容参数:已无按 show 过滤的必要,两档都处理")
    args = ap.parse_args()
    return prep_workspace(Path(args.ws).resolve())


if __name__ == "__main__":
    raise SystemExit(main())
