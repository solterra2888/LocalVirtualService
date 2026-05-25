# -*- coding: utf-8 -*-
"""
文本分块服务。

策略：
- LlamaParse 输出的 Markdown（pdf/word/web/text）：
  优先按 H1/H2/H3 标题切大段，再按 target_chars 长度切小段；标题级联到 chapter。
- ASR 转写 / YouTube 字幕（audio/podcast/youtube/feed_youtube）：
  纯文本按 target_chars 切；当来源是 asr_with_diarization JSON 时调用方应先合并为 text。
- 短内容（feed_twitter）：单条 < target_chars 直接整段返回。

分块参数从环境变量读取，默认 CHUNK_TARGET_CHARS=600 / CHUNK_OVERLAP_CHARS=80。
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence


@dataclass(frozen=True)
class ChunkDraft:
    """分块产物（尚未持久化）。"""

    chunk_idx: int
    chapter: Optional[str]
    page: Optional[int]
    char_start: int
    char_end: int
    text: str

    @property
    def token_count(self) -> int:
        # 粗估：中文 1 token ≈ 1 char，英文 ≈ 0.3-0.5 token/char。
        # 这里用更安全的近似，给 Qwen embedding 容量计算留余量。
        return max(1, int(len(self.text) * 0.7))


# ─── Markdown heading detector ────────────────────────────────────────────
_HEADING_RE = re.compile(r"^(#{1,4})\s+(.+?)\s*$")
# 句末标点（中英），优先按这些切分以避免句中断
_SENTENCE_END = re.compile(r"[。！？!?\.\n](?![\d])")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def sha256_of(text: str) -> str:
    """对内容算 sha256，用于幂等判定。"""
    return hashlib.sha256((text or "").encode("utf-8", errors="replace")).hexdigest()


def chunk_text(
    content: str,
    file_type: str,
    target_chars: Optional[int] = None,
    overlap_chars: Optional[int] = None,
) -> List[ChunkDraft]:
    """根据 file_type 选择合适的策略分块。"""
    if not content or not content.strip():
        return []

    target = target_chars or _env_int("CHUNK_TARGET_CHARS", 600)
    overlap = overlap_chars or _env_int("CHUNK_OVERLAP_CHARS", 80)
    overlap = min(overlap, target // 2)

    file_type = (file_type or "").lower().strip()

    if file_type in {"pdf", "word", "document", "web"}:
        return _chunk_markdown(content, target, overlap)
    # text / 笔记 / ASR / Feed 长文：按长度切
    if file_type in {"text", "audio", "podcast", "youtube", "feed_youtube"}:
        return _chunk_plain(content, target, overlap, base_chapter=None)
    # 推文等短文本：< target 直接整段
    if file_type in {"feed_twitter", "feed_news", "feed_rss", "feed"}:
        if len(content) <= target:
            text = content.strip()
            if not text:
                return []
            return [ChunkDraft(0, None, None, 0, len(content), text)]
        return _chunk_plain(content, target, overlap, base_chapter=None)
    # 兜底：长度切
    return _chunk_plain(content, target, overlap, base_chapter=None)


# ───────────────────────── Markdown 策略 ─────────────────────────────────
def _chunk_markdown(content: str, target: int, overlap: int) -> List[ChunkDraft]:
    """按 Markdown 标题分段，再对每段做长度切分。"""
    sections = _split_by_headings(content)
    if not sections:
        return _chunk_plain(content, target, overlap, base_chapter=None)

    drafts: List[ChunkDraft] = []
    chunk_idx = 0
    for sec_text, chapter, sec_start in sections:
        if not sec_text.strip():
            continue
        for sub in _split_plain(sec_text, target, overlap):
            sub_text, sub_local_start, sub_local_end = sub
            drafts.append(
                ChunkDraft(
                    chunk_idx=chunk_idx,
                    chapter=chapter,
                    page=None,
                    char_start=sec_start + sub_local_start,
                    char_end=sec_start + sub_local_end,
                    text=sub_text,
                )
            )
            chunk_idx += 1
    return drafts


def _split_by_headings(content: str) -> List[tuple]:
    """切分 Markdown 文本为 [(text, chapter_path, char_start), ...]。"""
    lines = content.splitlines(keepends=True)
    # 维护 H1..H4 的当前标题栈
    stack: List[Optional[str]] = [None, None, None, None]

    sections: List[tuple] = []
    buf: List[str] = []
    buf_chapter: Optional[str] = None
    buf_start = 0
    cursor = 0

    def flush(end_pos: int) -> None:
        if buf:
            text = "".join(buf)
            sections.append((text, buf_chapter, buf_start))

    for line in lines:
        m = _HEADING_RE.match(line.strip())
        if m:
            flush(cursor)
            level = len(m.group(1))
            title = m.group(2).strip()
            if 1 <= level <= len(stack):
                stack[level - 1] = title
                for k in range(level, len(stack)):
                    stack[k] = None
            chapter = " / ".join([s for s in stack if s])
            buf = []
            buf_chapter = chapter or None
            buf_start = cursor + len(line)
        else:
            buf.append(line)
        cursor += len(line)
    flush(cursor)

    return sections


# ───────────────────────── 通用长度切分 ─────────────────────────────────
def _chunk_plain(
    content: str,
    target: int,
    overlap: int,
    base_chapter: Optional[str],
) -> List[ChunkDraft]:
    pieces = _split_plain(content, target, overlap)
    return [
        ChunkDraft(
            chunk_idx=i,
            chapter=base_chapter,
            page=None,
            char_start=start,
            char_end=end,
            text=text,
        )
        for i, (text, start, end) in enumerate(pieces)
    ]


def _split_plain(text: str, target: int, overlap: int) -> List[tuple]:
    """把纯文本切成 [(piece_text, char_start, char_end)]。"""
    if not text:
        return []
    if len(text) <= target:
        stripped = text.strip()
        if not stripped:
            return []
        # 保留原 char_start/char_end 的相对位置（相对 text 本身）
        return [(stripped, 0, len(text))]

    out: List[tuple] = []
    n = len(text)
    start = 0
    while start < n:
        end = min(start + target, n)
        # 调整 end 到最近的句末标点，避免截断句子
        if end < n:
            window_start = max(start + int(target * 0.7), start + 1)
            cut = _find_sentence_cut(text, window_start, end)
            if cut > start:
                end = cut
        piece = text[start:end].strip()
        if piece:
            out.append((piece, start, end))
        if end >= n:
            break
        # 下一块的开始位置：留 overlap
        start = max(end - overlap, start + 1)
    return out


def _find_sentence_cut(text: str, lo: int, hi: int) -> int:
    """在 [lo, hi] 范围内找最后一个句末标点位置；找不到返回 hi。"""
    if lo >= hi:
        return hi
    snippet = text[lo:hi]
    last = -1
    for m in _SENTENCE_END.finditer(snippet):
        last = m.end()
    if last == -1:
        return hi
    return lo + last


# ───────────────────────── 公开辅助 ─────────────────────────────────────
def estimate_chunk_count(content_length: int) -> int:
    """根据长度估算 chunk 数（粗略，用于配额检查与日志）。"""
    target = _env_int("CHUNK_TARGET_CHARS", 600)
    overlap = _env_int("CHUNK_OVERLAP_CHARS", 80)
    stride = max(1, target - overlap)
    if content_length <= 0:
        return 0
    return max(1, (content_length + stride - 1) // stride)
