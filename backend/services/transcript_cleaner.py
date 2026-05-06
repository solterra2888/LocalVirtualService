# -*- coding: utf-8 -*-
"""
Transcript 清洗服务

对 subscription_content 表中 channel_type='youtube' 的 transcript 字段做四阶段清洗，
将结果写入 transcript_cleaned 字段，原始数据不受影响。

四个阶段：
  Phase 0  格式检测   — 识别文本属于哪种抓取来源格式
  Phase 1  字符规范化 — 统一编码、去除时间戳前缀、清理标记
  Phase 2  断句重组   — 把人工断行拼回完整句子
  Phase 3  段落成形   — 把句子流分割成可读段落
"""

from __future__ import annotations

import re
import unicodedata
from enum import Enum
from typing import List, Optional


# ──────────────────────────────────────────────────────────────────────────────
# 公共常量
# ──────────────────────────────────────────────────────────────────────────────

# 匹配 "[0.12s - 4.60s]" 或 "[0.12s-4.60s]" 开头的时间戳前缀（Type A）
_RE_TS_PREFIX = re.compile(r"^\[\d+(?:\.\d+)?s\s*-\s*\d+(?:\.\d+)?s\]\s*", re.IGNORECASE)

# 非语音标记（大小写不敏感）：[music] [applause] [laughter] [silence] …
_RE_NON_SPEECH = re.compile(r"\[(?:music|applause|laughter|cheering|silence|pause|noise)\]", re.IGNORECASE)

# 中文句末标点（判断是否为句子结束）
_CJK_SENTENCE_END = frozenset("。！？…")

# 英文句末标点
_EN_SENTENCE_END = frozenset(".!?")

# 英文话题过渡词（遇到这些词开头时强制新段落）
_TOPIC_TRANSITION_WORDS = re.compile(
    r"^(?:So[,\s]|Now[,\s]|But[,\s]|And[,\s]|Well[,\s]|However[,\s]|Meanwhile[,\s]|"
    r"In\s+fact[,\s]|By\s+the\s+way[,\s]|Moving\s+on[,\s]|Let(?:'s|us)\s+talk)",
    re.IGNORECASE,
)

# 判断字符串是否含有 CJK 字符
_RE_CJK = re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf\u20000-\u2a6df]")

# 目标段落句子数范围（规则分段时每段约 4-6 句）
_PARA_MIN_SENTENCES = 3
_PARA_MAX_SENTENCES = 6


# ──────────────────────────────────────────────────────────────────────────────
# Phase 0：格式检测
# ──────────────────────────────────────────────────────────────────────────────

class TranscriptFormat(str, Enum):
    """Transcript 格式类型"""
    TIMESTAMP = "timestamp"   # Type A: [0.12s - 4.60s] 前缀
    SPEAKER   = "speaker"     # Type B: >> 说话人提示
    CHINESE   = "chinese"     # Type D: 中文解说
    PLAIN     = "plain"       # Type C: 英文纯旁白


def detect_format(text: str) -> TranscriptFormat:
    """
    Phase 0: 检测 transcript 的格式类型。
    优先级：timestamp > speaker > chinese > plain
    """
    if not text or not text.strip():
        return TranscriptFormat.PLAIN

    first_line = text.lstrip()
    if _RE_TS_PREFIX.match(first_line):
        return TranscriptFormat.TIMESTAMP

    if ">> " in text:
        return TranscriptFormat.SPEAKER

    if _RE_CJK.search(text):
        return TranscriptFormat.CHINESE

    return TranscriptFormat.PLAIN


# ──────────────────────────────────────────────────────────────────────────────
# Phase 1：字符级规范化
# ──────────────────────────────────────────────────────────────────────────────

def _normalize_chars(text: str) -> str:
    """
    统一换行符、去除行首尾空白、合并连续空行（保留最多一个空行）。
    不改变语义，只做字符级清洁。
    """
    # CRLF / CR → LF
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # 逐行去首尾空白
    lines = [line.strip() for line in text.split("\n")]

    # 合并连续空行为单个空行（最多保留一个）
    cleaned: List[str] = []
    prev_empty = False
    for line in lines:
        if line == "":
            if not prev_empty:
                cleaned.append("")
            prev_empty = True
        else:
            cleaned.append(line)
            prev_empty = False

    return "\n".join(cleaned).strip()


def _strip_timestamp_prefix(text: str) -> str:
    """Type A 专用：剥离每行开头的 [Xs - Ys] 时间戳前缀。"""
    result_lines = []
    for line in text.split("\n"):
        result_lines.append(_RE_TS_PREFIX.sub("", line))
    return "\n".join(result_lines)


def _handle_non_speech_tags(text: str, keep: bool = False) -> str:
    """
    处理 [music] [applause] 等非语音标记。
    keep=True 时保留并替换为更简洁形式；keep=False 时直接删除。
    默认删除，保持文本干净。
    """
    if keep:
        return text
    return _RE_NON_SPEECH.sub("", text)


def phase1_normalize(text: str, fmt: TranscriptFormat) -> str:
    """
    Phase 1：字符级规范化。
    返回逐行整理后的文本（每行仍为短片段，不做合并）。
    """
    text = _normalize_chars(text)

    if fmt == TranscriptFormat.TIMESTAMP:
        text = _strip_timestamp_prefix(text)

    text = _handle_non_speech_tags(text, keep=False)

    # 再次规范化（清理因删除标签产生的多余空行 / 空格）
    text = _normalize_chars(text)

    return text


# ──────────────────────────────────────────────────────────────────────────────
# Phase 2：断句重组
# ──────────────────────────────────────────────────────────────────────────────

def _is_cjk_line(line: str) -> bool:
    """判断该行是否主要为中文（CJK 比例 > 40%）"""
    if not line:
        return False
    cjk_count = sum(1 for ch in line if _RE_CJK.match(ch))
    return cjk_count / len(line) > 0.4


def _ends_sentence_en(line: str) -> bool:
    """英文行是否以句末标点结尾（忽略引号、括号等后缀）"""
    stripped = line.rstrip('"\')]')
    return bool(stripped) and stripped[-1] in _EN_SENTENCE_END


def _starts_new_sentence_en(line: str) -> bool:
    """英文行是否以大写字母开头（排除全大写的缩写，如 IMF、GDP）"""
    if not line:
        return False
    first = line[0]
    if not first.isupper():
        return False
    # 如果前几个字符全是大写且长度 <= 5，认为是缩写，不算句子开头
    word = re.match(r"^[A-Z]+", line)
    if word and len(word.group()) <= 5 and (len(line) == len(word.group()) or line[len(word.group())] in " ,"):
        # 保守处理：缩写词当作续行
        pass
    return True


def _ends_sentence_cjk(line: str) -> bool:
    """中文行是否以句末标点结尾"""
    stripped = line.rstrip()
    return bool(stripped) and stripped[-1] in _CJK_SENTENCE_END


def _merge_lines_en(lines: List[str]) -> List[str]:
    """
    英文断句合并：把被人工截断的短行拼接成完整句子。
    规则：
    - 上行不以句末标点结尾 → 下行与上行拼接（中间加空格）
    - 上行以句末标点结尾   → 下行是新句子（保留换行）
    """
    if not lines:
        return lines

    merged: List[str] = []
    buffer = lines[0]

    for line in lines[1:]:
        if not line:
            # 空行 = 强制段落边界，flush buffer
            if buffer:
                merged.append(buffer)
                buffer = ""
            merged.append("")
            continue

        if not buffer:
            buffer = line
            continue

        if _ends_sentence_en(buffer):
            # 当前 buffer 是完整句子，下一行开新句
            merged.append(buffer)
            buffer = line
        else:
            # 未结束，拼接
            buffer = buffer + " " + line

    if buffer:
        merged.append(buffer)

    return merged


def _merge_lines_cjk(lines: List[str]) -> List[str]:
    """
    中文断句合并：把逐行短句拼成完整中文句子。
    规则：
    - 上行不以中文句末标点结尾 → 与下行拼接（不加空格）
    - 上行以中文句末标点结尾   → 下行是新句子
    """
    if not lines:
        return lines

    merged: List[str] = []
    buffer = lines[0]

    for line in lines[1:]:
        if not line:
            if buffer:
                merged.append(buffer)
                buffer = ""
            merged.append("")
            continue

        if not buffer:
            buffer = line
            continue

        if _ends_sentence_cjk(buffer):
            merged.append(buffer)
            buffer = line
        else:
            # 中文拼接不加空格
            buffer = buffer + line

    if buffer:
        merged.append(buffer)

    return merged


def _merge_speaker_blocks(text: str) -> str:
    """
    Type B 专用：在 >> 分隔的每个说话人轮次内部做英文合并，不跨轮次合并。
    """
    # 拆成以 >> 开头的 block，每个 block 单独处理
    blocks = re.split(r"(?=^>> )", text, flags=re.MULTILINE)
    result_blocks: List[str] = []

    for block in blocks:
        if not block.strip():
            continue
        lines = block.split("\n")
        # 第一行是 ">> Some text"，其余行是续行
        if lines[0].startswith(">> "):
            speaker_prefix = ">> "
            first_content = lines[0][3:].strip()
            rest_lines = [l.strip() for l in lines[1:] if l.strip()]
            all_content = [first_content] + rest_lines if first_content else rest_lines
            merged = _merge_lines_en(all_content)
            block_text = speaker_prefix + ("\n".join(merged))
        else:
            merged = _merge_lines_en([l for l in lines if l.strip()])
            block_text = "\n".join(merged)
        result_blocks.append(block_text)

    return "\n".join(result_blocks)


def phase2_merge_sentences(text: str, fmt: TranscriptFormat) -> str:
    """
    Phase 2：将人工断行的短片段拼接成完整句子。
    """
    if fmt == TranscriptFormat.SPEAKER:
        return _merge_speaker_blocks(text)

    lines = text.split("\n")

    if fmt == TranscriptFormat.CHINESE:
        merged = _merge_lines_cjk(lines)
    else:
        # PLAIN 或 TIMESTAMP（已在 Phase 1 去掉前缀）
        merged = _merge_lines_en(lines)

    return "\n".join(merged)


# ──────────────────────────────────────────────────────────────────────────────
# Phase 3：段落成形
# ──────────────────────────────────────────────────────────────────────────────

def _split_into_sentences_en(text: str) -> List[str]:
    """简单英文句子分割（不引入 nltk 等外部依赖）"""
    # 按 .!? 分割，保留分隔符
    raw = re.split(r"(?<=[.!?])\s+(?=[A-Z])", text)
    return [s.strip() for s in raw if s.strip()]


def _split_into_sentences_cjk(text: str) -> List[str]:
    """中文句子分割（按句末标点分割）"""
    raw = re.split(r"(?<=[。！？…])", text)
    return [s.strip() for s in raw if s.strip()]


def _group_sentences_into_paragraphs(
    sentences: List[str],
    is_cjk: bool,
    min_s: int = _PARA_MIN_SENTENCES,
    max_s: int = _PARA_MAX_SENTENCES,
) -> List[str]:
    """
    将句子列表分组成段落。
    - 遇到话题过渡词（英文）时强制断段
    - 每段句子数在 [min_s, max_s] 之间
    """
    paragraphs: List[str] = []
    current: List[str] = []

    for sent in sentences:
        force_break = (
            not is_cjk
            and bool(_TOPIC_TRANSITION_WORDS.match(sent))
            and len(current) >= min_s
        )
        if force_break or len(current) >= max_s:
            if current:
                sep = "" if is_cjk else " "
                paragraphs.append(sep.join(current))
            current = [sent]
        else:
            current.append(sent)

    if current:
        sep = "" if is_cjk else " "
        paragraphs.append(sep.join(current))

    return paragraphs


def _form_paragraphs_speaker(text: str) -> str:
    """
    Type B 专用：每个 >> 轮次独立成段（已经是自然段落边界）。
    """
    blocks = [b.strip() for b in re.split(r"\n(?=>> )", text) if b.strip()]
    return "\n\n".join(blocks)


def _form_paragraphs_generic(text: str, is_cjk: bool) -> str:
    """
    Type A / C / D 通用段落成形。
    先把合并后的文本分成句子，再分组成段落。
    """
    # 已有空行的地方直接作为段落边界
    rough_paras = [p.strip() for p in text.split("\n\n") if p.strip()]
    final_paras: List[str] = []

    for para in rough_paras:
        if is_cjk:
            sentences = _split_into_sentences_cjk(para)
        else:
            sentences = _split_into_sentences_en(para)

        if not sentences:
            final_paras.append(para)
            continue

        # 若段落已足够短（句子数 <= max），不再细分
        if len(sentences) <= _PARA_MAX_SENTENCES:
            final_paras.append(para)
        else:
            grouped = _group_sentences_into_paragraphs(sentences, is_cjk)
            final_paras.extend(grouped)

    return "\n\n".join(final_paras)


def phase3_form_paragraphs(text: str, fmt: TranscriptFormat) -> str:
    """
    Phase 3：将句子流分割成可读段落。
    """
    if fmt == TranscriptFormat.SPEAKER:
        return _form_paragraphs_speaker(text)

    is_cjk = fmt == TranscriptFormat.CHINESE
    return _form_paragraphs_generic(text, is_cjk)


# ──────────────────────────────────────────────────────────────────────────────
# 主入口
# ──────────────────────────────────────────────────────────────────────────────

def clean_transcript(raw: Optional[str]) -> Optional[str]:
    """
    对单条 transcript 执行完整四阶段清洗，返回 transcript_cleaned。

    Args:
        raw: 原始 transcript 文本（可能为 None 或空字符串）

    Returns:
        清洗后的文本，或 None（若原始为空）
    """
    if not raw or not raw.strip():
        return None

    # Phase 0 — 格式检测
    fmt = detect_format(raw)

    # Phase 1 — 字符规范化
    text = phase1_normalize(raw, fmt)

    if not text:
        return None

    # Phase 2 — 断句重组
    text = phase2_merge_sentences(text, fmt)

    # Phase 3 — 段落成形
    text = phase3_form_paragraphs(text, fmt)

    return text.strip() or None


def clean_transcript_with_meta(raw: Optional[str]) -> dict:
    """
    同 clean_transcript，但同时返回调试元数据（格式类型、字符变化量）。
    可在批量处理时用于监控效果。

    Returns:
        {
            "cleaned": str | None,
            "format": str,           # 检测到的格式
            "original_len": int,
            "cleaned_len": int,
        }
    """
    original_len = len(raw) if raw else 0
    fmt = detect_format(raw) if raw else TranscriptFormat.PLAIN
    cleaned = clean_transcript(raw)
    cleaned_len = len(cleaned) if cleaned else 0

    return {
        "cleaned": cleaned,
        "format": fmt.value,
        "original_len": original_len,
        "cleaned_len": cleaned_len,
    }
