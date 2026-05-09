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

已修复问题：
  [BUG-1]  _RE_CJK 正则 \u20000-\u2a6df 在 Python narrow build 下被解析为
           \u2000 (EN QUAD) + 字面字符 '0'，导致几乎所有 ASCII 字符被误判为 CJK，
           英文 transcript 全部走中文管道 → 行间连接不加空格 → 单词粘连。
           修复：改用 re.UNICODE 配合正确的 SMP 范围写法，或直接改用 unicodedata 逐字检测。

  [BUG-2]  英文缩写词结尾的 . (p.m. / Mr. / U.S.) 被误判为句末标点，
           导致 "3 p." 之后的行被切断成新句子。
           修复：增加缩写词白名单，带缩写结尾的行不视为句末。

  [FEAT-1] 无标点英文行（如 Morgan Stanley、Bloomberg 播客类型）
           原有规则完全无法切割句子边界，导致整段被合并成一行。
           修复：检测「低标点密度」文本，使用 spaCy en_core_web_sm senter 组件
           替代规则进行句子边界判断；spaCy 不可用时退化为字数分段兜底。
"""

from __future__ import annotations

import re
import logging
from enum import Enum
from functools import lru_cache
from typing import List, Optional

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# spaCy 懒加载（仅在需要时导入，避免启动开销；不可用时降级）
# ──────────────────────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _get_spacy_nlp():
    """
    懒加载 spaCy en_core_web_sm，仅保留 tok2vec + parser 管道。

    en_core_web_sm v3.8 已不再包含独立的 senter 组件，句子边界由 parser 负责。
    禁用 tagger / attribute_ruler / lemmatizer / ner 以降低处理开销。
    返回 nlp 对象，或 None（若 spaCy / 模型未安装）。
    """
    try:
        import spacy  # type: ignore[import-untyped]
        nlp = spacy.load(
            "en_core_web_sm",
            exclude=["tagger", "attribute_ruler", "lemmatizer", "ner"],
        )
        logger.debug("spaCy en_core_web_sm loaded. pipes=%s", nlp.pipe_names)
        return nlp
    except Exception as exc:
        logger.warning("spaCy not available, will use fallback segmentation: %s", exc)
        return None


# ──────────────────────────────────────────────────────────────────────────────
# 公共常量
# ──────────────────────────────────────────────────────────────────────────────

# 匹配 "[0.12s - 4.60s]" 或 "[0.12s-4.60s]" 开头的时间戳前缀（Type A）
_RE_TS_PREFIX = re.compile(r"^\[\d+(?:\.\d+)?s\s*-\s*\d+(?:\.\d+)?s\]\s*", re.IGNORECASE)

# 非语音标记（大小写不敏感）：[music] [applause] [laughter] [silence] …
_RE_NON_SPEECH = re.compile(
    r"\[(?:music|applause|laughter|cheering|silence|pause|noise)\]",
    re.IGNORECASE,
)

# 中文句末标点
_CJK_SENTENCE_END = frozenset("。！？…")

# 英文句末标点
_EN_SENTENCE_END = frozenset(".!?")

# 英文常见缩写词（结尾带 . 但不是句末）—— 仅匹配行尾
_RE_ABBREVIATION_TAIL = re.compile(
    r"""(?:
        \b(?:Mr|Mrs|Ms|Dr|Prof|Sr|Jr|vs|etc|approx|est|dept|govt|corp|inc|ltd|co)\.  # 头衔 / 通用
      | \b[A-Z](?:\.[A-Z])+\.       # 首字母缩写，如 U.S. / U.K. / E.U.
      | \b(?:Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.  # 月份缩写
      | \b(?:a\.m|p\.m|A\.M|P\.M)\.  # 时刻 a.m. / p.m.
      | \b\d+\.                       # 列表序号 1. 2. 3.
    )$""",
    re.VERBOSE,
)

# 英文话题过渡词（在段落分组时强制断段）
_TOPIC_TRANSITION_WORDS = re.compile(
    r"^(?:So[,\s]|Now[,\s]|But[,\s]|And[,\s]|Well[,\s]|However[,\s]|Meanwhile[,\s]|"
    r"In\s+fact[,\s]|By\s+the\s+way[,\s]|Moving\s+on[,\s]|Let(?:'s|us)\s+talk)",
    re.IGNORECASE,
)

# ── CJK 判断（BUG-1 修复）────────────────────────────────────────────────────
# 改用 unicodedata.name() 不靠谱；改用精确 Unicode 区间，并用 re.compile() 的
# re.UNICODE 标志（Python 3 默认）+ 正确区间字面量。
# SMP 汉字（CJK Ext-B）范围 U+20000-U+2A6DF 需用 "\U00020000-\U0002A6DF"（8位转义）。
_RE_CJK = re.compile(
    "[\u4e00-\u9fff"      # CJK 基本区
    "\u3400-\u4dbf"       # CJK 扩展 A
    "\U00020000-\U0002a6df"  # CJK 扩展 B（SMP，必须用 \U 8位写法）
    "\u3000-\u303f"       # CJK 符号和标点
    "\uff00-\uffef"       # 全角字符
    "]"
)

# 低标点密度阈值：行末含 .!? 的行占比低于此值时，启用 spaCy 分段
_LOW_PUNCT_RATIO_THRESHOLD = 0.12

# 目标段落句子数范围（规则分段时每段约 4-6 句）
_PARA_MIN_SENTENCES = 3
_PARA_MAX_SENTENCES = 6

# 无标点兜底：每隔 N 个单词强制新段（spaCy 不可用时）
_FALLBACK_WORDS_PER_PARA = 120


# ──────────────────────────────────────────────────────────────────────────────
# Phase 0：格式检测
# ──────────────────────────────────────────────────────────────────────────────

class TranscriptFormat(str, Enum):
    """Transcript 格式类型"""
    TIMESTAMP = "timestamp"   # Type A: [0.12s - 4.60s] 前缀
    SPEAKER   = "speaker"     # Type B: >> 说话人提示
    CHINESE   = "chinese"     # Type D: 中文解说（CJK 比例 > 30%）
    PLAIN     = "plain"       # Type C: 英文纯旁白


def _cjk_ratio(text: str) -> float:
    """计算文本中 CJK 字符的占比（按非空白字符统计）"""
    non_space = [c for c in text if not c.isspace()]
    if not non_space:
        return 0.0
    return sum(1 for c in non_space if _RE_CJK.match(c)) / len(non_space)


def detect_format(text: str) -> TranscriptFormat:
    """
    Phase 0: 检测 transcript 的格式类型。
    优先级：timestamp > speaker > chinese（CJK 占比 > 30%）> plain

    注：原来用 _RE_CJK.search() 存在误判风险（BUG-1）；
    改为统计 CJK 字符占比，阈值 30% 更稳健。
    """
    if not text or not text.strip():
        return TranscriptFormat.PLAIN

    first_line = text.lstrip()
    if _RE_TS_PREFIX.match(first_line):
        return TranscriptFormat.TIMESTAMP

    if ">> " in text:
        return TranscriptFormat.SPEAKER

    if _cjk_ratio(text) > 0.30:
        return TranscriptFormat.CHINESE

    return TranscriptFormat.PLAIN


# ──────────────────────────────────────────────────────────────────────────────
# Phase 1：字符级规范化
# ──────────────────────────────────────────────────────────────────────────────

def _normalize_chars(text: str) -> str:
    """统一换行符、去除行首尾空白、合并连续空行（保留最多一个）。"""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.strip() for line in text.split("\n")]
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
    return "\n".join(_RE_TS_PREFIX.sub("", line) for line in text.split("\n"))


def _handle_non_speech_tags(text: str) -> str:
    """删除 [music] [applause] 等非语音标记，清理多余空格。"""
    # 删除标记，避免留下多余空格
    text = _RE_NON_SPEECH.sub(" ", text)
    # 压缩行内连续空格
    text = re.sub(r"  +", " ", text)
    return text


def phase1_normalize(text: str, fmt: TranscriptFormat) -> str:
    """Phase 1：字符级规范化，返回逐行整理后的文本（不做合并）。"""
    text = _normalize_chars(text)
    if fmt == TranscriptFormat.TIMESTAMP:
        text = _strip_timestamp_prefix(text)
    text = _handle_non_speech_tags(text)
    text = _normalize_chars(text)
    return text


# ──────────────────────────────────────────────────────────────────────────────
# Phase 2：断句重组
# ──────────────────────────────────────────────────────────────────────────────

def _ends_sentence_en(line: str) -> bool:
    """
    英文行是否为句子结尾。
    修复 BUG-2：缩写词（p.m. / Mr. / U.S.）末尾的 . 不算句末。
    """
    stripped = line.rstrip('"\')]').rstrip()
    if not stripped:
        return False
    if stripped[-1] not in _EN_SENTENCE_END:
        return False
    # 排除缩写词
    if _RE_ABBREVIATION_TAIL.search(stripped):
        return False
    return True


def _ends_sentence_cjk(line: str) -> bool:
    """中文行是否以句末标点结尾。"""
    stripped = line.rstrip()
    return bool(stripped) and stripped[-1] in _CJK_SENTENCE_END


def _merge_lines_en(lines: List[str]) -> List[str]:
    """
    英文断句合并：把被人工截断的短行拼接成完整句子。
    - 上行以句末标点结尾 → 下行开新句
    - 上行未结束         → 下行拼接（加一个空格）
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

        if _ends_sentence_en(buffer):
            merged.append(buffer)
            buffer = line
        else:
            buffer = buffer + " " + line

    if buffer:
        merged.append(buffer)

    return merged


def _merge_lines_cjk(lines: List[str]) -> List[str]:
    """
    中文断句合并：把逐行短片段拼成完整中文句子（无空格拼接）。
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
            buffer = buffer + line

    if buffer:
        merged.append(buffer)

    return merged


def _merge_speaker_blocks(text: str) -> str:
    """
    Type B 专用：在 >> 分隔的每个说话人轮次内部做英文合并，不跨轮次合并。
    """
    blocks = re.split(r"(?=^>> )", text, flags=re.MULTILINE)
    result_blocks: List[str] = []

    for block in blocks:
        if not block.strip():
            continue
        lines = block.split("\n")
        if lines[0].startswith(">> "):
            first_content = lines[0][3:].strip()
            rest_lines = [l.strip() for l in lines[1:] if l.strip()]
            all_content = ([first_content] + rest_lines) if first_content else rest_lines
            merged = _merge_lines_en(all_content)
            block_text = ">> " + "\n".join(merged)
        else:
            merged = _merge_lines_en([l for l in lines if l.strip()])
            block_text = "\n".join(merged)
        result_blocks.append(block_text)

    return "\n".join(result_blocks)


def phase2_merge_sentences(text: str, fmt: TranscriptFormat) -> str:
    """Phase 2：将人工断行的短片段拼接成完整句子。"""
    if fmt == TranscriptFormat.SPEAKER:
        return _merge_speaker_blocks(text)

    lines = text.split("\n")

    if fmt == TranscriptFormat.CHINESE:
        merged = _merge_lines_cjk(lines)
    else:
        merged = _merge_lines_en(lines)

    return "\n".join(merged)


# ──────────────────────────────────────────────────────────────────────────────
# Phase 3：段落成形
# ──────────────────────────────────────────────────────────────────────────────

def _punct_sentence_end_ratio(lines: List[str]) -> float:
    """
    计算行列表中「以 .!? 结尾的行」占非空行的比例。
    用于判断是否属于「低标点密度」文本。
    """
    non_empty = [l for l in lines if l.strip()]
    if not non_empty:
        return 1.0
    has_end = sum(1 for l in non_empty if _ends_sentence_en(l))
    return has_end / len(non_empty)


def _split_sentences_spacy(text: str) -> Optional[List[str]]:
    """
    用 spaCy parser 切分英文句子。
    返回句子列表，或 None（spaCy 不可用 / 完全无法切分时）。

    当 spaCy 只返回 1 句且文本 > 60 词时，说明无标点信号，返回 None 让调用方退化。
    """
    nlp = _get_spacy_nlp()
    if nlp is None:
        return None

    doc = nlp(text)
    sentences = [sent.text.strip() for sent in doc.sents if sent.text.strip()]

    if len(sentences) <= 1 and len(text.split()) > 60:
        return None  # 无法检测到有效边界

    return sentences if sentences else None


def _split_sentences_fallback(text: str) -> List[str]:
    """
    按词数强制分段（无法检测句子边界时的最终兜底）。
    每段约 _FALLBACK_WORDS_PER_PARA 个词，每段独立成段落。
    """
    words = text.split()
    if not words:
        return [text]
    chunks: List[str] = []
    for i in range(0, len(words), _FALLBACK_WORDS_PER_PARA):
        chunks.append(" ".join(words[i: i + _FALLBACK_WORDS_PER_PARA]))
    return chunks


def _split_into_sentences_en(text: str) -> List[str]:
    """英文句子分割（基于标点的快速版，用于正常标点文本）。"""
    raw = re.split(r"(?<=[.!?])\s+(?=[A-Z\"\'])", text)
    return [s.strip() for s in raw if s.strip()]


def _split_into_sentences_cjk(text: str) -> List[str]:
    """中文句子分割（按句末标点）。"""
    raw = re.split(r"(?<=[。！？…])", text)
    return [s.strip() for s in raw if s.strip()]


def _group_sentences_into_paragraphs(
    sentences: List[str],
    is_cjk: bool,
    min_s: int = _PARA_MIN_SENTENCES,
    max_s: int = _PARA_MAX_SENTENCES,
) -> List[str]:
    """将句子列表分组成段落，遇过渡词强制断段，每段句子数 [min_s, max_s]。"""
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
    """Type B 专用：每个 >> 轮次独立成段。"""
    blocks = [b.strip() for b in re.split(r"\n(?=>> )", text) if b.strip()]
    return "\n\n".join(blocks)


def _form_paragraphs_generic(text: str, fmt: TranscriptFormat) -> str:
    """
    Type A / C / D 通用段落成形。

    对于低标点密度的英文文本（FEAT-1）：
      - 先用 spaCy 切出句子，再分组成段落。
    对于正常标点的英文及中文：
      - 沿用原有规则切句 + 分组。
    """
    is_cjk = (fmt == TranscriptFormat.CHINESE)
    rough_paras = [p.strip() for p in text.split("\n\n") if p.strip()]
    final_paras: List[str] = []

    for para in rough_paras:
        if is_cjk:
            sentences = _split_into_sentences_cjk(para)
            cjk_char_count = sum(1 for c in para if _RE_CJK.match(c))

            # 无句末标点 或 整段只有 1 句但字数很多 → 按字数分段
            if len(sentences) <= 1 and cjk_char_count > 80:
                chunk_size = 100  # 每段约 100 个 CJK 字符
                chars = list(para)
                chunks: List[str] = []
                buf: List[str] = []
                cjk_in_buf = 0
                for ch in chars:
                    buf.append(ch)
                    if _RE_CJK.match(ch):
                        cjk_in_buf += 1
                    if cjk_in_buf >= chunk_size:
                        chunks.append("".join(buf).strip())
                        buf = []
                        cjk_in_buf = 0
                if buf:
                    chunks.append("".join(buf).strip())
                final_paras.extend(c for c in chunks if c)
            elif len(sentences) <= _PARA_MAX_SENTENCES:
                final_paras.append(para)
            else:
                final_paras.extend(_group_sentences_into_paragraphs(sentences, is_cjk=True))
        else:
            # 检查该段是否为低标点密度
            lines_in_para = [l for l in para.split("\n") if l.strip()]
            ratio = _punct_sentence_end_ratio(lines_in_para)

            if ratio < _LOW_PUNCT_RATIO_THRESHOLD:
                # 低标点密度：先尝试 spaCy，失败则词数分段
                spacy_result = _split_sentences_spacy(para)
                if spacy_result is not None:
                    # spaCy 成功切出多句 → 分组成段落
                    if len(spacy_result) <= _PARA_MAX_SENTENCES:
                        final_paras.append(" ".join(spacy_result))
                    else:
                        final_paras.extend(
                            _group_sentences_into_paragraphs(spacy_result, is_cjk=False)
                        )
                else:
                    # spaCy 无效 → 词数分段，每个 chunk 独立成一段
                    final_paras.extend(_split_sentences_fallback(para))
                continue

            sentences = _split_into_sentences_en(para)
            if not sentences:
                final_paras.append(para)
                continue

            if len(sentences) <= _PARA_MAX_SENTENCES:
                final_paras.append(" ".join(sentences))
            else:
                final_paras.extend(_group_sentences_into_paragraphs(sentences, is_cjk=False))

    return "\n\n".join(final_paras)


def phase3_form_paragraphs(text: str, fmt: TranscriptFormat) -> str:
    """Phase 3：将句子流分割成可读段落。"""
    if fmt == TranscriptFormat.SPEAKER:
        return _form_paragraphs_speaker(text)
    return _form_paragraphs_generic(text, fmt)


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

    fmt = detect_format(raw)
    text = phase1_normalize(raw, fmt)
    if not text:
        return None
    text = phase2_merge_sentences(text, fmt)
    text = phase3_form_paragraphs(text, fmt)
    return text.strip() or None


def clean_transcript_with_meta(raw: Optional[str]) -> dict:
    """
    同 clean_transcript，但额外返回调试元数据。

    Returns:
        {
            "cleaned": str | None,
            "format": str,
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
