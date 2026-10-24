"""
AI 分析报告文字版 PDF 生成服务
================================

Sources 模块的「Export PDF」原本在前端用 html2canvas 截图生成**图片版** PDF
（文字不可选中、文件大）。本服务在**后端**把 `processing_stats.final_report`
（Markdown）渲染为**文字版** PDF：文字可选中/可搜索/可复制，且通过 PyMuPDF
内置的 CJK 回退字体天然支持中文等非拉丁字符，无需额外字体文件。

实现要点：
- 轻量 Markdown → HTML 转换（无第三方依赖），覆盖 LLM 报告常见语法：
  标题 / 粗斜体 / 行内代码 / 围栏代码块 / 有序无序列表 / 引用 / GFM 表格 /
  链接 / 分割线。
- PyMuPDF Story 引擎负责分页排版（A4、页边距 36pt），输出真实文本对象。
- 生成后 `subset_fonts()` 只内嵌实际用到的字形，并 deflate 压缩：
  典型报告从 ~3.7MB 降到几十 KB。
"""

from __future__ import annotations

import html as html_module
import io
import re
from datetime import datetime, timezone
from typing import Optional

import pymupdf

# A4 尺寸（单位 pt）与页边距，与前端图片版 PDF 的观感保持一致
PAGE_WIDTH_PT = 595
PAGE_HEIGHT_PT = 842
PAGE_MARGIN_PT = 36

# 与前端 buildAiAnalysisPdfFilename 的排版 CSS 保持近似的样式
_REPORT_CSS = """
body { font-family: sans-serif; font-size: 10pt; line-height: 1.55; color: #1a1a1a; }
h1 { font-size: 16pt; color: #111111; margin: 0 0 4pt 0; }
h2 { font-size: 13pt; color: #111111; margin: 14pt 0 6pt 0; }
h3 { font-size: 11.5pt; color: #222222; margin: 12pt 0 5pt 0; }
h4, h5, h6 { font-size: 10.5pt; color: #222222; margin: 10pt 0 4pt 0; }
p  { margin: 4pt 0; }
ul, ol { margin: 4pt 0 6pt 0; }
li { margin: 2pt 0; }
table { border-collapse: collapse; margin: 6pt 0; }
th, td { border: 0.5pt solid #999999; padding: 3pt 5pt; font-size: 9.5pt; }
th { background-color: #f0f0f0; font-weight: bold; }
code { background-color: #f4f4f4; font-family: monospace; font-size: 9pt; }
pre  { background-color: #f4f4f4; padding: 6pt; margin: 6pt 0; }
pre code { background-color: transparent; }
blockquote { color: #555555; margin: 6pt 0 6pt 0; padding-left: 8pt;
             border-left: 2pt solid #bbbbbb; }
a { color: #0645ad; text-decoration: underline; }
hr { border: none; border-top: 0.5pt solid #cccccc; margin: 10pt 0; }
.meta { color: #666666; font-size: 9pt; margin-bottom: 2pt; }
"""


def _sanitize_filename_part(name: str) -> str:
    """去掉文件名中常见操作系统不允许的字符（与前端 sanitizeFilenamePart 对齐）。"""
    return re.sub(r'[/\\:*?"<>|]', '_', name or '').strip() or 'Document'


def build_ai_report_pdf_filename(
    document_name: Optional[str],
    analyzed_at: Optional[datetime | str],
    model_name: Optional[str],
) -> str:
    """
    构建下载文件名：`{Document Name} {YYYY-MM-DD} (Analyzed by {model}) (Text).pdf`
    与前端图片版 buildAiAnalysisPdfFilename 同规则，仅追加 ` (Text)` 以示区分。
    """
    doc = _sanitize_filename_part((document_name or '').strip()) or 'Document'
    if isinstance(analyzed_at, str):
        try:
            analyzed_at = datetime.fromisoformat(analyzed_at.replace('Z', '+00:00'))
        except ValueError:
            analyzed_at = None
    if isinstance(analyzed_at, datetime):
        if analyzed_at.tzinfo is not None:
            analyzed_at = analyzed_at.astimezone(timezone.utc).replace(tzinfo=None)
        date_part = analyzed_at.strftime('%Y-%m-%d')
    else:
        date_part = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    analyzed_by = f' (Analyzed by {model_name.strip()})' if model_name and model_name.strip() else ''
    return f'{doc} {date_part}{analyzed_by} (Text).pdf'


# ---------------------------------------------------------------------------
# Markdown → HTML（轻量实现，专注 LLM 报告常见语法）
# ---------------------------------------------------------------------------

_INLINE_CODE_RE = re.compile(r'`([^`\n]+)`')
_BOLD_RE = re.compile(r'\*\*(.+?)\*\*|__(.+?)__')
_ITALIC_RE = re.compile(r'(?<!\*)\*([^*\n]+)\*(?!\*)|(?<!_)_([^_\n]+)_(?!_)')
_STRIKE_RE = re.compile(r'~~(.+?)~~')
_LINK_RE = re.compile(r'\[([^\]\n]*)\]\(([^)\s]+)\)')
_IMAGE_RE = re.compile(r'!\[([^\]\n]*)\]\(([^)\s]+)\)')


def _inline(md: str) -> str:
    """行内语法转换：先转义 HTML，再依次处理代码/粗体/斜体/删除线/链接/图片。"""
    text = html_module.escape(md, quote=False)

    # 行内代码优先（避免内部再被加粗斜体规则二次处理）
    placeholders: list[str] = []

    def _stash_code(m: re.Match) -> str:
        placeholders.append(f'<code>{m.group(1)}</code>')
        return f'\x00{len(placeholders) - 1}\x00'

    text = _INLINE_CODE_RE.sub(_stash_code, text)

    # 图片：LLM 报告里少见，渲染为「图片说明 (image)」避免死链
    text = _IMAGE_RE.sub(lambda m: f'{m.group(1) or "image"} (image)', text)
    text = _LINK_RE.sub(lambda m: f'<a href="{m.group(2)}">{m.group(1) or m.group(2)}</a>', text)
    text = _BOLD_RE.sub(lambda m: f'<b>{m.group(1) or m.group(2)}</b>', text)
    text = _ITALIC_RE.sub(lambda m: f'<i>{m.group(1) or m.group(2)}</i>', text)
    text = _STRIKE_RE.sub(lambda m: f'<s>{m.group(1)}</s>', text)

    for idx, restored in enumerate(placeholders):
        text = text.replace(f'\x00{idx}\x00', restored)
    return text


def _split_table_row(line: str) -> list[str]:
    inner = line.strip()
    if inner.startswith('|'):
        inner = inner[1:]
    if inner.endswith('|'):
        inner = inner[:-1]
    return [cell.strip() for cell in inner.split('|')]


def _is_table_separator(line: str) -> bool:
    return bool(re.fullmatch(r'\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)*\|?', line.strip()))


def _markdown_to_html(md_text: str) -> str:
    """把 Markdown 渲染为 Story 支持的 HTML 片段（不含 <body>）。"""
    lines = md_text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
    out: list[str] = []

    list_stack: list[str] = []  # 'ul' | 'ol'
    paragraph: list[str] = []

    def _close_lists(to_depth: int = 0) -> None:
        while len(list_stack) > to_depth:
            out.append(f'</{list_stack.pop()}>')

    def _flush_paragraph() -> None:
        if paragraph:
            out.append(f'<p>{_inline(" ".join(paragraph))}</p>')
            paragraph.clear()

    i = 0
    n = len(lines)
    while i < n:
        raw = lines[i]
        line = raw.rstrip()

        stripped = line.strip()

        # 空行：结束当前段落 / 列表层级回退
        if not stripped:
            _flush_paragraph()
            _close_lists()
            i += 1
            continue

        # 围栏代码块 ``` / ~~~
        fence_match = re.match(r'^(```+|~~~+)\s*(\S*)\s*$', stripped)
        if fence_match:
            _flush_paragraph()
            _close_lists()
            fence_token = fence_match.group(1)
            code_lines: list[str] = []
            i += 1
            while i < n and not lines[i].strip().startswith(fence_token[:3]):
                code_lines.append(html_module.escape(lines[i], quote=False))
                i += 1
            i += 1  # 跳过收尾围栏
            out.append(f'<pre><code>{"<br/>".join(code_lines)}</code></pre>')
            continue

        # 标题 # ~ ######
        heading_match = re.match(r'^(#{1,6})\s+(.*)$', stripped)
        if heading_match:
            _flush_paragraph()
            _close_lists()
            level = len(heading_match.group(1))
            out.append(f'<h{level}>{_inline(heading_match.group(2))}</h{level}>')
            i += 1
            continue

        # 分割线
        if re.fullmatch(r'(-{3,}|\*{3,}|_{3,})', stripped):
            _flush_paragraph()
            _close_lists()
            out.append('<hr/>')
            i += 1
            continue

        # GFM 表格：当前行含 | 且下一行是分隔行
        if '|' in stripped and i + 1 < n and _is_table_separator(lines[i + 1]):
            _flush_paragraph()
            _close_lists()
            header_cells = _split_table_row(stripped)
            i += 2
            body_rows: list[list[str]] = []
            while i < n and lines[i].strip() and '|' in lines[i]:
                body_rows.append(_split_table_row(lines[i].strip()))
                i += 1
            out.append('<table>')
            out.append('<tr>' + ''.join(f'<th>{_inline(c)}</th>' for c in header_cells) + '</tr>')
            for row in body_rows:
                cells = row + [''] * (len(header_cells) - len(row))
                out.append('<tr>' + ''.join(f'<td>{_inline(c)}</td>' for c in cells[: len(header_cells)]) + '</tr>')
            out.append('</table>')
            continue

        # 引用块（连续 > 行合并）
        if stripped.startswith('>'):
            _flush_paragraph()
            _close_lists()
            quote_lines: list[str] = []
            while i < n and lines[i].strip().startswith('>'):
                quote_lines.append(lines[i].strip().lstrip('>').strip())
                i += 1
            out.append(f'<blockquote>{_inline(" ".join(q for q in quote_lines if q))}</blockquote>')
            continue

        # 列表项（支持两层缩进嵌套：无缩进 / 2+ 空格缩进）
        ul_match = re.match(r'^(\s*)([-*+])\s+(.*)$', raw)
        ol_match = re.match(r'^(\s*)(\d+)[.)]\s+(.*)$', raw)
        if ul_match or ol_match:
            _flush_paragraph()
            indent, item_text = (
                (ul_match.group(1), ul_match.group(3)) if ul_match else (ol_match.group(1), ol_match.group(3))
            )
            tag = 'ul' if ul_match else 'ol'
            depth = 2 if len(indent.expandtabs(4)) >= 2 else 1
            while len(list_stack) >= depth + 1:
                out.append(f'</{list_stack.pop()}>')
            if len(list_stack) < depth:
                # 列表类型变化时（ul→ol）先关闭旧的
                if list_stack and list_stack[-1] != tag:
                    out.append(f'</{list_stack.pop()}>')
                list_stack.append(tag)
                out.append(f'<{tag}>')
            elif list_stack and list_stack[-1] != tag and depth == len(list_stack):
                out.append(f'</{list_stack.pop()}>')
                list_stack.append(tag)
                out.append(f'<{tag}>')
            out.append(f'<li>{_inline(item_text)}</li>')
            i += 1
            continue

        # 普通文本行：并入当前段落（Markdown 单换行不换段）
        paragraph.append(stripped)
        i += 1

    _flush_paragraph()
    _close_lists()
    return ''.join(out)


# ---------------------------------------------------------------------------
# PDF 渲染
# ---------------------------------------------------------------------------

def build_report_html(
    markdown_text: str,
    document_name: Optional[str] = None,
    analyzed_at: Optional[datetime | str] = None,
    model_name: Optional[str] = None,
) -> str:
    """组装完整 HTML：报告标题 + 元信息行 + 正文。"""
    if isinstance(analyzed_at, str):
        try:
            parsed = datetime.fromisoformat(analyzed_at.replace('Z', '+00:00'))
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            analyzed_at = parsed
        except ValueError:
            analyzed_at = None

    parts: list[str] = []
    if document_name:
        parts.append(f'<h1>{html_module.escape(document_name, quote=False)}</h1>')
        meta_bits = []
        if analyzed_at:
            meta_bits.append(f'Analysis Time: {analyzed_at.strftime("%Y-%m-%d %H:%M UTC")}')
        if model_name and model_name.strip():
            meta_bits.append(f'Analyzed by {html_module.escape(model_name.strip(), quote=False)}')
        if meta_bits:
            parts.append(f'<p class="meta">{" &nbsp;|&nbsp; ".join(meta_bits)}</p>')
        parts.append('<hr/>')

    parts.append(_markdown_to_html(markdown_text))
    return ''.join(parts)


def render_pdf_bytes(
    html: str,
    *,
    title: Optional[str] = None,
    author: Optional[str] = None,
    subject: Optional[str] = None,
) -> bytes:
    """用 PyMuPDF Story 渲染分页文字版 PDF，返回压缩后的字节流。"""
    story = pymupdf.Story(html=html, user_css=_REPORT_CSS)

    def _rectfn(_rect_num: int, _filled: pymupdf.Rect):
        mediabox = pymupdf.Rect(0, 0, PAGE_WIDTH_PT, PAGE_HEIGHT_PT)
        content = pymupdf.Rect(
            PAGE_MARGIN_PT, PAGE_MARGIN_PT,
            PAGE_WIDTH_PT - PAGE_MARGIN_PT, PAGE_HEIGHT_PT - PAGE_MARGIN_PT,
        )
        return mediabox, content, pymupdf.Matrix(1, 1)

    buffer = io.BytesIO()
    writer = pymupdf.DocumentWriter(buffer)
    story.write(writer, _rectfn)
    writer.close()

    doc = pymupdf.open('pdf', buffer.getvalue())
    doc.set_metadata({
        'title': title or 'AI Analysis Report',
        'author': author or 'GuanHe Daily',
        'subject': subject or 'AI Analysis Report',
        'creator': 'GuanHe Daily AI Analysis Tool',
    })
    # 只内嵌实际用到的字形：中文回退字体从 ~3.7MB 缩到几十 KB
    doc.subset_fonts()
    out = io.BytesIO()
    doc.save(out, garbage=4, deflate=True)
    doc.close()
    return out.getvalue()


def generate_ai_report_pdf(
    markdown_text: str,
    document_name: Optional[str] = None,
    analyzed_at: Optional[datetime | str] = None,
    model_name: Optional[str] = None,
    author: Optional[str] = None,
) -> tuple[bytes, str]:
    """
    生成 AI 分析报告文字版 PDF。

    Returns:
        (pdf_bytes, filename)
    """
    if not markdown_text or not markdown_text.strip():
        raise ValueError('报告内容为空，无法生成 PDF')

    filename = build_ai_report_pdf_filename(document_name, analyzed_at, model_name)
    html = build_report_html(markdown_text, document_name, analyzed_at, model_name)
    pdf_bytes = render_pdf_bytes(
        html,
        title=document_name or 'AI Analysis Report',
        author=author,
        subject='AI Analysis Report',
    )
    return pdf_bytes, filename
