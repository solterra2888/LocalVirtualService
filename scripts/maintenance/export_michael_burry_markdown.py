#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 subscription_content 导出 Michael Burry（Cassandra Unchained）全部文章为 Markdown。

Substack 正文存的是 HTML（含 data-component-name 自定义组件、paywall、picture/srcset 等），
导出前会清洗再转成 Markdown。每个一级标题对应表里的 title。

默认按发布日期倒序（最新文章在上）。

用法:
  python scripts/maintenance/export_michael_burry_markdown.py
  python scripts/maintenance/export_michael_burry_markdown.py --output /tmp/burry.md
  python scripts/maintenance/export_michael_burry_markdown.py --oldest-first
"""
from __future__ import annotations

import argparse
import html as html_lib
import os
import re
import sys
from datetime import datetime, timezone
from typing import List, Optional, Sequence
from urllib.parse import urlparse

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from backend.utils.datetime_utils import install_psycopg2_utc_adapter  # noqa: E402

install_psycopg2_utc_adapter()

from backend.services.substack_subscription_service import _get_conn  # noqa: E402
from backend.utils.logger import get_logger  # noqa: E402

logger = get_logger("export_michael_burry_markdown")

DEFAULT_OUTPUT = os.path.join(ROOT, "exports", "michael_burry_substack.md")
PUB_URL = "https://michaeljburry.substack.com"

REMOVE_COMPONENTS = {
    "PaywallToDOM",
    "SubscribeWidgetToDOM",
    "ButtonCreateButton",
    "CommunityPostPlaceholder",
    "CommunityChatRenderPlaceholder",
    "CommentPlaceholder",
    "EmbeddedPublicationToDOMWithSubscribe",
}
SKIP_TAGS = {
    "script", "style", "svg", "button", "form", "input",
    "path", "g", "polyline", "line", "defs", "use",
}

HEADING_SHIFT = {"h1": 2, "h2": 3, "h3": 4, "h4": 5, "h5": 6, "h6": 6}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="导出 Michael Burry Substack 文章为 Markdown")
    parser.add_argument("--output", "-o", default=DEFAULT_OUTPUT, help="输出 md 路径")
    parser.add_argument(
        "--oldest-first",
        action="store_true",
        help="按发布时间正序（默认倒序，最新在上）",
    )
    parser.add_argument("--limit", type=int, default=0, help="最多导出 N 篇（0=全部）")
    return parser.parse_args()


def _fetch_articles(newest_first: bool, limit: int) -> List[dict]:
    order = "ASC" if not newest_first else "DESC"
    sql = f"""
        SELECT id, title, url, content, published_at, author_name, metadata
        FROM subscription_content
        WHERE channel_type = 'substack'
          AND content_id ~ '^[0-9]+$'
          AND (
                author_identifier = %s
             OR url ILIKE %s
             OR author_name ILIKE %s
          )
        ORDER BY published_at {order} NULLS LAST, id {order}
    """
    params: list = [PUB_URL, "%michaeljburry.substack.com%", "%Burry%"]
    if limit and limit > 0:
        sql += " LIMIT %s"
        params.append(limit)

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------- Substack HTML → Markdown ----------

def _soup(html: str):
    from bs4 import BeautifulSoup

    try:
        return BeautifulSoup(html or "", "lxml")
    except Exception:
        return BeautifulSoup(html or "", "html.parser")


def _text(value: Optional[str]) -> str:
    if not value:
        return ""
    return html_lib.unescape(value.replace("\xa0", " "))


def _collapse_ws(text: str) -> str:
    return re.sub(r"[ \t]+", " ", text).strip()


def _best_image_url(el) -> Optional[str]:
    """对齐前端 substackHtmlUtils.extractImageUrl。"""
    link = el if (getattr(el, "name", None) == "a" and "image-link" in (el.get("class") or [])) else el.find("a", class_=lambda c: c and "image-link" in c)
    if link and link.get("href"):
        return link["href"].strip()
    img = el.find("img") if getattr(el, "name", None) != "img" else el
    if img is not None:
        src = (img.get("src") or "").strip()
        if src:
            return src
        srcset = (img.get("srcset") or "").strip()
        if srcset:
            return srcset.split(",")[0].strip().split()[0]
    source = el.find("source")
    if source is not None:
        srcset = (source.get("srcset") or "").strip()
        if srcset:
            return srcset.split(",")[0].strip().split()[0]
    return None


def _clean_substack_dom(soup) -> None:
    for el in list(soup.select(".subscription-widget, .paywall-jump, .image-link-expand")):
        el.decompose()
    for el in list(soup.find_all(attrs={"data-component-name": True})):
        name = el.get("data-component-name") or ""
        if name in REMOVE_COMPONENTS:
            el.decompose()
    for el in list(soup.find_all(list(SKIP_TAGS))):
        el.decompose()


def _escape_md_cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def _inline_children(el) -> str:
    parts: List[str] = []
    for child in el.children:
        parts.append(_inline(child))
    return "".join(parts)


def _inline(node) -> str:
    from bs4 import NavigableString, Tag

    if isinstance(node, NavigableString):
        return _text(str(node))
    if not isinstance(node, Tag):
        return ""
    name = node.name.lower()
    if name in SKIP_TAGS:
        return ""
    if name in ("br",):
        return "  \n"
    if name in ("strong", "b"):
        inner = _collapse_ws(_inline_children(node))
        return f"**{inner}**" if inner else ""
    if name in ("em", "i"):
        inner = _collapse_ws(_inline_children(node))
        return f"*{inner}*" if inner else ""
    if name == "code":
        inner = _inline_children(node).strip()
        return f"`{inner}`" if inner else ""
    if name in ("sup",):
        inner = _collapse_ws(_inline_children(node))
        return f"^{inner}^" if inner else ""
    if name in ("sub",):
        inner = _collapse_ws(_inline_children(node))
        return inner
    if name == "a":
        href = (node.get("href") or "").strip()
        classes = node.get("class") or []
        if "image-link" in classes:
            url = _best_image_url(node)
            return f"\n\n![]({url})\n\n" if url else ""
        inner = _collapse_ws(_inline_children(node))
        if not inner and not href:
            return ""
        if href and inner:
            return f"[{inner}]({href})"
        return inner or href
    if name == "img":
        url = _best_image_url(node)
        alt = _collapse_ws(_text(node.get("alt") or ""))
        return f"![{alt}]({url})" if url else ""
    if name in ("span", "font", "u", "small", "mark"):
        return _inline_children(node)
    return _inline_children(node)


def _heading_md(el, article_title: str) -> str:
    level = HEADING_SHIFT.get(el.name.lower(), 3)
    text = _collapse_ws(_inline_children(el))
    if not text:
        return ""
    if text.strip().lower() == (article_title or "").strip().lower():
        return ""
    return f"{'#' * level} {text}"


def _list_md(el, ordered: bool, depth: int = 0) -> str:
    lines: List[str] = []
    index = 1
    for child in el.find_all("li", recursive=False):
        nested = []
        for sub in list(child.find_all(["ul", "ol"], recursive=False)):
            nested.append(_list_md(sub, sub.name == "ol", depth + 1))
            sub.extract()
        item = _collapse_ws(_inline_children(child))
        bullet = f"{index}." if ordered else "-"
        indent = "  " * depth
        if item:
            lines.append(f"{indent}{bullet} {item}")
        lines.extend([n for n in nested if n])
        index += 1
    return "\n".join(lines)


def _blockquote_md(el) -> str:
    inner = _blocks(el, article_title="")
    quoted = []
    for line in inner.splitlines():
        quoted.append(f"> {line}" if line.strip() else ">")
    return "\n".join(quoted).strip()


def _youtube_url(src: str) -> str:
    if not src:
        return src
    parsed = urlparse(src)
    host = (parsed.hostname or "").lower()
    if host in (
        "www.youtube.com",
        "youtube.com",
        "www.youtube-nocookie.com",
        "youtube-nocookie.com",
    ) and "/embed/" in parsed.path:
        vid = parsed.path.rstrip("/").split("/")[-1]
        start = ""
        if parsed.query:
            from urllib.parse import parse_qs

            qs = parse_qs(parsed.query)
            if qs.get("start"):
                start = f"&t={qs['start'][0]}s"
        if vid:
            return f"https://www.youtube.com/watch?v={vid}{start}"
    return src


def _component_md(el) -> str:
    name = el.get("data-component-name") or ""
    if name == "Image2ToDOM" or "image2" in (el.get("class") or []) or el.select_one("a.image-link, picture, img"):
        container = el.find_parent(class_="captioned-image-container") or el.find("figure") or el
        url = _best_image_url(container)
        caption = ""
        figcaption = container.find("figcaption") if container else None
        if figcaption:
            caption = _collapse_ws(_inline_children(figcaption))
        if url:
            alt = caption
            line = f"![{alt}]({url})"
            if caption:
                line += f"\n*{caption}*"
            return line
    if name in ("Youtube2ToDOM", "ApplePodcastToDom", "Spotify2ToDOM"):
        iframe = el.find("iframe")
        src = (iframe.get("src") if iframe is not None else "") or ""
        src = _youtube_url(src.strip())
        label = "YouTube" if "youtube" in name.lower() else "Embed"
        return f"[{label}]({src})" if src else ""
    if name == "FileToDOM":
        link = el.find("a", href=True)
        if link is not None:
            href = link["href"].strip()
            label = _collapse_ws(_inline_children(link)) or os.path.basename(urlparse(href).path) or "附件"
            return f"[{label}]({href})"
        return _collapse_ws(_inline_children(el))
    if name in ("EmbeddedPostToDOM", "MentionToDOM"):
        link = el.find("a", href=True)
        text = _collapse_ws(_inline_children(el))
        if link is not None:
            href = link["href"].strip()
            label = text or href
            return f"[{label}]({href})"
        return text
    iframe = el.find("iframe")
    if iframe is not None and iframe.get("src"):
        return f"[Embed]({iframe['src'].strip()})"
    return _collapse_ws(_inline_children(el))


def _blocks(root, article_title: str) -> str:
    chunks: List[str] = []

    def emit(text: str) -> None:
        text = (text or "").strip("\n")
        if text.strip():
            chunks.append(text)

    from bs4 import NavigableString, Tag

    for child in list(root.children):
        if isinstance(child, NavigableString):
            raw = _collapse_ws(_text(str(child)))
            if raw:
                emit(raw)
            continue
        if not isinstance(child, Tag):
            continue
        name = child.name.lower()
        if name in SKIP_TAGS:
            continue
        if name in ("h1", "h2", "h3", "h4", "h5", "h6"):
            emit(_heading_md(child, article_title))
            continue
        if name == "p":
            emit(_collapse_ws(_inline_children(child)))
            continue
        if name == "blockquote":
            emit(_blockquote_md(child))
            continue
        if name == "ul":
            emit(_list_md(child, ordered=False))
            continue
        if name == "ol":
            emit(_list_md(child, ordered=True))
            continue
        if name == "hr":
            emit("---")
            continue
        if name in ("pre",):
            emit("```\n" + child.get_text() + "\n```")
            continue
        if name == "figure" or name == "picture" or (child.get("class") and "captioned-image-container" in child.get("class")):
            emit(_component_md(child))
            continue
        if child.get("data-component-name"):
            emit(_component_md(child))
            continue
        if name in ("div", "section", "article", "main"):
            nested = _blocks(child, article_title)
            emit(nested)
            continue
        if name in ("iframe",):
            src = (child.get("src") or "").strip()
            if src:
                emit(f"[Embed]({_youtube_url(src)})")
            continue
        if name == "img":
            emit(_inline(child))
            continue
        inline = _collapse_ws(_inline_children(child))
        if inline:
            emit(inline)

    return "\n\n".join(chunks)


def html_to_markdown(raw_html: str, article_title: str = "") -> str:
    raw = (raw_html or "").strip()
    if not raw:
        return ""
    soup = _soup(raw)
    body = soup.body or soup
    _clean_substack_dom(body)
    md = _blocks(body, article_title)
    md = re.sub(r"\n{3,}", "\n\n", md)
    # Substack 常把 <strong> 紧贴单词：be<strong>inside</strong>the
    md = re.sub(r"\*\*(.+?)\*\*", lambda m: f" **{m.group(1).strip()}** ", md)
    md = re.sub(r"[ \t]{2,}", " ", md)
    md = re.sub(r" +\n", "\n", md)
    return md.strip()


def _fmt_published(value) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    return str(value)[:10]


def _safe_h1(title: str) -> str:
    text = (title or "").strip() or "(untitled)"
    text = text.replace("\n", " ").strip()
    if text.startswith("#"):
        text = text.lstrip("#").strip()
    return text


def render_document(articles: Sequence[dict]) -> str:
    parts: List[str] = []
    for art in articles:
        title = _safe_h1(art.get("title") or "")
        body = html_to_markdown(art.get("content") or "", article_title=title)
        meta_bits = []
        published = _fmt_published(art.get("published_at"))
        if published:
            meta_bits.append(published)
        url = (art.get("url") or "").strip()
        if url:
            meta_bits.append(url)
        parts.append(f"# {title}")
        if meta_bits:
            parts.append("*" + " · ".join(meta_bits) + "*")
        if body:
            parts.append(body)
        else:
            parts.append("*(无正文)*")
    return "\n\n".join(parts).strip() + "\n"


def main() -> int:
    args = _parse_args()
    articles = _fetch_articles(newest_first=not args.oldest_first, limit=args.limit)
    if not articles:
        logger.error("未找到 Michael Burry 的 Substack 文章")
        return 1

    logger.info(f"导出 {len(articles)} 篇 → {args.output}")
    md = render_document(articles)
    out_dir = os.path.dirname(os.path.abspath(args.output))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as fh:
        fh.write(md)
    logger.info(f"完成: {args.output}  ({len(md):,} chars)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
