# -*- coding: utf-8 -*-
"""
PDF 解析服务
使用 pymupdf4llm 将 PDF 公告转换为 Markdown 格式
"""

import os
import logging
from typing import Optional

try:
    import pymupdf4llm
except ImportError:
    raise ImportError("请先安装依赖: pip install pymupdf4llm")


logger = logging.getLogger(__name__)


def parse_pdf_to_markdown(pdf_path: str, output_dir: str) -> Optional[str]:
    """
    将 PDF 文件解析为 Markdown 并保存。

    Args:
        pdf_path:   PDF 文件的完整路径
        output_dir: Markdown 文件的输出目录

    Returns:
        成功时返回输出的 .md 文件路径，失败时返回 None
    """
    if not os.path.exists(pdf_path):
        logger.warning(f"PDF 文件不存在: {pdf_path}")
        return None

    os.makedirs(output_dir, exist_ok=True)

    filename_no_ext = os.path.splitext(os.path.basename(pdf_path))[0]
    md_path = os.path.join(output_dir, filename_no_ext + ".md")

    if os.path.exists(md_path):
        logger.info(f"Markdown 已存在，跳过解析: {os.path.basename(md_path)}")
        return md_path

    try:
        md_content = pymupdf4llm.to_markdown(pdf_path)
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(md_content)
        logger.info(f"解析完成: {os.path.basename(md_path)}")
        return md_path
    except Exception as e:
        logger.error(f"PDF 解析失败 [{os.path.basename(pdf_path)}]: {e}")
        return None


def batch_parse_directory(pdf_dir: str, output_dir: str) -> dict:
    """
    批量解析目录下所有 PDF 文件。

    Args:
        pdf_dir:    包含 PDF 文件的目录
        output_dir: Markdown 输出目录

    Returns:
        {'success': int, 'skipped': int, 'failed': int}
    """
    stats = {"success": 0, "skipped": 0, "failed": 0}

    pdf_files = [f for f in os.listdir(pdf_dir) if f.lower().endswith(".pdf")]
    total = len(pdf_files)
    logger.info(f"共发现 {total} 个 PDF 文件，开始批量解析...")

    for i, filename in enumerate(sorted(pdf_files), 1):
        pdf_path = os.path.join(pdf_dir, filename)
        md_path = os.path.join(output_dir, os.path.splitext(filename)[0] + ".md")

        if os.path.exists(md_path):
            stats["skipped"] += 1
            continue

        logger.info(f"[{i}/{total}] 解析: {filename}")
        result = parse_pdf_to_markdown(pdf_path, output_dir)
        if result:
            stats["success"] += 1
        else:
            stats["failed"] += 1

    logger.info(
        f"批量解析完成 - 成功: {stats['success']}, "
        f"跳过: {stats['skipped']}, 失败: {stats['failed']}"
    )
    return stats


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    base = os.path.dirname(os.path.abspath(__file__))
    batch_parse_directory(
        pdf_dir=os.path.join(base, "download"),
        output_dir=os.path.join(base, "markdown"),
    )
