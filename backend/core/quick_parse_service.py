# -*- coding: utf-8 -*-
"""
Quick Parse Service - 轻量化本地文档解析
使用 pymupdf4llm / mammoth / openpyxl 等本地库将文档转换为 Markdown，
无需发送到 LlamaParse 远程服务。

接口与 LlamaService.process_file() 完全兼容，可作为 drop-in replacement。

PDF 解析策略（2026-05）：
  - 超过 MAX_PAGES_HARD_LIMIT（默认 2000）：拒绝解析，防止 OOM。
  - 超过 PDF_STREAMING_THRESHOLD_PAGES（默认 50）：分批流式解析，
    每批 PDF_STREAMING_BATCH_SIZE（默认 50）页，显著降低峰值内存。
  - 普通文档：一次性全量解析（保留原有行为）。
"""

import gc
import os
import time
import logging
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# ── PDF 分批解析阈值（可通过环境变量覆盖）──────────────────────────────────
def _pdf_env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


class QuickParseService:
    """轻量化本地文档解析服务"""

    def __init__(self):
        self._check_dependencies()

    @staticmethod
    def _check_dependencies():
        """Lazy-check: only warn, don't crash at import time."""
        pass

    # ------------------------------------------------------------------
    # PDF
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_pdf(file_path: str, params: Optional[Dict] = None) -> str:
        import pymupdf4llm
        import pymupdf

        params = params or {}

        # ── 1. 获取页数 ─────────────────────────────────────────────────
        doc = pymupdf.open(file_path)
        total_pages = doc.page_count
        doc.close()

        hard_limit = _pdf_env_int("PDF_MAX_PAGES_HARD_LIMIT", 2000)
        if total_pages > hard_limit:
            raise ValueError(
                f"PDF 页数 ({total_pages}) 超过上限 {hard_limit}，"
                f"请拆分文档后重试，或联系管理员调整 PDF_MAX_PAGES_HARD_LIMIT。"
            )

        # ── 2. 支持旧 max_pages 参数（截取前 N 页）───────────────────────
        max_pages = params.get("max_pages")
        if max_pages and int(max_pages) < total_pages:
            total_pages = int(max_pages)

        # ── 3. 决定是否分批流式解析 ─────────────────────────────────────
        stream_threshold = _pdf_env_int("PDF_STREAMING_THRESHOLD_PAGES", 50)
        batch_size = _pdf_env_int("PDF_STREAMING_BATCH_SIZE", 50)

        page_range = list(range(total_pages))

        if total_pages <= stream_threshold:
            # 普通文档：一次性解析（保留原有行为）
            kwargs: Dict[str, Any] = {}
            if max_pages:
                kwargs["pages"] = page_range
            return pymupdf4llm.to_markdown(file_path, **kwargs)

        # 超出阈值：分批流式解析，降低峰值内存
        logger.info(
            "QuickParse: PDF %d 页，超过 %d 阈值，启用分批流式解析（批次大小=%d）",
            total_pages, stream_threshold, batch_size,
        )
        parts: List[str] = []
        for start in range(0, total_pages, batch_size):
            end = min(start + batch_size, total_pages)
            batch_pages = page_range[start:end]
            batch_md = pymupdf4llm.to_markdown(file_path, pages=batch_pages)
            parts.append(batch_md)
            del batch_md
            gc.collect()
            logger.debug("QuickParse: 批次 %d-%d / %d 完成", start + 1, end, total_pages)

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Word (.docx / .doc)
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_word(file_path: str) -> str:
        import mammoth

        with open(file_path, "rb") as f:
            result = mammoth.convert_to_markdown(f)

        md = result.value
        if result.messages:
            warnings = [str(m) for m in result.messages]
            logger.warning("mammoth warnings for %s: %s", file_path, "; ".join(warnings))
        return md

    # ------------------------------------------------------------------
    # Excel (.xlsx / .xls)
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_excel(file_path: str) -> str:
        import openpyxl

        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        parts: List[str] = []

        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            rows = list(ws.iter_rows(values_only=True))
            if not rows:
                continue

            parts.append(f"## {sheet_name}\n")

            max_cols = max(len(r) for r in rows)
            def _cell(v: Any) -> str:
                if v is None:
                    return ""
                return str(v).replace("|", "\\|").replace("\n", " ")

            header = rows[0]
            header_cells = [_cell(c) for c in header] + [""] * (max_cols - len(header))
            parts.append("| " + " | ".join(header_cells) + " |")
            parts.append("| " + " | ".join(["---"] * max_cols) + " |")

            for row in rows[1:]:
                cells = [_cell(c) for c in row] + [""] * (max_cols - len(row))
                parts.append("| " + " | ".join(cells) + " |")

            parts.append("")

        wb.close()
        return "\n".join(parts)

    # ------------------------------------------------------------------
    # CSV
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_csv(file_path: str) -> str:
        import csv

        with open(file_path, "r", encoding="utf-8-sig", errors="replace") as f:
            raw_content = f.read()

        if not raw_content.strip():
            return ""

        from backend.services.trading_report_extractor import (
            is_ib_trading_report,
            extract_tables_from_content,
            to_structured_content,
        )

        if is_ib_trading_report(raw_content):
            tables, orders = extract_tables_from_content(raw_content)
            return to_structured_content(tables, orders)

        parts: List[str] = []
        rows = list(csv.reader(raw_content.splitlines()))

        if not rows:
            return ""

        max_cols = max(len(r) for r in rows)
        def _cell(v: str) -> str:
            return v.replace("|", "\\|").replace("\n", " ")

        header = rows[0]
        header_cells = [_cell(c) for c in header] + [""] * (max_cols - len(header))
        parts.append("| " + " | ".join(header_cells) + " |")
        parts.append("| " + " | ".join(["---"] * max_cols) + " |")

        for row in rows[1:]:
            cells = [_cell(c) for c in row] + [""] * (max_cols - len(row))
            parts.append("| " + " | ".join(cells) + " |")

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Images — Quick Process 不支持 OCR，返回提示
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_image(file_path: str) -> str:
        basename = os.path.basename(file_path)
        return (
            f"![{basename}]({basename})\n\n"
            f"> **Note**: Quick Process does not support OCR for images. "
            f"Please use LlamaParse mode to extract text from scanned documents or images."
        )

    # ------------------------------------------------------------------
    # 主入口 — 与 LlamaService.process_file() 签名/返回格式完全一致
    # ------------------------------------------------------------------
    async def process_file(
        self,
        file_path: str,
        results_collector: List[Dict[str, Any]],
        failed_files: List[Dict[str, Any]],
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], float]:
        logger.info("QuickParse: processing %s", file_path)
        file_start_time = time.time()

        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        file_extension = os.path.splitext(file_path)[1].lower()
        file_type = file_extension[1:] if file_extension else "unknown"

        try:
            if file_extension == ".pdf":
                content = self._parse_pdf(file_path, params)
            elif file_extension in (".doc", ".docx"):
                content = self._parse_word(file_path)
            elif file_extension in (".xlsx", ".xls"):
                content = self._parse_excel(file_path)
            elif file_extension == ".csv":
                content = self._parse_csv(file_path)
            elif file_extension in (".jpg", ".jpeg", ".png", ".gif", ".bmp"):
                content = self._parse_image(file_path)
            else:
                raise ValueError(f"Unsupported file type for Quick Process: {file_extension}")

            if not content:
                raise ValueError("Parsed content is empty")

            file_total_time = time.time() - file_start_time
            content_length = len(content.split())

            results_collector.append({
                "content": content,
                "original_file": file_path,
                "content_length": content_length,
                "processing_time": file_total_time,
            })

            logger.info(
                "QuickParse: completed %s (%.1fs, %d words)",
                os.path.basename(file_path), file_total_time, content_length,
            )

            file_info = {
                "file_path": file_path,
                "file_name": os.path.basename(file_path),
                "file_type": file_type,
                "file_size_mb": file_size_mb,
                "processing_time": file_total_time,
                "content": content,
                "content_length": content_length,
                "is_success": True,
            }
            return file_info, file_total_time

        except Exception as e:
            file_total_time = time.time() - file_start_time
            error_msg = f"Quick Process failed: {e}"
            logger.error("QuickParse: %s — %s", os.path.basename(file_path), error_msg)

            failed_files.append({
                "file_path": file_path,
                "error": error_msg,
            })

            file_info = {
                "file_path": file_path,
                "file_name": os.path.basename(file_path),
                "file_type": file_type,
                "file_size_mb": file_size_mb,
                "processing_time": file_total_time,
                "content": None,
                "content_length": 0,
                "is_success": False,
                "error": error_msg,
            }
            return file_info, file_total_time
