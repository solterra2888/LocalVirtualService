# -*- coding: utf-8 -*-
"""Close Brief Report（The Close 独立报告）服务逻辑测试"""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from backend.services.close_brief_report_service import CloseBriefReportService

UTC = timezone.utc
REPORT_DATE = date(2026, 9, 16)
SUMMARY_TS = datetime(2026, 9, 16, 1, 0, tzinfo=UTC)


def _summary_row(reason=None, content="## Main Narrative\nTest **brief**."):
    metadata = {"reason": reason} if reason else {"generation_time_seconds": 5}
    return (
        1, REPORT_DATE, "vid", "Stocks Pare | The Close", "https://youtu.be/vid",
        14299, content, metadata, SUMMARY_TS,
    )


# ── _summary_row_to_dict ────────────────────────────────────────────────

def test_summary_row_placeholder_detection():
    d = CloseBriefReportService._summary_row_to_dict(_summary_row(reason="no_video_found"))
    assert d["placeholder"] is True
    assert d["placeholder_reason"] == "no_video_found"

    d = CloseBriefReportService._summary_row_to_dict(_summary_row(reason="no_transcript"))
    assert d["placeholder"] is True

    d = CloseBriefReportService._summary_row_to_dict(_summary_row())
    assert d["placeholder"] is False


def test_summary_row_metadata_json_string():
    row = _summary_row()
    row = row[:7] + ('{"reason": "no_video_found"}',) + row[8:]
    d = CloseBriefReportService._summary_row_to_dict(row)
    assert d["placeholder"] is True


# ── _is_fresh ───────────────────────────────────────────────────────────

def test_is_fresh_requires_completed():
    assert not CloseBriefReportService._is_fresh(
        {"status": "failed", "updated_at": SUMMARY_TS + timedelta(hours=1)},
        {"updated_at": SUMMARY_TS},
    )


def test_is_fresh_compares_timestamps():
    summary = {"updated_at": SUMMARY_TS}
    assert CloseBriefReportService._is_fresh(
        {"status": "completed", "updated_at": SUMMARY_TS + timedelta(minutes=5)}, summary
    )
    assert not CloseBriefReportService._is_fresh(
        {"status": "completed", "updated_at": SUMMARY_TS - timedelta(minutes=5)}, summary
    )


# ── _render_markdown_to_html ────────────────────────────────────────────

def test_render_markdown_with_table():
    html = CloseBriefReportService._render_markdown_to_html(
        "## Head\n\n**bold**\n\n| A | B |\n|---|---|\n| 1 | 2 |\n"
    )
    assert "<h2>Head</h2>" in html
    assert "<strong>bold</strong>" in html
    assert "<table>" in html


def test_render_markdown_empty():
    assert CloseBriefReportService._render_markdown_to_html("") == ""
    assert CloseBriefReportService._render_markdown_to_html(None) == ""


# ── generate() 分支（mock DB） ──────────────────────────────────────────

def _run_generate(summary_row, existing_row, force=False):
    """以 mock 连接运行 generate()，返回结果 dict。"""
    conn = MagicMock()

    def execute(sql, params=None):
        if "the_close_summaries" in sql:
            cur.fetchone.return_value = summary_row
        elif "RETURNING id" in sql:
            cur.fetchone.return_value = (99,)
        elif "close_brief_reports" in sql and "status" in sql:
            cur.fetchone.return_value = existing_row

    cur = MagicMock()
    cur.execute.side_effect = execute
    conn.cursor.return_value = cur

    import backend.services.close_brief_report_service as svc_mod
    with patch.object(svc_mod.DatabaseManager, "get_connection", return_value=conn), \
         patch.object(svc_mod, "REPORTS_DIR", "/tmp/test_close_brief_reports"):
        return svc_mod.CloseBriefReportService().generate(
            report_date=REPORT_DATE, force=force
        )


def test_generate_skips_when_summary_missing():
    r = _run_generate(None, None)
    assert r["success"] is True and r["skipped"] is True
    assert r["reason"] == "summary_not_ready"


def test_generate_skips_placeholder_summary():
    r = _run_generate(_summary_row(reason="no_video_found"), None)
    assert r["skipped"] is True and r["reason"] == "no_video_found"


def test_generate_skips_fresh_report():
    existing = (50, "completed", SUMMARY_TS + timedelta(minutes=5))
    r = _run_generate(_summary_row(), existing)
    assert r["skipped"] is True and r["reason"] == "report_up_to_date"


def test_generate_renders_when_stale():
    existing = (50, "completed", SUMMARY_TS - timedelta(minutes=5))
    r = _run_generate(_summary_row(), existing)
    assert r["success"] is True and r["skipped"] is False
    assert r["share_token"]


def test_generate_renders_when_missing_or_failed():
    r = _run_generate(_summary_row(), None)
    assert r["skipped"] is False
    r = _run_generate(_summary_row(), (50, "failed", SUMMARY_TS + timedelta(hours=1)))
    assert r["skipped"] is False


def test_generate_force_ignores_freshness():
    existing = (50, "completed", SUMMARY_TS + timedelta(hours=1))
    r = _run_generate(_summary_row(), existing, force=True)
    assert r["skipped"] is False
