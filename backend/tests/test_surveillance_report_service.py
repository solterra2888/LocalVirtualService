# -*- coding: utf-8 -*-
"""Surveillance Report（Bloomberg Surveillance 独立报告）服务逻辑测试"""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from backend.services.surveillance_report_service import SurveillanceReportService

UTC = timezone.utc
REPORT_DATE = date(2026, 9, 16)
SUMMARY_TS = datetime(2026, 9, 16, 1, 0, tzinfo=UTC)


def _summary_row(reason=None, content="## Morning Narrative\nTest **brief**."):
    metadata = {"reason": reason} if reason else {
        "generation_time_seconds": 6, "transcript_segment_count": 2,
    }
    return (
        1, REPORT_DATE, "vid", "Bloomberg Surveillance - 09/16/2026",
        "https://youtu.be/vid", 25486, content, metadata, SUMMARY_TS,
    )


# ── _summary_row_to_dict ────────────────────────────────────────────────

def test_summary_row_placeholder_detection():
    d = SurveillanceReportService._summary_row_to_dict(_summary_row(reason="no_video_found"))
    assert d["placeholder"] is True
    assert d["placeholder_reason"] == "no_video_found"

    d = SurveillanceReportService._summary_row_to_dict(_summary_row(reason="no_transcript"))
    assert d["placeholder"] is True

    d = SurveillanceReportService._summary_row_to_dict(_summary_row())
    assert d["placeholder"] is False


def test_summary_row_video_count_from_merged_transcript():
    d = SurveillanceReportService._summary_row_to_dict(_summary_row())
    assert d["video_count"] == 2  # metadata.transcript_segment_count

    row = _summary_row()
    row = row[:7] + ({},) + row[8:]
    assert SurveillanceReportService._summary_row_to_dict(row)["video_count"] == 1


def test_summary_row_metadata_json_string():
    row = _summary_row()
    row = row[:7] + ('{"reason": "no_video_found"}',) + row[8:]
    d = SurveillanceReportService._summary_row_to_dict(row)
    assert d["placeholder"] is True


# ── _is_fresh ───────────────────────────────────────────────────────────

def test_is_fresh_requires_completed():
    assert not SurveillanceReportService._is_fresh(
        {"status": "failed", "updated_at": SUMMARY_TS + timedelta(hours=1)},
        {"updated_at": SUMMARY_TS},
    )


def test_is_fresh_compares_timestamps():
    summary = {"updated_at": SUMMARY_TS}
    assert SurveillanceReportService._is_fresh(
        {"status": "completed", "updated_at": SUMMARY_TS + timedelta(minutes=5)}, summary
    )
    assert not SurveillanceReportService._is_fresh(
        {"status": "completed", "updated_at": SUMMARY_TS - timedelta(minutes=5)}, summary
    )


# ── _render_markdown_to_html ────────────────────────────────────────────

def test_render_markdown_with_table():
    html = SurveillanceReportService._render_markdown_to_html(
        "## Head\n\n**bold**\n\n| A | B |\n|---|---|\n| 1 | 2 |\n"
    )
    assert "<h2>Head</h2>" in html
    assert "<strong>bold</strong>" in html
    assert "<table>" in html


def test_render_markdown_empty():
    assert SurveillanceReportService._render_markdown_to_html("") == ""
    assert SurveillanceReportService._render_markdown_to_html(None) == ""


# ── generate() 分支（mock DB） ──────────────────────────────────────────

def _run_generate(summary_row, existing_row, force=False):
    """以 mock 连接运行 generate()，返回结果 dict。"""
    conn = MagicMock()

    def execute(sql, params=None):
        if "bloomberg_surveillance_summaries" in sql:
            cur.fetchone.return_value = summary_row
        elif "RETURNING id" in sql:
            cur.fetchone.return_value = (99,)
        elif "RETURNING share_token" in sql:
            cur.fetchone.return_value = ("token-123",)
        elif "surveillance_reports" in sql and "status" in sql:
            cur.fetchone.return_value = existing_row

    cur = MagicMock()
    cur.execute.side_effect = execute
    conn.cursor.return_value = cur

    import backend.services.surveillance_report_service as svc_mod
    with patch.object(svc_mod.DatabaseManager, "get_connection", return_value=conn), \
         patch.object(svc_mod, "REPORTS_DIR", "/tmp/test_surveillance_reports"):
        return svc_mod.SurveillanceReportService().generate(
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
