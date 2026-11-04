# -*- coding: utf-8 -*-
"""
信息分级 (L/E 双轴) 打标测试 — 2026-11-03

覆盖 FeedContentTaggingService 的 L/E 相关改动:
  1. _normalize_grade        — 分级值归一化 (大小写/纯数字容错/非法置 None)
  2. _normalize_tag_data     — 两个 schema (feeds_v1 / tweet_signal_v1) 下
                               info_level / evidence_level 的校验与容错
  3. save_tags               — UPSERT SQL 携带 info_level / evidence_level 列
  4. update_tags             — 手动编辑分级 (归一化 + 非法值 ValueError + None 清除)
  5. map_api_tags            — EODHD 轻量映射按来源规则补 evidence_level='E2'

标准依据: tools/morning-brief/INFO_GRADING_STANDARD.md
DB / LLM 全部 mock; 仅验证业务逻辑与 SQL 契约。
"""
import pytest


# ─────────────────────────────────────────────────────────────────────
# Fakes (对标 test_feed_scheduler_fixes.py 的 FakeCursor 模式)
# ─────────────────────────────────────────────────────────────────────

class FakeCursor:
    """记录 execute 调用 + 可控 fetchone 队列的假 cursor。"""

    def __init__(self, fetchone_results=None):
        self.executed = []  # [(sql, params), ...]
        self._fetchone = list(fetchone_results or [])

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._fetchone.pop(0) if self._fetchone else None

    def close(self):
        pass


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


class FakeDbManager:
    def __init__(self, cursor):
        self._conn = FakeConn(cursor)

    def get_connection(self):
        return self._conn


def _make_service(cursor=None):
    from backend.services.feed_content_tagging_service import (
        FeedContentTaggingService,
    )
    return FeedContentTaggingService(
        db_manager=FakeDbManager(cursor or FakeCursor()),
    )


def _flat(sql):
    return " ".join((sql or "").split())


def _base_feeds_v1_data(**extra):
    data = {
        "sector": "信息技术",
        "sector_en": "Information Technology",
        "industry": "半导体",
        "industry_en": "Semiconductors",
        "tickers": ["NVDA.US"],
        "concepts": ["AI芯片"],
        "people": [],
        "countries": ["US"],
        "confidence": 0.9,
        "summary": "英伟达数据中心营收超预期。",
    }
    data.update(extra)
    return data


def _base_tweet_signal_data(**extra):
    data = {
        "tickers": [],
        "concepts": ["AI芯片"],
        "people": [],
        "countries": ["US"],
        "stance": "bullish",
        "signal_type": "opinion",
        "importance": "medium",
        "summary": "KOL 看多 AI 算力。",
        "confidence": 0.8,
    }
    data.update(extra)
    return data


# ─────────────────────────────────────────────────────────────────────
# 1. _normalize_grade
# ─────────────────────────────────────────────────────────────────────

class TestNormalizeGrade:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("L1", "L1"),
            ("l2", "L2"),          # 小写容错
            (" E3 ", "E3"),        # 空白容错
            ("2", "L2"),           # 纯数字 → 按 axis 补前缀
            ("3", "E3"),
            (None, None),
        ],
    )
    def test_valid_values(self, raw, expected):
        from backend.services.feed_content_tagging_service import (
            FeedContentTaggingService,
        )
        axis = "L" if expected is None or expected.startswith("L") else "E"
        allowed = (
            {"L1", "L2", "L3", "L4"} if axis == "L" else {"E1", "E2", "E3"}
        )
        assert (
            FeedContentTaggingService._normalize_grade(raw, allowed, axis)
            == expected
        )

    @pytest.mark.parametrize("raw", ["L5", "X9", "high", "L0", "", "LL1"])
    def test_invalid_values_return_none(self, raw):
        from backend.services.feed_content_tagging_service import (
            FeedContentTaggingService,
        )
        assert (
            FeedContentTaggingService._normalize_grade(
                raw, {"L1", "L2", "L3", "L4"}, "L"
            )
            is None
        )


# ─────────────────────────────────────────────────────────────────────
# 2. _normalize_tag_data
# ─────────────────────────────────────────────────────────────────────

class TestNormalizeTagDataGrading:
    def test_feeds_v1_keeps_and_normalizes_grades(self):
        svc = _make_service()
        data = svc._normalize_tag_data(
            _base_feeds_v1_data(info_level="l1", evidence_level="E2"),
            "feeds_v1",
        )
        assert data["info_level"] == "L1"
        assert data["evidence_level"] == "E2"

    def test_feeds_v1_missing_grades_default_none(self):
        svc = _make_service()
        data = svc._normalize_tag_data(_base_feeds_v1_data(), "feeds_v1")
        assert data["info_level"] is None
        assert data["evidence_level"] is None

    def test_feeds_v1_invalid_grades_coerce_none(self):
        svc = _make_service()
        data = svc._normalize_tag_data(
            _base_feeds_v1_data(info_level="high", evidence_level="rumor"),
            "feeds_v1",
        )
        assert data["info_level"] is None
        assert data["evidence_level"] is None

    def test_tweet_signal_v1_grades(self):
        svc = _make_service()
        data = svc._normalize_tag_data(
            _base_tweet_signal_data(info_level="2", evidence_level="e3"),
            "tweet_signal_v1",
        )
        assert data["info_level"] == "L2"
        assert data["evidence_level"] == "E3"

    def test_legacy_llm_output_without_grades_still_passes(self):
        """旧模板/降级输出没有 L/E 字段时不应阻断打标主链路."""
        svc = _make_service()
        data = svc._normalize_tag_data(
            _base_tweet_signal_data(), "tweet_signal_v1"
        )
        assert data["importance"] == "medium"
        assert data["info_level"] is None


# ─────────────────────────────────────────────────────────────────────
# 3. save_tags — UPSERT SQL 契约
# ─────────────────────────────────────────────────────────────────────

class TestSaveTagsGradingColumns:
    def test_insert_and_upsert_carry_grade_columns(self):
        cursor = FakeCursor(fetchone_results=[(101,)])
        svc = _make_service(cursor)
        result = svc.save_tags(
            content_id=7,
            channel_type="twitter",
            tag_data=_base_tweet_signal_data(info_level="L1", evidence_level="E3"),
            tag_schema_version="tweet_signal_v1",
        )
        assert result["tag_id"] == 101
        sql = cursor.executed[0][0]
        flat = _flat(sql)
        # INSERT 列与 ON CONFLICT UPDATE 均携带新列
        assert "info_level, evidence_level" in flat
        assert "info_level = EXCLUDED.info_level" in flat
        assert "evidence_level = EXCLUDED.evidence_level" in flat
        # 参数顺序: ..., info_level, evidence_level, tagged_by
        params = cursor.executed[0][1]
        assert params[-3] == "L1"
        assert params[-2] == "E3"

    def test_ungraded_tags_persist_null(self):
        cursor = FakeCursor(fetchone_results=[(102,)])
        svc = _make_service(cursor)
        svc.save_tags(
            content_id=8,
            channel_type="news",
            tag_data=_base_feeds_v1_data(),
            tag_schema_version="feeds_v1",
        )
        params = cursor.executed[0][1]
        assert params[-3] is None
        assert params[-2] is None


# ─────────────────────────────────────────────────────────────────────
# 4. update_tags — 手动编辑分级
# ─────────────────────────────────────────────────────────────────────

class TestUpdateTagsGrading:
    def test_valid_grade_normalized_and_written(self):
        cursor = FakeCursor(fetchone_results=[(101,)])
        svc = _make_service(cursor)
        svc.update_tags(7, {"info_level": "l2", "evidence_level": "E1"})
        sql, params = cursor.executed[0]
        flat = _flat(sql)
        assert "info_level = %s" in flat
        assert "evidence_level = %s" in flat
        assert "L2" in params
        assert "E1" in params

    def test_invalid_grade_raises_before_sql(self):
        cursor = FakeCursor(fetchone_results=[(101,)])
        svc = _make_service(cursor)
        with pytest.raises(ValueError, match="info_level"):
            svc.update_tags(7, {"info_level": "L9"})
        assert cursor.executed == []  # 未发出任何 SQL

    def test_none_grade_sets_null(self):
        cursor = FakeCursor(fetchone_results=[(101,)])
        svc = _make_service(cursor)
        svc.update_tags(7, {"evidence_level": None})
        _, params = cursor.executed[0]
        assert None in params


# ─────────────────────────────────────────────────────────────────────
# 5. map_api_tags — EODHD 轻量映射补 E2
# ─────────────────────────────────────────────────────────────────────

class TestMapApiTagsEvidenceLevel:
    def test_eodhd_mapping_writes_e2_and_null_info_level(self):
        # fetchone 队列: get_content 行 → get_existing_tags (无) → save_tags RETURNING
        content_row = (
            9,                                    # id
            "news",                               # channel_type
            "ext-9",                              # content_id_ext
            "Nvidia beats expectations",          # title
            "Full article body ...",              # content
            "EODHD",                              # author_identifier
            "EODHD News",                         # author_name
            False, None, None,                    # is_retweet / retweeted_*
            False, None, None,                    # is_quote / quoted_*
            None,                                 # transcript
            {"symbols": ["NVDA.US", "TSLA.US"]},  # metadata
            ["Finance", "Earnings"],              # categories
        )
        cursor = FakeCursor(
            fetchone_results=[content_row, None, (601,)]
        )
        svc = _make_service(cursor)
        result = svc.map_api_tags(9)

        assert result["success"] is True
        assert result["skipped"] is False
        # INSERT 参数契约: ..., info_level(NULL), evidence_level('E2'), tagged_by
        insert_sql, insert_params = cursor.executed[-1]
        flat = _flat(insert_sql)
        assert "INSERT INTO subscription_content_tags" in flat
        assert "info_level, evidence_level" in flat
        assert insert_params[-3] is None        # info_level: 无文本依据留空
        assert insert_params[-2] == "E2"        # evidence_level: 聚合来源保守 E2

    def test_existing_tags_short_circuits(self):
        content_row = (
            9, "news", "ext-9", "T", "C", "EODHD", "EODHD",
            False, None, None, False, None, None, None, {}, [],
        )
        existing_row = tuple([99] * 20) + ("L2", "E2")  # 22 列 get_existing_tags
        cursor = FakeCursor(
            fetchone_results=[content_row, existing_row]
        )
        svc = _make_service(cursor)
        result = svc.map_api_tags(9)
        assert result["skipped"] is True
        # 只发生了两次 SELECT, 没有任何 INSERT
        assert all("INSERT" not in _flat(s) for s, _ in cursor.executed)
