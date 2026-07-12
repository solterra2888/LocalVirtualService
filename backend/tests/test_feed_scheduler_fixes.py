"""
针对 2026-07-12 Feeds Scheduler 一致性修复 (P0+P1) 的测试。

覆盖:
  B  - subscription_status_service (统一状态/history/utcnow helper)
  A3 - CCTV published_at 时区换算
  A2 - trigger_auto_news_processing Celery 化 + backfill_untagged_news_task 自愈
  A4 - backfill_podcast_transcripts_task 回填
  A1 - YouTube 字幕批处理后派发 finalize_youtube_feed_task
  A5 - Substack notes 孤儿回退归属

DB / Celery / 外部网络全部 mock; 仅验证业务逻辑分支与派发契约。
"""
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest import mock

import pytest


# ─────────────────────────────────────────────────────────────────────
# B. subscription_status_service
# ─────────────────────────────────────────────────────────────────────

class FakeCursor:
    """记录 execute 调用 + 可控 fetchall/fetchone 的假 cursor。"""

    def __init__(self, fetchall_results=None, fetchone_results=None):
        self.executed = []  # [(sql, params), ...]
        self._fetchall = list(fetchall_results or [])
        self._fetchone = list(fetchone_results or [(1,)])  # 默认: 订阅存在

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self._fetchall.pop(0) if self._fetchall else []

    def fetchone(self):
        return self._fetchone.pop(0) if self._fetchone else None

    def close(self):
        pass


def _flat(sql):
    return " ".join((sql or "").split())


class TestSubscriptionStatusService:
    def test_success_path_resets_failures_and_writes_history(self):
        from backend.services.subscription_status_service import record_fetch_outcome
        c = FakeCursor()
        record_fetch_outcome(c, 42, status="success",
                             items_fetched=10, items_new=7, items_duplicated=3,
                             execution_duration_ms=1234)
        sqls = [_flat(s) for s, _ in c.executed]
        assert any("consecutive_failures = 0" in s for s in sqls)
        assert any("total_items_fetched = total_items_fetched + %s" in s for s in sqls)
        assert any("INSERT INTO subscription_fetch_history" in s for s in sqls)
        # last_fetched_at 必须是参数 (aware UTC), 不能用 CURRENT_TIMESTAMP
        assert not any("CURRENT_TIMESTAMP" in s for s in sqls)
        upd_params = [p for s, p in c.executed if "UPDATE subscription_list" in s][0]
        ts = upd_params[0]
        assert isinstance(ts, datetime)
        assert ts.utcoffset() is not None  # aware
        assert ts.utcoffset().total_seconds() == 0  # UTC

    def test_failure_path_increments_failures(self):
        from backend.services.subscription_status_service import record_fetch_outcome
        c = FakeCursor()
        record_fetch_outcome(c, 42, status="failed", error="boom")
        sqls = [_flat(s) for s, _ in c.executed]
        assert any("consecutive_failures = consecutive_failures + 1" in s for s in sqls)
        assert any("last_fetch_items_count = 0" in s for s in sqls)
        assert any("INSERT INTO subscription_fetch_history" in s for s in sqls)

    def test_deleted_subscription_skips_history(self):
        from backend.services.subscription_status_service import record_fetch_outcome
        c = FakeCursor(fetchone_results=[None])  # SELECT 1 ... fetchone -> 订阅不存在
        record_fetch_outcome(c, 999, status="success", items_new=1)
        sqls = [_flat(s) for s, _ in c.executed]
        # UPDATE 状态照写, 但 history INSERT 不应发生
        assert not any("INSERT INTO subscription_fetch_history" in s for s in sqls)

    def test_record_fetch_outcome_auto_uses_connection_context(self):
        from backend.services import subscription_status_service as svc
        c = FakeCursor()

        @contextmanager
        def fake_ctx():
            class FakeConn:
                def cursor(self_inner):
                    return c
            yield FakeConn()

        fake_db = mock.MagicMock()
        fake_db.get_connection_context = fake_ctx
        with mock.patch("backend.database.db_manager.DatabaseManager", return_value=fake_db):
            svc.record_fetch_outcome_auto(7, status="success", items_new=3)
        assert any("UPDATE subscription_list" in _flat(s) for s, _ in c.executed)
        assert any("INSERT INTO subscription_fetch_history" in _flat(s) for s, _ in c.executed)

    def test_record_fetch_outcome_for_channel_writes_per_subscription(self):
        from backend.services import subscription_status_service as svc
        c = FakeCursor(
            fetchall_results=[[(10,), (20,), (30,)]],   # 3 个 enabled 订阅
            fetchone_results=[(1,), (1,), (1,)],        # 都存在
        )

        @contextmanager
        def fake_ctx():
            class FakeConn:
                def cursor(self_inner):
                    return c
            yield FakeConn()

        fake_db = mock.MagicMock()
        fake_db.get_connection_context = fake_ctx
        with mock.patch("backend.database.db_manager.DatabaseManager", return_value=fake_db):
            svc.record_fetch_outcome_for_channel("weibo", status="success", items_new=5)
        # 应有 3 次 UPDATE subscription_list (每个订阅一次)
        updates = [p for s, p in c.executed if "UPDATE subscription_list" in s]
        assert len(updates) == 3
        # 历史 INSERT 也 3 次
        inserts = [s for s, _ in c.executed if "INSERT INTO subscription_fetch_history" in s]
        assert len(inserts) == 3


# ─────────────────────────────────────────────────────────────────────
# A3. CCTV published_at 时区
# ─────────────────────────────────────────────────────────────────────

class TestCCTVPublishedAtTimezone:
    def test_beijing_date_converts_to_utc(self):
        from backend.services.cctv_news_feed_service import CCTVNewsFeedService
        svc = CCTVNewsFeedService()
        raw = svc._to_raw_data({"date": "2026-07-12", "title": "t", "content": "c", "url": "http://x"})
        # 北京 2026-07-12 午夜 = UTC 2026-07-11 16:00
        assert raw["date"].startswith("2026-07-11T16:00:00")
        assert raw["date"].endswith("+00:00")

    def test_no_date_falls_back_to_utcnow(self):
        from backend.services.cctv_news_feed_service import CCTVNewsFeedService
        svc = CCTVNewsFeedService()
        raw = svc._to_raw_data({"title": "t", "content": "c", "url": "http://x"})
        # fallback utcnow().isoformat() 含 +00:00
        assert "+00:00" in raw["date"]

    def test_bad_date_falls_back(self):
        from backend.services.cctv_news_feed_service import CCTVNewsFeedService
        svc = CCTVNewsFeedService()
        raw = svc._to_raw_data({"date": "not-a-date", "title": "t", "content": "c", "url": "http://x"})
        assert "+00:00" in raw["date"]


# ─────────────────────────────────────────────────────────────────────
# A2. News 打标触发 Celery 化 + 自愈 sweep
# ─────────────────────────────────────────────────────────────────────

class TestNewsTriggerAndBackfill:
    def test_trigger_auto_news_processing_is_celery_task(self):
        from backend.tasks.file_tasks import trigger_auto_news_processing
        # Celery Task 对象有 apply_async/delay, 且 name 正确
        assert hasattr(trigger_auto_news_processing, "apply_async")
        assert hasattr(trigger_auto_news_processing, "delay")
        from backend.celery_app import app
        assert "trigger_auto_news_processing" in app.tasks

    def test_backfill_untagged_news_dispatches_for_untagged_rows(self):
        from backend.tasks import feed_tagging_tasks as ft
        rows = [(101,), (102,), (103,)]

        @contextmanager
        def fake_ctx():
            class FakeCur:
                def __init__(self_inner):
                    self_inner._rows = list(rows)
                def execute(self_inner, sql, params=None):
                    pass
                def fetchall(self_inner):
                    return self_inner._rows
            class FakeConn:
                def cursor(self_inner):
                    return FakeCur()
            yield FakeConn()

        fake_db = mock.MagicMock()
        fake_db.get_connection_context = fake_ctx
        with mock.patch("backend.tasks.feed_tagging_tasks.DatabaseManager", return_value=fake_db), \
             mock.patch("backend.tasks.file_tasks.trigger_auto_news_processing") as trig:
            result = ft.backfill_untagged_news_task()
        assert result["success"] is True
        assert result["count"] == 3
        # 复用 trigger 路由, 传入 3 个 id
        trig.assert_called_once()
        called_ids = trig.call_args.args[0]
        assert set(called_ids) == {101, 102, 103}

    def test_backfill_untagged_news_no_rows_is_noop(self):
        from backend.tasks import feed_tagging_tasks as ft

        @contextmanager
        def fake_ctx():
            class FakeCur:
                def execute(self_inner, sql, params=None):
                    pass
                def fetchall(self_inner):
                    return []
            class FakeConn:
                def cursor(self_inner):
                    return FakeCur()
            yield FakeConn()

        fake_db = mock.MagicMock()
        fake_db.get_connection_context = fake_ctx
        with mock.patch("backend.tasks.feed_tagging_tasks.DatabaseManager", return_value=fake_db), \
             mock.patch("backend.tasks.file_tasks.trigger_auto_news_processing") as trig:
            result = ft.backfill_untagged_news_task()
        assert result["success"] is True
        assert result["count"] == 0
        trig.assert_not_called()

    def test_backfill_beat_entry_exists(self):
        from backend.celery_config import beat_schedule
        assert "backfill-untagged-news" in beat_schedule
        assert beat_schedule["backfill-untagged-news"]["task"] == "backfill_untagged_news_task"


# ─────────────────────────────────────────────────────────────────────
# A4. Podcast 批量回填
# ─────────────────────────────────────────────────────────────────────

class TestPodcastBackfill:
    def test_backfill_dispatches_asr_for_null_transcript_episodes(self):
        from backend.tasks import subscription_tasks as st
        rows = [
            (11, "Ep A", "https://audio/a.mp3"),
            (22, "Ep B", "https://audio/b.mp3"),
        ]

        @contextmanager
        def fake_ctx():
            class FakeCur:
                def __init__(self_inner):
                    self_inner._rows = list(rows)
                def execute(self_inner, sql, params=None):
                    pass
                def fetchall(self_inner):
                    return self_inner._rows
            class FakeConn:
                def cursor(self_inner):
                    return FakeCur()
            yield FakeConn()

        fake_db = mock.MagicMock()
        fake_db.get_connection_context = fake_ctx
        with mock.patch("backend.database.db_manager.DatabaseManager", return_value=fake_db), \
             mock.patch("backend.services.podcast_subscription_service"
                        ".PodcastSubscriptionService._dispatch_asr_tasks") as dispatch:
            result = st.backfill_podcast_transcripts_task(subscription_id=5, batch_limit=50)
        assert result["success"] is True
        assert result["dispatched"] == 2
        dispatch.assert_called_once()
        items = dispatch.call_args.args[0]
        assert items == [
            {"content_db_id": 11, "audio_url": "https://audio/a.mp3", "episode_title": "Ep A"},
            {"content_db_id": 22, "audio_url": "https://audio/b.mp3", "episode_title": "Ep B"},
        ]

    def test_backfill_no_rows_is_noop(self):
        from backend.tasks import subscription_tasks as st

        @contextmanager
        def fake_ctx():
            class FakeCur:
                def execute(self_inner, sql, params=None):
                    pass
                def fetchall(self_inner):
                    return []
            class FakeConn:
                def cursor(self_inner):
                    return FakeCur()
            yield FakeConn()

        fake_db = mock.MagicMock()
        fake_db.get_connection_context = fake_ctx
        with mock.patch("backend.database.db_manager.DatabaseManager", return_value=fake_db), \
             mock.patch("backend.services.podcast_subscription_service"
                        ".PodcastSubscriptionService._dispatch_asr_tasks") as dispatch:
            result = st.backfill_podcast_transcripts_task()
        assert result["success"] is True
        assert result["dispatched"] == 0
        dispatch.assert_not_called()


# ─────────────────────────────────────────────────────────────────────
# A1. YouTube 字幕批处理后派发 finalize
# ─────────────────────────────────────────────────────────────────────

class TestYouTubeFinalizeDispatch:
    def test_batch_task_dispatches_finalize_per_transcript(self):
        from backend.tasks import subscription_tasks as st
        video_items = [
            {"content_db_id": 111, "video_id": "v1", "video_url": "https://u/1"},
            {"content_db_id": 222, "video_id": "v2", "video_url": "https://u/2"},
            {"content_db_id": 333, "video_id": "v3", "video_url": "https://u/3"},
        ]
        with mock.patch("backend.services.youtube_service.YouTubeService") as YT, \
             mock.patch("backend.services.subscription_content_service"
                        ".SubscriptionContentService") as CS, \
             mock.patch("backend.tasks.subscription_tasks.finalize_youtube_feed_task") as finalize:
            YT.fetch_transcript.return_value = {"language": "en"}
            YT.get_full_text.return_value = "full text"
            result = st.fetch_youtube_transcripts_batch_task(video_items)

        assert result["success"] is True
        assert result["transcripts_ok"] == 3
        # 每条成功字幕派发一次 finalize, 全部路由到 file_processing
        assert finalize.apply_async.call_count == 3
        calls = finalize.apply_async.call_args_list
        dispatched_ids = sorted(c.kwargs["args"][0] for c in calls)
        assert dispatched_ids == [111, 222, 333]
        for c in calls:
            assert c.kwargs.get("queue") == "file_processing"

    def test_batch_task_skips_finalize_on_transcript_failure(self):
        from backend.tasks import subscription_tasks as st
        video_items = [
            {"content_db_id": 111, "video_id": "v1", "video_url": "https://u/1"},
            {"content_db_id": 222, "video_id": "v2", "video_url": "https://u/2"},
        ]

        def fake_fetch(url):
            if "u/2" in url:
                raise RuntimeError("no transcript")
            return {"language": "en"}

        with mock.patch("backend.services.youtube_service.YouTubeService") as YT, \
             mock.patch("backend.services.subscription_content_service"
                        ".SubscriptionContentService"), \
             mock.patch("backend.tasks.subscription_tasks.finalize_youtube_feed_task") as finalize:
            YT.fetch_transcript.side_effect = fake_fetch
            YT.get_full_text.return_value = "full text"
            result = st.fetch_youtube_transcripts_batch_task(video_items)

        assert result["success"] is True
        assert result["transcripts_ok"] == 1
        assert result["transcripts_failed"] == 1
        # 只有成功的那条派发 finalize
        assert finalize.apply_async.call_count == 1
        assert finalize.apply_async.call_args.kwargs["args"][0] == 111


# ─────────────────────────────────────────────────────────────────────
# A5. Substack notes 孤儿回退归属
# ─────────────────────────────────────────────────────────────────────

class TestSubstackOrphanFallback:
    @staticmethod
    def _make_author(pub_url):
        return {
            "substack_user_id": 9001,
            "author_handle": "notesonly",
            "author_name": "Notes Only",
            "publication_url": pub_url,
            "avatar": "",
            "recent_note_ids": [],
            "backfill_complete": True,
        }

    def _run(self, monkeypatch, user_ids_by_pub, author_pub_url):
        """跑一次 _crawl_notes_for_author, 返回传给 _store_batch 的 user_ids。"""
        import backend.services.substack_subscription_service as sss

        captured = {}

        def fake_fetch_notes(auth, uid, cursor=None, limit=10):
            # 第一页返回 1 条 note, 无 nextCursor -> 循环一页即停
            return {"items": [{"id": "note-1", "body": "x"}], "nextCursor": None}

        def fake_note_to_row(it, profile):
            return {"note_id": "note-1", "content": "x"}

        def fake_store_batch(content_service, collected, user_ids, name, report=None):
            captured["user_ids"] = list(user_ids)
            captured["pub_urls"] = [r.get("publication_url") for r in collected]
            return 1, 0, []

        @contextmanager
        def fake_get_conn():
            class FakeConn:
                def cursor(self_inner):
                    class C:
                        def execute(self_inner, *a, **k):
                            pass
                        def __enter__(self_inner):
                            return self_inner
                        def __exit__(self_inner, *a):
                            pass
                    return C()
                def commit(self_inner):
                    pass
            yield FakeConn()

        monkeypatch.setattr(sss, "fetch_author_notes_via_auth", fake_fetch_notes)
        monkeypatch.setattr(sss, "_note_to_row", fake_note_to_row)
        monkeypatch.setattr(sss, "_store_batch", fake_store_batch)
        monkeypatch.setattr(sss, "_trigger_tagging", lambda *a, **k: None)
        monkeypatch.setattr(sss, "_save_author_state", lambda *a, **k: None)
        monkeypatch.setattr(sss, "_get_conn", fake_get_conn)
        monkeypatch.setattr(sss.time, "sleep", lambda *a: None)

        svc = sss.SubstackSubscriptionService()
        svc._crawl_notes_for_author(
            auth=mock.MagicMock(),
            author=self._make_author(author_pub_url),
            user_ids_by_pub=user_ids_by_pub,
            report=None, idx=0, total=1,
        )
        return captured

    def test_orphan_author_attributed_to_all_substack_users(self, monkeypatch):
        # followed author 的 pub 不在任何用户订阅中 -> 回退为所有启用 substack 订阅用户
        user_ids_by_pub = {"https://b.substack.com": [5, 6]}
        got = self._run(monkeypatch, user_ids_by_pub, "https://notesonly.substack.com")
        assert got["user_ids"] == [5, 6]

    def test_subscribed_pub_with_www_mismatch_matches_via_normalization(self, monkeypatch):
        # 已订阅 pub, 但 author.publication_url 带 www -> 归一化后命中, 不走回退
        user_ids_by_pub = {"https://acme.substack.com": [1, 2]}
        got = self._run(monkeypatch, user_ids_by_pub, "https://www.acme.substack.com")
        assert got["user_ids"] == [1, 2]

    def test_no_substack_subs_at_all_yields_empty(self, monkeypatch):
        got = self._run(monkeypatch, {}, "https://notesonly.substack.com")
        assert got["user_ids"] == []

    def test_capitalflows_notes_align_to_subscribed_substack_url(self, monkeypatch):
        user_ids_by_pub = {"https://capitalflows.substack.com": [2]}
        import backend.services.substack_subscription_service as sss

        author = self._make_author("https://capitalflowsresearch.com")
        author["author_handle"] = "capitalflows"
        captured = {}

        def fake_fetch_notes(auth, uid, cursor=None, limit=10):
            return {"items": [{"entity_key": "c-1", "comment": {"body": "x"}}], "nextCursor": None}

        def fake_store_batch(content_service, collected, user_ids, name, report=None):
            captured["user_ids"] = list(user_ids)
            captured["pub_urls"] = [r.get("publication_url") for r in collected]
            return 1, 0, []

        @contextmanager
        def fake_get_conn():
            class FakeConn:
                def cursor(self_inner):
                    class C:
                        def execute(self_inner, *a, **k):
                            pass
                        def __enter__(self_inner):
                            return self_inner
                        def __exit__(self_inner, *a):
                            pass
                    return C()
                def commit(self_inner):
                    pass
            yield FakeConn()

        monkeypatch.setattr(sss, "fetch_author_notes_via_auth", fake_fetch_notes)
        monkeypatch.setattr(sss, "_store_batch", fake_store_batch)
        monkeypatch.setattr(sss, "_trigger_tagging", lambda *a, **k: None)
        monkeypatch.setattr(sss, "_save_author_state", lambda *a, **k: None)
        monkeypatch.setattr(sss, "_get_conn", fake_get_conn)
        monkeypatch.setattr(sss.time, "sleep", lambda *a: None)

        sss.SubstackSubscriptionService()._crawl_notes_for_author(
            auth=mock.MagicMock(),
            author=author,
            user_ids_by_pub=user_ids_by_pub,
            report=None, idx=0, total=1,
        )
        assert captured["user_ids"] == [2]
        assert captured["pub_urls"] == ["https://capitalflows.substack.com"]


# ─────────────────────────────────────────────────────────────────────
# 接线契约: 确认 7 源 / ASR 路径 / News 调用方都接入了修复
# (源码级断言, 防止漏接/回归)
# ─────────────────────────────────────────────────────────────────────

_BASE = "backend/services"
_TASKS = "backend/tasks"


def _src(rel):
    with open(rel, encoding="utf-8") as f:
        return f.read()


class TestWiringContracts:
    def test_seven_sources_reference_status_helper(self):
        """P1: 7 个抓取源都应引用 subscription_status_service (统一状态/history 写入)."""
        files = {
            "twitter": f"{_BASE}/twitter_subscription_service.py",
            "youtube": f"{_BASE}/youtube_subscription_service.py",
            "podcast": f"{_BASE}/podcast_subscription_service.py",
            "zsxq": f"{_BASE}/zsxq_subscription_service.py",
            "substack": f"{_BASE}/substack_subscription_service.py",
            "weibo": f"{_BASE}/weibo_subscription_service.py",
            "news(scheduler)": f"{_BASE}/scheduler_service.py",
        }
        missing = [name for name, path in files.items()
                   if "subscription_status_service" not in _src(path)]
        assert not missing, f"未接入 subscription_status_service 的源: {missing}"

    def test_youtube_and_podcast_no_longer_use_current_timestamp(self):
        """P1-4: YouTube/Podcast 不应再用 CURRENT_TIMESTAMP 写 last_fetched_at."""
        for path in (f"{_BASE}/youtube_subscription_service.py",
                     f"{_BASE}/podcast_subscription_service.py"):
            src = _src(path)
            # 允许出现在注释里, 但 UPDATE subscription_list ... CURRENT_TIMESTAMP 已移除
            assert "last_fetched_at = CURRENT_TIMESTAMP" not in src, \
                f"{path} 仍在用 CURRENT_TIMESTAMP 写 last_fetched_at"

    def test_youtube_asr_path_dispatches_finalize(self):
        """A1: YouTube ASR 兜底路径 (file_tasks) 写完字幕应派发 finalize_youtube_feed_task."""
        src = _src(f"{_TASKS}/file_tasks.py")
        assert "finalize_youtube_feed_task" in src, "ASR 路径未派发 finalize"

    def test_news_services_dispatch_trigger_via_apply_async(self):
        """A2: EODHD/CCTV 应以 .apply_async 派发 trigger_auto_news_processing (持久化)."""
        for path in (f"{_BASE}/eodhd_news_feed_service.py",
                     f"{_BASE}/cctv_news_feed_service.py"):
            src = _src(path)
            assert "trigger_auto_news_processing.apply_async" in src, \
                f"{path} 未用 apply_async 派发 trigger_auto_news_processing"

    def test_cctv_uses_day_window_utc(self):
        """A3: CCTV published_at 应使用 day_window_utc 做北京->UTC 换算."""
        src = _src(f"{_BASE}/cctv_news_feed_service.py")
        assert "day_window_utc" in src
        assert '+ "+00:00"' not in src, "CCTV 仍残留 naive +00:00 误标"

    def test_backfill_tasks_registered_and_routed(self):
        from backend.celery_app import app
        from backend.celery_config import task_routes
        for name, queue in [
            ("trigger_auto_news_processing", "file_processing"),
            ("backfill_untagged_news_task", "file_processing"),
            ("backfill_podcast_transcripts_task", "data_fetching"),
        ]:
            assert name in app.tasks, f"{name} 未注册为 Celery task"
            assert task_routes.get(name, {}).get("queue") == queue, \
                f"{name} 路由错误: {task_routes.get(name)}"


# ─────────────────────────────────────────────────────────────────────
# 真实 DB 集成测试 (只读) —— 验证回填 SQL 的列引用/语法对实际 schema 有效
# (mock cursor 不校验 SQL, 这一层专门抓 "列名写错" 这类 bug)
# ─────────────────────────────────────────────────────────────────────

def _db_reachable():
    try:
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        db = DatabaseManager(get_database_config())
        with db.get_connection_context() as conn:
            conn.cursor().execute("SELECT 1")
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _db_reachable(), reason="无可达 DB")
class TestBackfillSqlAgainstRealSchema:
    def test_news_backfill_select_executes(self):
        """与 backfill_untagged_news_task 完全一致的 SELECT 必须能对真实 schema 跑通."""
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        db = DatabaseManager(get_database_config())
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT sc.id
                FROM subscription_content sc
                LEFT JOIN subscription_content_tags t
                       ON t.subscription_content_id = sc.id
                WHERE sc.channel_type = 'news'
                  AND t.id IS NULL
                  AND sc.fetched_at < now() - ('15' || ' minutes')::interval
                ORDER BY sc.fetched_at DESC
                LIMIT %s
            """, (1,))
            cur.fetchall()  # 不关心结果, 只关心不抛

    def test_podcast_backfill_select_executes(self):
        """与 backfill_podcast_transcripts_task 完全一致的 SELECT 必须能跑通."""
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        db = DatabaseManager(get_database_config())
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT sc.id, sc.title, sc.metadata->>'audio_url' AS audio_url
                FROM subscription_content sc
                WHERE sc.channel_type = 'podcast'
                  AND (sc.transcript IS NULL OR sc.transcript = '')
                  AND sc.metadata->>'audio_url' IS NOT NULL
                  AND sc.metadata->>'audio_url' <> ''
                ORDER BY sc.published_at DESC
                LIMIT %s
            """, (1,))
            cur.fetchall()


# ─────────────────────────────────────────────────────────────────────
# P2-2: 移除 fetch_interval_minutes / next_fetch_at 后, 验证 SubscriptionResponse
# 的位置索引 row[N] 映射仍正确 (对真实行用 RealDictCursor 做 ground truth 比对)
# ─────────────────────────────────────────────────────────────────────

_P22_SELECT = """
    SELECT
        id, user_id, channel_type, channel_identifier,
        channel_name, channel_description, channel_avatar, enabled, priority,
        max_items_per_fetch, filter_config,
        metadata, created_at, updated_at, last_fetched_at,
        total_items_fetched, last_fetch_items_count,
        consecutive_failures, unread_count, sort_order
    FROM subscription_list
    ORDER BY id
    LIMIT 1
"""


@pytest.mark.skipif(not _db_reachable(), reason="无可达 DB")
class TestSubscriptionListMappingAfterDeadFieldRemoval:
    def test_positional_mapping_matches_dict_ground_truth(self):
        """位置 row[N] 映射 (与 api/subscriptions.py 的 SubscriptionResponse 一致)
        必须与按列名取值(RealDictCursor)逐字段一致 —— 直接抓 renumber 错位."""
        import psycopg2.extras
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        db = DatabaseManager(get_database_config())
        with db.get_connection_context() as conn:
            cur = conn.cursor()
            cur.execute(_P22_SELECT)
            row = cur.fetchone()
            assert row is not None, "需要至少一条 subscription_list 行来验证映射"
            # 与 subscriptions.py 中 SubscriptionResponse(...) 的 row[N] 完全一致
            mapped = {
                "id": row[0], "user_id": row[1], "channel_type": row[2],
                "channel_identifier": row[3], "channel_name": row[4],
                "channel_description": row[5], "channel_avatar": row[6],
                "enabled": row[7], "priority": row[8],
                "max_items_per_fetch": row[9], "filter_config": row[10] or {},
                "metadata": row[11] or {}, "created_at": row[12],
                "updated_at": row[13], "last_fetched_at": row[14],
                "total_items_fetched": row[15], "last_fetch_items_count": row[16],
                "consecutive_failures": row[17], "unread_count": row[18] or 0,
                "sort_order": row[19],
            }
            dcur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            dcur.execute("SELECT * FROM subscription_list WHERE id = %s", (mapped["id"],))
            truth = dict(dcur.fetchone())

        # 逐字段比对, 重点覆盖 renumber 影响区 (max_items_per_fetch 及之后)
        for key in ["priority", "max_items_per_fetch", "filter_config", "metadata",
                    "last_fetched_at", "total_items_fetched", "last_fetch_items_count",
                    "consecutive_failures", "unread_count", "sort_order",
                    "enabled", "channel_name", "channel_identifier", "channel_avatar"]:
            pv, tv = mapped[key], truth.get(key)
            ok = pv == tv or (pv in ({}, None) and tv in ({}, None))
            assert ok, f"字段 {key} 位置映射错位: positional={pv!r} ground_truth={tv!r}"

    def test_no_backend_code_references_to_dead_fields(self):
        """移除后 subscriptions.py 不应再出现这两个死字段名."""
        with open("backend/api/subscriptions.py", encoding="utf-8") as f:
            src = f.read()
        assert "fetch_interval_minutes" not in src
        assert "next_fetch_at" not in src

    def test_kept_fields_still_referenced(self):
        """priority / max_items_per_fetch 必须仍被引用 (确认没有误删)."""
        with open("backend/api/subscriptions.py", encoding="utf-8") as f:
            src = f.read()
        assert "priority" in src
        assert "max_items_per_fetch" in src




