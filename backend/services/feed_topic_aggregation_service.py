# -*- coding: utf-8 -*-
"""
Feeds Dashboard 主题聚合服务 (P0, 零 LLM).

把 auto-tag 产物 (subscription_content_tags) 按「主题 × 重要性」重新组织,
解决用户看不完每日订阅信息的问题:

  1. 拉取窗口内 (北京自然日) 全部已打标内容 (concepts 非空, 全局内容池)
  2. concepts 归一化: feed_topics 词典 (topic_key/name_cn/name_en/aliases)
     精确匹配 (strip + lower); 未命中的高频词自动建 topic
     (source='auto', needs_review=true)
  3. importance 评分:
     - twitter 且有 tweet_signal (tweet_signal_v1 模板) → 直接采信 LLM importance
     - 其余渠道 → 规则评分 (见 _score_item, 权重表写在常量里)
  4. 全量重建当日 feed_topic_daily_items (DELETE + 批量 INSERT, 幂等;
     评分随 engagement 更新而变化, 重复执行总是收敛到最新状态)

设计边界:
  - 不依赖 content_theme_matches / embedding; 主题从内容标签涌现
  - 条目命中多主题时全部入表, 仅 concepts 顺序首位的主题计 is_primary_topic
  - report_date 为北京自然日标签 (与摘要类调度任务统一口径,
    见 docs/instructions/SUMMARY_SCHEDULER_DEVELOPMENT_GUIDE.md §2.2)
  - P1 将在同一调度任务 (feeds_topic_summary) 中追加 Phase A 主题日摘要
"""

import json
import math
from datetime import date as date_type
from typing import Any, Dict, List, Optional, Tuple

from backend.database.db_manager import DatabaseManager
from backend.utils.datetime_utils import day_window_utc, today_in_tz
from backend.utils.logger import get_logger

logger = get_logger("feed_topic_aggregation_service")

# report_date 标签日时区 (与摘要类任务统一: 北京自然日)
DASHBOARD_TZ = "Asia/Shanghai"

# 未命中词典的概念词在当日窗口内出现 >= N 次才自动建 topic (避免长尾噪声)
AUTO_TOPIC_MIN_COUNT = 3

# 自动建 topic 的词长下限 (单字符噪声)
AUTO_TOPIC_MIN_LEN = 2

# 规则评分 → importance 分档阈值
IMPORTANCE_HIGH_THRESHOLD = 0.75
IMPORTANCE_MEDIUM_THRESHOLD = 0.55

# twitter tweet_signal.importance → 数值分映射 (保证 ORDER BY score 跨渠道可比)
_TWEET_SIGNAL_SCORE = {"high": 0.90, "medium": 0.65, "low": 0.35}

# 规则评分权重表 (非 twitter 渠道; 总分上界 1.0):
#   base 0.50
#   + 0.12 有 tickers (提及具体标的)
#   + 0.06 有 people (关键人物参与)
#   + 0.03 有 countries
#   + 0.04 有 LLM summary (说明被打标链路完整处理)
#   + 0.08 * confidence (LLM 自评置信度)
#   + engagement 对数加分, 上限 0.15 (twitter: engagement_count; youtube: view_count;
#     其余渠道: engagement_count 兜底 heat_level)
#   + heat_level * 0.006, 上限 0.06 (News/Twitter 热度等级)
#   + 0.04 youtube/podcast 且有 transcript (实质性长内容)
# 示例: 无任何信号的普通条目 ≈ 0.50-0.56 (low);
#       带 2 tickers + summary + confidence 0.85 + 万级互动 ≈ 0.89 (high)
_SCORE_BASE = 0.50
_SCORE_TICKERS = 0.12
_SCORE_PEOPLE = 0.06
_SCORE_COUNTRIES = 0.03
_SCORE_SUMMARY = 0.04
_SCORE_CONFIDENCE_MAX = 0.08
_SCORE_ENGAGEMENT_MAX = 0.15
_SCORE_HEAT_MAX = 0.06
_SCORE_TRANSCRIPT = 0.04


def _norm_key(text: Any) -> str:
    """归一化匹配键: strip + lower (中文不受 lower 影响, 英文别名大小写不敏感)."""
    return str(text or "").strip().lower()


class FeedTopicAggregationService:
    """Feeds Dashboard 主题聚合器 (零 LLM, 幂等, 每次全量重建当日快照)."""

    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        from backend.utils.database_config import get_database_config
        self.db_manager = DatabaseManager(db_config or get_database_config())

    # ─── 主入口 ────────────────────────────────────────────────

    def aggregate(
        self,
        report_date: Optional[date_type] = None,
        tz: str = DASHBOARD_TZ,
    ) -> Dict[str, Any]:
        """聚合并全量重建某日的 feed_topic_daily_items.

        Args:
            report_date: 北京自然日标签; None = 今天(tz 口径)
            tz: report_date 的标签时区

        Returns:
            {'report_date', 'window_start', 'window_end', 'content_count',
             'topics_count', 'rows_written', 'auto_topics_created': [...], 'success'}
        """
        if report_date is None:
            report_date = today_in_tz(tz)

        # day_window_utc 返回 aware UTC — 必须保持 aware 直接传参:
        # psycopg2 适配器会把 naive 误判为 HK 本地时间再转 UTC (偏 8h), 见其 docstring
        ws, we = day_window_utc(report_date, tz)

        conn = self.db_manager.get_connection()
        if not conn:
            return {"success": False, "error": "db_connection_failed", "report_date": str(report_date)}

        try:
            cursor = conn.cursor()

            rows = self._fetch_tagged_content(cursor, ws, we)
            exact_lookup, substring_index = self._load_topic_lookup(cursor)
            auto_created = self._auto_create_topics_for_unmatched(
                cursor, rows, exact_lookup, substring_index
            )

            snapshot_rows, topics_seen, content_matched, learned = self._build_snapshot_rows(
                rows, exact_lookup, substring_index
            )

            # 幂等: 当日全量重建 (published_at 不变, 但词典禁用/评分变化需收敛)
            cursor.execute(
                "DELETE FROM feed_topic_daily_items WHERE report_date = %s",
                (report_date,),
            )
            self._bulk_insert(cursor, report_date, snapshot_rows)
            # 包含式命中的词学习进别名 (下次精确命中, 词典自增长)
            self._learn_aliases(cursor, learned)
            conn.commit()

            result = {
                "success": True,
                "report_date": str(report_date),
                "window_start": ws.isoformat() + "Z",
                "window_end": we.isoformat() + "Z",
                "content_count": len(rows),
                "content_matched": content_matched,
                "topics_count": len(topics_seen),
                "rows_written": len(snapshot_rows),
                "auto_topics_created": auto_created,
            }
            logger.info(
                "Feeds Dashboard 聚合完成",
                report_date=result["report_date"],
                content_count=result["content_count"],
                content_matched=result["content_matched"],
                topics_count=result["topics_count"],
                rows_written=result["rows_written"],
                auto_topics=len(result["auto_topics_created"]),
            )
            return result
        except Exception as exc:
            conn.rollback()
            logger.error(f"Feeds Dashboard 聚合失败 ({report_date}): {exc}")
            return {"success": False, "error": str(exc), "report_date": str(report_date)}
        finally:
            conn.close()

    def aggregate_days(self, days: int = 7, tz: str = DASHBOARD_TZ) -> List[Dict[str, Any]]:
        """从今天起往前聚合 N 天 (默认 7, 覆盖 Dashboard「近一周」查询 + 跨午夜尾段)."""
        from datetime import timedelta
        today = today_in_tz(tz)
        results = []
        for offset in range(days):
            results.append(self.aggregate(today - timedelta(days=offset), tz=tz))
        return results

    # ─── 读路径匹配器 (词云折并用) ─────────────────────────────

    def build_topic_matcher(self) -> Tuple[Optional[Any], Dict[int, str]]:
        """构造 word → topic_id 匹配器, 与聚合归一化严格同口径 (精确→包含式最长命中).

        供 API 读路径把词云原始概念词折并到主题 (「地缘政治风险」→「地缘政治」),
        避免同一主题的 LLM 变体在词云里重复出现.

        Returns:
            (match_fn | None, {topic_id: name_cn}); DB 不可用时 match_fn=None, 调用方降级为不折并
        """
        from typing import Callable

        conn = self.db_manager.get_connection()
        if not conn:
            return None, {}
        try:
            cursor = conn.cursor()
            exact_lookup, substring_index = self._load_topic_lookup(cursor)
            cursor.execute("SELECT id, name_cn FROM feed_topics WHERE is_enabled = true")
            names = {tid: name for tid, name in cursor.fetchall()}

            def match(word: str) -> Optional[int]:
                topic_id, _ = self._match_topic(_norm_key(word), exact_lookup, substring_index)
                return topic_id

            return match, names
        except Exception as exc:
            logger.warning(f"build_topic_matcher 构建失败 (词云将不折并): {exc}")
            return None, {}
        finally:
            conn.close()

    # ─── 数据获取 ──────────────────────────────────────────────

    def _fetch_tagged_content(self, cursor, ws, we) -> List[Dict[str, Any]]:
        """窗口内全部已打标内容 (全局内容池, 不按用户过滤 — 快照全局共享)."""
        cursor.execute(
            """
            SELECT
                sc.id,
                sc.channel_type,
                sc.heat_level,
                sc.engagement_count,
                sc.view_count,
                COALESCE(LENGTH(sc.transcript), 0) AS transcript_len,
                sct.concepts,
                sct.ticker,
                sct.people,
                sct.countries,
                sct.confidence,
                sct.summary,
                sct.additional_tags,
                sct.tag_schema_version
            FROM subscription_content sc
            JOIN subscription_content_tags sct
                ON sct.subscription_content_id = sc.id
            WHERE sc.published_at >= %s
              AND sc.published_at < %s
              AND sct.concepts IS NOT NULL
              AND jsonb_array_length(sct.concepts) > 0
            """,
            (ws, we),
        )
        out: List[Dict[str, Any]] = []
        for row in cursor.fetchall():
            (
                content_id, channel_type, heat_level, engagement_count, view_count,
                transcript_len, concepts_raw, ticker_raw, people_raw, countries_raw,
                confidence, summary, additional_tags, tag_schema_version,
            ) = row
            out.append({
                "content_id": content_id,
                "channel_type": channel_type or "",
                "heat_level": heat_level or 0,
                "engagement_count": engagement_count or 0,
                "view_count": view_count or 0,
                "transcript_len": transcript_len or 0,
                "concepts": _ensure_str_list(concepts_raw),
                "tickers": _ensure_str_list(ticker_raw),
                "people": people_raw if isinstance(people_raw, list) else [],
                "countries": _ensure_str_list(countries_raw),
                "confidence": confidence,
                "summary": summary,
                "additional_tags": _parse_jsonb_dict(additional_tags),
                "tag_schema_version": tag_schema_version or "",
            })
        return out

    # ─── 词典归一化 ────────────────────────────────────────────

    def _load_topic_lookup(self, cursor) -> Tuple[Dict[str, int], List[Tuple[str, int]]]:
        """载入启用的主题词典.

        Returns:
            exact_lookup: {归一化键(topic_key/name_cn/name_en/alias): topic_id} 精确等值
            substring_index: [(归一化键, topic_id)] 供包含式兜底匹配 (防 LLM 长尾变体)
        """
        cursor.execute(
            """
            SELECT id, topic_key, name_cn, name_en, aliases
            FROM feed_topics
            WHERE is_enabled = true
            """
        )
        exact_lookup: Dict[str, int] = {}
        substring_index: List[Tuple[str, int]] = []
        for topic_id, topic_key, name_cn, name_en, aliases_raw in cursor.fetchall():
            keys = [topic_key, name_cn, name_en or ""]
            for alias in _ensure_str_list(aliases_raw):
                keys.append(alias)
            for key in keys:
                normalized = _norm_key(key)
                if not normalized:
                    continue
                # curated 显式定义优先 (后写覆盖会把 curated 冲掉, 这里跳过已有)
                if normalized not in exact_lookup:
                    exact_lookup[normalized] = topic_id
                substring_index.append((normalized, topic_id))
        return exact_lookup, substring_index

    @staticmethod
    def _match_topic(
        concept_norm: str,
        exact_lookup: Dict[str, int],
        substring_index: List[Tuple[str, int]],
    ) -> Tuple[Optional[int], bool]:
        """概念词 → topic. 精确等值优先, 其次包含式兜底 (词典键是概念词的子串).

        典型场景: LLM 长尾变体「地缘政治风险」「能源地缘政治」均包含词典键「地缘政治」
        → 归并到同一 topic, 而不是各自 auto 新建.

        Returns:
            (topic_id | None, is_containment) — is_containment=True 表示经包含式命中
            (供调用方做别名自动学习)
        """
        if not concept_norm:
            return None, False
        exact = exact_lookup.get(concept_norm)
        if exact is not None:
            return exact, False

        # 包含式: 键必须是概念词的真子串 (等值已被精确分支覆盖), 取最长命中 (最具体)
        # 长度下限: 中文键 ≥2 字, ASCII 键 ≥3 字 (避免 "AI" ⊆ "maintain" 类误命中)
        min_len = 3 if concept_norm.isascii() else 2
        best_id: Optional[int] = None
        best_len = min_len - 1
        for key, topic_id in substring_index:
            if len(key) <= best_len or len(key) >= len(concept_norm):
                continue
            if key in concept_norm:
                best_id, best_len = topic_id, len(key)
        return best_id, best_id is not None

    def _auto_create_topics_for_unmatched(
        self,
        cursor,
        rows: List[Dict[str, Any]],
        exact_lookup: Dict[str, int],
        substring_index: List[Tuple[str, int]],
    ) -> List[str]:
        """统计未命中词典 (精确+包含式均未中) 的概念词频, 高频词自动建 topic."""
        unmatched: Dict[str, int] = {}
        for row in rows:
            for concept in row["concepts"]:
                concept = concept.strip()
                if not concept:
                    continue
                topic_id, _ = self._match_topic(_norm_key(concept), exact_lookup, substring_index)
                if topic_id is not None:
                    continue
                unmatched[concept] = unmatched.get(concept, 0) + 1

        created: List[str] = []
        for concept, count in sorted(unmatched.items(), key=lambda x: -x[1]):
            if count < AUTO_TOPIC_MIN_COUNT or len(concept) < AUTO_TOPIC_MIN_LEN:
                continue
            cursor.execute(
                """
                INSERT INTO feed_topics (topic_key, name_cn, category, source, needs_review)
                VALUES (%s, %s, '未分类', 'auto', true)
                ON CONFLICT (topic_key) DO NOTHING
                RETURNING id
                """,
                (concept, concept),
            )
            inserted = cursor.fetchone()
            if inserted:
                exact_lookup[_norm_key(concept)] = inserted[0]
                substring_index.append((_norm_key(concept), inserted[0]))
                created.append(concept)
        if created:
            logger.info(f"自动新建 {len(created)} 个主题 (needs_review): {created[:20]}")
        return created

    def _learn_aliases(self, cursor, learned: Dict[int, set]) -> None:
        """把包含式命中的概念词学习进 topic 别名, 下次可直接精确命中.

        词典随真实 LLM 词汇自增长; 去重按归一化键比较, 保留原词形.
        """
        if not learned:
            return
        for topic_id, concepts in learned.items():
            if not concepts:
                continue
            cursor.execute(
                "SELECT aliases FROM feed_topics WHERE id = %s",
                (topic_id,),
            )
            row = cursor.fetchone()
            if not row:
                continue
            existing = _ensure_str_list(row[0])
            existing_norm = {_norm_key(a) for a in existing}
            additions = [c for c in sorted(concepts) if _norm_key(c) not in existing_norm]
            if not additions:
                continue
            merged = existing + additions[:20]  # 单 topic 学习上限, 防无限膨胀
            cursor.execute(
                "UPDATE feed_topics SET aliases = %s WHERE id = %s",
                (json.dumps(merged, ensure_ascii=False), topic_id),
            )
        logger.info(f"别名自动学习: {sum(len(v) for v in learned.values())} 个候选写入 {len(learned)} 个 topic")

    # ─── 评分与快照 ────────────────────────────────────────────

    def _build_snapshot_rows(
        self,
        rows: List[Dict[str, Any]],
        exact_lookup: Dict[str, int],
        substring_index: List[Tuple[str, int]],
    ) -> Tuple[List[Tuple[Any, ...]], set, int, Dict[int, set]]:
        """逐条评分 + 归属主题, 产出 (topic_id, content_id, is_primary, importance, score) 行.

        同时收集包含式命中的 (topic_id → 原词) 供别名学习.
        """
        snapshot: List[Tuple[Any, ...]] = []
        topics_seen: set = set()
        content_matched = 0
        learned: Dict[int, set] = {}

        for row in rows:
            # concepts 顺序即 LLM 排序的重要性顺序; 去重保持首次出现位置
            matched_topic_ids: List[int] = []
            seen_topics: set = set()
            for concept in row["concepts"]:
                topic_id, by_containment = self._match_topic(
                    _norm_key(concept), exact_lookup, substring_index
                )
                if topic_id is None:
                    continue
                if by_containment:
                    learned.setdefault(topic_id, set()).add(concept.strip())
                if topic_id not in seen_topics:
                    seen_topics.add(topic_id)
                    matched_topic_ids.append(topic_id)
            if not matched_topic_ids:
                continue

            content_matched += 1
            score = self._score_item(row)
            importance = _score_to_importance(score)

            for position, topic_id in enumerate(matched_topic_ids):
                snapshot.append((
                    topic_id,
                    row["content_id"],
                    position == 0,  # 首个命中主题计主
                    importance,
                    round(score, 4),
                ))
                topics_seen.add(topic_id)

        return snapshot, topics_seen, content_matched, learned

    def _score_item(self, row: Dict[str, Any]) -> float:
        """单条内容数值分 (0-1). twitter 有 tweet_signal 时直接采信 LLM 判断."""
        tweet_signal = (row.get("additional_tags") or {}).get("tweet_signal") or {}
        llm_importance = tweet_signal.get("importance") if isinstance(tweet_signal, dict) else None
        if row["channel_type"] == "twitter" and llm_importance in _TWEET_SIGNAL_SCORE:
            return _TWEET_SIGNAL_SCORE[llm_importance]

        score = _SCORE_BASE
        if row["tickers"]:
            score += _SCORE_TICKERS
        if row["people"]:
            score += _SCORE_PEOPLE
        if row["countries"]:
            score += _SCORE_COUNTRIES
        if row["summary"]:
            score += _SCORE_SUMMARY
        confidence = row.get("confidence")
        if confidence is not None:
            score += _SCORE_CONFIDENCE_MAX * max(0.0, min(1.0, float(confidence)))

        engagement = row["engagement_count"]
        if row["channel_type"] == "youtube":
            engagement = max(engagement, row["view_count"])
        if engagement > 0:
            score += min(_SCORE_ENGAGEMENT_MAX, math.log10(1 + engagement) / 40.0)
        if row["heat_level"]:
            score += min(_SCORE_HEAT_MAX, row["heat_level"] * 0.006)
        if row["channel_type"] in ("youtube", "podcast") and row["transcript_len"] > 0:
            score += _SCORE_TRANSCRIPT

        return min(1.0, score)

    def _bulk_insert(self, cursor, report_date, snapshot_rows: List[Tuple[Any, ...]]) -> None:
        if not snapshot_rows:
            return
        from psycopg2.extras import execute_values
        execute_values(
            cursor,
            """
            INSERT INTO feed_topic_daily_items
                (report_date, topic_id, subscription_content_id, is_primary_topic, importance, score)
            VALUES %s
            """,
            [(report_date, tid, cid, is_primary, importance, score)
             for tid, cid, is_primary, importance, score in snapshot_rows],
            page_size=1000,
        )


# ─── 模块级工具 ─────────────────────────────────────────────


def _ensure_str_list(value: Any) -> List[str]:
    """JSONB 字段读出可能是 None/list/str, 统一成 str 列表."""
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return [value] if value.strip() else []
    if not isinstance(value, list):
        return []
    return [str(x).strip() for x in value if x is not None and str(x).strip()]


def _parse_jsonb_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (str, bytes, memoryview)):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
            return {}
    return {}


def _score_to_importance(score: float) -> str:
    if score >= IMPORTANCE_HIGH_THRESHOLD:
        return "high"
    if score >= IMPORTANCE_MEDIUM_THRESHOLD:
        return "medium"
    return "low"
