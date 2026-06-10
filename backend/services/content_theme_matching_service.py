# -*- coding: utf-8 -*-
"""
内容主题匹配服务

在内容打标完成后，自动将内容（File / Feed）关联到 Investment Themes：
  - ticker 模式: 内容 tickers 与 theme_company_associations 的交集（零 LLM 成本）
  - vector 模式: 内容 embedding 与主题 embedding 余弦相似度（极低 LLM 成本）

匹配结果存入 content_theme_matches 表，供 Daily / Files / Feeds / Agent API 消费。
"""

import json
import os
from typing import Any, Dict, List, Optional

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger

logger = get_logger("content_theme_matching_service")


class ContentThemeMatchingService:
    """内容 ↔ 投资主题自动匹配服务"""

    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        from backend.utils.database_config import get_database_config
        self.db_manager = DatabaseManager(db_config or get_database_config())
        self.use_pgvector: Optional[bool] = None  # 延迟检测

    # ─── Phase 1: Ticker 匹配 ──────────────────────────────────

    def match_by_tickers(self, content_type: str, content_id: int) -> Dict[str, Any]:
        """
        Phase 1: 基于 ticker 重叠的主题匹配（纯 SQL，零 LLM）。

        1. 从 file_tags / subscription_content_tags 读取 ticker JSONB
        2. 与 theme_company_associations 做 SQL intersection
        3. UPSERT 到 content_theme_matches

        Args:
            content_type: 'file' | 'feed'
            content_id: file_processing_details.id 或 subscription_content.id

        Returns:
            {'status', 'matches_count', 'matches': [...]}
        """
        tickers = self._get_content_tickers(content_type, content_id)
        if not tickers:
            logger.debug(
                f"Ticker 匹配跳过: content_type={content_type} content_id={content_id} 无 tickers"
            )
            return {"status": "skipped", "matches_count": 0, "reason": "no_tickers"}

        conn = self.db_manager.get_connection()
        if not conn:
            return {"status": "failed", "matches_count": 0, "reason": "db_connection_failed"}

        try:
            cursor = conn.cursor()

            # 先清理该内容的旧 ticker 匹配（避免 ticker 被删除后残留旧记录）
            cursor.execute(
                "DELETE FROM content_theme_matches "
                "WHERE content_type = %s AND content_id = %s AND match_mode = 'ticker'",
                (content_type, content_id),
            )

            # 核心 UPSERT: 找所有 ticker 有交集的、且启用匹配的主题
            cursor.execute(
                """
                INSERT INTO content_theme_matches
                    (content_type, content_id, theme_id, match_mode, matched_tickers, confidence)
                SELECT
                    %s, %s, tca.theme_id, 'ticker',
                    (SELECT jsonb_agg(sub.ticker)
                     FROM theme_company_associations sub
                     WHERE sub.theme_id = tca.theme_id
                       AND sub.ticker = ANY(%s)),
                    1.0
                FROM (SELECT DISTINCT theme_id
                      FROM theme_company_associations
                      WHERE ticker = ANY(%s)) tca
                JOIN investment_theme_embeddings ite
                    ON ite.theme_id = tca.theme_id
                    AND ite.is_enabled_for_matching = TRUE
                ON CONFLICT (content_type, content_id, theme_id, match_mode)
                DO UPDATE SET
                    matched_tickers = EXCLUDED.matched_tickers,
                    confidence = EXCLUDED.confidence,
                    updated_at = NOW()
                """,
                (content_type, content_id, tickers, tickers),
            )

            matches_count = cursor.rowcount
            conn.commit()

            # 读回结果用于日志和返回
            matches = self._read_matches(cursor, content_type, content_id, match_mode="ticker")

            logger.info(
                f"Ticker 匹配完成: content_type={content_type} content_id={content_id} "
                f"tickers={tickers} matches={matches_count}"
            )
            return {
                "status": "completed",
                "matches_count": matches_count,
                "matches": matches,
            }

        except Exception as e:
            conn.rollback()
            logger.error(f"Ticker 匹配失败: content_type={content_type} content_id={content_id} err={e}")
            return {"status": "failed", "matches_count": 0, "reason": str(e)}
        finally:
            conn.close()

    # ─── Phase 2: Vector 匹配 ──────────────────────────────────

    async def match_by_vector(
        self,
        content_type: str,
        content_id: int,
        top_k: Optional[int] = None,
        min_similarity: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Phase 2: 向量语义匹配（一次 Qwen embedding 调用）。

        1. 构建内容文本 (title + summary)
        2. 生成 embedding
        3. 余弦相似度检索
        4. UPSERT 到 content_theme_matches

        Args:
            content_type: 'file' | 'feed'
            content_id: 内容 ID
            top_k: 最大返回主题数（默认从环境变量读取）
            min_similarity: 最低相似度阈值（默认从环境变量读取）
        """
        if top_k is None:
            top_k = int(os.getenv("THEME_VECTOR_MATCH_TOP_K", "5"))
        if min_similarity is None:
            min_similarity = float(os.getenv("THEME_VECTOR_MATCH_SIMILARITY_THRESHOLD", "0.45"))

        # 1. 获取内容 embedding 文本
        text = self._build_content_embedding_text(content_type, content_id)
        if not text or len(text.strip()) < 10:
            logger.debug(
                f"Vector 匹配跳过: content_type={content_type} content_id={content_id} 文本不足"
            )
            return {"status": "skipped", "matches_count": 0, "reason": "no_text"}

        try:
            # 2. 生成 embedding
            from backend.core.llm_framework import llm_service

            if "qwen" not in llm_service.providers:
                return {"status": "failed", "matches_count": 0, "reason": "qwen_not_configured"}

            logger.info(
                f"生成内容向量: content_type={content_type} content_id={content_id} "
                f"text_len={len(text)}"
            )

            embeddings = await llm_service.providers["qwen"].generate_embeddings(
                texts=[text],
                model="text-embedding-v3",
                text_type="document",  # 与主题侧一致，确保同一向量空间
            )

            if not embeddings or len(embeddings) == 0:
                return {"status": "failed", "matches_count": 0, "reason": "embedding_failed"}

            content_vector = embeddings[0]

            # 3. 余弦相似度检索
            conn = self.db_manager.get_connection()
            if not conn:
                return {"status": "failed", "matches_count": 0, "reason": "db_connection_failed"}

            try:
                cursor = conn.cursor()
                use_pgvector = self._detect_pgvector_mode(cursor)

                # 先清理旧的 vector 匹配
                cursor.execute(
                    "DELETE FROM content_theme_matches "
                    "WHERE content_type = %s AND content_id = %s AND match_mode = 'vector'",
                    (content_type, content_id),
                )

                if use_pgvector:
                    # pgvector 模式: SQL 余弦距离
                    cursor.execute(
                        """
                        SELECT e.theme_id, e.theme_name, e.category,
                               1 - (e.embedding_vector <=> %s::vector) AS similarity
                        FROM investment_theme_embeddings e
                        WHERE e.is_enabled_for_matching = TRUE
                          AND e.embedding_vector IS NOT NULL
                        ORDER BY e.embedding_vector <=> %s::vector
                        LIMIT %s
                        """,
                        [content_vector, content_vector, top_k],
                    )
                    rows = cursor.fetchall()
                else:
                    # JSONB fallback: Python numpy 计算
                    import numpy as np

                    cursor.execute(
                        """
                        SELECT e.theme_id, e.theme_name, e.category, e.embedding_data
                        FROM investment_theme_embeddings e
                        WHERE e.is_enabled_for_matching = TRUE
                          AND e.embedding_data IS NOT NULL
                        """
                    )
                    all_themes = cursor.fetchall()
                    theme_scores = []
                    for row in all_themes:
                        theme_id, theme_name, category, embedding_data = row
                        if isinstance(embedding_data, str):
                            theme_vector = json.loads(embedding_data)
                        else:
                            theme_vector = embedding_data
                        if not theme_vector:
                            continue
                        sim = self._cosine_similarity(content_vector, theme_vector)
                        theme_scores.append((theme_id, theme_name, category, sim))

                    theme_scores.sort(key=lambda x: x[3], reverse=True)
                    rows = theme_scores[:top_k]

                # 4. 过滤 + UPSERT
                matches = []
                for row in rows:
                    theme_id, theme_name, category, similarity = row[0], row[1], row[2], float(row[3])
                    if similarity < min_similarity:
                        continue

                    cursor.execute(
                        """
                        INSERT INTO content_theme_matches
                            (content_type, content_id, theme_id, match_mode, similarity, confidence)
                        VALUES (%s, %s, %s, 'vector', %s, %s)
                        ON CONFLICT (content_type, content_id, theme_id, match_mode)
                        DO UPDATE SET
                            similarity = EXCLUDED.similarity,
                            confidence = EXCLUDED.confidence,
                            updated_at = NOW()
                        """,
                        (content_type, content_id, theme_id, similarity, similarity),
                    )
                    matches.append({
                        "theme_id": theme_id,
                        "theme_name": theme_name,
                        "category": category,
                        "similarity": similarity,
                        "match_mode": "vector",
                    })

                conn.commit()
                logger.info(
                    f"Vector 匹配完成: content_type={content_type} content_id={content_id} "
                    f"matches={len(matches)} (threshold={min_similarity})"
                )
                return {
                    "status": "completed",
                    "matches_count": len(matches),
                    "matches": matches,
                }

            except Exception as e:
                conn.rollback()
                logger.error(f"Vector 匹配失败: {e}")
                return {"status": "failed", "matches_count": 0, "reason": str(e)}
            finally:
                conn.close()

        except Exception as e:
            logger.error(f"Vector 匹配异常: content_type={content_type} content_id={content_id} err={e}")
            return {"status": "failed", "matches_count": 0, "reason": str(e)}

    # ─── 查询方法 ──────────────────────────────────────────────

    def get_matches_for_content(
        self,
        content_type: str,
        content_id: int,
        match_mode: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """查询指定内容的主题匹配"""
        conn = self.db_manager.get_connection()
        if not conn:
            return []

        try:
            cursor = conn.cursor()
            return self._read_matches(cursor, content_type, content_id, match_mode=match_mode)
        finally:
            conn.close()

    def get_matches_for_theme(
        self,
        theme_id: str,
        content_type: Optional[str] = None,
        match_mode: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """查询指定主题的所有关联内容（分页）"""
        conn = self.db_manager.get_connection()
        if not conn:
            return {"items": [], "total": 0}

        try:
            cursor = conn.cursor()

            where_parts = ["ctm.theme_id = %s"]
            params: list = [theme_id]

            if content_type:
                where_parts.append("ctm.content_type = %s")
                params.append(content_type)
            if match_mode:
                where_parts.append("ctm.match_mode = %s")
                params.append(match_mode)

            where_clause = " AND ".join(where_parts)

            # 总数
            cursor.execute(
                f"SELECT COUNT(*) FROM content_theme_matches ctm WHERE {where_clause}",
                params,
            )
            total = cursor.fetchone()[0]

            # 分页数据
            cursor.execute(
                f"""
                SELECT ctm.content_type, ctm.content_id, ctm.match_mode,
                       ctm.matched_tickers, ctm.similarity, ctm.confidence,
                       ctm.created_at
                FROM content_theme_matches ctm
                WHERE {where_clause}
                ORDER BY ctm.confidence DESC, ctm.created_at DESC
                LIMIT %s OFFSET %s
                """,
                params + [limit, offset],
            )

            items = []
            for row in cursor.fetchall():
                items.append({
                    "content_type": row[0],
                    "content_id": row[1],
                    "match_mode": row[2],
                    "matched_tickers": row[3] if row[3] else [],
                    "similarity": float(row[4]) if row[4] is not None else None,
                    "confidence": float(row[5]) if row[5] is not None else None,
                    "matched_at": row[6].isoformat() if row[6] else None,
                })

            return {"items": items, "total": total}

        finally:
            conn.close()

    # ─── 内部工具方法 ──────────────────────────────────────────

    def _get_content_tickers(self, content_type: str, content_id: int) -> List[str]:
        """从 file_tags.ticker 或 subscription_content_tags.ticker 读取 JSONB 数组"""
        conn = self.db_manager.get_connection()
        if not conn:
            return []

        try:
            cursor = conn.cursor()

            if content_type == "file":
                cursor.execute(
                    "SELECT ticker FROM file_tags WHERE file_id = %s",
                    (content_id,),
                )
            elif content_type == "feed":
                cursor.execute(
                    "SELECT ticker FROM subscription_content_tags WHERE subscription_content_id = %s",
                    (content_id,),
                )
            else:
                logger.warning(f"未知的 content_type: {content_type}")
                return []

            row = cursor.fetchone()
            if not row or not row[0]:
                return []

            ticker_jsonb = row[0]
            if isinstance(ticker_jsonb, list):
                return [str(t) for t in ticker_jsonb if t]
            if isinstance(ticker_jsonb, str):
                try:
                    parsed = json.loads(ticker_jsonb)
                    return [str(t) for t in parsed if t] if isinstance(parsed, list) else []
                except (json.JSONDecodeError, TypeError):
                    return []
            return []

        finally:
            conn.close()

    def _build_content_embedding_text(self, content_type: str, content_id: int) -> Optional[str]:
        """
        构建用于向量匹配的文本 (title + summary)。
        与主题侧 theme_name + "\n\n" + description 对齐。
        """
        conn = self.db_manager.get_connection()
        if not conn:
            return None

        try:
            cursor = conn.cursor()

            if content_type == "file":
                cursor.execute(
                    """
                    SELECT COALESCE(NULLIF(fpd.display_name, ''), NULLIF(fpd.filename, ''), ''),
                           ft.summary,
                           LEFT(COALESCE(NULLIF(fpd.content, ''), ''), 500)
                    FROM file_processing_details fpd
                    LEFT JOIN file_tags ft ON ft.file_id = fpd.id
                    WHERE fpd.id = %s
                    """,
                    (content_id,),
                )
            elif content_type == "feed":
                cursor.execute(
                    """
                    SELECT COALESCE(NULLIF(sc.title, ''), NULLIF(sc.author_name, ''), ''),
                           sct.summary,
                           LEFT(COALESCE(NULLIF(sc.content, ''), ''), 500)
                    FROM subscription_content sc
                    LEFT JOIN subscription_content_tags sct
                        ON sct.subscription_content_id = sc.id
                    WHERE sc.id = %s
                    """,
                    (content_id,),
                )
            else:
                return None

            row = cursor.fetchone()
            if not row:
                return None

            title, summary, content_fallback = row
            # 优先 summary，其次 content 截取
            body = summary or content_fallback or ""
            if not title and not body:
                return None

            parts = [p for p in [title, body] if p and p.strip()]
            return "\n\n".join(parts)

        finally:
            conn.close()

    def _detect_pgvector_mode(self, cursor) -> bool:
        """检测当前 DB 是否使用 pgvector（结果缓存）"""
        if self.use_pgvector is not None:
            return self.use_pgvector

        try:
            cursor.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'investment_theme_embeddings'
                  AND column_name = 'embedding_vector'
                """
            )
            self.use_pgvector = cursor.fetchone() is not None
        except Exception:
            self.use_pgvector = False

        return self.use_pgvector

    def _read_matches(
        self,
        cursor,
        content_type: str,
        content_id: int,
        match_mode: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """从 DB 读取匹配结果"""
        if match_mode:
            cursor.execute(
                """
                SELECT ctm.theme_id, ctm.match_mode, ctm.matched_tickers,
                       ctm.similarity, ctm.confidence,
                       ite.theme_name, ite.category
                FROM content_theme_matches ctm
                JOIN investment_theme_embeddings ite ON ite.theme_id = ctm.theme_id
                WHERE ctm.content_type = %s AND ctm.content_id = %s AND ctm.match_mode = %s
                ORDER BY ctm.confidence DESC
                """,
                (content_type, content_id, match_mode),
            )
        else:
            cursor.execute(
                """
                SELECT ctm.theme_id, ctm.match_mode, ctm.matched_tickers,
                       ctm.similarity, ctm.confidence,
                       ite.theme_name, ite.category
                FROM content_theme_matches ctm
                JOIN investment_theme_embeddings ite ON ite.theme_id = ctm.theme_id
                WHERE ctm.content_type = %s AND ctm.content_id = %s
                ORDER BY ctm.confidence DESC
                """,
                (content_type, content_id),
            )

        results = []
        for row in cursor.fetchall():
            results.append({
                "theme_id": row[0],
                "match_mode": row[1],
                "matched_tickers": row[2] if row[2] else [],
                "similarity": float(row[3]) if row[3] is not None else None,
                "confidence": float(row[4]) if row[4] is not None else None,
                "theme_name": row[5],
                "category": row[6],
            })
        return results

    @staticmethod
    def _cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
        """numpy 余弦相似度（JSONB fallback 模式使用）"""
        import numpy as np

        a = np.array(vec1, dtype=np.float64)
        b = np.array(vec2, dtype=np.float64)
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(a, b) / (norm_a * norm_b))
