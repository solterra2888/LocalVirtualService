-- =====================================================
-- 将 fomc_documents / fomc_decisions.meeting_date 从会议首日
-- 统一为会议末日（与声明/纪要 URL 中的 YYYYMMDD、fomc_meetings 对齐）
-- 幂等：已是末日的行不会再改。
-- =====================================================

BEGIN;

-- 1) 文档：从 URL 解析末日
UPDATE fomc_documents
SET meeting_date = to_date(m[1], 'YYYYMMDD'),
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT id,
           COALESCE(
               regexp_match(source_url, 'monetary(\d{8})a\.htm'),
               regexp_match(source_url, 'fomcminutes(\d{8})\.htm')
           ) AS m
    FROM fomc_documents
) parsed
WHERE fomc_documents.id = parsed.id
  AND parsed.m IS NOT NULL
  AND (fomc_documents.meeting_date IS DISTINCT FROM to_date(parsed.m[1], 'YYYYMMDD'));

-- 2) 决议：优先 decision_date（抽取时已是 URL 末日），否则跟声明文档
UPDATE fomc_decisions d
SET meeting_date = COALESCE(
        d.decision_date,
        (SELECT fd.meeting_date FROM fomc_documents fd
         WHERE fd.id = d.statement_doc_id)
    ),
    updated_at = CURRENT_TIMESTAMP
WHERE COALESCE(
        d.decision_date,
        (SELECT fd.meeting_date FROM fomc_documents fd
         WHERE fd.id = d.statement_doc_id)
    ) IS NOT NULL
  AND d.meeting_date IS DISTINCT FROM COALESCE(
        d.decision_date,
        (SELECT fd.meeting_date FROM fomc_documents fd
         WHERE fd.id = d.statement_doc_id)
    );

-- 3) 概括对照上次会议：按新 meeting_date 重算
UPDATE fomc_summaries s
SET prev_meeting_date = (
        SELECT d.meeting_date
        FROM fomc_decisions d
        WHERE d.central_bank = s.central_bank
          AND d.meeting_date < (
              SELECT fd.meeting_date FROM fomc_documents fd WHERE fd.id = s.document_id
          )
        ORDER BY d.meeting_date DESC
        LIMIT 1
    ),
    updated_at = CURRENT_TIMESTAMP
WHERE EXISTS (
    SELECT 1 FROM fomc_documents fd WHERE fd.id = s.document_id
);

COMMIT;
