-- =====================================================
-- subscription_content_tags: 新增信息分级列 (L / E 双轴)
-- =====================================================
-- 创建时间: 2026-11-03
-- 说明: 落地 docs 标准 tools/morning-brief/INFO_GRADING_STANDARD.md 的 L/E 双轴分级,
--       把分级从"报告方法论"前移到 Feeds 入库打标阶段.
--         info_level     L1-L4  候选重要度 (对内容所聚焦标的/行业的投资判断冲击潜力;
--                                 消费端按具体标的浮动重定, 此处为全局候选值)
--         evidence_level E1-E3  证据等级 (来源属性与可独立核验性, 与重要度正交)
--       由 content_tag_feeds_v1 / tweet_signal_v1 两个 LLM 打标模板产出;
--       EODHD 轻量映射 (news_eodhd_v1) 按来源规则补 evidence_level='E2'.
--
-- 设计决策 (列 vs JSONB): L/E 是固定枚举 + 核心用途是 SQL 过滤/聚合
-- (如 morning-brief 选材 WHERE evidence_level != 'E3', Dashboard WHERE info_level IN ('L1','L2')),
-- 故放独立列; additional_tags 仍保留给开放载荷 (ai_metadata / tweet_signal 等).
-- 反例教训: tweet_signal.importance 放 JSONB 只能 Python 侧解包、SQL 无法过滤.
--
-- NULL 语义: 存量行与未升级模板产物为 NULL = "未评级", 无需回填.
-- 索引暂不建 (表 ~2-3 万行顺序扫足够), 待 Dashboard 上 L/E 筛选再补
-- (info_level, evidence_level) 复合索引.

ALTER TABLE subscription_content_tags
    ADD COLUMN IF NOT EXISTS info_level VARCHAR(2),
    ADD COLUMN IF NOT EXISTS evidence_level VARCHAR(2);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'sct_info_level_check'
    ) THEN
        ALTER TABLE subscription_content_tags
            ADD CONSTRAINT sct_info_level_check
            CHECK (info_level IS NULL OR info_level IN ('L1', 'L2', 'L3', 'L4'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'sct_evidence_level_check'
    ) THEN
        ALTER TABLE subscription_content_tags
            ADD CONSTRAINT sct_evidence_level_check
            CHECK (evidence_level IS NULL OR evidence_level IN ('E1', 'E2', 'E3'));
    END IF;
END $$;

-- =====================================================
-- 验证
-- =====================================================
-- SELECT column_name, data_type FROM information_schema.columns
-- WHERE table_name = 'subscription_content_tags' AND column_name IN ('info_level', 'evidence_level');
