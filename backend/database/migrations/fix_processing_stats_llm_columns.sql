-- =====================================================
-- processing_stats: 补齐 LLM 元数据列
-- =====================================================
-- 创建时间: 2026-04-27
-- 来源: docs/instructions/LLM_REFACTOR_PLAN.md §2.2 Step 3 (PR-2 ②)
--
-- 背景:
--   processing_stats 表的 llm_model 和 llm_provider 列在生产库已存在
--   (file_worker.py / file_tasks.py 多处 UPDATE / INSERT),
--   但既不在 db_manager.py 的基线 CREATE TABLE 中,
--   也不在 migrations 目录的任何 ALTER 中. 这意味着:
--     - 部署到全新空库会因为这两列不存在导致 INSERT/UPDATE 报错
--     - schema 漂移: 生产实际 ≠ 代码声明
--
--   本 migration 把这两列加入版本控制. 对生产已有列无副作用 (IF NOT EXISTS).
--
-- 类型 (与生产对齐, 来源: 2026-04-27 information_schema 反查):
--   - llm_model    VARCHAR(255): 模型名如 'qwen-max-2024-09-19' / 'doubao-pro-32k'
--   - llm_provider VARCHAR(255): 'qwen' / 'doubao' / 'glm'
--   - 默认值 NULL: 未跑过 LLM 分析的执行行该列为空
--   注: IF NOT EXISTS 对生产已有列无副作用; 新建库会按 VARCHAR(255) 创建.

ALTER TABLE processing_stats
    ADD COLUMN IF NOT EXISTS llm_model    VARCHAR(255),
    ADD COLUMN IF NOT EXISTS llm_provider VARCHAR(255);

COMMENT ON COLUMN processing_stats.llm_model
    IS 'LLM 模型名 (如 qwen-max-2024-09-19), 由 analyze_*_task 写入';
COMMENT ON COLUMN processing_stats.llm_provider
    IS 'LLM 提供商 (qwen / doubao / glm), 由 analyze_*_task 写入';

-- =====================================================
-- 验证
-- =====================================================
-- \d processing_stats
-- 应能看到 llm_model / llm_provider 两列
