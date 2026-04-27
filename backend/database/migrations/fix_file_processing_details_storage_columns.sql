-- =====================================================
-- file_processing_details: 补齐 OSS 存储元数据列
-- =====================================================
-- 创建时间: 2026-04-27
-- 来源: docs/instructions/LLM_REFACTOR_PLAN.md §2.2 Step 3 (PR-2 ③)
--
-- 背景:
--   file_processing_details 表的 file_storage_type 和 oss_bucket 列在生产库
--   已存在 (file_worker.py 第 743-748 行 INSERT, file_tasks.py 第 910-940 行
--   SELECT 后用于决定是否走 OSS 下载),
--   但既不在 db_manager.py 的基线 CREATE TABLE 中, 也不在 migrations 目录的
--   _migrate_add_* 系列里. 部署到全新空库会因为这两列不存在导致 INSERT 报错.
--
-- 类型 (与生产对齐, 来源: 2026-04-27 information_schema 反查):
--   - file_storage_type VARCHAR(10) DEFAULT 'local': 取值 'oss' / 'local'
--   - oss_bucket        VARCHAR(100): 来自环境变量 OSS_BUCKET_NAME
--   注: IF NOT EXISTS 对生产已有列无副作用; 新建库按此类型创建.

ALTER TABLE file_processing_details
    ADD COLUMN IF NOT EXISTS file_storage_type VARCHAR(10) DEFAULT 'local',
    ADD COLUMN IF NOT EXISTS oss_bucket        VARCHAR(100);

COMMENT ON COLUMN file_processing_details.file_storage_type
    IS '存储类型: oss (阿里云 OSS) / local (本地磁盘); 决定 original_file_path 是对象键还是绝对路径';
COMMENT ON COLUMN file_processing_details.oss_bucket
    IS 'OSS Bucket 名 (file_storage_type=oss 时使用; 来自 OSS_BUCKET_NAME 环境变量)';

-- =====================================================
-- 验证
-- =====================================================
-- \d file_processing_details
-- 应能看到 file_storage_type / oss_bucket 两列
