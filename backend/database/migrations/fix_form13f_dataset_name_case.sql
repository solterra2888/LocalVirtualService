-- SEC Form 13F ZIP 文件名大小写不稳定（01JUN2025 vs 01jun2025）。
-- 按 LOWER(dataset_name) 唯一，避免 poll 把已导入季度当成新数据集。
CREATE UNIQUE INDEX IF NOT EXISTS idx_f13f_datasets_name_lower
ON form13f_datasets (LOWER(dataset_name));
