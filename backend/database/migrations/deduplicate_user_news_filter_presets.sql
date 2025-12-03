-- =====================================================
-- 去重 user_news_filter_presets 表并重置 ID
-- 创建日期: 2025-12-02
-- 描述: 删除重复的预设记录，保留每组重复中 ID 最小的记录，然后重置 ID 序列
-- =====================================================

-- 开始事务
BEGIN;

-- 1. 查看重复情况（仅显示，不删除）
SELECT 
    preset_name, 
    is_system_global,
    user_id,
    COUNT(*) as duplicate_count,
    array_agg(id ORDER BY id) as all_ids
FROM user_news_filter_presets
GROUP BY preset_name, is_system_global, user_id
HAVING COUNT(*) > 1
ORDER BY preset_name;

-- 2. 创建临时表存储要保留的记录
CREATE TEMP TABLE presets_to_keep AS
SELECT DISTINCT ON (preset_name, COALESCE(user_id, -1), is_system_global)
    id
FROM user_news_filter_presets
ORDER BY preset_name, COALESCE(user_id, -1), is_system_global, id ASC;

-- 3. 显示将要删除的记录数
SELECT 
    (SELECT COUNT(*) FROM user_news_filter_presets) as total_before,
    (SELECT COUNT(*) FROM presets_to_keep) as will_keep,
    (SELECT COUNT(*) FROM user_news_filter_presets) - (SELECT COUNT(*) FROM presets_to_keep) as will_delete;

-- 4. 删除重复记录（保留 ID 最小的）
DELETE FROM user_news_filter_presets
WHERE id NOT IN (SELECT id FROM presets_to_keep);

-- 5. 创建新表存储去重后的数据
CREATE TABLE user_news_filter_presets_new (LIKE user_news_filter_presets INCLUDING ALL);

-- 6. 将数据复制到新表并重新生成连续的 ID
INSERT INTO user_news_filter_presets_new (
    user_id, preset_name, preset_type, description, icon,
    filter_config, scope_type, symbol_code, is_default, is_favorite,
    sort_order, usage_count, last_used_at, created_at, updated_at,
    is_system_global, created_by_admin_id
)
SELECT 
    user_id, preset_name, preset_type, description, icon,
    filter_config, scope_type, symbol_code, is_default, is_favorite,
    sort_order, usage_count, last_used_at, created_at, updated_at,
    is_system_global, created_by_admin_id
FROM user_news_filter_presets
ORDER BY 
    CASE WHEN is_system_global THEN 0 ELSE 1 END,  -- 系统预设在前
    sort_order ASC,
    created_at ASC NULLS LAST,
    id ASC;

-- 7. 删除旧表
DROP TABLE user_news_filter_presets;

-- 8. 重命名新表
ALTER TABLE user_news_filter_presets_new RENAME TO user_news_filter_presets;

-- 9. 重建索引（如果需要）
CREATE INDEX IF NOT EXISTS idx_user_presets_user_id 
ON user_news_filter_presets(user_id) 
WHERE user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_user_presets_scope_type 
ON user_news_filter_presets(scope_type);

CREATE INDEX IF NOT EXISTS idx_user_presets_is_default 
ON user_news_filter_presets(is_default) 
WHERE is_default = true;

CREATE INDEX IF NOT EXISTS idx_user_presets_symbol_code 
ON user_news_filter_presets(symbol_code) 
WHERE symbol_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_user_presets_is_favorite 
ON user_news_filter_presets(is_favorite) 
WHERE is_favorite = true;

CREATE INDEX IF NOT EXISTS idx_user_presets_usage_count 
ON user_news_filter_presets(usage_count DESC);

CREATE INDEX IF NOT EXISTS idx_user_presets_filter_config 
ON user_news_filter_presets USING GIN(filter_config);

CREATE INDEX IF NOT EXISTS idx_user_presets_system_global 
ON user_news_filter_presets(is_system_global) 
WHERE is_system_global = true;

CREATE INDEX IF NOT EXISTS idx_user_presets_created_by_admin 
ON user_news_filter_presets(created_by_admin_id) 
WHERE created_by_admin_id IS NOT NULL;

-- 10. 重置序列
SELECT setval(
    pg_get_serial_sequence('user_news_filter_presets', 'id'),
    COALESCE((SELECT MAX(id) FROM user_news_filter_presets), 1),
    true
);

-- 11. 显示最终结果
SELECT 
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE is_system_global = true) as system_presets,
    COUNT(*) FILTER (WHERE is_system_global = false) as user_presets
FROM user_news_filter_presets;

-- 12. 显示系统预设
SELECT id, preset_name, icon, is_system_global, sort_order
FROM user_news_filter_presets
WHERE is_system_global = true
ORDER BY sort_order;

-- 提交事务
COMMIT;

-- =====================================================
-- 完成
-- =====================================================

