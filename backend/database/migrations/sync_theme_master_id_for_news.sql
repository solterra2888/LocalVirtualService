-- =====================================================
-- 同步 theme_master_id：让同一新闻的所有主题记录都有 theme_master_id
-- 创建日期: 2025-10-30
-- 描述: 当新闻有 primary theme 时，其他来源的记录自动继承这个 theme_master_id
-- =====================================================

BEGIN;

-- ============================================================
-- 步骤1: 创建触发器函数 - 自动同步 theme_master_id
-- ============================================================

CREATE OR REPLACE FUNCTION sync_theme_master_id_for_news()
RETURNS TRIGGER AS $$
DECLARE
    v_primary_theme_master_id INTEGER;
BEGIN
    -- 只处理 primary theme 的更新
    IF NEW.is_primary = true AND NEW.theme_master_id IS NOT NULL THEN
        -- 更新同一新闻的其他记录（非 primary 的记录）
        UPDATE news_themes_unified
        SET theme_master_id = NEW.theme_master_id,
            updated_at = CURRENT_TIMESTAMP
        WHERE news_id = NEW.news_id
          AND is_primary = false
          AND (theme_master_id IS NULL OR theme_master_id != NEW.theme_master_id);
        
        RAISE NOTICE 'Synced theme_master_id % for news_id %', NEW.theme_master_id, NEW.news_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION sync_theme_master_id_for_news IS 
'触发器函数 - 当设置 primary theme 时，自动同步 theme_master_id 到该新闻的其他主题记录';

-- ============================================================
-- 步骤2: 创建触发器
-- ============================================================

DROP TRIGGER IF EXISTS trigger_sync_theme_master_id ON news_themes_unified;

CREATE TRIGGER trigger_sync_theme_master_id
    AFTER INSERT OR UPDATE OF theme_master_id, is_primary
    ON news_themes_unified
    FOR EACH ROW
    WHEN (NEW.is_primary = true AND NEW.theme_master_id IS NOT NULL)
    EXECUTE FUNCTION sync_theme_master_id_for_news();

COMMENT ON TRIGGER trigger_sync_theme_master_id ON news_themes_unified IS
'当设置 primary theme 时，自动同步 theme_master_id 到该新闻的其他主题记录';

-- ============================================================
-- 步骤3: 创建手动同步函数（用于批量处理现有数据）
-- ============================================================

CREATE OR REPLACE FUNCTION sync_all_theme_master_ids()
RETURNS TABLE(
    news_id INTEGER,
    updated_count INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH primary_themes AS (
        -- 找出所有有 primary theme 的新闻
        SELECT 
            ntu.news_id,
            ntu.theme_master_id
        FROM news_themes_unified ntu
        WHERE ntu.is_primary = true
          AND ntu.theme_master_id IS NOT NULL
    ),
    updates AS (
        -- 更新同一新闻的其他记录
        UPDATE news_themes_unified ntu
        SET theme_master_id = pt.theme_master_id,
            updated_at = CURRENT_TIMESTAMP
        FROM primary_themes pt
        WHERE ntu.news_id = pt.news_id
          AND ntu.is_primary = false
          AND (ntu.theme_master_id IS NULL OR ntu.theme_master_id != pt.theme_master_id)
        RETURNING ntu.news_id
    )
    SELECT 
        u.news_id,
        COUNT(*)::INTEGER as updated_count
    FROM updates u
    GROUP BY u.news_id
    ORDER BY u.news_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION sync_all_theme_master_ids IS
'批量同步函数 - 将所有新闻的 primary theme_master_id 同步到其他主题记录';

-- ============================================================
-- 步骤4: 执行一次性批量同步（处理现有数据）
-- ============================================================

DO $$
DECLARE
    v_total_news INTEGER;
    v_updated_records INTEGER;
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Starting bulk sync of theme_master_id...';
    RAISE NOTICE '==============================================';
    
    -- 执行批量同步
    WITH sync_result AS (
        SELECT * FROM sync_all_theme_master_ids()
    )
    SELECT 
        COUNT(DISTINCT news_id),
        SUM(updated_count)
    INTO v_total_news, v_updated_records
    FROM sync_result;
    
    RAISE NOTICE 'Sync completed:';
    RAISE NOTICE '  - News affected: %', COALESCE(v_total_news, 0);
    RAISE NOTICE '  - Records updated: %', COALESCE(v_updated_records, 0);
    RAISE NOTICE '==============================================';
END $$;

-- ============================================================
-- 步骤5: 创建视图 - 验证同步结果
-- ============================================================

CREATE OR REPLACE VIEW v_news_theme_master_coverage AS
SELECT 
    n.id as news_id,
    n.title,
    COUNT(DISTINCT ntu.source_type) as source_count,
    SUM(CASE WHEN ntu.is_primary THEN 1 ELSE 0 END) as primary_count,
    SUM(CASE WHEN ntu.theme_master_id IS NOT NULL THEN 1 ELSE 0 END) as has_master_id_count,
    SUM(CASE WHEN ntu.theme_master_id IS NULL THEN 1 ELSE 0 END) as no_master_id_count,
    MAX(CASE WHEN ntu.is_primary THEN ntu.theme_master_id END) as primary_theme_master_id,
    ARRAY_AGG(DISTINCT ntu.source_type ORDER BY ntu.source_type) as sources,
    ARRAY_AGG(
        CASE WHEN ntu.theme_master_id IS NULL 
        THEN ntu.source_type || ':NULL' 
        ELSE ntu.source_type || ':' || ntu.theme_master_id::TEXT 
        END
        ORDER BY ntu.is_primary DESC, ntu.source_type
    ) as theme_mapping_detail
FROM news n
LEFT JOIN news_themes_unified ntu ON n.id = ntu.news_id
WHERE ntu.is_active = true
GROUP BY n.id, n.title
ORDER BY n.id;

COMMENT ON VIEW v_news_theme_master_coverage IS
'新闻主题覆盖视图 - 检查每条新闻的 theme_master_id 覆盖情况';

-- ============================================================
-- 步骤6: 输出统计信息
-- ============================================================

DO $$
DECLARE
    v_total_news INTEGER;
    v_news_with_primary INTEGER;
    v_fully_synced INTEGER;
    v_partially_synced INTEGER;
    v_not_synced INTEGER;
BEGIN
    -- 统计总新闻数
    SELECT COUNT(DISTINCT news_id) INTO v_total_news
    FROM news_themes_unified
    WHERE is_active = true;
    
    -- 统计有 primary theme 的新闻数
    SELECT COUNT(DISTINCT news_id) INTO v_news_with_primary
    FROM news_themes_unified
    WHERE is_primary = true
      AND is_active = true;
    
    -- 统计完全同步的新闻（所有记录都有 theme_master_id）
    SELECT COUNT(*) INTO v_fully_synced
    FROM (
        SELECT news_id
        FROM news_themes_unified
        WHERE is_active = true
        GROUP BY news_id
        HAVING COUNT(*) = SUM(CASE WHEN theme_master_id IS NOT NULL THEN 1 ELSE 0 END)
    ) t;
    
    -- 统计部分同步的新闻（有些记录有，有些没有）
    SELECT COUNT(*) INTO v_partially_synced
    FROM (
        SELECT news_id
        FROM news_themes_unified
        WHERE is_active = true
        GROUP BY news_id
        HAVING SUM(CASE WHEN theme_master_id IS NOT NULL THEN 1 ELSE 0 END) > 0
           AND SUM(CASE WHEN theme_master_id IS NULL THEN 1 ELSE 0 END) > 0
    ) t;
    
    -- 统计完全未同步的新闻（所有记录都没有 theme_master_id）
    SELECT COUNT(*) INTO v_not_synced
    FROM (
        SELECT news_id
        FROM news_themes_unified
        WHERE is_active = true
        GROUP BY news_id
        HAVING SUM(CASE WHEN theme_master_id IS NOT NULL THEN 1 ELSE 0 END) = 0
    ) t;
    
    RAISE NOTICE '';
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Theme Master ID Coverage Statistics';
    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Total news: %', v_total_news;
    RAISE NOTICE 'News with primary theme: %', v_news_with_primary;
    RAISE NOTICE '';
    RAISE NOTICE 'Coverage:';
    RAISE NOTICE '  ✅ Fully synced: % (%.1f%%)', 
        v_fully_synced, 
        CASE WHEN v_total_news > 0 THEN (v_fully_synced::FLOAT / v_total_news * 100) ELSE 0 END;
    RAISE NOTICE '  ⚠️  Partially synced: % (%.1f%%)', 
        v_partially_synced,
        CASE WHEN v_total_news > 0 THEN (v_partially_synced::FLOAT / v_total_news * 100) ELSE 0 END;
    RAISE NOTICE '  ❌ Not synced: % (%.1f%%)', 
        v_not_synced,
        CASE WHEN v_total_news > 0 THEN (v_not_synced::FLOAT / v_total_news * 100) ELSE 0 END;
    RAISE NOTICE '';
    RAISE NOTICE 'Trigger: trigger_sync_theme_master_id (ACTIVE)';
    RAISE NOTICE 'Function: sync_all_theme_master_ids() (available)';
    RAISE NOTICE 'View: v_news_theme_master_coverage (available)';
    RAISE NOTICE '==============================================';
END $$;

COMMIT;

