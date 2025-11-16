-- ============================================================
-- Migration: Add category column to news_theme_matches table
-- Purpose: Record theme category alongside theme_name for better classification
-- Date: 2025-11-14
-- ============================================================

-- Step 1: Add category column
ALTER TABLE news_theme_matches 
ADD COLUMN IF NOT EXISTS category VARCHAR(100);

-- Step 2: Add comment
COMMENT ON COLUMN news_theme_matches.category IS 
'主题分类/类别（如 Technology, Healthcare, Energy 等）';

-- Step 3: Create index for category filtering
CREATE INDEX IF NOT EXISTS idx_news_theme_category 
ON news_theme_matches(category);

COMMENT ON INDEX idx_news_theme_category IS 
'分类索引 - 支持按分类筛选新闻主题匹配';

-- Step 4: Backfill existing records with category from investment_theme_embeddings
UPDATE news_theme_matches ntm
SET category = ite.category
FROM investment_theme_embeddings ite
WHERE ntm.theme_id = ite.theme_id
AND ntm.category IS NULL;

-- Step 5: Update the view to include category
CREATE OR REPLACE VIEW v_news_with_themes AS
SELECT 
    n.id as news_id,
    n.date,
    n.title,
    n.source,
    n.analysis_status,
    ntm.theme_id,
    ntm.theme_name,
    ntm.category,
    ntm.similarity_score,
    ntm.match_rank,
    ntm.matched_at
FROM news n
LEFT JOIN news_theme_matches ntm ON n.id = ntm.news_id
WHERE ntm.match_rank <= 3  -- 只显示Top-3匹配
ORDER BY n.date DESC, ntm.match_rank ASC;

COMMENT ON VIEW v_news_with_themes IS 
'新闻与主题关联视图 - 便于查询每条新闻的Top-3匹配主题（含分类）';

-- ============================================================
-- Verification queries
-- ============================================================

DO $$
DECLARE
    total_matches INTEGER;
    matches_with_category INTEGER;
    backfill_count INTEGER;
BEGIN
    -- Count total matches
    SELECT COUNT(*) INTO total_matches FROM news_theme_matches;
    
    -- Count matches with category
    SELECT COUNT(*) INTO matches_with_category 
    FROM news_theme_matches 
    WHERE category IS NOT NULL;
    
    backfill_count := matches_with_category;
    
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ Migration completed successfully!';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE 'Statistics:';
    RAISE NOTICE '  Total matches: %', total_matches;
    RAISE NOTICE '  Matches with category: %', matches_with_category;
    RAISE NOTICE '  Backfilled: %', backfill_count;
    RAISE NOTICE '';
    
    IF matches_with_category < total_matches THEN
        RAISE NOTICE '⚠️  Warning: % matches still have NULL category', 
            total_matches - matches_with_category;
        RAISE NOTICE '   This may happen if theme_id not found in investment_theme_embeddings';
    END IF;
    
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '  1. Verify category data: SELECT theme_id, theme_name, category FROM news_theme_matches LIMIT 10;';
    RAISE NOTICE '  2. Check view: SELECT * FROM v_news_with_themes LIMIT 5;';
    RAISE NOTICE '  3. Restart backend services to apply code changes';
END $$;











