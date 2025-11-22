-- Migration: Add user_id to theme_tags for personalized tagging
-- This allows each user to have their own tags for themes
-- 
-- Key Changes:
-- 1. Add user_id column to track who added each tag
-- 2. Update unique constraint to allow multiple users to tag same theme with same tag
-- 3. Migrate existing data (assign tags to theme creators by default)

-- Step 1: Add user_id column (nullable first for migration)
ALTER TABLE theme_tags 
ADD COLUMN IF NOT EXISTS user_id VARCHAR(100);

-- Step 2: Migrate existing data
-- For existing tags, assign them to the theme creator
UPDATE theme_tags tt
SET user_id = ite.created_by
FROM investment_theme_embeddings ite
WHERE tt.theme_id = ite.theme_id
  AND tt.user_id IS NULL;

-- Step 3: Set user_id as NOT NULL (now that data is migrated)
ALTER TABLE theme_tags 
ALTER COLUMN user_id SET NOT NULL;

-- Step 4: Drop old unique constraint
ALTER TABLE theme_tags
DROP CONSTRAINT IF EXISTS theme_tags_theme_id_tag_name_key;

-- Step 5: Add new unique constraint (theme_id + tag_name + user_id)
ALTER TABLE theme_tags
ADD CONSTRAINT theme_tags_theme_id_tag_name_user_id_key 
UNIQUE(theme_id, tag_name, user_id);

-- Step 6: Create index on user_id for performance
CREATE INDEX IF NOT EXISTS idx_theme_tags_user_id ON theme_tags(user_id);

-- Step 7: Create composite index for common queries
CREATE INDEX IF NOT EXISTS idx_theme_tags_user_tag ON theme_tags(user_id, tag_name);

-- Update comment
COMMENT ON TABLE theme_tags IS 'User-specific tags for themes. Each user can independently tag themes for their own organization (e.g., Market Weekly, Broker Report). The same theme can have different tags for different users.';
COMMENT ON COLUMN theme_tags.user_id IS 'User who added this tag. Each user maintains their own set of tags for themes.';

-- Verification queries
-- Check that all tags now have user_id
-- SELECT COUNT(*) FROM theme_tags WHERE user_id IS NULL;  -- Should return 0

-- Example: View tags by user
-- SELECT user_id, tag_name, COUNT(*) as theme_count
-- FROM theme_tags
-- GROUP BY user_id, tag_name
-- ORDER BY user_id, theme_count DESC;


