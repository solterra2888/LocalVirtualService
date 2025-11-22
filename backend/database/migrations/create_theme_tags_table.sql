-- Create theme_tags table for multi-tag support
-- Replaces the single `group` field with a many-to-many relationship

-- 1. Create theme_tags table
CREATE TABLE IF NOT EXISTS theme_tags (
    id SERIAL PRIMARY KEY,
    theme_id VARCHAR(255) NOT NULL,
    tag_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(100),
    
    FOREIGN KEY (theme_id) REFERENCES investment_theme_embeddings(theme_id) ON DELETE CASCADE,
    UNIQUE(theme_id, tag_name)  -- Prevent duplicate tags for same theme
);

-- 2. Create indexes for performance
CREATE INDEX idx_theme_tags_theme_id ON theme_tags(theme_id);
CREATE INDEX idx_theme_tags_tag_name ON theme_tags(tag_name);
CREATE INDEX idx_theme_tags_created_by ON theme_tags(created_by);

-- 3. Migrate existing group data to theme_tags
INSERT INTO theme_tags (theme_id, tag_name, created_at, created_by)
SELECT 
    theme_id,
    "group" as tag_name,
    created_at,
    created_by
FROM investment_theme_embeddings
WHERE "group" IS NOT NULL AND "group" != ''
ON CONFLICT (theme_id, tag_name) DO NOTHING;

-- 4. Keep the group column for backward compatibility (optional - can be removed later)
-- ALTER TABLE investment_theme_embeddings DROP COLUMN "group";

-- Note: We keep the `group` column for now to ensure backward compatibility
-- After verifying the new system works, you can safely drop it using the command above

COMMENT ON TABLE theme_tags IS 'Many-to-many relationship table for themes and tags (e.g., Market Weekly, Broker Report)';
COMMENT ON COLUMN theme_tags.theme_id IS 'Reference to investment_theme_embeddings.theme_id';
COMMENT ON COLUMN theme_tags.tag_name IS 'Tag name (e.g., Market Weekly, Broker Report, Daily Report)';
COMMENT ON COLUMN theme_tags.created_by IS 'User who added this tag';

