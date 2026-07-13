-- =====================================================
-- zsxq_groups: avatar（星球封面 / 主理人头像，对齐 substack_publications.avatar）
-- =====================================================

ALTER TABLE zsxq_groups
    ADD COLUMN IF NOT EXISTS avatar TEXT NOT NULL DEFAULT '';

COMMENT ON COLUMN zsxq_groups.avatar IS
    '星球头像 URL（优先 background_url，回退 owner.avatar_url；来自 /v2/groups）';

SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'zsxq_groups'
  AND column_name = 'avatar';
