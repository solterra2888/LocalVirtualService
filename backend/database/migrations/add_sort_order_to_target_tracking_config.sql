-- Migration: add sort_order column to target_tracking_config for watchlist drag-and-drop ordering

ALTER TABLE target_tracking_config
    ADD COLUMN IF NOT EXISTS sort_order INTEGER;

WITH ranked_watchlist AS (
    SELECT 
        id,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, COALESCE(group_name, 'default')
            ORDER BY priority DESC, added_at DESC, id DESC
        ) AS rn
    FROM target_tracking_config
)
UPDATE target_tracking_config AS t
SET sort_order = ranked_watchlist.rn
FROM ranked_watchlist
WHERE ranked_watchlist.id = t.id
  AND (t.sort_order IS NULL OR t.sort_order = 0);

CREATE INDEX IF NOT EXISTS idx_target_tracking_config_user_group_sort
    ON target_tracking_config (
        user_id,
        (COALESCE(group_name, 'default')),
        sort_order
    );

