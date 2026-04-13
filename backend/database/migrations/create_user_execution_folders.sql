-- Migration: create user_execution_folders table
-- Replaces per-file folder_id on file_processing_details with per-user execution-level mapping
-- This allows each user (including shared-to users) to independently organize files into folders

BEGIN;

-- 1. Create the new mapping table
CREATE TABLE IF NOT EXISTS user_execution_folders (
    id              SERIAL PRIMARY KEY,
    username        TEXT NOT NULL,
    processing_stats_id INTEGER NOT NULL REFERENCES processing_stats(id) ON DELETE CASCADE,
    folder_id       INTEGER REFERENCES file_folders(id) ON DELETE SET NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (username, processing_stats_id)
);

CREATE INDEX IF NOT EXISTS idx_uef_username ON user_execution_folders(username);
CREATE INDEX IF NOT EXISTS idx_uef_folder_id ON user_execution_folders(folder_id);
CREATE INDEX IF NOT EXISTS idx_uef_processing_stats_id ON user_execution_folders(processing_stats_id);

-- 2. Migrate existing data from file_processing_details.folder_id
--    For each (processing_stats_id, folder_id) pair, take the owner's username from processing_stats
INSERT INTO user_execution_folders (username, processing_stats_id, folder_id)
SELECT DISTINCT ps."user", fpd.processing_stats_id, fpd.folder_id
FROM file_processing_details fpd
JOIN processing_stats ps ON ps.id = fpd.processing_stats_id
WHERE fpd.folder_id IS NOT NULL
ON CONFLICT (username, processing_stats_id) DO NOTHING;

-- 3. Clear the old column (optional — keep column for now to allow rollback, set all to NULL)
UPDATE file_processing_details SET folder_id = NULL WHERE folder_id IS NOT NULL;

COMMIT;
