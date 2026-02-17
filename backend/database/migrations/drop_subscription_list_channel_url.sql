-- Drop channel_url from subscription_list (Feed Reader optimization)
-- Reason: Not used in fetch or business logic; display name (channel_name) kept and auto-filled by API.
-- Run after deploying backend/frontend that no longer reference channel_url.

BEGIN;

-- 1. Drop column
ALTER TABLE subscription_list DROP COLUMN IF EXISTS channel_url;

-- 2. Verify
DO $$
DECLARE
  col_exists boolean;
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'subscription_list' AND column_name = 'channel_url'
  ) INTO col_exists;
  IF col_exists THEN
    RAISE EXCEPTION 'Migration failed: channel_url still exists on subscription_list';
  END IF;
END $$;

COMMIT;
