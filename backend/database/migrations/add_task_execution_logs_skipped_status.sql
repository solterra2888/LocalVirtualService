-- Allow task_execution_logs.status = 'skipped' when scheduler skips a run (e.g. Redis lock: same task type already running).
-- Error without this: new row violates check constraint "valid_execution_status"

ALTER TABLE task_execution_logs DROP CONSTRAINT IF EXISTS valid_execution_status;

ALTER TABLE task_execution_logs ADD CONSTRAINT valid_execution_status
    CHECK (status IN ('running', 'success', 'failed', 'timeout', 'skipped', 'cancelled'));
