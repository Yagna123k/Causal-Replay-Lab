DROP TABLE IF EXISTS replay_execution_attempts;
DROP TABLE IF EXISTS replay_checkpoints;

ALTER TABLE replay_executions
    DROP CONSTRAINT IF EXISTS replay_executions_status_check,
    DROP COLUMN IF EXISTS is_dead_letter,
    DROP COLUMN IF EXISTS next_retry_at,
    DROP COLUMN IF EXISTS attempt_count,
    ADD CONSTRAINT replay_executions_status_check
        CHECK (status IN ('SUCCEEDED', 'FAILED'));

DROP INDEX IF EXISTS replay_sessions_lease_idx;
ALTER TABLE replay_sessions
    DROP CONSTRAINT IF EXISTS replay_sessions_status_check,
    DROP COLUMN IF EXISTS last_checkpoint_seq,
    DROP COLUMN IF EXISTS heartbeat_at,
    DROP COLUMN IF EXISTS lease_expires_at,
    DROP COLUMN IF EXISTS locked_by,
    ADD CONSTRAINT replay_sessions_status_check
        CHECK (status IN ('CREATED', 'RUNNING', 'COMPLETED', 'FAILED'));
