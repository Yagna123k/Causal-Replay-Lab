DROP TABLE IF EXISTS committed_effects;
DROP TABLE IF EXISTS pending_events;
DROP TABLE IF EXISTS idempotency_records;

DROP INDEX IF EXISTS event_processing_status_idx;
ALTER TABLE event_processing
    DROP CONSTRAINT IF EXISTS event_processing_status_check,
    DROP COLUMN IF EXISTS expires_at,
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS attempts,
    DROP COLUMN IF EXISTS idempotency_key;

ALTER TABLE transactions
    DROP COLUMN IF EXISTS gap_details,
    DROP COLUMN IF EXISTS has_sequence_gap,
    DROP COLUMN IF EXISTS current_state_hash,
    DROP COLUMN IF EXISTS current_state,
    DROP COLUMN IF EXISTS processed_sequence;
