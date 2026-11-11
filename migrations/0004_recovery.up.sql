-- Extend replay_sessions with Phase 4 lifecycle states and lease-based ownership.
ALTER TABLE replay_sessions
    DROP CONSTRAINT IF EXISTS replay_sessions_status_check;

ALTER TABLE replay_sessions
    ADD COLUMN locked_by           TEXT,
    ADD COLUMN lease_expires_at    TIMESTAMPTZ,
    ADD COLUMN heartbeat_at        TIMESTAMPTZ,
    ADD COLUMN last_checkpoint_seq BIGINT NOT NULL DEFAULT 0 CHECK (last_checkpoint_seq >= 0),
    ADD CONSTRAINT replay_sessions_status_check
        CHECK (status IN ('CREATED', 'RUNNING', 'PAUSED', 'INTERRUPTED', 'RESUMING', 'COMPLETED', 'FAILED'));

CREATE INDEX replay_sessions_lease_idx ON replay_sessions (status, lease_expires_at);

-- Extend replay_executions with retry tracking and dead-letter status.
ALTER TABLE replay_executions
    DROP CONSTRAINT IF EXISTS replay_executions_status_check;

ALTER TABLE replay_executions
    ADD COLUMN attempt_count  INTEGER NOT NULL DEFAULT 1 CHECK (attempt_count >= 1),
    ADD COLUMN next_retry_at  TIMESTAMPTZ,
    ADD COLUMN is_dead_letter BOOLEAN NOT NULL DEFAULT false,
    ADD CONSTRAINT replay_executions_status_check
        CHECK (status IN ('SUCCEEDED', 'RETRYING', 'FAILED', 'DEAD'));

-- Atomic replay checkpoints written in the same transaction as execution state.
CREATE TABLE replay_checkpoints (
    checkpoint_id  BIGSERIAL   PRIMARY KEY,
    session_id     TEXT        NOT NULL REFERENCES replay_sessions (session_id) ON DELETE CASCADE,
    last_sequence  BIGINT      NOT NULL CHECK (last_sequence >= 1),
    state_hash     TEXT        NOT NULL,
    state_snapshot JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT replay_checkpoints_session_seq_key UNIQUE (session_id, last_sequence)
);

CREATE INDEX replay_checkpoints_session_idx ON replay_checkpoints (session_id, last_sequence DESC);

-- Full audit log of every attempt (including retried and dead-lettered attempts).
CREATE TABLE replay_execution_attempts (
    attempt_id     BIGSERIAL   PRIMARY KEY,
    session_id     TEXT        NOT NULL REFERENCES replay_sessions (session_id) ON DELETE CASCADE,
    event_id       TEXT        NOT NULL,
    sequence_number BIGINT     NOT NULL,
    attempt_number INTEGER     NOT NULL CHECK (attempt_number >= 1),
    status         TEXT        NOT NULL CHECK (status IN ('SUCCEEDED', 'RETRYING', 'FAILED', 'DEAD')),
    error          TEXT,
    next_retry_at  TIMESTAMPTZ,
    attempted_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX replay_execution_attempts_session_idx ON replay_execution_attempts (session_id, sequence_number, attempt_number);
