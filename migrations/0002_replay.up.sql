CREATE TABLE replay_sessions (
    session_id       TEXT        PRIMARY KEY,
    transaction_id   TEXT        NOT NULL REFERENCES transactions (transaction_id),
    mode             TEXT        NOT NULL CHECK (mode IN ('ORIGINAL', 'REPLAY')),
    status           TEXT        NOT NULL DEFAULT 'CREATED'
                                 CHECK (status IN ('CREATED', 'RUNNING', 'COMPLETED', 'FAILED')),
    from_sequence    BIGINT      CHECK (from_sequence IS NULL OR from_sequence >= 1),
    to_sequence      BIGINT      CHECK (to_sequence IS NULL OR to_sequence >= 1),
    initial_state    JSONB,
    event_count      INTEGER     NOT NULL DEFAULT 0,
    final_state      JSONB,
    final_state_hash TEXT,
    error            TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    CHECK (from_sequence IS NULL OR to_sequence IS NULL OR from_sequence <= to_sequence)
);

CREATE INDEX replay_sessions_transaction_idx ON replay_sessions (transaction_id, mode, created_at DESC);

CREATE TABLE replay_executions (
    execution_id    BIGSERIAL   PRIMARY KEY,
    session_id      TEXT        NOT NULL REFERENCES replay_sessions (session_id) ON DELETE CASCADE,
    event_id        TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    sequence_number BIGINT      NOT NULL,
    status          TEXT        NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED')),
    state_hash      TEXT        NOT NULL,
    state_after     JSONB       NOT NULL,
    effects         JSONB       NOT NULL DEFAULT '[]'::jsonb,
    error           TEXT,
    executed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT replay_executions_session_sequence_key UNIQUE (session_id, sequence_number)
);
