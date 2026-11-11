-- Extend transactions with processing progress and gap tracking.
ALTER TABLE transactions
    ADD COLUMN processed_sequence BIGINT  NOT NULL DEFAULT 0 CHECK (processed_sequence >= 0),
    ADD COLUMN current_state      JSONB,
    ADD COLUMN current_state_hash TEXT,
    ADD COLUMN has_sequence_gap   BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN gap_details        TEXT;

-- Enforce the event-processing state machine:
-- RECEIVED -> PENDING_PREREQUISITE -> PROCESSING -> PROCESSED | FAILED | DEAD
ALTER TABLE event_processing
    ADD COLUMN idempotency_key TEXT,
    ADD COLUMN attempts        INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    ADD COLUMN last_error      TEXT,
    ADD COLUMN expires_at      TIMESTAMPTZ,
    ADD CONSTRAINT event_processing_status_check
        CHECK (status IN ('RECEIVED', 'PENDING_PREREQUISITE', 'PROCESSING', 'PROCESSED', 'FAILED', 'DEAD'));

CREATE INDEX event_processing_status_idx ON event_processing (status, updated_at);

-- Idempotency records guard processing effects across retries and concurrent workers.
CREATE TABLE idempotency_records (
    idempotency_key TEXT        PRIMARY KEY CHECK (length(idempotency_key) BETWEEN 1 AND 256),
    event_id        TEXT        NOT NULL REFERENCES events (event_id),
    request_hash    TEXT        NOT NULL,
    status          TEXT        NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMMITTED', 'FAILED')),
    result          JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX idempotency_records_event_idx ON idempotency_records (event_id);
CREATE INDEX idempotency_records_expires_idx ON idempotency_records (expires_at);

-- Pending buffer for out-of-order events waiting on prerequisite sequences (BUFFER policy).
CREATE TABLE pending_events (
    event_id        TEXT        PRIMARY KEY REFERENCES events (event_id),
    transaction_id  TEXT        NOT NULL REFERENCES transactions (transaction_id),
    sequence_number BIGINT      NOT NULL CHECK (sequence_number >= 1),
    status          TEXT        NOT NULL DEFAULT 'AWAITING'
                                CHECK (status IN ('AWAITING', 'RELEASED', 'EXPIRED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL,
    CONSTRAINT pending_events_txn_seq_key UNIQUE (transaction_id, sequence_number)
);

CREATE INDEX pending_events_txn_status_idx ON pending_events (transaction_id, status, sequence_number);

-- Committed effects enforce at-most-once processing and gap-free ordering in the database.
CREATE TABLE committed_effects (
    event_id        TEXT        PRIMARY KEY REFERENCES events (event_id),
    idempotency_key TEXT        NOT NULL UNIQUE,
    transaction_id  TEXT        NOT NULL REFERENCES transactions (transaction_id),
    sequence_number BIGINT      NOT NULL CHECK (sequence_number >= 1),
    state_after     JSONB       NOT NULL,
    state_hash      TEXT        NOT NULL,
    effects         JSONB       NOT NULL DEFAULT '[]'::jsonb,
    committed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT committed_effects_txn_seq_key UNIQUE (transaction_id, sequence_number)
);
