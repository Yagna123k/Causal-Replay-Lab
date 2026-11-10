-- Aggregate (transaction) registry.
CREATE TABLE transactions (
    transaction_id TEXT        PRIMARY KEY CHECK (length(transaction_id) BETWEEN 1 AND 128),
    status         TEXT        NOT NULL DEFAULT 'OPEN',
    last_sequence  BIGINT      NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Immutable event log.
CREATE TABLE events (
    event_id        TEXT        PRIMARY KEY CHECK (length(event_id) BETWEEN 1 AND 128),
    transaction_id  TEXT        NOT NULL REFERENCES transactions (transaction_id),
    event_type      TEXT        NOT NULL CHECK (length(event_type) BETWEEN 1 AND 64),
    payload         JSONB       NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    sequence_number BIGINT      NOT NULL CHECK (sequence_number >= 1),
    created_at      TIMESTAMPTZ NOT NULL,                   -- producer time
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),     -- server time
    payload_hash    TEXT        NOT NULL,
    metadata        JSONB       NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    schema_version  INTEGER     NOT NULL DEFAULT 1,
    CONSTRAINT events_transaction_sequence_key UNIQUE (transaction_id, sequence_number)
);

-- Mutable processing state, kept separate so events themselves never change.
CREATE TABLE event_processing (
    event_id   TEXT        PRIMARY KEY REFERENCES events (event_id),
    status     TEXT        NOT NULL DEFAULT 'RECEIVED',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Enforce immutability of the event log at the database level.
CREATE FUNCTION events_reject_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'events table is immutable: % is not allowed', TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER events_no_update_delete
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_reject_mutation();

CREATE TRIGGER events_no_truncate
    BEFORE TRUNCATE ON events
    FOR EACH STATEMENT EXECUTE FUNCTION events_reject_mutation();
