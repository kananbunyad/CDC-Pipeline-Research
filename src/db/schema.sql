-- ─────────────────────────────────────────────────────────────────────────────
-- CDC Pipeline Research — Database Schema
-- Target: PostgreSQL 15
-- ─────────────────────────────────────────────────────────────────────────────

-- Players: core CRM entity
CREATE TABLE IF NOT EXISTS players (
    id            SERIAL PRIMARY KEY,
    external_id   VARCHAR(64)  UNIQUE NOT NULL,
    username      VARCHAR(64)  NOT NULL,
    email         VARCHAR(255) NOT NULL,
    tier          VARCHAR(16)  NOT NULL CHECK (tier IN ('bronze', 'silver', 'gold', 'platinum')),
    registered_at TIMESTAMP    NOT NULL,
    platform      VARCHAR(16)  NOT NULL CHECK (platform IN ('PC', 'Mobile', 'Console')),
    created_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Game sessions: individual play events
CREATE TABLE IF NOT EXISTS game_sessions (
    id               SERIAL PRIMARY KEY,
    player_id        INT     NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    start_time       TIMESTAMP NOT NULL,
    duration_minutes FLOAT   NOT NULL,
    game_type        VARCHAR(32) NOT NULL,
    platform         VARCHAR(16) NOT NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Transactions: financial events
CREATE TABLE IF NOT EXISTS transactions (
    id             SERIAL PRIMARY KEY,
    player_id      INT            NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    amount         DECIMAL(10, 2) NOT NULL,
    currency       VARCHAR(8)     NOT NULL DEFAULT 'USD',
    payment_method VARCHAR(32)    NOT NULL,
    created_at     TIMESTAMP      NOT NULL DEFAULT NOW()
);

-- Events: behavioural tracking
CREATE TABLE IF NOT EXISTS events (
    id         SERIAL PRIMARY KEY,
    player_id  INT       NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    event_type VARCHAR(64) NOT NULL,
    metadata   JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- RFM scores: computed analytics, one row per player
CREATE TABLE IF NOT EXISTS rfm_scores (
    id              SERIAL PRIMARY KEY,
    player_id       INT  NOT NULL UNIQUE REFERENCES players(id) ON DELETE CASCADE,
    recency_score   INT  NOT NULL CHECK (recency_score   BETWEEN 1 AND 5),
    frequency_score INT  NOT NULL CHECK (frequency_score BETWEEN 1 AND 5),
    monetary_score  INT  NOT NULL CHECK (monetary_score  BETWEEN 1 AND 5),
    rfm_segment     VARCHAR(32) NOT NULL,
    last_computed   TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ─── Indexes ─────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_game_sessions_player_id   ON game_sessions (player_id);
CREATE INDEX IF NOT EXISTS idx_game_sessions_created_at  ON game_sessions (created_at);

CREATE INDEX IF NOT EXISTS idx_transactions_player_id    ON transactions  (player_id);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at   ON transactions  (created_at);

CREATE INDEX IF NOT EXISTS idx_events_player_id          ON events        (player_id);
CREATE INDEX IF NOT EXISTS idx_events_created_at         ON events        (created_at);
CREATE INDEX IF NOT EXISTS idx_events_type               ON events        (event_type);

CREATE INDEX IF NOT EXISTS idx_rfm_scores_player_id      ON rfm_scores    (player_id);
CREATE INDEX IF NOT EXISTS idx_rfm_scores_segment        ON rfm_scores    (rfm_segment);

-- ─── Logical Replication Publication (Debezium) ───────────────────────────────

-- Only create publication on the source instance; target does not need it.
-- We wrap in a DO block so the schema.sql is safe to run on both instances.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication'
    ) THEN
        EXECUTE 'CREATE PUBLICATION dbz_publication FOR ALL TABLES';
    END IF;
END
$$;
