-- Migrations

-- Collections
CREATE TABLE IF NOT EXISTS collections (
    address         TEXT PRIMARY KEY,
    name            TEXT,
    symbol          TEXT,
    type            TEXT NOT NULL DEFAULT 'erc721',
    total_supply    BIGINT,
    start_index     SMALLINT NOT NULL DEFAULT 0,
    snapshot_done   BOOLEAN NOT NULL DEFAULT FALSE,
    snapshot_block  BIGINT,
    reindexing      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tokens (ownership)
CREATE TABLE IF NOT EXISTS tokens (
    contract         TEXT NOT NULL,
    token_id         TEXT NOT NULL,
    owner            TEXT NOT NULL,
    metadata_fetched BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (contract, token_id)
);

-- Metadata
CREATE TABLE IF NOT EXISTS metadata (
    contract     TEXT NOT NULL,
    token_id     TEXT NOT NULL,
    name         TEXT,
    description  TEXT,
    image        TEXT,
    attributes   JSONB,
    PRIMARY KEY (contract, token_id)
);

-- Reverse index for owner lookups (avoids full scan on GET /v1/owners/:address)
CREATE TABLE IF NOT EXISTS owner_tokens (
    owner       TEXT NOT NULL,
    contract    TEXT NOT NULL,
    token_id    TEXT NOT NULL,
    PRIMARY KEY (owner, contract, token_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_tokens_owner       ON tokens (owner);
CREATE INDEX IF NOT EXISTS idx_tokens_contract    ON tokens (contract);
CREATE INDEX IF NOT EXISTS idx_metadata_contract  ON metadata (contract);
CREATE INDEX IF NOT EXISTS idx_owner_tokens_owner ON owner_tokens (owner);

-- Burned address constants (filtered at query layer)
-- 0x0000000000000000000000000000000000000000
-- 0x000000000000000000000000000000000000dEaD
