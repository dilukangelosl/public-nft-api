-- Metadata error log: last 20 per collection (pruned on insert via trigger)
CREATE TABLE IF NOT EXISTS metadata_errors (
    id          BIGSERIAL PRIMARY KEY,
    contract    TEXT NOT NULL,
    token_id    TEXT NOT NULL,
    error       TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metadata_errors_contract ON metadata_errors (contract, occurred_at DESC);

-- Function: keep only the 20 most recent errors per contract
CREATE OR REPLACE FUNCTION prune_metadata_errors() RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM metadata_errors
    WHERE contract = NEW.contract
      AND id NOT IN (
        SELECT id FROM metadata_errors
        WHERE contract = NEW.contract
        ORDER BY occurred_at DESC
        LIMIT 20
    );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prune_metadata_errors ON metadata_errors;
CREATE TRIGGER trg_prune_metadata_errors
    AFTER INSERT ON metadata_errors
    FOR EACH ROW EXECUTE FUNCTION prune_metadata_errors();
