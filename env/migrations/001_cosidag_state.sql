CREATE TABLE IF NOT EXISTS cosiflow_cosidag_state (
    dag_id VARCHAR(250) NOT NULL,
    path TEXT NOT NULL,
    status VARCHAR(16) NOT NULL,
    owner_run_id VARCHAR(250),
    monitoring_policy VARCHAR(32),
    claimed_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    PRIMARY KEY (dag_id, path),
    CONSTRAINT cosiflow_cosidag_state_status_check
        CHECK (status IN ('queued', 'claimed', 'succeeded', 'failed', 'discarded')),
    CONSTRAINT cosiflow_cosidag_state_owner_check
        CHECK (status <> 'claimed' OR owner_run_id IS NOT NULL)
);

ALTER TABLE cosiflow_cosidag_state
    ADD COLUMN IF NOT EXISTS observed_path TEXT;
ALTER TABLE cosiflow_cosidag_state
    ADD COLUMN IF NOT EXISTS monitoring_root TEXT;
ALTER TABLE cosiflow_cosidag_state
    ADD COLUMN IF NOT EXISTS candidate_snapshot TEXT;
ALTER TABLE cosiflow_cosidag_state
    ADD COLUMN IF NOT EXISTS snapshot_observed_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE cosiflow_cosidag_state
    ADD COLUMN IF NOT EXISTS queued_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE cosiflow_cosidag_state
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE cosiflow_cosidag_state
    ADD COLUMN IF NOT EXISTS queue_priority INTEGER NOT NULL DEFAULT 0;
ALTER TABLE cosiflow_cosidag_state
    ADD COLUMN IF NOT EXISTS manual_retry_by VARCHAR(250);
ALTER TABLE cosiflow_cosidag_state
    ADD COLUMN IF NOT EXISTS manual_retry_reason TEXT;
ALTER TABLE cosiflow_cosidag_state
    ADD COLUMN IF NOT EXISTS manual_retry_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE cosiflow_cosidag_state
    ADD COLUMN IF NOT EXISTS runtime_overrides TEXT;

ALTER TABLE cosiflow_cosidag_state
    DROP CONSTRAINT IF EXISTS cosiflow_cosidag_state_status_check;
ALTER TABLE cosiflow_cosidag_state
    ADD CONSTRAINT cosiflow_cosidag_state_status_check
        CHECK (status IN ('queued', 'claimed', 'succeeded', 'failed', 'discarded'));

UPDATE cosiflow_cosidag_state
SET observed_path = COALESCE(observed_path, path),
    queued_at = COALESCE(queued_at, claimed_at, updated_at),
    next_attempt_at = COALESCE(next_attempt_at, updated_at)
WHERE observed_path IS NULL OR queued_at IS NULL OR next_attempt_at IS NULL;

CREATE INDEX IF NOT EXISTS cosiflow_cosidag_state_status_idx
    ON cosiflow_cosidag_state (dag_id, status, claimed_at);

CREATE INDEX IF NOT EXISTS cosiflow_cosidag_state_owner_idx
    ON cosiflow_cosidag_state (dag_id, owner_run_id)
    WHERE status = 'claimed';

CREATE INDEX IF NOT EXISTS cosiflow_cosidag_state_queue_idx
    ON cosiflow_cosidag_state (dag_id, status, next_attempt_at, queued_at, queue_priority)
    WHERE status = 'queued';

CREATE TABLE IF NOT EXISTS cosiflow_cosidag_stability (
    dag_id VARCHAR(250) NOT NULL,
    scope VARCHAR(64) NOT NULL,
    identity_hash CHAR(64) NOT NULL,
    identity TEXT NOT NULL,
    snapshot TEXT NOT NULL,
    stable_since TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dag_id, scope, identity_hash)
);

CREATE INDEX IF NOT EXISTS cosiflow_cosidag_stability_updated_idx
    ON cosiflow_cosidag_stability (updated_at);

CREATE TABLE IF NOT EXISTS cosiflow_cosidag_state_migration (
    dag_id VARCHAR(250) PRIMARY KEY,
    source_key VARCHAR(512) NOT NULL,
    item_count INTEGER NOT NULL,
    migrated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
