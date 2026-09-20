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
        CHECK (status IN ('claimed', 'succeeded', 'failed')),
    CONSTRAINT cosiflow_cosidag_state_owner_check
        CHECK (status <> 'claimed' OR owner_run_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS cosiflow_cosidag_state_status_idx
    ON cosiflow_cosidag_state (dag_id, status, claimed_at);

CREATE INDEX IF NOT EXISTS cosiflow_cosidag_state_owner_idx
    ON cosiflow_cosidag_state (dag_id, owner_run_id)
    WHERE status = 'claimed';

CREATE TABLE IF NOT EXISTS cosiflow_cosidag_state_migration (
    dag_id VARCHAR(250) PRIMARY KEY,
    source_key VARCHAR(512) NOT NULL,
    item_count INTEGER NOT NULL,
    migrated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
