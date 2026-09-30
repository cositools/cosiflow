CREATE TABLE IF NOT EXISTS cosiflow_notification_subscription (
    id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES ab_user(id) ON DELETE CASCADE,
    event_type VARCHAR(32) NOT NULL,
    dag_pattern VARCHAR(250) NOT NULL DEFAULT '*',
    task_pattern VARCHAR(250) NOT NULL DEFAULT '*',
    operator_pattern VARCHAR(250) NOT NULL DEFAULT '*',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(250) NOT NULL DEFAULT 'system',
    CONSTRAINT cosiflow_notification_event_check CHECK (
        event_type IN (
            'task_failure', 'task_retry', 'task_success',
            'dag_failure', 'dag_success'
        )
    ),
    CONSTRAINT cosiflow_notification_dag_pattern_check CHECK (
        length(dag_pattern) BETWEEN 1 AND 250
    ),
    CONSTRAINT cosiflow_notification_task_pattern_check CHECK (
        length(task_pattern) BETWEEN 1 AND 250
    ),
    CONSTRAINT cosiflow_notification_operator_pattern_check CHECK (
        length(operator_pattern) BETWEEN 1 AND 250
    ),
    CONSTRAINT cosiflow_notification_subscription_unique UNIQUE (
        user_id, event_type, dag_pattern, task_pattern, operator_pattern
    )
);

CREATE INDEX IF NOT EXISTS cosiflow_notification_subscription_event_idx
    ON cosiflow_notification_subscription (event_type, enabled);

CREATE INDEX IF NOT EXISTS cosiflow_notification_subscription_user_idx
    ON cosiflow_notification_subscription (user_id);
