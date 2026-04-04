-- Individual scheduled tasks (backing each step of a durable execution)
-- SQLite variant: TEXT instead of JSONB, no TIMESTAMPTZ, no partial indexes.

CREATE TABLE IF NOT EXISTS zart_tasks (
    task_id        TEXT PRIMARY KEY,
    task_name      TEXT NOT NULL,

    -- Scheduling
    execution_time TEXT NOT NULL,
    recurrence     TEXT,

    -- Payload and state
    data           TEXT NOT NULL DEFAULT '{}',
    state          TEXT NOT NULL DEFAULT '{"steps": {}, "data": {}, "retry_count": 0}',

    -- Concurrency & lifecycle
    status         TEXT NOT NULL DEFAULT 'scheduled',
    worker_id      TEXT,
    locked_at      TEXT,
    attempt        INTEGER NOT NULL DEFAULT 0,

    -- Result storage (for step results)
    result         TEXT,

    -- Link to parent durable execution
    execution_id   TEXT,

    -- Error tracking
    last_error     TEXT,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at   TEXT
);

-- Durable execution tracking (higher-level workflow state)
CREATE TABLE IF NOT EXISTS zart_executions (
    execution_id   TEXT PRIMARY KEY,
    task_name      TEXT NOT NULL,

    -- Input and output
    payload        TEXT NOT NULL DEFAULT '{}',
    result         TEXT,

    -- Lifecycle
    status         TEXT NOT NULL DEFAULT 'scheduled',
    scheduled_at   TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at   TEXT,

    -- Versioning (for code deployment tracking)
    version        INTEGER NOT NULL DEFAULT 1,

    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Indexes (SQLite doesn't support partial indexes, so we use regular ones)
CREATE INDEX IF NOT EXISTS idx_zart_tasks_poll
    ON zart_tasks (execution_time, status);

CREATE INDEX IF NOT EXISTS idx_zart_tasks_execution_id
    ON zart_tasks (execution_id);

CREATE INDEX IF NOT EXISTS idx_zart_tasks_recurrence
    ON zart_tasks (task_name);

CREATE INDEX IF NOT EXISTS idx_zart_executions_status
    ON zart_executions (status, scheduled_at);
