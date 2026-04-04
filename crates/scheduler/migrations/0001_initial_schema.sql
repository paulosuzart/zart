-- Individual scheduled tasks (backing each step of a durable execution)
CREATE TABLE IF NOT EXISTS zart_tasks (
    task_id        TEXT PRIMARY KEY,
    task_name      TEXT NOT NULL,

    -- Scheduling
    execution_time TIMESTAMPTZ NOT NULL,
    recurrence     JSONB,

    -- Payload and state
    data           JSONB NOT NULL DEFAULT '{}',
    state          JSONB NOT NULL DEFAULT '{"steps": {}, "data": {}, "retry_count": 0}',

    -- Concurrency & lifecycle
    status         TEXT NOT NULL DEFAULT 'scheduled',
    worker_id      TEXT,
    locked_at      TIMESTAMPTZ,
    attempt        INTEGER NOT NULL DEFAULT 0,

    -- Result storage (for step results)
    result         JSONB,

    -- Link to parent durable execution
    execution_id   TEXT,

    -- Error tracking
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at   TIMESTAMPTZ
);

-- Durable execution tracking (higher-level workflow state)
CREATE TABLE IF NOT EXISTS zart_executions (
    execution_id   TEXT PRIMARY KEY,
    task_name      TEXT NOT NULL,

    -- Input and output
    payload        JSONB NOT NULL DEFAULT '{}',
    result         JSONB,

    -- Lifecycle
    status         TEXT NOT NULL DEFAULT 'scheduled',
    scheduled_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at   TIMESTAMPTZ,

    -- Versioning (for code deployment tracking)
    version        INTEGER NOT NULL DEFAULT 1,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Poll index: only scheduled tasks ordered by due time
CREATE INDEX IF NOT EXISTS idx_zart_tasks_poll
    ON zart_tasks (execution_time, status)
    WHERE status = 'scheduled';

-- Lookup tasks belonging to a durable execution
CREATE INDEX IF NOT EXISTS idx_zart_tasks_execution_id
    ON zart_tasks (execution_id)
    WHERE execution_id IS NOT NULL;

-- Lookup recurring tasks by name
CREATE INDEX IF NOT EXISTS idx_zart_tasks_recurrence
    ON zart_tasks (task_name)
    WHERE recurrence IS NOT NULL;

-- Lookup executions by status and schedule time (for listing / observability)
CREATE INDEX IF NOT EXISTS idx_zart_executions_status
    ON zart_executions (status, scheduled_at);
