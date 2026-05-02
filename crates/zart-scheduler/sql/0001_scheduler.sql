-- Lifecycle status of a task row (scheduler domain only).
CREATE TYPE task_status AS ENUM ('scheduled', 'picked_up', 'completed', 'failed', 'dead', 'cancelled');

-- Individual scheduled tasks (backing each step of a durable execution).
CREATE TABLE IF NOT EXISTS zart_tasks (
    task_id        TEXT PRIMARY KEY,
    task_name      TEXT NOT NULL,

    -- Scheduling
    execution_time TIMESTAMPTZ NOT NULL,
    recurrence     JSONB,

    -- Payload and state
    data           JSONB NOT NULL DEFAULT '{}',
    state          JSONB NOT NULL DEFAULT '{"data": {}, "retry_count": 0}',

    -- Concurrency & lifecycle
    status         task_status NOT NULL DEFAULT 'scheduled',
    worker_id      TEXT,
    locked_at      TIMESTAMPTZ,
    attempt        INTEGER NOT NULL DEFAULT 0,

    -- Result storage (for step results)
    result         JSONB,

    -- Execution model metadata (mode, run_id, step_name, step_type, etc.)
    metadata       JSONB NOT NULL DEFAULT '{}',

    -- Error tracking
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at   TIMESTAMPTZ
);

-- Poll index: only scheduled tasks ordered by due time.
CREATE INDEX IF NOT EXISTS idx_zart_tasks_poll
    ON zart_tasks (execution_time, status)
    WHERE status = 'scheduled';

-- Lookup recurring tasks by name.
CREATE INDEX IF NOT EXISTS idx_zart_tasks_recurrence
    ON zart_tasks (task_name)
    WHERE recurrence IS NOT NULL;
