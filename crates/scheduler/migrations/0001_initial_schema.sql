-- Individual scheduled tasks (backing each step of a durable execution)
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
    status         TEXT NOT NULL DEFAULT 'scheduled',
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

-- Durable execution tracking (stable identity — never mutated except current_run_id)
CREATE TABLE IF NOT EXISTS zart_executions (
    execution_id    TEXT PRIMARY KEY,
    task_name       TEXT NOT NULL,
    current_run_id  TEXT,   -- pointer to active run; NULL before first run
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Append-only run history — one row per run starting from run_index 0
CREATE TABLE IF NOT EXISTS zart_execution_runs (
    run_id          TEXT PRIMARY KEY,
    execution_id    TEXT NOT NULL REFERENCES zart_executions(execution_id),
    run_index       INTEGER NOT NULL,  -- 0 = first, 1 = first restart, …
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'scheduled',
    result          JSONB,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    triggered_by    TEXT,
    trigger         TEXT NOT NULL DEFAULT 'initial',  -- 'initial' | 'restart' | 'selective_rerun'

    UNIQUE (execution_id, run_index)
);

-- Authoritative step lifecycle record — replaces ExecutionState.steps blob
CREATE TABLE IF NOT EXISTS zart_steps (
    step_id         TEXT PRIMARY KEY,   -- same as step task_id
    run_id          TEXT NOT NULL REFERENCES zart_execution_runs(run_id),
    step_name       TEXT NOT NULL,
    step_kind       TEXT NOT NULL DEFAULT 'step',

    -- The task currently responsible for this step.
    -- Updated when a retry creates a new task row.
    task_id         TEXT,

    status          TEXT NOT NULL DEFAULT 'scheduled',
    retry_attempt   INTEGER NOT NULL DEFAULT 0,
    retry_config    JSONB,

    result          JSONB,
    last_error      TEXT,

    -- Wait-group inline state (NULL for non-wait-group steps)
    wg_total        INTEGER,
    wg_remaining    INTEGER,
    wg_threshold    INTEGER,
    wg_first_failed BOOLEAN,

    scheduled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,

    UNIQUE (run_id, step_name)
);

-- Append-only attempt history for step retries (symmetric with zart_execution_runs)
CREATE TABLE IF NOT EXISTS zart_step_attempts (
    attempt_id      TEXT PRIMARY KEY,   -- "{step_id}:attempt:{n}"
    step_id         TEXT NOT NULL REFERENCES zart_steps(step_id),
    attempt_number  INTEGER NOT NULL,   -- 1-indexed
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    status          TEXT NOT NULL,      -- 'completed' | 'failed'
    result          JSONB,
    error           TEXT,
    UNIQUE (step_id, attempt_number)
);

CREATE INDEX IF NOT EXISTS idx_zart_step_attempts_step
    ON zart_step_attempts (step_id);

-- Poll index: only scheduled tasks ordered by due time
CREATE INDEX IF NOT EXISTS idx_zart_tasks_poll
    ON zart_tasks (execution_time, status)
    WHERE status = 'scheduled';

-- Lookup recurring tasks by name
CREATE INDEX IF NOT EXISTS idx_zart_tasks_recurrence
    ON zart_tasks (task_name)
    WHERE recurrence IS NOT NULL;

-- Lookup executions by creation time (for listing / observability)
CREATE INDEX IF NOT EXISTS idx_zart_executions_created
    ON zart_executions (created_at);

-- Run lookup by execution_id
CREATE INDEX IF NOT EXISTS idx_zart_execution_runs_execution
    ON zart_execution_runs (execution_id);

-- Step lookup by run_id
CREATE INDEX IF NOT EXISTS idx_zart_steps_run ON zart_steps (run_id);

-- Step lookup by task_id (for finding step responsible for a task)
CREATE INDEX IF NOT EXISTS idx_zart_steps_task_id ON zart_steps (task_id) WHERE task_id IS NOT NULL;
