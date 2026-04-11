-- Outcome discriminant for completed steps.
CREATE TYPE step_result_kind AS ENUM ('ok', 'err', 'rx', 'timeout', 'dl');

-- Lifecycle status of a task row.
CREATE TYPE task_status AS ENUM ('scheduled', 'picked_up', 'completed', 'failed', 'dead', 'cancelled');

-- Lifecycle status of an execution run.
CREATE TYPE execution_status AS ENUM ('scheduled', 'running', 'completed', 'failed', 'cancelled');

-- What triggered a run.
CREATE TYPE execution_trigger AS ENUM ('initial', 'restart', 'selective_rerun');

-- Lifecycle status of a step row.
CREATE TYPE step_status AS ENUM ('scheduled', 'running', 'completed', 'dead');

-- Kind of step stored in zart_steps.
CREATE TYPE step_kind AS ENUM ('step', 'sleep', 'wait_all', 'wait_for_event', 'wait_group', 'capture');

-- Status of a single step attempt.
CREATE TYPE step_attempt_status AS ENUM ('completed', 'failed');

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
    status          execution_status NOT NULL DEFAULT 'scheduled',
    result          JSONB,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    triggered_by    TEXT,
    trigger         execution_trigger NOT NULL DEFAULT 'initial',

    UNIQUE (execution_id, run_index)
);

-- Authoritative step lifecycle record — replaces ExecutionState.steps blob
CREATE TABLE IF NOT EXISTS zart_steps (
    step_id         TEXT PRIMARY KEY,   -- same as step task_id
    run_id          TEXT NOT NULL REFERENCES zart_execution_runs(run_id),
    step_name       TEXT NOT NULL,
    step_kind       step_kind NOT NULL DEFAULT 'step',

    -- The task currently responsible for this step.
    -- Updated when a retry creates a new task row.
    task_id         TEXT,

    status          step_status NOT NULL DEFAULT 'scheduled',
    retry_attempt   INTEGER NOT NULL DEFAULT 0,
    retry_config    JSONB,

    result          JSONB,
    -- Outcome discriminant: 'ok' | 'err' | 'rx' | 'timeout' | 'dl'
    result_kind     step_result_kind,

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
    status          step_attempt_status NOT NULL,
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
