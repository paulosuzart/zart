-- Outcome discriminant for completed steps.
CREATE TYPE step_result_kind AS ENUM ('ok', 'err', 'rx', 'timeout', 'dl');

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

-- Durable execution tracking (stable identity — never mutated except current_run_id).
CREATE TABLE IF NOT EXISTS zart_executions (
    execution_id    TEXT PRIMARY KEY,
    task_name       TEXT NOT NULL,
    current_run_id  TEXT,   -- pointer to active run; NULL before first run
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Append-only run history — one row per run starting from run_index 0.
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

-- Authoritative step lifecycle record.
CREATE TABLE IF NOT EXISTS zart_steps (
    step_id         TEXT PRIMARY KEY,   -- same as step task_id
    run_id          TEXT NOT NULL REFERENCES zart_execution_runs(run_id),
    step_name       TEXT NOT NULL,
    step_kind       step_kind NOT NULL DEFAULT 'step',

    -- The task currently responsible for this step.
    task_id         TEXT,

    status          step_status NOT NULL DEFAULT 'scheduled',
    retry_attempt   INTEGER NOT NULL DEFAULT 0,
    retry_config    JSONB,

    result          JSONB,
    result_kind     step_result_kind,

    last_error      TEXT,

    -- Wait-group inline state (NULL for non-wait-group steps).
    wg_total        INTEGER,
    wg_remaining    INTEGER,
    wg_threshold    INTEGER,
    wg_first_failed BOOLEAN,

    scheduled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,

    UNIQUE (run_id, step_name)
);

-- Append-only attempt history for step retries.
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

-- Lookup executions by creation time.
CREATE INDEX IF NOT EXISTS idx_zart_executions_created
    ON zart_executions (created_at);

-- Run lookup by execution_id.
CREATE INDEX IF NOT EXISTS idx_zart_execution_runs_execution
    ON zart_execution_runs (execution_id);

-- Step lookup by run_id.
CREATE INDEX IF NOT EXISTS idx_zart_steps_run ON zart_steps (run_id);

-- Step lookup by task_id.
CREATE INDEX IF NOT EXISTS idx_zart_steps_task_id ON zart_steps (task_id) WHERE task_id IS NOT NULL;

-- Step attempt lookup by step.
CREATE INDEX IF NOT EXISTS idx_zart_step_attempts_step
    ON zart_step_attempts (step_id);

-- ── Pause/Resume Rules ───────────────────────────────────────────────────────

-- Operational controls that temporarily stop step dispatch.
CREATE TABLE IF NOT EXISTS zart_pause_rules (
    rule_id       TEXT PRIMARY KEY,
    execution_id  TEXT,
    task_name     TEXT,
    step_pattern  TEXT,
    reason        TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at    TIMESTAMPTZ,
    created_by    TEXT,
    deleted_at    TIMESTAMPTZ,
    deleted_by    TEXT
);

-- Only active rules are queried at scheduling time.
CREATE INDEX IF NOT EXISTS idx_zart_pause_rules_active
    ON zart_pause_rules (execution_id, task_name)
    WHERE deleted_at IS NULL;

