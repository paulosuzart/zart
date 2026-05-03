# Spec 0054 — Task Queues: Priority, Partitioning, and Concurrency Limits

## Context / Problem

`poll_due` in `PostgresTaskScheduler` fetches tasks from a single flat pool ordered
only by `execution_time ASC`. All tasks compete equally regardless of urgency,
task type, or which worker picks them up. This means:

- A flood of low-priority background tasks can starve time-sensitive work.
- Workers cannot be dedicated to specific task types or tenants.
- There is no way to cap how many tasks of a given type run concurrently at the
  scheduler level (only the global `max_concurrent_tasks` semaphore exists).
- Operators have no knob to tune throughput per task family without changing
  application code.

## Goals

1. Introduce **named queues** with a `priority` integer (lower = higher urgency).
2. Allow tasks to be **assigned to a queue** at schedule time; unassigned tasks
   land in the implicit `default` queue (backward-compatible).
3. Allow a queue to be **partitioned** by a caller-supplied string key (e.g.
   tenant ID, shard ID). When a queue is partitioned, every task in that queue
   must carry a `partition_key`; priority and `max_concurrent` are enforced
   **per (queue, partition_key) pair**, not globally across the queue.
4. Allow workers to **subscribe to explicit (queue, partition) pairs** so a
   worker processes only the tasks assigned to it.
5. Enforce a **concurrency cap** per (queue, partition) pair; poll skips
   slots that are at capacity.
6. Support **priority-ordered polling**: within a poll cycle, eligible slots
   are ordered by queue priority then `execution_time ASC`.
7. All existing callers that do not set a queue or partition continue to work
   unchanged.

## Non-Goals

- Distributed priority queues (e.g. Redis, AMQP) — Postgres only.
- Per-queue retry policies, timeouts, or dead-letter queues (future spec).
- Dynamic queue/partition creation via the API/CLI (future spec).
- Fair-share scheduling across tenants beyond what priority + concurrency caps give.
- Automatic partition discovery — callers declare partition keys explicitly.

## Before / After Scenarios

### Before — schedule a task (no change)

```rust
scheduler.schedule_now("t1", "send_email", json!({"to": "x@y.z"})).await?;
```

### After — schedule into a named queue (no partition)

```rust
scheduler.schedule_at(ScheduleAtParams {
    task_id: "t2".into(),
    task_name: "generate_report".into(),
    queue: Some("reports".into()),         // ← new optional field
    partition_key: None,                   // ← no partition; queue config applies globally
    execution_time: Utc::now(),
    data: json!({}),
    recurrence: None,
    metadata: json!(null),
}).await?;
```

### After — schedule into a partitioned queue

The queue `"reports"` is declared as partitioned. Each tenant owns its own
isolated slot. Priority and `max_concurrent` are enforced independently per
`(queue, partition_key)` pair.

```rust
// Declare a partitioned queue once (e.g. at startup or migration):
scheduler.upsert_queue(QueueSpec {
    name: "reports".into(),
    priority: 10,             // lower = higher urgency; default queue gets priority 0
    partitioned: true,        // enables per-(queue, partition_key) config enforcement
    max_concurrent: Some(2),  // at most 2 simultaneous report tasks per tenant
}).await?;

// Schedule for tenant "acme" — its own concurrency slot
scheduler.schedule_at(ScheduleAtParams {
    task_id: "t3".into(),
    task_name: "generate_report".into(),
    queue: Some("reports".into()),
    partition_key: Some("acme".into()),    // ← identifies the partition
    execution_time: Utc::now(),
    data: json!({"tenant": "acme"}),
    recurrence: None,
    metadata: json!(null),
}).await?;

// Schedule for tenant "globex" — independent concurrency slot
scheduler.schedule_at(ScheduleAtParams {
    task_id: "t4".into(),
    task_name: "generate_report".into(),
    queue: Some("reports".into()),
    partition_key: Some("globex".into()),
    execution_time: Utc::now(),
    data: json!({"tenant": "globex"}),
    recurrence: None,
    metadata: json!(null),
}).await?;
```

### Before — worker polls globally

```rust
Worker::new(scheduler.clone(), registry.clone(), config, vec![])
```

### After — worker subscribes to specific (queue, partition) pairs

```rust
use zart_scheduler::QueueSubscription;

// Worker dedicated to the "acme" partition of the "reports" queue only
let config = WorkerConfig {
    subscriptions: vec![
        QueueSubscription { queue: "reports".into(), partition_key: Some("acme".into()) },
    ],
    ..Default::default()
};

// Worker handling all partitions of "critical" plus the unpartitioned "default" queue
let config = WorkerConfig {
    subscriptions: vec![
        QueueSubscription { queue: "critical".into(), partition_key: None }, // all partitions
        QueueSubscription { queue: "default".into(),  partition_key: None },
    ],
    ..Default::default()
};

// Backward-compatible: empty subscriptions → poll everything (existing behaviour)
let config = WorkerConfig::default();
```

**Subscription semantics:**

| `partition_key` in `QueueSubscription` | Effect |
|---|---|
| `None` | Worker polls all partitions of that queue (or the whole queue if unpartitioned) |
| `Some("acme")` | Worker polls only tasks where `partition_key = 'acme'` in that queue |

### Queue definition (opt-in, done once at startup or migration)

```rust
scheduler.upsert_queue(QueueSpec {
    name: "critical".into(),
    priority: 1,
    partitioned: false,
    max_concurrent: None,   // unlimited
}).await?;
```

---

## Design

### Part 1 — Schema

#### 1a. `zart_queues` table (new)

```sql
CREATE TABLE zart_queues (
    name            TEXT PRIMARY KEY,
    priority        INTEGER NOT NULL DEFAULT 100,
    partitioned     BOOLEAN NOT NULL DEFAULT FALSE,
    max_concurrent  INTEGER,          -- NULL = unlimited; applied per (queue, partition_key) when partitioned=TRUE
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed the default queue so it always exists
INSERT INTO zart_queues (name, priority, partitioned, max_concurrent)
VALUES ('default', 0, FALSE, NULL)
ON CONFLICT DO NOTHING;
```

`max_concurrent` meaning:
- `partitioned = FALSE` → global cap across the entire queue.
- `partitioned = TRUE`  → cap applied independently per distinct `partition_key` value found in `zart_tasks`. No separate rows are needed per partition; the cap is evaluated dynamically against the live count of `picked_up` tasks sharing that `(queue, partition_key)`.

#### 1b. `zart_tasks` — add `queue` and `partition_key` columns

```sql
ALTER TABLE zart_tasks
    ADD COLUMN queue         TEXT NOT NULL DEFAULT 'default'
        REFERENCES zart_queues(name),
    ADD COLUMN partition_key TEXT;    -- NULL for unpartitioned queues

-- Covers the common poll filter: status + queue + partition + execution_time
CREATE INDEX zart_tasks_queue_partition_status_et_idx
    ON zart_tasks (queue, partition_key, status, execution_time);
```

Rules enforced at the application layer (not by a DB constraint, to keep the
migration simple):

- If `zart_queues.partitioned = TRUE`, tasks must supply a non-NULL `partition_key`.
- If `zart_queues.partitioned = FALSE`, `partition_key` must be NULL.

A future spec can add a DB-level check function if stricter enforcement is
required.

### Part 2 — Rust types

#### 2a. `ScheduleAtParams` gains `queue` and `partition_key`

```rust
pub struct ScheduleAtParams {
    pub task_id: String,
    pub task_name: String,
    pub queue: Option<String>,         // None → "default"
    pub partition_key: Option<String>, // None for unpartitioned queues
    pub execution_time: DateTime<Utc>,
    pub data: serde_json::Value,
    pub recurrence: Option<Recurrence>,
    pub metadata: serde_json::Value,
}
```

`queue = None` is coerced to `"default"` inside `schedule_at_sql`.
`partition_key = None` is stored as SQL NULL. All existing call-sites compile
without modification (additive fields with sensible defaults).

#### 2b. `QueueSpec` (new)

```rust
pub struct QueueSpec {
    pub name: String,
    pub priority: i32,
    /// When true, `max_concurrent` is enforced per distinct `partition_key`
    /// in the task table, not across the whole queue.
    pub partitioned: bool,
    pub max_concurrent: Option<i32>,
}
```

#### 2c. `QueueSubscription` (new)

```rust
/// Describes which tasks a worker will poll.
pub struct QueueSubscription {
    pub queue: String,
    /// `None` → worker handles all partitions of this queue.
    /// `Some(key)` → worker handles only tasks where `partition_key = key`.
    pub partition_key: Option<String>,
}
```

#### 2d. `FetchedTask` gains `queue` and `partition_key`

```rust
pub struct FetchedTask {
    // existing fields …
    pub queue: String,
    pub partition_key: Option<String>,
}
```

### Part 3 — `TaskScheduler` trait additions

```rust
/// Insert or replace a queue definition.
async fn upsert_queue(&self, spec: QueueSpec) -> Result<(), StorageError> {
    Err(StorageError::NotImplemented("upsert_queue"))
}

/// Poll for due tasks respecting queue priority, partition affinity, and
/// concurrency caps.
///
/// `subscriptions` — if empty, poll across all queues and partitions
/// (backward-compatible with the old `poll_due` behaviour).
///
/// Each `QueueSubscription` entry restricts the poll to a specific
/// (queue, optional partition_key) slot. A subscription with
/// `partition_key = None` matches all partitions of that queue.
async fn poll_due_queued(
    &self,
    now: DateTime<Utc>,
    limit: usize,
    subscriptions: &[QueueSubscription],
) -> Result<Vec<FetchedTask>, StorageError> {
    Err(StorageError::NotImplemented("poll_due_queued"))
}
```

`poll_due` (the original signature) is **not removed**. `PostgresTaskScheduler`
delegates its existing `poll_due` to `poll_due_queued` with
`subscriptions = &[]`.

### Part 4 — PostgreSQL `poll_due_queued` algorithm

The algorithm runs inside a single transaction with `FOR UPDATE SKIP LOCKED`.

**Inputs supplied as bound parameters:**

| Parameter | Type | Description |
|---|---|---|
| `$1` | `TIMESTAMPTZ` | `now` — upper bound for `execution_time` |
| `$2` | `BIGINT` | global `limit` |
| `$3` | `TEXT[]` | subscribed queue names (NULL/empty = all) |
| `$4` | `TEXT[]` | subscribed partition keys, parallel array to `$3`; NULL element = all partitions of that queue |

Because `$3` and `$4` are parallel arrays the driver passes them together.
The Rust layer encodes `subscriptions: &[QueueSubscription]` into these two
arrays before binding.

```sql
WITH
-- Step 1: resolve which (queue, partition_key) slots this worker subscribes to.
-- An unpartitioned queue has partition_key IS NULL on all its tasks, which
-- matches a subscription with partition_key = NULL (any).
subscribed AS (
    SELECT
        unnest($3::text[])                    AS sub_queue,
        unnest($4::text[])                    AS sub_partition   -- NULL = any partition
),

-- Step 2: identify eligible (queue, partition_key) slots that are below their
-- concurrency cap.  For unpartitioned queues (partitioned=FALSE) the
-- partition_key on tasks is NULL and the cap is queue-wide.
eligible_slots AS (
    SELECT
        q.name          AS queue,
        q.priority,
        q.partitioned,
        q.max_concurrent,
        CASE WHEN q.partitioned THEN t_slot.partition_key ELSE NULL END AS partition_key
    FROM zart_queues q
    -- join to discover which distinct partition_key values exist for this queue
    LEFT JOIN LATERAL (
        SELECT DISTINCT partition_key
        FROM zart_tasks
        WHERE queue = q.name
          AND status IN ('scheduled', 'picked_up')
    ) t_slot ON TRUE
    -- filter to only subscribed slots
    WHERE EXISTS (
        SELECT 1 FROM subscribed s
        WHERE s.sub_queue = q.name
          AND (s.sub_partition IS NULL OR s.sub_partition =
               CASE WHEN q.partitioned THEN t_slot.partition_key ELSE NULL END)
    )
      -- concurrency cap check
      AND (
          q.max_concurrent IS NULL
          OR (
              SELECT COUNT(*)
              FROM zart_tasks cap
              WHERE cap.queue = q.name
                AND cap.status = 'picked_up'
                AND (NOT q.partitioned
                     OR cap.partition_key IS NOT DISTINCT FROM
                        CASE WHEN q.partitioned THEN t_slot.partition_key ELSE NULL END)
          ) < q.max_concurrent
      )
),

-- Step 3: fetch due tasks for eligible slots, ordered by queue priority then
-- execution_time (existing ordering preserved).
due_tasks AS (
    SELECT t.task_id, t.task_name, t.data, t.state, t.attempt,
           t.recurrence, t.metadata, t.execution_time,
           t.queue, t.partition_key
    FROM zart_tasks t
    JOIN eligible_slots es
      ON t.queue = es.queue
     AND (NOT es.partitioned
          OR t.partition_key IS NOT DISTINCT FROM es.partition_key)
    WHERE t.status = 'scheduled'
      AND t.execution_time <= $1
    ORDER BY es.priority ASC, t.execution_time ASC
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
SELECT * FROM due_tasks;
```

Then the existing UPDATE loop stamps each selected row with
`status = 'picked_up'`, `worker_id = lock_token`, `attempt = attempt + 1`.

**Empty subscriptions (backward-compatible path):**  
When `$3` is NULL or empty the `subscribed` CTE returns no rows, so the
`EXISTS` filter is removed and all queues/partitions are eligible — identical
to the behaviour of the old `poll_due`.

### Part 5 — `WorkerConfig` gains `subscriptions`

```rust
pub struct WorkerConfig {
    // existing fields …
    /// Queue + partition slots this worker will poll.
    ///
    /// Empty → poll everything (backward-compatible with the old behaviour).
    ///
    /// Examples:
    ///   // All tasks in the "critical" queue regardless of partition
    ///   QueueSubscription { queue: "critical".into(), partition_key: None }
    ///
    ///   // Only "acme" tasks in the "reports" partitioned queue
    ///   QueueSubscription { queue: "reports".into(), partition_key: Some("acme".into()) }
    pub subscriptions: Vec<QueueSubscription>,
}
```

`Worker::run()` passes `&self.config.subscriptions` to `poll_due_queued`.
Existing workers compiled before this spec see `subscriptions: vec![]` from
`Default::default()`, which is the all-queues / all-partitions path.

---

## Files Affected

| File | Change |
|---|---|
| `crates/zart-scheduler/migrations/XXXX_add_queues.sql` | New migration: `zart_queues` table + `queue` + `partition_key` columns on `zart_tasks` |
| `crates/zart-scheduler/src/types.rs` | Add `queue`, `partition_key` to `ScheduleAtParams` and `FetchedTask`; add `QueueSpec`, `QueueSubscription` |
| `crates/zart-scheduler/src/store.rs` | Add `upsert_queue`, `poll_due_queued` trait methods with default impls |
| `crates/zart-scheduler/src/postgres/scheduler_impl.rs` | Implement `upsert_queue`, `poll_due_queued`; delegate `poll_due` to `poll_due_queued(&[])` |
| `crates/zart-scheduler/src/postgres/sql_helpers.rs` | Extract `poll_due_queued_sql` helper with CTE-based algorithm |
| `crates/zart-scheduler/src/worker_config.rs` | Replace `queues: Vec<String>` with `subscriptions: Vec<QueueSubscription>` |
| `crates/zart-scheduler/src/worker.rs` | Use `poll_due_queued` instead of `poll_due` |
| `crates/zart-scheduler/src/lib.rs` | Re-export `QueueSpec`, `QueueSubscription` |
| `crates/zart-scheduler/tests/integration_test.rs` | Add tests: priority ordering, per-queue cap, per-partition cap, worker affinity |
| `webpage/` | Document queue concept, partitioning model, `WorkerConfig::subscriptions` |

---

## Phase Plans

### Phase 1 — Schema + types (non-breaking additions)

- Add migration: `zart_queues` table (`name`, `priority`, `partitioned`, `max_concurrent`) + `queue TEXT DEFAULT 'default'` + `partition_key TEXT` columns on `zart_tasks`; add composite index.
- Extend `ScheduleAtParams` with `queue: Option<String>`, `partition_key: Option<String>`.
- Extend `FetchedTask` with `queue: String`, `partition_key: Option<String>`.
- Add `QueueSpec`, `QueueSubscription`.
- Re-export new types from `lib.rs`.
- **Gate**: migration applies cleanly; `just fmt` + `just lint` pass.

### Phase 2 — Trait + PostgreSQL implementation

- Add `upsert_queue` and `poll_due_queued` to `TaskScheduler`.
- Implement both in `PostgresTaskScheduler`.
- Delegate existing `poll_due` to `poll_due_queued(&[])`.
- **Gate**: all existing integration tests pass unchanged.

### Phase 3 — Worker wiring

- Add `queues: Vec<String>` to `WorkerConfig` (default `vec![]`).
- Switch `Worker::run()` to call `poll_due_queued`.
- **Gate**: existing workers built with `Default::default()` config behave identically.

### Phase 4 — Tests + docs

- Integration tests: priority ordering, per-queue concurrency cap, worker affinity.
- Update webpage docs.
- **Gate**: all quality gates pass (fmt, lint, unit, integration, coverage, no module > 700 lines).

---

## Rationale

**Why a `zart_queues` table instead of hard-coding queues in config?**  
Queues defined in the DB are visible to all workers and the admin UI. Config-only
queues would require coordinating restarts across a fleet to add a new queue.

**Why `priority` as a plain integer (not enum)?**  
Integers are open for extension (users pick their own bands: 0, 10, 100) and map
directly to SQL `ORDER BY priority ASC`.

**Why keep `poll_due` unchanged?**  
Many callers — tests, the `zart` crate's own helpers, and downstream users — call
`poll_due` directly. Removing it is a breaking change with no benefit; delegating
internally costs nothing.

**Why is `partitioned` a boolean flag on `zart_queues` rather than inferred from task data?**  
Having an explicit flag lets the scheduler enforce the partition-key discipline
at insert time (application-level check) and makes the intent of each queue
self-documenting. Inferring partitioning from live task data would make the model
ambiguous when a queue is empty.

**Why is `max_concurrent` enforced per `(queue, partition_key)` dynamically, not in a separate table per partition?**  
Partitions are not pre-declared — they emerge from the `partition_key` strings
callers supply. Pre-declaring every tenant ID in a table would be cumbersome and
would require a migration per new tenant. The CTE counts live `picked_up` rows
at poll time, which is accurate and requires no extra bookkeeping table.

**Why `subscriptions: Vec<QueueSubscription>` (empty = all) instead of `Option<Vec<…>>`?**  
`Option<Vec<…>>` would require callers to write `subscriptions: None` explicitly.
An empty `Vec` with the "all queues / all partitions" semantic is idiomatic and
zero-cost for existing code using `Default::default()`.

**Why are `$3` and `$4` passed as parallel arrays rather than a single JSONB structure?**  
Parallel `TEXT[]` arrays bind directly to Postgres without a JSONB parse step
and are more amenable to `ANY($3)` style predicates. The Rust layer encodes
`Vec<QueueSubscription>` into two `Vec<Option<String>>` before binding.

---

## Risk & Mitigation

| Risk | Mitigation |
|---|---|
| `ORDER BY priority, execution_time` across a CTE scan causes full index scan on large tables | New composite index `(queue, partition_key, status, execution_time)` covers the join + WHERE clause |
| Concurrent workers racing on the per-partition concurrency cap check (TOCTOU) | Cap subquery runs inside the same `FOR UPDATE SKIP LOCKED` transaction; row-level locks prevent double-pick |
| `zart_queues` FK violated if user inserts task with unknown queue name | FK on `zart_tasks.queue` surfaces the error immediately with a descriptive Postgres error |
| `partition_key` supplied for an unpartitioned queue (or omitted for a partitioned one) | Application-layer validation in `schedule_at_sql` before INSERT; returns `StorageError::InvalidInput` |
| Dynamic partition discovery (DISTINCT scan) slows poll on high-cardinality partition columns | Index on `(queue, partition_key, status)` makes the DISTINCT scan index-only |
| Migration adds two columns — `DEFAULT 'default'` + nullable `partition_key` may rewrite table | Both are metadata-only changes on PG 11+ (no rewrite); `partition_key` is nullable, no default needed |
| Existing test fixtures that call `poll_due` directly may break | `poll_due` delegates to `poll_due_queued(&[])` — semantics unchanged; all existing tests pass |

---

## Breaking Changes

- `ScheduleAtParams` gains `queue: Option<String>` and `partition_key: Option<String>`. Exhaustive struct literals in downstream code will not compile; callers must add `queue: None, partition_key: None` or use `..Default::default()`.
- `FetchedTask` gains `queue: String` and `partition_key: Option<String>`. Same compiler impact.
- `WorkerConfig` gains `subscriptions: Vec<QueueSubscription>`. Default is `vec![]` (all-queues behaviour).
- No SQL schema breaks for existing data — `queue DEFAULT 'default'` back-fills automatically; `partition_key` is nullable with no default required.

---

## Definition of Done

- [ ] `just fmt` passes
- [ ] `just lint` passes
- [ ] All unit tests pass
- [ ] All integration tests pass (including example tests)
- [ ] New integration tests cover: priority ordering, per-queue concurrency cap, per-partition concurrency cap, worker queue affinity, worker partition affinity
- [ ] No module exceeds 700 lines (excluding tests)
- [ ] Website documentation updated to describe queues concept, partitioning model, and `WorkerConfig::subscriptions`
- [ ] `upsert_queue`, `poll_due_queued` implemented in `PostgresTaskScheduler`
- [ ] Existing callers with no `queue` field continue to work unchanged (backward compat verified by existing test suite)

## Notes

- Future spec could expose queue CRUD via the admin CLI/API (cross-ref spec 0031).
- Per-queue retry policy (max attempts, backoff) is a natural follow-on.
- `poll_due_queued` is intentionally not `#[deprecated]`-replacing `poll_due`; they coexist.
