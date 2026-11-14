# Spec 0055 — Notify-Hint Wakeups, Backpressure, and Adaptive Drain

## Context / Problem

Every step and continuation in Zart coordinates through the `zart_tasks` table. In
`crates/zart-scheduler/src/worker.rs`, `Worker::run` (`L94–191`) currently drives
execution with a fixed-interval sleep loop (`L119–130`):

```rust
tokio::select! {
    biased;
    _ = &mut shutdown_notified => break,
    _ = &mut cancellation => break,
    _ = tokio::time::sleep(self.config.poll_interval) => {}
}
// Followed by poll_due(Utc::now(), self.config.max_tasks_per_poll)
```

This creates three structural bottlenecks under both idle and high-load workloads:

1. **The Durable Workflow `Body -> Step -> Body` Multiplier:**
   Unlike a flat one-shot job queue, Zart's replay + step execution model sends
   each sequential step through `zart_tasks` at least twice:
   - `DurableScheduler::start` (`crates/zart/src/durable.rs:132–210`) schedules
     `"{run_id}:body:start"`.
   - The worker runs the body until the first uncompleted step, delegates to
     `StepStore::schedule_step` (`crates/zart/src/postgres/step_storage_impl.rs:260–309`),
     which schedules `"{run_id}:step:{step_name}"`, and completes the body task.
   - The worker sleeps for up to `poll_interval` before fetching the step task.
   - When the step finishes, `ZartStepCompletion` (`crates/zart/src/step_completion.rs:13–34`)
     schedules `"{run_id}:body:after:{step_name}"` via `ExecutionOps::complete_in_tx`
     (`crates/zart-scheduler/src/ops.rs:87–103`), and the worker sleeps up to another
     `poll_interval` before resuming the body.
   - Fan-out (`ZartWaitGroupChildCompletion` at `step_completion.rs:36–59`) and external
     events (`EventStore::deliver_event` at `crates/zart/src/postgres/event_storage_impl.rs:14–150`)
     incur the same wakeup delay.

   With the default `WorkerConfig::poll_interval` of `5s`
   (`crates/zart-scheduler/src/worker_config.rs:46`), a 5-step workflow can spend up to
   `5 × 2 × 5s = 50s` merely waiting between poll ticks.

2. **Idle Polling Cost Across Fleets:**
   To avoid multi-second step latency, operators lower `poll_interval` to `50ms–200ms`.
   Across 50 workers at `100ms`, the fleet executes **500 `BEGIN ... SELECT FOR UPDATE SKIP LOCKED ... ROLLBACK`
   transactions per second** against PostgreSQL even when `zart_tasks` is empty.

3. **Semaphore Exhaustion & Premature Row Locking in `Worker::run`:**
   In `crates/zart-scheduler/src/worker.rs:146–168`, `Worker::run` calls
   `self.scheduler.poll_due(chrono::Utc::now(), self.config.max_tasks_per_poll)` **before**
   checking `semaphore.available_permits()` (`L110`). When all `max_concurrent_tasks`
   (default `16`) permits are occupied by slow I/O handlers, the worker still fetches up
   to `max_tasks_per_poll` (default `10`) more rows, updates them to
   `status = 'picked_up'` with a `worker_id` lock token, and then blocks in
   `semaphore.clone().acquire_owned().await` (`L165`). Those locked rows sit idle in
   memory (consuming `orphan_timeout` lease time and starving other free workers in the
   fleet). Meanwhile, `recover_orphans` (`L134`) is tied to `poll_count.is_multiple_of(10)`,
   which fires far too slowly during long fallback sleeps and far too frequently during
   high-throughput bursts.

## Goals

1. **Notifications as Hints, Never Truth (`D1`):** Use PostgreSQL `LISTEN / NOTIFY` as an
   ephemeral doorbell to wake idle workers immediately (`< 10ms` typical latency) while
   keeping `zart_tasks` (`SELECT ... FOR UPDATE SKIP LOCKED`) as the sole source of truth
   and preserving `poll_interval` as a slow reconciliation fallback (`30s–60s` in production).
2. **Dynamic `TableNames` Support Without DDL Triggers (`D2`):** Emit `SELECT pg_notify($channel, $payload)`
   directly inside `zart-scheduler` SQL helpers whenever `execution_time <= NOW()`,
   deriving the channel name from `TableNames` with zero database migrations or triggers.
3. **Unified `zart_tasks` Writes (`D3`):** Eliminate the remaining raw `INSERT INTO {tasks}`
   queries in `crates/zart/src/postgres/wait_group_storage_impl.rs` so all task insertions
   flow through `TaskScheduler`.
4. **Zero-Latency In-Process Fast-Path (`D4`):** Add `local_wakeup: Arc<tokio::sync::Notify>`
   to `PostgresTaskScheduler` so same-process `Body -> Step -> Body` transitions wake the
   local worker immediately after transaction commit, even if `LISTEN / NOTIFY` is disabled.
5. **PgBouncer Transaction-Mode Compatibility (`D5`):** Support an optional dedicated
   session-mode connection (`with_listener_options` / `with_listener_url`) for `PgListener`,
   and trigger reconciliation polling on `PgListener` reconnects.
6. **True Backpressure & Adaptive Drain (`D6`, `D7`):** Check `semaphore.available_permits()`
   before calling `poll_due`, cap polls at `min(max_tasks_per_poll, available_permits)`,
   continuously drain full batches without sleeping, coalesce concurrent wakeups, and
   decouple `recover_orphans` to an `Instant`-based interval.
7. **Forward Compatibility with Spec 0054 (`D8`):** Carry `{"queue":"default"}` in the
   `NOTIFY` payload so queue-subscribed workers can filter notifications once Spec 0054 lands.

## Non-Goals

- Carrying task payloads, step data, or execution state inside `NOTIFY` payloads.
- PostgreSQL DDL triggers or new SQL migrations (`0001_scheduler.sql` and `0002_execution.sql` remain unchanged).
- Replacing `poll_interval` with a dynamic `MIN(execution_time)` next-due database query
  (future optimization; future-dated `ctx.sleep` tasks are picked up by the fallback poll).
- Implementing Spec 0054's `zart_queues` schema, `QueueSpec`, or `WorkerConfig::subscriptions`
  in this spec (we only prepare the JSON payload format and wakeup filter hook).
- Changing user-facing workflow contracts (`#[zart_durable]`, `#[zart_step]`, `DurableExecution`, `ScheduledTask`).

---

## Before / After Scenarios

### Before — Fixed-Interval Polling

```rust
let pool = PgPool::connect(&database_url).await?;
let pg = PgBackend::new(pool);

// Operators must choose between high latency (5s) or heavy DB polling (100ms).
let worker = WorkerBuilder::from_backend(&pg)
    .register_durable_task("checkout", CheckoutWorkflow)
    .config(WorkerConfig {
        poll_interval: Duration::from_millis(100), // 10 queries/sec/worker when idle
        ..Default::default()
    })
    .build();
```

### After — Default Setup (Direct PostgreSQL or Session-Mode Pool)

Zero API changes required. `PgListener` borrows one session connection from the existing
`PgPool`, and `poll_interval` acts as a low-frequency reconciliation net:

```rust
// PgPool must allow at least 2 connections (1 for PgListener + >=1 for polls/writes).
let pool = PgPool::connect(&database_url).await?;
let pg = PgBackend::new(pool);

let worker = WorkerBuilder::from_backend(&pg)
    .register_durable_task("checkout", CheckoutWorkflow)
    .config(WorkerConfig {
        // Fallback reconciliation interval — no impact on step/task wakeup latency.
        poll_interval: Duration::from_secs(30),
        ..Default::default()
    })
    .build();
```

### After — Production PgBouncer Setup (Transaction Pooling + Dedicated Listener)

When the main `PgPool` connects through PgBouncer in `transaction` pooling mode
(which rejects session-scoped `LISTEN` commands), configure a dedicated direct
PostgreSQL connection for `PgListener`:

```rust
// Main pool talks to PgBouncer (port 6432, transaction pooling mode).
let tx_pool = PgPool::connect("postgres://user:pass@pgbouncer:6432/mydb").await?;

// Listener connects directly to PostgreSQL (port 5432, 1 persistent session per worker).
let pg = PgBackend::new(tx_pool)
    .with_listener_url("postgres://user:pass@postgres-primary:5432/mydb")?;

let worker = WorkerBuilder::from_backend(&pg)
    .register_durable_task("checkout", CheckoutWorkflow)
    .config(WorkerConfig {
        poll_interval: Duration::from_secs(60),
        max_tasks_per_poll: 10,
        max_concurrent_tasks: 16,
        enable_listen_notify: true,
        ..Default::default()
    })
    .build();
```

### After — `WorkerConfig` Tuning & `LISTEN/NOTIFY` Opt-Out

If an environment cannot permit any `LISTEN` session connection (e.g., restricted
serverless proxy without direct primary access), operators can disable `PgListener`
while still benefiting from `local_wakeup` for same-process `Body -> Step -> Body`
transitions, real backpressure, and drain mode:

```rust
let config = WorkerConfig {
    enable_listen_notify: false, // disables PgListener; local_wakeup + fallback poll stay active
    poll_interval: Duration::from_secs(5),
    ..Default::default()
};
```

---

## Design

### Part 1 — `TableNames::wakeup_channel` & `pg_notify` SQL Helpers

#### 1a. Channel Name Derivation (`crates/zart-scheduler/src/postgres/table_names.rs`)

In `crates/zart-scheduler/src/postgres/table_names.rs` (`L31–71`), `TableNames` stores
`tasks: String` resolved via `TableNameConfig::resolve("tasks")` (`crates/zart-core/src/table_names.rs:137–143`),
which produces double-quoted SQL identifiers such as `"\"zart_tasks\""`, `"\"sched_tasks\""`,
or `"\"tenant_a\".\"zart_tasks\""`.

`TableNames` gains a pre-resolved `wakeup_channel: String` field computed once inside
`TableNames::from_config`:

```rust
#[derive(Debug, Clone)]
pub struct TableNames {
    tasks: String,
    wakeup_channel: String,
}
```

Derivation rule from the resolved `tasks` identifier (zero changes to `zart-core`):
- Strip double quotes (`"`) and replace schema separator `.` with `_`, then append `_wakeup`:
  - Default (`"\"zart_tasks\""`) -> `"zart_tasks_wakeup"`
  - Custom prefix `"sched_"` (`"\"sched_tasks\""`) -> `"sched_tasks_wakeup"`
  - Schema `"tenant_a"` (`"\"tenant_a\".\"zart_tasks\""`) -> `"tenant_a_zart_tasks_wakeup"`
  - Prefix `"svc_"` + schema `"myschema"` (`"\"myschema\".\"svc_tasks\""`) -> `"myschema_svc_tasks_wakeup"`
- PostgreSQL `NAMEDATALEN` truncates `LISTEN`/`pg_notify` channel names at **63 bytes**.
  If the derived channel exceeds 63 bytes, truncate to 54 bytes and append `_` + an 8-hex-char
  FNV-1a/CRC hash of the full channel string so distinct long tenant/schema names never collide.

Accessor on `TableNames`:

```rust
pub fn wakeup_channel(&self) -> &str {
    &self.wakeup_channel
}
```

*(Note on `crates/zart/src/postgres/mod.rs` and `table_names.rs`: today `PgBackend::with_table_names` at `L203–211` and `PostgresStorage::with_table_names` at `L112–119` construct `PostgresTaskScheduler::new(pool.clone())` instead of passing custom table names. To keep `wakeup_channel` consistent across `PostgresStorage` and `PostgresTaskScheduler` for custom-prefix deployments, `zart::postgres::TableNames` should store or derive `zart_scheduler::TableNames` and pass it into `PostgresTaskScheduler::with_table_names`.)*

#### 1b. Explicit `pg_notify` in `zart-scheduler` SQL Helpers

Every write in `zart-scheduler` that puts a task into `status = 'scheduled'` emits
`SELECT pg_notify($channel, $payload)` **inside the same SQL statement / transaction**
only when `execution_time <= NOW()`. Because PostgreSQL delivers `NOTIFY` asynchronously
**only upon transaction `COMMIT`** (and discards it on `ROLLBACK`), workers never wake
for uncommitted or rolled-back rows.

Until Spec 0054 adds the `queue` column, `$payload` is the static JSON string:

```rust
pub(crate) const DEFAULT_WAKEUP_PAYLOAD: &str = r#"{"queue":"default"}"#;
```

**1. `schedule_at_sql` (`crates/zart-scheduler/src/postgres/sql_helpers.rs:200–235`):**

```sql
WITH inserted AS (
    INSERT INTO {tasks}
        (task_id, task_name, execution_time, data, recurrence, metadata)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (task_id) DO NOTHING
    RETURNING execution_time
),
notified AS (
    SELECT pg_notify($7, $8)
    FROM inserted
    WHERE execution_time <= NOW()
)
SELECT (SELECT COUNT(*)::BIGINT FROM notified)
```

- Single round-trip; `ON CONFLICT (task_id) DO NOTHING` produces 0 rows in `inserted`,
  so duplicate/idempotent `schedule_at` calls emit zero notifications.
- Future-dated tasks (`ctx.sleep`, `wait_for_event` sentinel `DateTime::MAX_UTC`) do not
  satisfy `execution_time <= NOW()` and emit zero notifications.

**2. `update_task_state_sql` (`crates/zart-scheduler/src/postgres/sql_helpers.rs:132–195`):**

Must preserve `StorageError::LockMismatch` detection (`rows_affected == 0`) even when
`execution_time > NOW()`. Using a two-counter outer `SELECT`:

```sql
WITH updated AS (
    UPDATE {tasks}
    SET state          = $1,
        execution_time = $2,
        status         = 'scheduled',
        locked_at      = NULL,
        worker_id      = NULL,
        updated_at     = NOW()
        -- plus metadata = COALESCE(metadata, '{}') || $5 when metadata_patch is Some
    WHERE task_id   = $3
      AND worker_id = $4
    RETURNING execution_time
),
notified AS (
    SELECT pg_notify($N, $M)
    FROM updated
    WHERE execution_time <= NOW()
)
SELECT
    (SELECT COUNT(*)::BIGINT FROM updated),
    (SELECT COUNT(*)::BIGINT FROM notified)
```

If `updated_count == 0`, return `Err(StorageError::LockMismatch(task_id.to_string()))`.

**3. `mark_failed_sql` (`crates/zart-scheduler/src/postgres/sql_helpers.rs:58–123`):**

- `Some(t)` (task rescheduled as `'scheduled'` for retry): wrap the `UPDATE` in the
  exact same `WITH updated AS (... RETURNING execution_time), notified AS (...)` CTE so
  immediate retries (`t <= NOW()`) emit `pg_notify`, while delayed backoff retries
  (`t > NOW()`) do not. Preserves `LockMismatch` when `updated_count == 0`.
- `None` (terminal `'failed'`): unchanged — no `pg_notify`.

**4. `complete_and_schedule` (`crates/zart-scheduler/src/postgres/scheduler_impl.rs:370–431`):**

Currently runs an inline `UPDATE {tasks}` (`L380–400`) and inline `INSERT INTO {tasks}`
(`L409–425`). Refactor to call `mark_completed_sql` followed by `schedule_at_sql` inside
the same transaction, automatically inheriting `pg_notify` emission and eliminating
duplicate SQL.

**5. `recover_orphans` (`crates/zart-scheduler/src/postgres/scheduler_impl.rs:321–346`):**

```sql
WITH recovered AS (
    UPDATE {tasks}
    SET status     = 'scheduled',
        locked_at  = NULL,
        worker_id  = NULL,
        updated_at = NOW()
    WHERE status    = 'picked_up'
      AND locked_at < $1
    RETURNING execution_time
),
notified AS (
    SELECT pg_notify($2, $3)
    FROM (SELECT 1 FROM recovered WHERE execution_time <= NOW() LIMIT 1) _
)
SELECT
    (SELECT COUNT(*)::BIGINT FROM recovered),
    (SELECT COUNT(*)::BIGINT FROM notified)
```

Using `LIMIT 1` emits at most **one** `pg_notify` regardless of how many stale orphans
were recovered in the sweep, returning `recovered_count as usize`.

---

### Part 2 — `TaskScheduler::subscribe_wakeups` & `local_wakeup` in `PostgresTaskScheduler` and `ExecutionOps`

#### 2a. `WorkerConfig` & `WakeupOptions` (`worker_config.rs`, `store.rs`)

Add `enable_listen_notify: bool` to `WorkerConfig` (`crates/zart-scheduler/src/worker_config.rs:18–54`):

```rust
pub struct WorkerConfig {
    /// How often the worker polls the database for due tasks.
    ///
    /// When `enable_listen_notify` is `true` (default), this acts as the
    /// fallback reconciliation poll interval (can safely be set to `30s`–`60s`
    /// in production without hurting task wakeup latency).
    pub poll_interval: Duration,

    /// Maximum number of tasks to fetch per poll cycle.
    pub max_tasks_per_poll: usize,

    /// Maximum number of tasks that can execute concurrently within this worker.
    pub max_concurrent_tasks: usize,

    /// How long to wait for in-flight tasks to finish during graceful shutdown.
    pub shutdown_timeout: Duration,

    /// Tasks stuck in `picked_up` longer than this are considered orphaned
    /// and will be reset to `scheduled` by the orphan recovery loop.
    pub orphan_timeout: Duration,

    /// How often to renew the task lease while a handler is executing.
    pub heartbeat_interval: Option<Duration>,

    /// Enable PostgreSQL `LISTEN / NOTIFY` wakeup hints when supported by the
    /// scheduler backend. Defaults to `true`.
    ///
    /// Even when `false`, in-process `local_wakeup` hints and the `poll_interval`
    /// fallback loop remain active.
    pub enable_listen_notify: bool,
}
```

`WorkerConfig::default()` sets `enable_listen_notify: true` and keeps `poll_interval: Duration::from_secs(5)`
so existing tests and non-Postgres mocks preserve identical defaults.

Define `WakeupOptions` (in `crates/zart-scheduler/src/types.rs` or `store.rs`, re-exported from `lib.rs`):

```rust
/// Configuration passed to [`TaskScheduler::subscribe_wakeups`].
#[derive(Debug, Clone, Default)]
pub struct WakeupOptions {
    /// Whether to subscribe to cross-process backend hints (e.g. Postgres `LISTEN/NOTIFY`).
    /// In-process wakeups remain active regardless when supported.
    pub enable_listen_notify: bool,
    /// Optional queue filter for Spec 0054 forward compatibility.
    /// Empty means wake on notifications for any queue.
    pub subscribed_queues: Vec<String>,
}
```

#### 2b. `TaskScheduler` Trait Additions (`crates/zart-scheduler/src/store.rs`)

Add two non-breaking methods with default implementations to `TaskScheduler`:

```rust
/// Subscribe to ephemeral task-readiness wakeup hints.
///
/// Returned `Notify` is signaled whenever a task may have become due
/// (e.g. via `LISTEN/NOTIFY` or an in-process scheduling commit).
/// Returning `None` (the default for simple/mock schedulers) tells the
/// worker to rely solely on `WorkerConfig::poll_interval`.
async fn subscribe_wakeups(
    &self,
    options: WakeupOptions,
    cancel: tokio_util::sync::CancellationToken,
) -> Option<std::sync::Arc<tokio::sync::Notify>> {
    let _ = (options, cancel);
    None
}

/// Signal in-process workers that a transaction scheduling due work has committed.
///
/// Called strictly *after* `tx.commit()` by `ExecutionOps` (and `zart` storage
/// paths that manage their own transaction around `schedule_at_in_tx`).
/// Default implementation is a no-op.
fn notify_local_wakeup(&self) {}
```

Because both methods have default implementations, existing mocks (`StubScheduler` in
`crates/zart-scheduler/src/lib.rs:61`, `RecordingScheduler` in `crates/zart/src/test_helpers.rs:88`,
`CapturingStorage` in `crates/zart/src/step_ops.rs:298`, and `MockScheduler` in `crates/zart/tests/macros.rs:65`)
continue to compile without modification.

#### 2c. `local_wakeup` Fast-Path in `PostgresTaskScheduler` and `ExecutionOps`

`PostgresTaskScheduler` (`crates/zart-scheduler/src/postgres/mod.rs:34–37`) maintains:
- `pub(crate) local_wakeup: Arc<tokio::sync::Notify>` — the canonical in-process wakeup doorbell.
- `pub(crate) wakeup_subscribers: Mutex<Vec<WakeupSubscriber>>` — weak handles to per-worker
  coalesced `Arc<Notify>` instances (plus optional `subscribed_queues` filter), pruned lazily
  on each wakeup so dropped workers leave no leaks.

When `PostgresTaskScheduler::notify_local_wakeup()` is called (or via an internal
`trigger_local_wakeup(queue: Option<&str>)` helper):
1. Call `self.local_wakeup.notify_one()`.
2. Signal `notify.notify_one()` on every live subscriber in `wakeup_subscribers` whose
   `subscribed_queues` is empty or matches `queue`.

**Exact Post-Commit Firing Rule (No Premature Wakeup Race):**
`notify_local_wakeup()` **MUST NEVER** fire before `tx.commit().await` succeeds. If it fired
before commit, the worker could wake, execute `poll_due`, fail to see the uncommitted row,
and return to sleep for up to `poll_interval`.

Post-commit firing sites:

1. **`PostgresTaskScheduler` (`crates/zart-scheduler/src/postgres/scheduler_impl.rs`):**
   - `schedule_at` (`L36–49`): after `tx.commit().await?`, if `params.execution_time <= Utc::now()`, call `self.trigger_local_wakeup(...)`.
   - `complete_and_schedule` (`L370–431`): after `tx.commit().await?`, if `params.new_execution_time <= Utc::now()`, call `self.trigger_local_wakeup(...)`.
   - `update_task_state` (`L157–179`): after autocommit `update_task_state_sql` succeeds, if `next_execution_time <= Utc::now()`, call `self.trigger_local_wakeup(None)`.
   - `mark_failed` (`L249–270`): after autocommit `mark_failed_sql` succeeds, if `matches!(next_execution_time, Some(t) if t <= Utc::now())`, call `self.trigger_local_wakeup(None)`.
   - `recover_orphans` (`L321–346`): if `notified_count > 0`, call `self.trigger_local_wakeup(None)`.
   - *(Note: `schedule_at_in_tx`, `update_task_state_in_tx`, `mark_completed_in_tx`, and `mark_failed_in_tx` do NOT commit; their callers trigger `notify_local_wakeup()` after `tx.commit()`.)*

2. **`ExecutionOps` (`crates/zart-scheduler/src/ops.rs:65–150`):**
   - `ExecutionOps::complete` (`L65–81`) and `ExecutionOps::complete_in_tx` (`L87–103`):
     Record `let has_due_next = schedule_next.iter().any(|p| p.execution_time <= Utc::now());`
     before consuming `schedule_next`, run `tx.commit().await?`, then if `has_due_next`, call
     `self.scheduler.notify_local_wakeup()`.
   - `ExecutionOps::reschedule` (`L110–126`) and `ExecutionOps::reschedule_in_tx` (`L131–150`):
     After `tx.commit().await?`, if `at <= Utc::now()`, call `self.scheduler.notify_local_wakeup()`.
   - Also expose a `pub(crate)` or `pub fn notify_local_wakeup(&self)` on `ExecutionOps` if needed
     by `ZartWaitGroupChildCompletion` (`crates/zart/src/step_completion.rs:54–57`), because
     `ZartWaitGroupChildCompletion` schedules the continuation body inside `complete_wait_group_child_in_tx`
     and passes `schedule_next: vec![]` to `ops.complete_in_tx(self.tx, None, vec![])`!

3. **`zart` Crate Caller-Owned Transaction Sites:**
   In Zart's durable workflow layer, `Body -> Step` transitions commit in
   `StepStore::schedule_step` (`crates/zart/src/postgres/step_storage_impl.rs:301`) *before*
   `ZartTask::execute` returns `OnComplete { result: None, schedule_next: vec![] }`
   (`crates/zart/src/task.rs:359–362`). Likewise, `EventStore::deliver_event` (`event_storage_impl.rs:145`),
   `WaitGroupStore::complete_wait_group_child` (`wait_group_storage_impl.rs:57`),
   `ZartWaitGroupChildCompletion::complete` (`step_completion.rs:55–57`),
   `DurableScheduler::start` (`durable.rs:205`), and `do_retry_dead_step` / `do_restart_run`
   (`admin_storage_impl.rs:139, 247, 345`) commit their own transactions around
   `self.task_scheduler.schedule_at_in_tx(...)`. Each of these sites invokes
   `self.task_scheduler.notify_local_wakeup()` immediately after `tx.commit().await?` when
   `execution_time <= Utc::now()` (or when `triggered == true` for WaitGroups), ensuring
   zero-latency `Body -> Step -> Body` even when `enable_listen_notify == false`.

---

### Part 3 — `PgListener` Background Task + `with_listener_options` / `with_listener_url` on `PostgresTaskScheduler` and `PgBackend`

#### 3a. Builders on `PostgresTaskScheduler` and `PgBackend`

Extend `PostgresTaskScheduler` (`crates/zart-scheduler/src/postgres/mod.rs:34–57`):

```rust
pub struct PostgresTaskScheduler {
    pub(crate) pool: PgPool,
    pub(crate) table_names: TableNames,
    pub(crate) listener_options: Option<PgConnectOptions>,
    pub(crate) local_wakeup: Arc<tokio::sync::Notify>,
    pub(crate) wakeup_subscribers: std::sync::Mutex<Vec<WakeupSubscriber>>,
}

impl PostgresTaskScheduler {
    pub fn new(pool: PgPool) -> Self { ... }
    pub fn with_table_names(pool: PgPool, table_names: TableNames) -> Self { ... }

    /// Configure dedicated direct connection options for `PgListener` (`LISTEN/NOTIFY`).
    ///
    /// Required when `pool` connects through a transaction-mode pooler (e.g. PgBouncer)
    /// that does not support session-level `LISTEN` commands.
    pub fn with_listener_options(mut self, options: PgConnectOptions) -> Self {
        self.listener_options = Some(options);
        self
    }

    /// Convenience wrapper around [`Self::with_listener_options`] that parses a
    /// PostgreSQL connection URL.
    pub fn with_listener_url(self, url: &str) -> Result<Self, StorageError> {
        let options: PgConnectOptions = url
            .parse()
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(self.with_listener_options(options))
    }
}
```

Extend `PgBackend` (`crates/zart/src/postgres/mod.rs:185–236`) with matching builders:

```rust
impl PgBackend {
    pub fn with_listener_options(self, options: PgConnectOptions) -> Self {
        let pool = self.storage.pool().clone();
        let table_names = self.storage.table_names().clone();
        let sched_names = table_names.to_scheduler_table_names();
        let scheduler = Arc::new(
            PostgresTaskScheduler::with_table_names(pool.clone(), sched_names)
                .with_listener_options(options),
        );
        let storage = Arc::new(PostgresStorage::with_table_names_and_scheduler(
            pool,
            table_names,
            scheduler.clone(),
        ));
        Self { storage, scheduler }
    }

    pub fn with_listener_url(self, url: &str) -> Result<Self, StorageError> {
        let options: PgConnectOptions = url
            .parse()
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(self.with_listener_options(options))
    }
}
```

*Important invariant:* rebuilding both `scheduler` and `storage` together ensures
`PgBackend::scheduler()` and `PostgresStorage::task_scheduler` continue to share the
**exact same `Arc<PostgresTaskScheduler>` instance**, so `local_wakeup` notifications
from `DurableScheduler` and `PostgresStorage` reach the `Worker` subscribed to
`backend.scheduler()`.

#### 3b. `PgListener` Lifecycle & Reconnect Loop (`crates/zart-scheduler/src/postgres/listener.rs`)

Keep the `PgListener` runtime logic in a dedicated submodule `crates/zart-scheduler/src/postgres/listener.rs`
so `scheduler_impl.rs` stays well below the 700-line module threshold.

When `PostgresTaskScheduler::subscribe_wakeups(options, cancel)` is invoked:

1. Create a coalescing `wakeup = Arc::new(tokio::sync::Notify::new())` and register it in
   `self.wakeup_subscribers` alongside `options.subscribed_queues`.
2. If `!options.enable_listen_notify`, return `Some(wakeup)` immediately without spawning
   any `PgListener` task (`local_wakeup` still signals `wakeup`).
3. If `options.enable_listen_notify`:
   - Spawn a background task bound to `cancel: CancellationToken` (exiting cleanly on
     `cancel.cancelled()`).
   - **Connection acquisition:**
     - If `self.listener_options` is `Some(opts)`, build a 1-connection lazy pool via
       `PgPoolOptions::new().max_connections(1).min_connections(0).connect_lazy_with(opts)`
       and connect with `PgListener::connect_with(&listener_pool).await`.
     - Otherwise, connect using the shared pool via `PgListener::connect_with(&self.pool).await`.
     - If initial connection or `listener.listen(&channel).await` fails (e.g., DB restart
       or transient network error), log a warning and retry with bounded exponential backoff
       (`1s -> 2s -> 5s -> 10s -> 30s` max, interruptible by `cancel.cancelled()`). Never
       panic or abort `Worker::run`.
     - Note: in default shared-pool mode, `PgListener` holds 1 persistent connection from
       `self.pool`; documentation notes `max_connections >= 2` is required.
   - **Receive & Reconnect Reconciliation (`try_recv` + `eager_reconnect`):**
     - `sqlx::postgres::PgListener` defaults to `eager_reconnect(true)`. Keep it enabled
       and drive the loop with `listener.try_recv().await` inside `tokio::select!` against
       `cancel.cancelled()` (rather than `recv()`, which hides connection drops):
       - `Ok(Some(notification))`: evaluate payload against `subscribed_queues` (Part 6);
         if matched, call `wakeup.notify_one()`.
       - `Ok(None)`: `sqlx` detected a dropped connection and eagerly reconnected &
         re-subscribed to `channel`. Because `NOTIFY` messages emitted while disconnected
         are permanently lost by PostgreSQL, **immediately call `wakeup.notify_one()`**
         (`D5`) to force a `poll_due` reconciliation sweep.
       - `Err(e)`: log `tracing::warn!` / `tracing::error!`, drop the broken `PgListener`,
         sleep with exponential backoff (`1s..=30s`), reconnect + `listen(&channel)`, and
         upon successful reconnect immediately call `wakeup.notify_one()`.
   - Return `Some(wakeup)` immediately after spawning the task (so `Worker::run` does not
     block startup if `PgListener` needs backoff retry).

---

### Part 4 — `Worker::run` Semaphore-First Backpressure, Drain Loop, Wakeup Select, and Time-Based Orphan Recovery

Refactor `Worker::run` in `crates/zart-scheduler/src/worker.rs` (`L94–191`) around the
following 5-stage loop:

```rust
let wakeup = self
    .scheduler
    .subscribe_wakeups(
        WakeupOptions {
            enable_listen_notify: self.config.enable_listen_notify,
            subscribed_queues: Vec::new(), // populated from WorkerConfig::subscriptions in Spec 0054
        },
        self.cancellation.clone(),
    )
    .await;

let max_concurrent = self.config.max_concurrent_tasks.max(1);
let max_per_poll = self.config.max_tasks_per_poll.max(1);
let semaphore = Arc::new(Semaphore::new(max_concurrent));
let permit_returned = Arc::new(Notify::new());

let orphan_recovery_interval = (self.config.orphan_timeout / 2).max(Duration::from_secs(10));
let mut last_orphan_recovery = tokio::time::Instant::now();
```

And inside each spawned task, use a small RAII `PermitDropGuard` to uphold strict ordering:

```rust
struct PermitDropGuard {
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    notify: Arc<Notify>,
}

impl Drop for PermitDropGuard {
    fn drop(&mut self) {
        // CRITICAL: release the semaphore permit BEFORE notifying the worker loop.
        // If notify_one() ran first, the loop could observe available_permits() == 0
        // and park indefinitely waiting for a permit notification already consumed.
        drop(self.permit.take());
        self.notify.notify_one();
    }
}
```

#### Loop Stages

1. **Time-Based Orphan Recovery (`D7`):**
   Replace `poll_count.is_multiple_of(10)` (`worker.rs:134`) with `Instant` elapsed time:
   ```rust
   if last_orphan_recovery.elapsed() >= orphan_recovery_interval {
       last_orphan_recovery = tokio::time::Instant::now();
       match self.scheduler.recover_orphans(self.config.orphan_timeout).await {
           Ok(n) if n > 0 => info!(recovered = n, "Orphan tasks recovered"),
           Ok(_) => {}
           Err(e) => error!(error = %e, "Orphan recovery failed"),
       }
   }
   ```

2. **Semaphore-First Backpressure Gate (`D6`):**
   Check `semaphore.available_permits()` **before** calling `poll_due`:
   ```rust
   let available = semaphore.available_permits();
   if available == 0 {
       tokio::select! {
           biased;
           _ = self.shutdown.notified() => break,
           _ = self.cancellation.cancelled() => break,
           _ = permit_returned.notified() => continue,
       }
   }
   ```
   While saturated (`available == 0`), the worker issues zero `poll_due` queries, locks
   zero additional rows as `'picked_up'`, and ignores `NOTIFY` bursts until a permit
   actually returns (with `Notify::notify_one()` storing 1 permit if a task finishes
   between `available_permits()` and `select!`).

3. **Bounded `poll_due` & Task Dispatch (`D6`):**
   ```rust
   let batch_limit = max_per_poll.min(available);
   let tasks = match self.scheduler.poll_due(chrono::Utc::now(), batch_limit).await {
       Ok(t) => t,
       Err(e) => {
           error!(error = %e, "Failed to poll for due tasks");
           tokio::select! {
               biased;
               _ = self.shutdown.notified() => break,
               _ = self.cancellation.cancelled() => break,
               _ = tokio::time::sleep(self.config.poll_interval) => continue,
           }
       }
   };
   ```
   Note that shutdown/cancellation is also respected during the DB-error backoff so a
   `30s–60s` `poll_interval` never delays worker shutdown.

   For each fetched task, acquire an owned permit via `semaphore.clone().try_acquire_owned()`
   (guaranteed non-blocking since `tasks.len() <= batch_limit <= available`), wrap it in
   `PermitDropGuard`, and `tokio::spawn` `dispatch_task`.

4. **Continuous Drain Loop (`D7`):**
   ```rust
   if fetched_count == batch_limit {
       if semaphore.available_permits() > 0 {
           tokio::task::yield_now().await;
       }
       continue;
   }
   ```
   - **Full batch + free permits (`available > 0`):** Yield once to let spawned tasks
     progress, then immediately poll the next batch without sleeping on `poll_interval`
     or waiting for another `NOTIFY`.
   - **Full batch + saturated (`available == 0`):** `continue` directly to Stage 2 so the
     worker parks on `permit_returned.notified()` instead of sleeping in the idle `select!`
     for `poll_interval` while more queued tasks remain in PostgreSQL.
   - **Partial or empty batch (`fetched_count < batch_limit`):** The queue is currently
     drained; fall through to Stage 5.

5. **Idle Mode (`D1`, `D7`):**
   ```rust
   tokio::select! {
       biased;
       _ = self.shutdown.notified() => {
           info!("Scheduler worker shutdown signal received, exiting poll loop");
           break;
       }
       _ = self.cancellation.cancelled() => {
           info!("Scheduler worker cancellation token triggered, exiting poll loop");
           break;
       }
       _ = async {
           match &wakeup {
               Some(notify) => notify.notified().await,
               None => std::future::pending::<()>().await,
           }
       } => {}
       _ = tokio::time::sleep(self.config.poll_interval) => {}
   }
   ```
   Also note loop ordering change: polling happens **before** the idle `select!` rather
   than sleeping `poll_interval` first (`worker.rs:119–130`), so tasks already present
   in `zart_tasks` at worker startup (including `pending_recurring` seeded at `L104–108`)
   are picked up immediately even when `poll_interval` is `30s–60s`.

---

### Part 5 — `wait_group_storage_impl.rs` Unification (`D3`)

In `crates/zart/src/postgres/wait_group_storage_impl.rs`, two call sites still insert
directly into `{tasks}` instead of delegating to `self.task_scheduler`:

1. **`complete_wait_group_child_in_tx_impl` (`L157–238`, direct `INSERT` at `L220–235`):**
   Add `task_scheduler: &dyn TaskScheduler` as a parameter (mirroring `schedule_step_sql`
   in `crates/zart/src/postgres/sql_helpers.rs:220–242`) and replace lines `220–235` with:

   ```rust
   if triggered {
       let body_metadata =
           TaskMetadata::body(&params.run_id, &params.execution_id).to_json_value();
       task_scheduler
           .schedule_at_in_tx(
               conn,
               ScheduleAtParams {
                   task_id: params.next_body_task_id.clone(),
                   task_name: crate::TASK_NAME.to_string(),
                   execution_time: chrono::Utc::now(),
                   data: params.data.clone(),
                   recurrence: None,
                   metadata: body_metadata,
               },
           )
           .await?;
   }
   ```

   Both callers pass `&*self.task_scheduler`:
   - `PostgresStorage::complete_wait_group_child` (`L44–61`): after `tx.commit().await?`,
     if `triggered`, calls `self.task_scheduler.notify_local_wakeup()`.
   - `PostgresStorage::complete_wait_group_child_in_tx` (`L63–69`): returns `triggered`;
     its caller `ZartWaitGroupChildCompletion::complete` (`crates/zart/src/step_completion.rs:54–57`)
     triggers `ops.notify_local_wakeup()` after `ops.complete_in_tx(...)` commits when
     `triggered == true`.

2. **`recover_wait_group_orphans` (`L98–154`, direct `INSERT` at `L128–146`):**
   Include `r.payload` in the existing `SELECT ... FROM {steps} s JOIN {execution_runs} r`
   query (`L100–118`) and replace the `INSERT INTO {tasks} ... SELECT ...` at `L128–146`
   with `self.task_scheduler.schedule_at(ScheduleAtParams { ... }).await?` (which executes
   `schedule_at_sql`, emits `pg_notify`, and fires `local_wakeup` on commit).

   *Subtlety on return count:* `ScheduleResult` intentionally remains `{ task_id, execution_time }`
   to avoid breaking struct literals across test doubles. Because `recover_wait_group_orphans`
   previously checked `if inserted > 0 { recovered += 1; }`, either filter out already-scheduled
   tasks via a non-breaking helper on `TaskScheduler` or document the idempotent `schedule_at`
   delegation cleanly in `S1.2`.

*(Audit note on other `zart` SQL sites: `event_storage_impl.rs:131`, `step_storage_impl.rs:271`,
and `admin_storage_impl.rs:98, 233, 331` already delegate through `self.task_scheduler`. The
only remaining non-scheduler `UPDATE {tasks} SET status = 'scheduled'` is
`StepStore::reschedule_step_for_retry` at `crates/zart/src/postgres/step_storage_impl.rs:426–440`,
which can optionally delegate to `self.task_scheduler.mark_failed_in_tx` so immediate step
retries also emit `pg_notify` + `local_wakeup`.)*

---

### Part 6 — Spec 0054 Queue Payload Compatibility (`D8`)

To ensure zero wire-format breakage when [0054-task-queues.md](file:///Users/paulosuzart/workspace/personal/zart/specs/0054-task-queues.md)
is implemented:

1. **Producer Payload Format:**
   Every `pg_notify($channel, $payload)` uses a compact JSON object:
   ```json
   {"queue":"default"}
   ```
   - Pre-Spec 0054: all helpers pass `r#"{"queue":"default"}"#`.
   - Post-Spec 0054:
     - `schedule_at_sql` builds `serde_json::json!({ "queue": params.queue.as_deref().unwrap_or("default") }).to_string()`.
     - `update_task_state_sql`, `mark_failed_sql`, and `recover_orphans` use `RETURNING execution_time, queue`
       and emit `pg_notify($channel, json_build_object('queue', queue)::text)`
       (in `recover_orphans`, `SELECT DISTINCT queue FROM recovered WHERE execution_time <= NOW()`).
   - PostgreSQL automatically deduplicates identical `(channel, payload)` notifications
     within the same transaction, so inserting 50 tasks into `"default"` in one
     transaction delivers a single `NOTIFY`.

2. **Consumer Filtering in `PgListener` & `local_wakeup`:**
   ```rust
   #[derive(Debug, serde::Deserialize)]
   struct WakeupPayload {
       queue: String,
   }

   fn matches_subscriptions(payload: &str, subscribed_queues: &[String]) -> bool {
       if subscribed_queues.is_empty() {
           return true;
       }
       match serde_json::from_str::<WakeupPayload>(payload) {
           Ok(parsed) => subscribed_queues.iter().any(|q| q == &parsed.queue),
           Err(_) => true, // Fail-open: hints are never truth; unknown payload triggers poll.
       }
   }
   ```
   - Empty `subscribed_queues` (all workers pre-Spec 0054, or `WorkerConfig::subscriptions = vec![]`
     post-Spec 0054): wakes on every notification.
   - Non-empty `subscribed_queues` (once Spec 0054 populates `WakeupOptions::subscribed_queues`
     from `WorkerConfig::subscriptions`): wakes only if the notification's `"queue"` matches,
     preventing cross-pool thundering herd when dedicated workers listen on shared databases.

---

## Files Affected

| File | Change |
|---|---|
| `crates/zart-scheduler/src/worker_config.rs` | Add `enable_listen_notify: bool` (default `true`); update `poll_interval` doc comments for fallback role |
| `crates/zart-scheduler/src/types.rs` | Export/re-export `WakeupOptions` (or define in `store.rs` and re-export from `lib.rs`) |
| `crates/zart-scheduler/src/store.rs` | Add `subscribe_wakeups` and `notify_local_wakeup` default methods on `TaskScheduler` |
| `crates/zart-scheduler/src/ops.rs` | Trigger `scheduler.notify_local_wakeup()` after `tx.commit()` when due work is scheduled/rescheduled; expose helper for `ZartWaitGroupChildCompletion` |
| `crates/zart-scheduler/src/lib.rs` | Re-export `WakeupOptions` |
| `crates/zart-scheduler/src/postgres/table_names.rs` | Add `wakeup_channel: String` field and `pub fn wakeup_channel(&self) -> &str` |
| `crates/zart-scheduler/src/postgres/sql_helpers.rs` | Add conditional `pg_notify($channel, $payload)` CTEs (`execution_time <= NOW()`) to `schedule_at_sql`, `update_task_state_sql`, `mark_failed_sql` |
| `crates/zart-scheduler/src/postgres/mod.rs` | Add `listener_options`, `local_wakeup`, `wakeup_subscribers`, `with_listener_options`, `with_listener_url` to `PostgresTaskScheduler` |
| `crates/zart-scheduler/src/postgres/listener.rs` | **New** submodule: `PgListener` background task (`try_recv`, reconnect reconciliation, backoff, queue payload filtering) |
| `crates/zart-scheduler/src/postgres/scheduler_impl.rs` | Implement `subscribe_wakeups` & `notify_local_wakeup`; refactor `complete_and_schedule`; add `pg_notify` to `recover_orphans`; trigger `local_wakeup` after commits |
| `crates/zart-scheduler/src/worker.rs` | Semaphore-first backpressure gate, `PermitDropGuard`, bounded `batch_limit`, drain loop, coalesced wakeup `select!`, `Instant`-based `recover_orphans` |
| `crates/zart/src/postgres/mod.rs` | Add `with_listener_options` and `with_listener_url` on `PgBackend`; propagate scheduler `TableNames` in `PgBackend::with_table_names` |
| `crates/zart/src/postgres/table_names.rs` | Helper to derive `zart_scheduler::TableNames` cleanly |
| `crates/zart/src/postgres/wait_group_storage_impl.rs` | Replace raw `INSERT INTO {tasks}` at `L128` and `L221` with `self.task_scheduler.schedule_at` / `schedule_at_in_tx` |
| `crates/zart/src/postgres/step_storage_impl.rs` | Call `self.task_scheduler.notify_local_wakeup()` after `schedule_step` commit; optionally delegate `reschedule_step_for_retry` (`L426`) to `mark_failed_in_tx` |
| `crates/zart/src/postgres/event_storage_impl.rs` | Call `self.task_scheduler.notify_local_wakeup()` after `deliver_event` commit |
| `crates/zart/src/postgres/admin_storage_impl.rs` | Call `self.task_scheduler.notify_local_wakeup()` after `do_retry_dead_step` and `do_restart_run` commits |
| `crates/zart/src/step_completion.rs` | Signal `ops.notify_local_wakeup()` after `ZartWaitGroupChildCompletion` commits when `triggered == true` |
| `crates/zart/src/durable.rs` | Call `self.scheduler.notify_local_wakeup()` after `DurableScheduler::start` transaction commits |
| `crates/zart-scheduler/tests/integration_test.rs` & `crates/zart/tests/integration/` | Integration tests for sub-100ms wakeup, `local_wakeup`, backpressure, drain, fallback poll, WaitGroup, custom `TableNames` |
| `webpage/src/content/docs/docs/deployment/options.mdx` | Document `LISTEN/NOTIFY` hint model, PgBouncer `with_listener_url`, `PgPool` sizing (`>= 2`), and `WorkerConfig` tuning (`30s–60s`) |

---

## Phase Plans

### Phase 1 — Foundations (`S1.1`, `S1.2`)

- **S1.1 — `WorkerConfig` wakeup flags & `TaskScheduler::subscribe_wakeups` (`zart-scheduler`):**
  - Add `enable_listen_notify: bool` (default `true`) to `WorkerConfig` and update `poll_interval` docs.
  - Define `WakeupOptions` and add default-impl `subscribe_wakeups` and `notify_local_wakeup` to `TaskScheduler` (`crates/zart-scheduler/src/store.rs`).
- **S1.2 — Unify `WaitGroup` `INSERT`s into `zart_tasks` via `TaskScheduler` (`zart`):**
  - Refactor `complete_wait_group_child_in_tx_impl` (`wait_group_storage_impl.rs:221`) and `recover_wait_group_orphans` (`wait_group_storage_impl.rs:128`) to use `self.task_scheduler.schedule_at_in_tx` and `self.task_scheduler.schedule_at`.
  - Fix `PgBackend::with_table_names` (`crates/zart/src/postgres/mod.rs:203`) so `PostgresTaskScheduler` receives matching `TableNames` (and note `step_storage_impl.rs:426` retry unification).
- **Gate:** `just fmt-check`, `just lint`, and `just test` pass with zero changes required to existing mock schedulers.

### Phase 2 — Notify & Listener (`S2.1`, `S2.2`, `S2.3`)

- **S2.1 — Explicit `pg_notify` in SQL helpers & `TableNames::wakeup_channel` (`zart-scheduler`, blocked by `S1.1`, `S1.2`):**
  - Add `TableNames::wakeup_channel()` (`"{tasks_table}_wakeup"`, default `"zart_tasks_wakeup"`, with 63-byte PG limit handling).
  - Emit `SELECT pg_notify($channel, $payload)` with `{"queue":"default"}` when `execution_time <= NOW()` in `schedule_at_sql`, `update_task_state_sql`, `mark_failed_sql` (retry branch), `complete_and_schedule`, and `recover_orphans`.
- **S2.2 — In-process `local_wakeup` fast-path after scheduling commits (`zart-scheduler`, `zart`, blocked by `S1.1`):**
  - Add `local_wakeup: Arc<Notify>` and subscriber registry to `PostgresTaskScheduler`.
  - Fire `notify_local_wakeup()` strictly **after** `tx.commit()` in `PostgresTaskScheduler`, `ExecutionOps::{complete, complete_in_tx, reschedule, reschedule_in_tx}`, and `zart` commit sites (`schedule_step`, `deliver_event`, `complete_wait_group_child`, `ZartWaitGroupChildCompletion`, `DurableScheduler::start`, admin retry/restart).
- **S2.3 — Background `PgListener` with reconnect & PgBouncer support (`zart-scheduler`, `zart`, blocked by `S2.1`, `S2.2`):**
  - Implement `crates/zart-scheduler/src/postgres/listener.rs` using `PgListener::try_recv()` + `eager_reconnect(true)`, exponential backoff on initial/drop errors, immediate reconciliation wakeup on reconnect, `WakeupPayload` filtering, and coalescing into a single `Arc<Notify>` per subscription.
  - Add `with_listener_options(PgConnectOptions)` and `with_listener_url(&str)` on `PostgresTaskScheduler` and `PgBackend` (keeping `storage.task_scheduler` and `backend.scheduler()` pointing to the same `Arc`).
- **Gate:** Unit tests for `TableNames::wakeup_channel`, `matches_subscriptions`, and `local_wakeup` post-commit firing pass; `just lint` + `just test` pass.

### Phase 3 — Worker Loop (`S3.1`, `S3.2`)

- **S3.1 — Real backpressure: semaphore check before `poll_due` (`zart-scheduler`, blocked by `S1.1`):**
  - Check `semaphore.available_permits()` before calling `poll_due`.
  - Park in `tokio::select!` on `permit_returned.notified()` when `available == 0`.
  - Use `PermitDropGuard` so `drop(permit)` strictly precedes `permit_returned.notify_one()`.
  - Cap `poll_due` limit at `max_tasks_per_poll.min(available)`.
- **S3.2 — Continuous drain loop, coalesced wakeup `select!`, and `Instant`-based orphan recovery (`zart-scheduler`, blocked by `S2.3`, `S3.1`):**
  - Drain full batches (`fetched_count == batch_limit`) immediately with `yield_now()` when permits remain, or jump to the Stage 2 permit wait when `available_permits() == 0`.
  - Replace pre-poll sleep with immediate startup poll + idle `tokio::select!` on `wakeup.notified()`, `sleep(poll_interval)`, and shutdown/cancellation (plus shutdown-aware DB error backoff).
  - Replace `poll_count.is_multiple_of(10)` with `(config.orphan_timeout / 2).max(Duration::from_secs(10))` using `tokio::time::Instant`.
- **Gate:** Mock `TaskScheduler` unit tests verify: zero `poll_due` calls while semaphore is full, multi-batch drain without sleeping `poll_interval`, immediate wakeup via `Notify`, and `Instant`-based orphan recovery. `just lint` + `just test` pass.

### Phase 4 — Tests & Docs (`S4.1`, `S4.2`)

- **S4.1 — Integration tests (`zart-scheduler`, `zart`, blocked by `S3.2`):**
  - Sub-100ms wakeup latency under `poll_interval = 30s` (both via `LISTEN/NOTIFY` and via `local_wakeup` with `enable_listen_notify = false`).
  - Backpressure saturation test (`max_concurrent_tasks = 2` with 5 slow tasks -> DB holds at most 2 `picked_up` rows, remaining 3 stay `scheduled`).
  - Drain mode test (batch burst > `max_tasks_per_poll` drains continuously under `poll_interval = 30s`).
  - Fallback poll reconciliation test (simulated disabled/missed notify still executes via `poll_interval`).
  - `WaitGroup` completion wakeup and custom `TableNames` channel isolation tests.
- **S4.2 — Production documentation (`docs` / `webpage/`, blocked by `S3.2`):**
  - Update `webpage/src/content/docs/docs/deployment/options.mdx` covering `LISTEN/NOTIFY` hint model, PgBouncer transaction-pooling setup via `PgBackend::with_listener_url` / `with_listener_options`, `PgPool::max_connections >= 2` guidance, and `WorkerConfig` tuning (`poll_interval: 30s–60s`).
- **Gate:** `just ci` (`fmt-check`, `lint`, `build`, `test`), `just test-integration-core`, and `just doc-check` pass; no module exceeds 700 lines (excluding tests).

---

## Rationale

- **D1 — Notifications as Hints, Never Truth (`LISTEN / NOTIFY` + Fallback Poll):**
  PostgreSQL `NOTIFY` is ephemeral and fire-and-forget: notifications emitted while a
  listener is reconnecting are lost. Keeping `zart_tasks` (`SELECT ... FOR UPDATE SKIP LOCKED`)
  as the single source of truth and retaining `poll_interval` as a slow reconciliation
  fallback guarantees zero lost tasks while allowing production deployments to raise
  `poll_interval` to `30s–60s` with sub-10ms dispatch latency.
- **D2 — Explicit `pg_notify` in `zart-scheduler` SQL Helpers (No PL/pgSQL Triggers):**
  Zart supports runtime-configured `TableNames` (custom prefixes and schemas per tenant).
  PL/pgSQL triggers would require DDL/migrations per dynamic table and would run per row
  or require complex dynamic trigger installation. Emitting `SELECT pg_notify($channel, $payload)`
  inside the five `zart-scheduler` SQL paths when `execution_time <= NOW()` keeps zero
  schema migrations, respects `TableNames` automatically via `"{tasks_table}_wakeup"`,
  and still benefits from PostgreSQL's built-in per-transaction `(channel, payload)` deduplication.
- **D3 — Unify `zart_tasks` Writes via `TaskScheduler`:**
  Every `INSERT INTO {tasks}` outside `zart-scheduler` would silently bypass explicit
  `pg_notify` and `local_wakeup`. Refactoring the final two raw inserts in
  `wait_group_storage_impl.rs` (`L128`, `L221`) to call `TaskScheduler::schedule_at` /
  `schedule_at_in_tx` finishes the architectural boundary already established by
  `event_storage_impl.rs` and `step_storage_impl.rs`.
- **D4 — In-Process Fast-Path (`local_wakeup`):**
  In Zart durable workflows, the vast majority of `zart_tasks` schedules (`Body -> Step -> Body`)
  occur inside a running worker process. Signaling `local_wakeup: Arc<tokio::sync::Notify>`
  strictly after `tx.commit()` ensures zero-latency step chaining even in restricted
  environments where `LISTEN/NOTIFY` is disabled (`enable_listen_notify = false`) or
  temporarily disconnected.
- **D5 — `PgListener` Isolation & PgBouncer Support:**
  `LISTEN` binds session state to a PostgreSQL backend connection, which breaks under
  PgBouncer `transaction` pooling mode. Defaulting to `PgListener::connect_with(&self.pool)`
  keeps out-of-the-box setup unchanged, while optional `with_listener_options` /
  `with_listener_url` builders let PgBouncer users supply a single direct session-mode
  connection for `LISTEN`. Using `try_recv()` + `eager_reconnect(true)` ensures every
  reconnect immediately triggers a reconciliation poll.
- **D6 — Semaphore-First Backpressure:**
  Polling before acquiring capacity causes a saturated worker to lock extra tasks as
  `'picked_up'` in Postgres while waiting on `semaphore.acquire_owned()`, inflating
  memory pressure and lease timeouts. Checking `semaphore.available_permits()` first and
  capping `batch_limit = min(max_tasks_per_poll, available_permits)` keeps unstarted tasks
  `'scheduled'` in Postgres for idle peers to claim.
- **D7 — Drain Mode, Coalescing, and Time-Based Orphan Recovery:**
  Under high load, a full batch (`fetched.len() == batch_limit`) signals remaining queued
  work; polling immediately via `yield_now()` maximizes throughput without waiting for
  extra `NOTIFY` round-trips. Merging `PgListener` and `local_wakeup` into one
  `Arc<tokio::sync::Notify>` naturally coalesces storms of `NOTIFY`s into a single
  wakeup permit. Switching `recover_orphans` from `poll_count.is_multiple_of(10)` to
  `Instant`-based intervals prevents both over-polling during drain bursts and
  under-checking during 60s idle periods.
- **D8 — Spec 0054 Forward-Compatible JSON Payload:**
  Sending `{"queue":"default"}` now avoids changing the `NOTIFY` wire contract when
  Spec 0054 introduces named queues and `WorkerConfig::subscriptions`, preventing
  cross-queue thundering herd among specialized worker pools.

---

## Risk & Mitigation

| Risk | Mitigation |
|---|---|
| `PgPool` configured with `max_connections = 1` deadlocks because `PgListener` holds 1 persistent connection | Document `max_connections >= 2` requirement in `PgBackend` docs and `webpage/`; if `PgListener::connect_with` fails or times out, worker falls back to `local_wakeup` + `poll_interval` |
| `PermitDropGuard` / wakeup race: `notify_one()` fires before `OwnedSemaphorePermit` is dropped, causing worker to re-read `available == 0` and park forever | `PermitDropGuard::drop` explicitly drops `self.permit.take()` *before* calling `self.notify.notify_one()`, and executes even on handler panic |
| Premature `local_wakeup`: firing before `tx.commit()` wakes worker before the row is visible in MVCC | Enforce strict post-commit rule: `notify_local_wakeup()` is called only after `tx.commit().await?` succeeds in `PostgresTaskScheduler`, `ExecutionOps`, and `zart` storage commit sites |
| Saturated worker sleeps for `poll_interval` when full batch exhausts remaining permits (`available_permits() == 0`) | Stage 4 checks `if fetched_count == batch_limit` unconditionally (even when `available_permits() == 0`) and `continue`s to Stage 2, parking on `permit_returned.notified()` instead of entering Stage 5 idle sleep |
| `TableNames` with long schema + prefix exceeds PostgreSQL's 63-byte `NAMEDATALEN` channel limit | Deterministically truncate + append an 8-hex-char hash suffix when `wakeup_channel.len() > 63` |
| Initial `PgListener` connection failure on startup or PgBouncer transient outage stalls worker | `subscribe_wakeups` spawns the listener asynchronously and returns `Some(wakeup)` immediately; listener retries with exponential backoff (`1s..=30s`) while `local_wakeup` and `poll_interval` keep the worker operational |
| `StepStore::reschedule_step_for_retry` (`step_storage_impl.rs:426`) updates `{tasks}` directly without `pg_notify` | Covered in Phase 1/2 cleanup by delegating retry update to `self.task_scheduler.mark_failed_in_tx` when `retry_time <= Utc::now()` (or always) |
| Custom mock `TaskScheduler` implementations in tests fail to compile when new trait methods are added | `subscribe_wakeups` (returns `None`) and `notify_local_wakeup` (no-op) both provide default implementations |

---

## Breaking Changes

**None — all changes are strictly additive and backward-compatible.**

- Existing `PgBackend::new(pool)`, `PgBackend::with_table_names(pool, names)`,
  `PostgresTaskScheduler::new(pool)`, and `PostgresTaskScheduler::with_table_names(pool, names)`
  keep their exact signatures.
- `with_listener_options` and `with_listener_url` are additive builder methods.
- `WorkerConfig` adds `enable_listen_notify: bool` with `Default::default() == true`;
  all callers using `..Default::default()` compile unchanged.
- `TaskScheduler` adds `subscribe_wakeups` and `notify_local_wakeup` with default impls;
  all existing trait implementations and mocks compile unchanged.
- No database migrations or schema modifications are introduced.

---

## Definition of Done

- [ ] `just fmt-check` passes
- [ ] `just lint` passes (`cargo clippy --workspace --all-targets --all-features -- -D warnings`)
- [ ] `just test` passes (all unit tests, including new `TableNames::wakeup_channel`, `WakeupPayload` filter, `local_wakeup`, backpressure, drain mode, and `Instant`-based orphan recovery unit tests)
- [ ] `just test-integration-core` passes (including new integration tests for `< 100ms` wakeup with `poll_interval = 30s`, `enable_listen_notify = false` fast-path, backpressure saturation, drain burst, fallback poll, WaitGroup wakeup, and custom `TableNames` channel isolation)
- [ ] `just doc-check` passes (`RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`)
- [ ] No Rust module exceeds 700 lines (excluding tests)
- [ ] `wait_group_storage_impl.rs` has zero direct `INSERT INTO {tasks}` queries
- [ ] `webpage/src/content/docs/docs/deployment/options.mdx` updated with `LISTEN/NOTIFY`, PgBouncer `with_listener_url`, `PgPool` sizing, and `WorkerConfig` production tuning
- [ ] `ExecutionOps` and `PostgresTaskScheduler` fire `local_wakeup` strictly after `tx.commit()`

---

## Notes

- Forward-compatible with [0054-task-queues.md](file:///Users/paulosuzart/workspace/personal/zart/specs/0054-task-queues.md): once Spec 0054 adds `zart_tasks.queue` and `WorkerConfig::subscriptions`, `Worker::run` populates `WakeupOptions::subscribed_queues` from `self.config.subscriptions` and `sql_helpers.rs` populates the `"queue"` field dynamically from `ScheduleAtParams::queue` / `RETURNING queue`.
- Future enhancement (deferred): a `MIN(execution_time) WHERE status = 'scheduled'` next-due wakeup timer in Stage 5 could wake `ctx.sleep(...)` tasks at their exact due timestamp when `sleep_duration < poll_interval`.
