# Spec 0053 — Recurring DurableExecution

## Status: proposed

---

## Context / Problem

`DurableExecution` (backed by `ZartTask`) has no native recurring support. All call
sites in the `zart` crate hardcode `recurrence: None`:

- `crates/zart/src/durable.rs:182,436`
- `crates/zart/src/task.rs:314,341`

The `Recurrence` type already exists in `zart-core` and drives rescheduling for
`zart-scheduler`'s `ScheduledTask`, but it was never wired into the durable execution
layer.

The root reason is structural: `zart_executions.execution_id` is a `PRIMARY KEY`.
A single stable ID cannot represent multiple independent occurrences. Each run of a
recurring workflow needs its own unique `execution_id`, but there is currently no
mechanism to generate, template, or manage those IDs on a schedule.

Today users who need periodic durable workflows must build this themselves: write a
plain `ScheduledTask` that manually calls `DurableScheduler::start()`, manage
overlap themselves, and template IDs by hand. This is error-prone and undiscoverable.

---

## Goals

- Introduce a `RecurringDurableTask` type that wraps a `DurableExecution` handler and
  is driven by the existing `zart-scheduler` recurring infrastructure (cron or fixed
  delay).
- On each tick, generate a unique `execution_id` from a user-defined template (e.g.
  `"report-{occurrence}"`).
- Let users choose an **overlap policy** that governs what happens when the previous
  execution has not yet finished when the next tick fires.
- Expose a single ergonomic registration method on `WorkerBuilder`.
- Reuse all existing types: `Recurrence`, `DurableScheduler::start_for`, cancellation
  API, `ExecutionStatus`.

## Non-Goals

- Changes to the `zart-scheduler` recurring infrastructure (`ScheduledTask`,
  `Recurrence`, worker rescheduling logic) — reused as-is.
- Changes to `zart_executions` schema.
- Recurring support for Durable Objects (spec 0052) — separate concern.
- UI / admin changes beyond what existing execution visibility already provides.

---

## Design

### `OverlapPolicy`

Controls what `RecurringDurableTask` does when a tick fires and an execution for the
same template ID is still in a non-terminal state.

```rust
/// Defined in `crates/zart/src/recurring.rs`
pub enum OverlapPolicy {
    /// Do nothing if a non-terminal execution already exists.
    /// The new occurrence is skipped silently.
    SkipIfRunning,

    /// Cancel the running execution, then start a fresh one with the same ID.
    CancelAndRestart,

    /// Always start a new execution, regardless of the previous one.
    /// The previous execution continues to run in parallel.
    AlwaysStart,
}
```

### Execution ID templating

The user supplies an `id_template` string. Supported placeholder:

| Placeholder | Value | Example output |
|---|---|---|
| `{occurrence}` | Monotonically incrementing `u64` stored in scheduler task metadata | `"report-42"` |

The counter is stored at `task.metadata["occurrence"]` in `zart_tasks`. Each tick the
`RecurringDurableTask` handler reads it, increments, and writes it back via
`ops.reschedule(next_time)` (which carries the updated metadata through the existing
`zart-scheduler` reschedule path).

`AlwaysStart` uses the counter unconditionally — each tick gets a distinct ID.
`SkipIfRunning` and `CancelAndRestart` use the same counter but may skip starting a new
execution.

### `RecurringDurableTask<H>`

A `ScheduledTask` implementation that dispatches into a `DurableExecution` handler.
Registered under the task name `__zart_recurring__:{task_id}`.

```rust
// crates/zart/src/recurring.rs

pub struct RecurringDurableTask<H: DurableExecution> {
    handler_name: String,
    id_template:  String,
    overlap:      OverlapPolicy,
    scheduler:    Arc<DurableScheduler>,
    _marker:      PhantomData<H>,
}

#[async_trait]
impl<H: DurableExecution> ScheduledTask for RecurringDurableTask<H> {
    async fn execute(
        &self,
        instance: &TaskInstance,
        ops: &mut ExecutionOps<'_>,
    ) -> Result<(), SchedulerTaskError> {
        let occurrence: u64 = instance
            .metadata
            .get("occurrence")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let execution_id = self.id_template.replace("{occurrence}", &occurrence.to_string());

        match self.overlap {
            OverlapPolicy::SkipIfRunning => {
                if self.is_running(&execution_id).await? {
                    // skip — do not start a new execution
                    return Ok(());
                }
            }
            OverlapPolicy::CancelAndRestart => {
                self.cancel_if_running(&execution_id).await?;
            }
            OverlapPolicy::AlwaysStart => {}
        }

        self.scheduler
            .start_for::<H>(&execution_id, instance.data.clone())
            .await
            .map_err(|e| SchedulerTaskError::handler(e.to_string()))?;

        // Persist incremented occurrence counter via metadata update
        // (written into zart_tasks.metadata by ops, carried through reschedule)
        ops.set_metadata(serde_json::json!({ "occurrence": occurrence + 1 }));

        Ok(())
    }
}
```

### `WorkerBuilder` registration

```rust
// crates/zart/src/builder.rs

impl WorkerBuilder {
    pub fn register_recurring_durable<H: DurableExecution + 'static>(
        mut self,
        task_id:     &str,          // scheduler task_id; must be unique per worker
        id_template: &str,          // e.g. "monthly-report-{occurrence}"
        recurrence:  Recurrence,
        overlap:     OverlapPolicy,
        initial_data: serde_json::Value,
    ) -> Self;
}
```

Internally `register_recurring_durable`:
1. Creates a `RecurringDurableTask<H>` and registers it under `__zart_recurring__:{task_id}`.
2. Calls `scheduler.schedule_at(ScheduleAtParams { task_id, recurrence: Some(recurrence), ... })` with `execution_time: Utc::now()` and `metadata: json!({ "occurrence": 0 })`.
   - Uses `ScheduleResult` to avoid double-scheduling across restarts (idempotent `schedule_at`).

### `ops.set_metadata`

`ExecutionOps` in `zart-scheduler` needs a way to update task metadata so the occurrence
counter survives reschedule. Two options:

**Option A** — add `set_metadata(&mut self, v: Value)` to `ExecutionOps` that is merged
into `zart_tasks.metadata` during the reschedule SQL update.

**Option B** — piggy-back the counter onto existing `data` field.

**Chosen: Option A.** Metadata is the correct semantic home for infrastructure-level
bookkeeping separate from handler business data. The change is additive and non-breaking.

### Execution-level recurring metadata

Introspection is covered by the existing `zart_tasks.metadata` column — no new schema
changes needed. The scheduler task row for a recurring durable already holds all context:

```json
{
  "occurrence": 42,
  "id_template": "report-{occurrence}",
  "overlap": "SkipIfRunning"
}
```

Because the `task_id` in `zart_tasks` is the stable scheduler task name (e.g.
`"monthly-report"`) and `metadata["occurrence"]` is the counter, any admin or operator
can query the scheduler task row to understand which executions belong to a recurring
scheme and what occurrence they represent. No extra column on `zart_executions` is
required at this stage.

---

## Before / After

### Before (manual, error-prone)

```rust
// User must write their own ScheduledTask, manage IDs, and handle overlap manually
struct MonthlyReportDriver {
    scheduler: Arc<DurableScheduler>,
}

#[async_trait]
impl ScheduledTask for MonthlyReportDriver {
    async fn execute(&self, instance: &TaskInstance, ops: &mut ExecutionOps<'_>)
        -> Result<(), SchedulerTaskError>
    {
        let id = format!("report-{}", chrono::Utc::now().format("%Y-%m"));
        // Overlap handling? Forgotten here.
        self.scheduler.start_for::<MonthlyReport>(&id, json!({})).await
            .map_err(|e| SchedulerTaskError::handler(e.to_string()))?;
        Ok(())
    }
}

// Registration: two separate steps with no type-safety linking them
worker_builder
    .register_scheduled_task("monthly-report", MonthlyReportDriver { scheduler: sched.clone() })
    // must also call schedule_at manually somewhere at startup
```

### After

```rust
worker_builder
    .register_recurring_durable::<MonthlyReport>(
        "monthly-report",
        "report-{occurrence}",
        Recurrence::Cron {
            expression: "0 0 1 * *".to_string(),   // 1st of every month
            timezone:   "UTC".to_string(),
        },
        OverlapPolicy::SkipIfRunning,
        json!({}),
    )
```

---

## Files Affected

| File | Change |
|---|---|
| `crates/zart/src/recurring.rs` | **New**: `OverlapPolicy`, `RecurringDurableTask<H>` |
| `crates/zart/src/builder.rs` | Add `register_recurring_durable()` |
| `crates/zart/src/lib.rs` | Re-export `OverlapPolicy` |
| `crates/zart-scheduler/src/ops.rs` | Add `set_metadata()` to `ExecutionOps` |
| `crates/zart-scheduler/src/worker.rs` | Merge metadata from `ops` into reschedule SQL update |
| `crates/zart-scheduler/src/store/*.rs` | Extend reschedule path to carry metadata |
| `examples/recurring-durable/` | **New** example: three scenarios (SkipIfRunning, CancelAndRestart, AlwaysStart) |
| `crates/zart/tests/recurring_durable.rs` | **New** integration tests for all three overlap policies + metadata persistence |
| `website/` (docs) | **New** Recurring Durable Executions page covering concept, quick-start, overlap policies, ID templating |

---

## Phase Plan

### Phase 1 — `ExecutionOps::set_metadata` (unblocks everything)

1. Add `pending_metadata: Option<Value>` to `ExecutionOps`; add `set_metadata()`.
2. Thread `pending_metadata` through the reschedule SQL path so it merges into `zart_tasks.metadata`.
3. Unit test: verify metadata survives a reschedule cycle.

**Blocking:** Phase 2 depends on this.

### Phase 2 — `RecurringDurableTask` and `OverlapPolicy`

1. Create `crates/zart/src/recurring.rs` with `OverlapPolicy` and
   `RecurringDurableTask<H>`.
2. Implement `is_running` and `cancel_if_running` helpers (query `ExecutionStatus`
   via `DurableScheduler`; call existing cancellation API).
3. Implement `ScheduledTask::execute` as designed above.

**Blocking:** Phase 3 depends on this.

### Phase 3 — `WorkerBuilder` registration

1. Add `register_recurring_durable()` to `WorkerBuilder`.
2. Wire `DurableScheduler` reference into the builder (may already be available;
   verify in `crates/zart/src/builder.rs`).

### Phase 4 — Example (`examples/recurring-durable/`)

Add a self-contained example that demonstrates recurring durable executions with a
realistic use case: a **nightly inventory snapshot** that reads product stock levels and
writes a report.

Structure:

```
examples/recurring-durable/
├── Cargo.toml
├── README.md
└── src/
    └── main.rs
```

The example covers three scenarios in sequence, each using `FixedDelay` for fast local
iteration (swappable for `Cron` in comments):

**Scenario A — `SkipIfRunning`** (default, recommended)
```rust
// Handler simulates a slow report (sleeps 200 ms) so the second tick fires while
// it is still running. The second tick is skipped.
worker_builder.register_recurring_durable::<InventorySnapshot>(
    "inventory-snapshot",
    "snapshot-{occurrence}",
    Recurrence::FixedDelay { duration_ms: 100 },
    OverlapPolicy::SkipIfRunning,
    json!({ "warehouse": "EU-1" }),
)
```

**Scenario B — `CancelAndRestart`**
```rust
// A configuration refresh job: always want the latest config, so cancel the stale run.
worker_builder.register_recurring_durable::<ConfigRefresh>(
    "config-refresh",
    "config-{occurrence}",
    Recurrence::FixedDelay { duration_ms: 150 },
    OverlapPolicy::CancelAndRestart,
    json!({}),
)
```

**Scenario C — `AlwaysStart`**
```rust
// Independent audit windows — each occurrence runs fully regardless of others.
worker_builder.register_recurring_durable::<AuditWindow>(
    "audit-window",
    "audit-{occurrence}",
    Recurrence::FixedDelay { duration_ms: 50 },
    OverlapPolicy::AlwaysStart,
    json!({}),
)
```

`main.rs` runs all three workers against a local Postgres, waits for a fixed number of
executions using `DurableScheduler::wait_for_completion` (or a polling loop), prints a
summary of execution IDs created, and asserts expected counts. This gives a
"real-usage feeling" while also serving as an end-to-end test in CI.

The `README.md` shows the cron equivalent of each scenario and explains when to choose
each overlap policy.

### Phase 5 — Integration tests

Integration tests in `crates/zart/tests/recurring_durable.rs` (separate file from
existing integration tests to keep file sizes in check):

1. **Basic recurrence**: `FixedDelay { duration_ms: 50 }`, assert executions
   `"job-0"`, `"job-1"`, `"job-2"` all complete.
2. **`SkipIfRunning`**: handler blocks until a channel signal; fire two ticks; assert
   only one execution exists.
3. **`CancelAndRestart`**: same blocking handler; assert first execution is cancelled
   and second one completes.
4. **`AlwaysStart`**: assert two executions with distinct IDs run in parallel.
5. **Metadata persistence**: assert `zart_tasks.metadata["occurrence"]` increments
   correctly across rescheduled ticks.

### Phase 6 — Website / documentation

Update the Zart website docs to add a **Recurring Durable Executions** page (or section
under the existing Durable Executions page). Content:

1. **Concept**: what recurring durable executions are and when to use them vs. plain
   recurring `ScheduledTask` (use durable when steps, retries, and admin visibility
   matter; use plain scheduler for lightweight fire-and-forget jobs).
2. **Quick start**: the `register_recurring_durable` snippet with cron and fixed-delay
   variants.
3. **Overlap policies**: table explaining `SkipIfRunning`, `CancelAndRestart`,
   `AlwaysStart` with the canonical use cases from the example.
4. **Execution IDs**: explain the `{occurrence}` template and how to query executions
   by scheduler task metadata for introspection.
5. **Cross-reference**: link to the Recurring Scheduler Tasks page (spec 0040) for
   lightweight alternatives.

---

## Rationale

**Why `{occurrence}` counter and not timestamps?**
Counters produce deterministic, collision-free IDs regardless of clock skew or cron
drift. Timestamps can collide if a task is restarted or the cron fires twice in the
same second. The counter is cheap to store and easy to reason about.

**Why store the counter in `task.metadata` rather than `task.data`?**
`data` is the handler's business payload — it should not be polluted with
infrastructure bookkeeping. `metadata` already exists as a separate JSONB column for
framework use (see `metadata["handler"]` in spec 0037).

**Why `OverlapPolicy` at the scheduler layer rather than inside the handler?**
The handler is already running when it could detect overlap — too late to prevent
double-starts. The policy check must happen *before* `DurableScheduler::start_for` is
called, which is at the `RecurringDurableTask` dispatch site.

**Why `AlwaysStart`?**
Some workflows are intentionally parallel: e.g., a nightly data export that takes 90
minutes but runs every hour (overlapping windows, each with its own independent ID).
Forcing `SkipIfRunning` in that case would silently drop expected runs.

---

## Risk & Mitigation

| Risk | Mitigation |
|---|---|
| `set_metadata` patch touches reschedule SQL path in `zart-scheduler` | Isolate to a single additive column merge; existing tests cover the reschedule path |
| Occurrence counter increments even when `SkipIfRunning` skips the execution | Accept: counter measures ticks, not successful starts. Alternative: only increment on actual start — document the choice |
| `CancelAndRestart` cancels an execution that finishes between the check and the cancel call | TOCTOU window is small; cancellation on a terminal execution is a no-op in the existing API |
| Template collision: two `register_recurring_durable` calls with the same `task_id` | `schedule_at` returns `ScheduleResult::AlreadyScheduled`; add a builder-level assert on duplicate `task_id` |
| Module size: `recurring.rs` grows large | Cap at ~300 lines; split helpers into `recurring/overlap.rs` if needed |

---

## Breaking Changes

None. All changes are additive:
- New `set_metadata` method on `ExecutionOps` (existing callers unaffected).
- New `register_recurring_durable` method on `WorkerBuilder`.
- No schema changes.
- No changes to existing `DurableExecution` or `ScheduledTask` traits.

---

## Definition of Done

- [ ] `just fmt` passes
- [ ] `just lint` passes
- [ ] All unit tests pass
- [ ] All integration tests pass (including example tests)
- [ ] `OverlapPolicy::SkipIfRunning` integration test passes
- [ ] `OverlapPolicy::CancelAndRestart` integration test passes
- [ ] `OverlapPolicy::AlwaysStart` integration test verifies parallel executions
- [ ] Metadata persistence test: `occurrence` increments correctly across ticks
- [ ] `examples/recurring-durable/` builds and all three scenarios run end-to-end in CI
- [ ] No module exceeds 600–700 lines (excluding tests)
- [ ] Website docs include Recurring Durable Executions page with overlap policy table and quick-start snippet

---

## Notes

- Spec 0040 documents recurring `ScheduledTask` support (the infrastructure this spec reuses).
- Spec 0052 covers Durable Objects (actor model) — a different primitive; recurring
  durable executions are orthogonal.
- A future spec could expose a `schedule_recurring_durable` convenience on
  `DurableScheduler` for cases where `WorkerBuilder` is not the entry point.
