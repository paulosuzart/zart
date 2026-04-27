# Scheduler-Only Example

Demonstrates using **zart-scheduler** as a standalone task queue without the durable execution runtime. This shows how to build background job processing with task chaining, future scheduling, and parallel execution using only the scheduling primitives.

## Features Used

- **`ScheduledTask` trait** — define task handlers with `execute(instance, ops)`
- **`ExecutionOps`** — complete tasks, reschedule, or chain new tasks atomically
- **`TaskRegistry`** — map task names to handler implementations
- **`Worker`** — poll loop with concurrent task dispatch and heartbeating
- **`PostgresTaskScheduler`** — PostgreSQL-backed task queue with `SKIP LOCKED` polling
- **`ScheduleAtParams`** — schedule tasks for immediate or future execution

## Flow

### Demo 1: Task Chaining
1. Schedule `send-welcome-email` with user data
2. Handler simulates sending an email, then chains `onboarding-cleanup` via `ops.schedule()`
3. Cleanup handler chains `generate-report`
4. All three tasks execute sequentially in a single chain

### Demo 2: Scheduled Future Task
1. Schedule `scheduled-greeting` with `execution_time` 3 seconds in the future
2. Worker picks it up when the time arrives

### Demo 3: Independent Parallel Tasks
1. Schedule 3 greeting tasks simultaneously
2. Worker processes them concurrently

## Running

```bash
# Ensure PostgreSQL is running
just up

# Run migrations
just migrate

# Build and run the example
just example-scheduler-only
```

## What You'll See

```
=== Zart Scheduler-Only Example ===

--- Demo 1: Task Chaining ---
Scheduling send-welcome-email...
  [send-welcome-email] Sending welcome email to alice@example.com (user-42)
  [send-welcome-email] Email sent, scheduled onboarding-cleanup
  [onboarding-cleanup] Running cleanup action 'remove-pending-flag' for user user-42
  [onboarding-cleanup] Cleanup done, scheduled generate-report
  [generate-report] Generating 'onboarding-complete' report for user user-42
  [generate-report] Report generated

--- Demo 2: Scheduled Future Task ---
Scheduled greeting for 2026-04-27T... (in 3 seconds)
  [scheduled-greeting] Hello, Paulo!
Future task completed!

--- Demo 3: Independent Parallel Tasks ---
Scheduled 3 parallel greeting tasks
  [scheduled-greeting] Hello, User-parallel-0!
  [scheduled-greeting] Hello, User-parallel-1!
  [scheduled-greeting] Hello, User-parallel-2!
All parallel tasks completed!

=== All demos completed ===
```

## Key Concepts

### No Durable Execution Required
This example uses **only** `zart-scheduler` — no `DurableExecution`, no `DurableRegistry`, no `DurableScheduler`, no step tracking. Just a pure task queue.

### Atomic Task Chaining
`ops.schedule()` enqueues a successor task within the same database transaction as the current task's completion. If the worker crashes between the two writes, neither happens — the chain is atomic.

### Worker Lifecycle
1. Create `PostgresTaskScheduler` with a `PgPool`
2. Register handlers in `TaskRegistry`
3. Spawn `Worker` in a background task
4. Schedule tasks via `scheduler.schedule_now()` or `scheduler.schedule_at()`
5. Worker polls, dispatches, and manages transactions automatically
6. Call `worker.stop()` for graceful shutdown
