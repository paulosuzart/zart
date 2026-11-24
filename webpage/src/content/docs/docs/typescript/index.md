---
title: TypeScript Bindings (Effect v4)
description: Durable execution for TypeScript and Effect v4 powered by Zart's Rust runtime.
---

Zart's TypeScript bindings (`@zart/effect`) bring durable, crash-proof workflows to Node.js and Effect v4 (`effect@^4.1.4`) via a native N-API extension backed by Zart's Rust + PostgreSQL runtime.

## Prerequisites & Installation

- **Node.js 22+** (with `--experimental-strip-types` for running `.ts` examples directly)
- **Effect v4** (`effect` `^4.1.4`)
- **Rust toolchain** (stable, for building the native N-API addon)
- **PostgreSQL** (default `DATABASE_URL`: `postgres://zart:zart@localhost:5432/zart`)

From `bindings/typescript/`:

```bash
npm install
npm run build:native
npm run build
```

## Quickstart

```ts
import { randomUUID } from "node:crypto";
import { Effect, Fiber, Schema } from "effect";
import {
  DurableScheduler,
  PgBackend,
  Retry,
  WorkerBuilder,
  Zart,
} from "@zart/effect";

const OnboardingInput = Schema.Struct({
  userId: Zart.UuidString,
  email: Schema.String,
});

const OnboardingOutput = Schema.Struct({
  welcome: Schema.String,
  workspace: Schema.String,
});

const sendWelcome = Zart.step(
  "send-welcome",
  (email: string) => Effect.succeed(`sent:${email}`),
  { retry: Retry.exponential(3, "1 second") },
);

const provisionWorkspace = Zart.step("provision-workspace", (userId: string) =>
  Effect.succeed(`ws:${userId}`),
);

const onboardUser = Zart.durable(
  "onboard-user",
  (data) =>
    Effect.gen(function* () {
      const welcome = yield* sendWelcome(data.email);
      const workspace = yield* provisionWorkspace(data.userId);
      return { welcome, workspace };
    }),
  {
    inputSchema: OnboardingInput,
    outputSchema: OnboardingOutput,
    timeout: "5 minutes",
  },
);

const main = Effect.scoped(
  Effect.gen(function* () {
    const dbUrl =
      process.env.DATABASE_URL ?? "postgres://zart:zart@localhost:5432/zart";
    const backend = yield* PgBackend.scoped(dbUrl);
    yield* backend.runMigrations();

    const scheduler = yield* DurableScheduler.fromBackend(backend);
    const worker = (yield* WorkerBuilder.fromBackend(backend))
      .registerDurable(onboardUser)
      .build();
    const workerFiber = yield* Effect.forkChild(worker.runScoped());

    const execId = `onboard-${randomUUID()}`;
    const result = yield* scheduler
      .startAndWait(
        execId,
        onboardUser,
        {
          userId: randomUUID(),
          email: "ada@example.com",
        },
        "30 seconds",
      )
      .pipe(Effect.ensuring(Fiber.interrupt(workerFiber)));

    console.log("Completed:", result);
  }),
);

await Effect.runPromise(main);
```

## Step Execution Patterns

| Pattern | When to use |
|---|---|
| `yield* myStep(...)` | **Default** for required sequential steps — returns the typed result `A` or fails with `StepError<E>`. |
| `yield* myStep(...).named("item-0")` | Inside loops or dynamic branches where each invocation needs a distinct durable step ID. |
| `yield* Zart.outcome(myStep(...))` | Inspect the `StepOutcome<A>` (`_tag`: `"ok"`, `"business_err"`, or `"zart_err"`) without failing the workflow. |
| `yield* Zart.stepOr(myStep(...), fallback)` | Recover from any typed step error with a fallback value instead of failing the workflow. |
| `yield* Zart.stepOrElse(myStep(...), fn)` | Compute a fallback on **business errors only** (`ZartTaggedError` framework failures still propagate). |
| `Zart.schedule(myStep(...))` + `yield* Zart.wait(h)` / `yield* Zart.waitAll([...])` | Parallel fan-out / fan-in across multiple independent steps, preserving handle order. |
| `yield* Zart.require(myStep(...))` | Explicit equivalent of `yield* myStep(...)` (optional; never needed for normal sequential steps). |

## Compile-Time Safety (`ZartWorkflow` vs `StepAllowedServices`)

Workflow orchestration primitives (`Zart.step`, `Zart.sleep`, `Zart.waitForEvent`, `Zart.schedule`, `Zart.capture`, `Zart.now`, `Zart.context`) require the `ZartWorkflow` service in Effect's `R` type parameter, which is provided by `Zart.durable`.

By contrast, `Zart.step` functions may only require `StepAllowedServices` (`ZartTrx | SqlClient.SqlClient | TrxSessionService | ZartContext`), never `ZartWorkflow`. Calling `Zart.sleep(...)`, `Zart.waitForEvent(...)`, or another `Zart.step(...)` inside a step body is rejected at compile time by `tsc`.

## Control Flow, Timers & Events

- `yield* Zart.sleep("cooldown", "5 minutes")` — Durable sleep (accepts Effect `Duration.Input`, e.g., `"500 millis"`, `"1 second"`, `"5 minutes"`, `Duration.seconds(30)`).
- `yield* Zart.sleepUntil("cutoff", wakeDate)` — Durable sleep until a `Date` timestamp.
- `yield* Zart.waitForEvent("manager-approval", { schema: ApprovalDecision, timeout: "2 minutes" })` — Suspend until an external event is delivered via `scheduler.offerEvent(...)`.
- `yield* Zart.capture("random-seed", () => ...)` — Durably record a non-deterministic computation (with optional `Schema.Codec`).
- `yield* Zart.now("ts")` — Deterministic, durably recorded `Date`.
- `yield* Zart.context` — Returns `ExecutionInfo` (`executionId`, `taskName`, `data`, `currentAttempt`, `isRetry`) inside both `Zart.durable` and `Zart.step`.

> **Note:** Zart's replay/suspension signals are lifted into Effect defects (`ControlFlowDefect` via `Effect.die`), so `Effect.catch` and `Zart.stepOr` in workflow code never accidentally swallow durable control flow.

## Typed Serialization (`effect/Schema`)

Pass `Schema.Codec` definitions to `Zart.durable` (`inputSchema`, `outputSchema`), `Zart.step` (`outputSchema`, `errorSchema`), `Zart.waitForEvent`, `scheduler.start`, and `scheduler.waitCompletion` for runtime validation and typed revival across the N-API boundary.

Built-in wire-aware schemas are exported on `Zart`:

- `Zart.UuidString` — UUID-validated string
- `Zart.DateFromRfc3339` — `Date` encoded as RFC 3339 string
- `Zart.BigIntFromDecimal` — `bigint` encoded as decimal string
- `Zart.BytesFromBase64` — `Uint8Array` encoded as base64 string

## Backend, Worker & Recurring Configuration

- **Backend options (`PgBackend`)**: pass `{ listenerUrl, tablePrefix, schema }` to `PgBackend.connect(...)` / `PgBackend.scoped(...)`, switch the listener URL with `backend.withListenerUrl(url)`, and check connectivity with `yield* backend.ping()`.
- **Worker configuration (`WorkerBuilder.configure`)**: configure `pollInterval`, `maxConcurrentTasks`, `maxTasksPerPoll`, `shutdownTimeout`, `orphanTimeout`, `heartbeatInterval`, and `enableListenNotify` (accepting Effect `Duration.Input`).
- **Centralized failure recovery**: pass `onFailure: (input, failure) => ...` in `Zart.durable` options to handle `ExecutionFailure` (`"step_failed"`, `"execution_deadline_exceeded"`, `"retries_exhausted"`).
- **Recurring durable workflows**: schedule cron or fixed-delay workflows with `WorkerBuilder.registerRecurringDurable(def, { idTemplate: "job-{occurrence}", recurrence: Recurrence.fixedDelay("5 seconds"), overlap: OverlapPolicy.SkipIfRunning })` and stop them via `yield* scheduler.stopRecurring(...)`.

## Querying, Admin Recovery & Pause/Resume

`DurableScheduler` exposes full introspection, recovery, and pause/resume controls:

- **Querying**: `listExecutions(params?)`, `stats()`, `getCurrentRunId(executionId)`, `listRuns(executionId)`, `executionDetail(executionId, runId?)`
- **Admin recovery**: `retryStep(runId, stepName, triggeredBy?)`, `retryStepCurrentRun(executionId, stepName, triggeredBy?)`, `restart(executionId, newPayload?, triggeredBy?)`, `rerunSteps(executionId, spec)`
- **Pause & resume**: `pause(scope)`, `resume(scope, deletedBy?)`, `resumeRuleById(ruleId, deletedBy?)`, `listPauseRules(filter?)`
