# Zart TypeScript Bindings

TypeScript ([Effect v4](https://effect.website/)) bindings (`@zart/effect`) for the [Zart](../../README.md) durable execution framework, powered by `napi-rs` and Tokio.

## Prerequisites

- **Node.js 22+** (used for `--experimental-strip-types` when running examples)
- **Effect v4** (`effect ^4.1.4`, installed via `npm install`)
- **Rust toolchain** (stable)
- **PostgreSQL** running locally (default connection string: `postgres://zart:zart@localhost:5432/zart`)

## Build & Install (Development)

From `bindings/typescript/`:

```bash
npm install
npm run build:native
npm run build
```

- `npm run build:native` compiles the Rust `napi-rs` native addon (`zart-ts`).
- `npm run build` compiles the TypeScript sources into `dist/`.

## Quickstart

```ts
import { Effect, Fiber, Schema } from "effect";
import {
	DurableScheduler,
	PgBackend,
	WorkerBuilder,
	Zart,
} from "@zart/effect";

const OrderInput = Schema.Struct({
	orderId: Schema.String,
	amount: Schema.Number,
});

const chargeCard = Zart.step(
	"charge-card",
	(orderId: string, amount: number) =>
		Effect.succeed(`charged:${orderId}:${amount}`),
	{ retry: Zart.Retry.fixed(3, "1 second") },
);

const checkout = Zart.durable(
	"checkout",
	(order) =>
		Effect.gen(function* () {
			// Default path for required sequential steps
			const receipt = yield* chargeCard(order.orderId, order.amount);
			return receipt;
		}),
	{
		inputSchema: OrderInput,
		outputSchema: Schema.String,
		timeout: "5 minutes",
	},
);

const main = Effect.scoped(
	Effect.gen(function* () {
		const dbUrl =
			process.env.DATABASE_URL ?? "postgres://zart:zart@localhost:5432/zart";
		const backend = yield* PgBackend.scoped(dbUrl);
		yield* backend.runMigrations();

		const worker = (yield* WorkerBuilder.fromBackend(backend))
			.registerDurable(checkout)
			.build();
		const workerFiber = yield* Effect.forkChild(worker.runScoped());

		const scheduler = yield* DurableScheduler.fromBackend(backend);
		const receipt = yield* scheduler
			.startAndWait(
				"checkout-order-1",
				checkout,
				{ orderId: "order-1", amount: 49.99 },
				"30 seconds",
			)
			.pipe(Effect.ensuring(Fiber.interrupt(workerFiber)));

		console.log(receipt);
	}),
);

await Effect.runPromise(main);
```

### Step Execution & Error Handling Patterns

| Pattern | When to use |
|---|---|
| `yield* myStep(...)` | **Default** for required sequential steps — returns the typed result or fails the workflow Effect (`E \| StepFailedError \| ZartTaggedError`). |
| `yield* myStep(...).named("item-0")` | Explicit durable step ID (required inside durable loops to distinguish iterations). |
| `yield* Zart.outcome(myStep(...))` | Reifies the result into a non-failing `StepOutcome` (`_tag`: `"ok"` \| `"business_err"` \| `"zart_err"`). |
| `yield* Zart.stepOr(myStep(...), fallback)` | Returns `fallback` on any typed step failure instead of failing the workflow. |
| `yield* Zart.stepOrElse(myStep(...), (err) => ...)` | Computes a fallback on business errors (`E \| StepFailedError`) while letting framework errors (`RetryExhaustedError`, `StepTimeoutError`, `DeadlineExceededError`, etc.) propagate. |
| `yield* Zart.schedule(myStep(...))` + `yield* Zart.wait(h)` / `yield* Zart.waitAll([...])` | Parallel fan-out / fan-in across multiple independent steps. |
| `yield* Zart.require(myStep(...))` | Explicit equivalent of `yield* myStep(...)`. |

### Durable Primitives & Transactions

- **Time & Events**: `Zart.sleep(name, duration)`, `Zart.sleepUntil(name, date)`, `Zart.waitForEvent(name, { schema?, timeout? })`, `Zart.capture(name, fn, schema?)`, `Zart.now(name?)`, and `Zart.context` (`ExecutionInfo`).
- **Zart-Managed Step Transactions**: Inside `Zart.step`, yield `Zart.ZartTrx` (or use `Zart.Trx.execute` / `fetchOne` / `fetchAll` / `fetchOptional`) or `SqlClient.SqlClient` (backed by `ZartSqlClientLive`) to share the step's atomic database transaction. Pass `{ trx: false }` to opt out.
- **User-Managed Transactions**: Configure `workerBuilder.withTrxProvider(provider)` and yield `Zart.TrxSessionService` inside steps.
- **Transactional Scheduling**: Use `backend.withTransaction((tx) => scheduler.startInTx(tx, { executionId, definition, input }))` to enqueue workflows atomically with your own SQL writes.

## Recurring, Admin & Introspection

### Recurring Durable Workflows

```ts
import { OverlapPolicy, Recurrence, WorkerBuilder } from "@zart/effect";

const worker = (yield* WorkerBuilder.fromBackend(backend))
	.registerRecurringDurable(myWorkflow, {
		recurrence: Recurrence.cron("0 * * * * *", "UTC"), // or Recurrence.fixedDelay("30 seconds")
		overlap: OverlapPolicy.SkipIfRunning,
		idTemplate: "my-workflow-{occurrence}",
		initialData: { source: "cron" },
	})
	.build();

// Stop future ticks later via DurableScheduler:
yield* scheduler.stopRecurring(myWorkflow);
```

### Introspection & Admin Operations

`DurableScheduler` exposes full introspection, recovery, and pause/resume controls:

- **Querying**: `status(id)`, `listExecutions(params?)`, `stats()`, `getCurrentRunId(executionId)`, `listRuns(executionId)`, `executionDetail(executionId, runId?)`.
- **Lifecycle & Recovery**: `cancel(id)`, `offerEvent(id, eventName, payloadOrSchema, payload?)`, `retryStep(runId, stepName, triggeredBy?)`, `retryStepCurrentRun(executionId, stepName, triggeredBy?)`, `restart(executionId, ...)`, `rerunSteps(executionId, spec)`.
- **Pause / Resume**: `pause(scope)`, `resume(scope, deletedBy?)`, `resumeRuleById(ruleId, deletedBy?)`, `listPauseRules(filter?)`.

## Running Examples

Make sure PostgreSQL is running (or set `DATABASE_URL`):

```bash
export DATABASE_URL="postgres://zart:zart@localhost:5432/zart"

# Transactional scheduling (startInTx) and atomic step transactions (Zart.Trx / SqlClient)
npm run example:transactions

# Human-in-the-loop approval workflow (waitForEvent + offerEvent)
npm run example:approval

# Parallel saga with durable loops, schedule/waitAll fan-out, stepOr fallback, and sleep
npm run example:parallel-saga
```

## Running Tests, Typecheck & Lint

From `bindings/typescript/`:

```bash
npm run typecheck   # tsc --noEmit
npm run lint        # biome check
npm test            # vitest run
```
