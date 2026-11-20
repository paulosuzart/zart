import { Duration, Effect, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { describe, expect, it } from "vitest";
import {
	type ClientTrx,
	type DurableScheduler,
	PgBackend,
	type PgBackendConnectOptions,
	type WorkerOptions,
	Zart,
	type ZartSchemaError,
	type ZartTaggedError,
} from "../ts/src/index.js";

/**
 * Compile-time checks enforced by `tsc --noEmit` (never executed).
 * Workflow primitives need `ZartWorkflow`; `Zart.step` functions may only use
 * `ZartTrx` / `SqlClient` / `TrxSessionService` / `ZartContext` (never `ZartWorkflow`).
 */
const neverRun = () => {
	// @ts-expect-error ZartWorkflow primitives are not allowed inside Zart.step
	Zart.step("bad-sleep", () => Zart.sleep("nap", "1 second"));

	// @ts-expect-error scheduling inside a step is not allowed either
	Zart.step("bad-event", () => Zart.waitForEvent("x"));

	// @ts-expect-error nested step calls require ZartWorkflow
	Zart.step("bad-nested", () => Zart.step("inner", () => Effect.void)());

	// OK: pure effects, Zart.Trx, and effect/sql SqlClient
	Zart.step("fine", (n: number) => Effect.succeed(n));
	Zart.step("fine-trx", () => Zart.Trx.execute("SELECT 1"));
	Zart.step("fine-sql", () =>
		Effect.gen(function* () {
			const sql = yield* SqlClient.SqlClient;
			return yield* sql`SELECT 1`;
		}),
	);

	// OK: Zart.context inside Zart.step exposes ExecutionInfo including isRetry
	const stepContext = Zart.step("fine-context", () =>
		Effect.gen(function* () {
			const ctx = yield* Zart.context;
			const isRetry: boolean = ctx.isRetry;
			const attempt: number = ctx.currentAttempt;
			return `${ctx.executionId}:${attempt}:${isRetry}`;
		}),
	)();

	// OK: Zart.stepOrElse with value or effect fallback
	const stepForElse = Zart.step("step-for-else", () => Effect.succeed(123))();
	const _fallbackVal = Zart.stepOrElse(stepForElse, (_err) => 456);
	const _fallbackEff = Zart.stepOrElse(stepForElse, (_err) =>
		Effect.succeed(456),
	);

	// OK: Zart.durable with sub-second timeout and onFailure callback
	const _durableTask = Zart.durable(
		"durable-with-failure",
		(input: { val: number }) => Effect.succeed(input.val * 2),
		{
			timeout: "50 millis",
			onFailure: (input, failure) => {
				if (failure.kind === "step_failed") {
					const _step: string = failure.step;
					const _raw: unknown = failure.raw;
					void [_step, _raw];
				} else if (failure.kind === "retries_exhausted") {
					const _attempts: number = failure.attempts;
					void _attempts;
				}
				return Effect.succeed(input.val);
			},
		},
	);

	// OK: PgBackend connect overloads & withListenerUrl typing
	const _connectDefault: Effect.Effect<PgBackend, ZartTaggedError> =
		PgBackend.connect("postgres://localhost/zart");
	const _connectLegacy: Effect.Effect<PgBackend, ZartTaggedError> =
		PgBackend.connect("postgres://localhost/zart", "postgres://direct/zart");
	const opts: PgBackendConnectOptions = {
		listenerUrl: "postgres://direct/zart",
		tablePrefix: "svc_",
		schema: "tenant_a",
	};
	const _connectOpts: Effect.Effect<PgBackend, ZartTaggedError> =
		PgBackend.connect("postgres://localhost/zart", opts);
	const _scopedOpts = PgBackend.scoped("postgres://localhost/zart", opts);
	const _withListener = (
		b: PgBackend,
	): Effect.Effect<PgBackend, ZartTaggedError> =>
		b.withListenerUrl("postgres://direct/zart");

	// @ts-expect-error tablePrefix must be a string
	PgBackend.connect("postgres://localhost/zart", { tablePrefix: 123 });

	// OK: WorkerOptions with Duration.Input, *Secs, nullable heartbeat, enableListenNotify
	const _workerOpts: WorkerOptions = {
		pollInterval: "5 seconds",
		pollIntervalSecs: 5,
		maxConcurrentTasks: 8,
		maxTasksPerPoll: 4,
		shutdownTimeout: Duration.seconds(10),
		orphanTimeout: { minutes: 5 },
		heartbeatInterval: null,
		heartbeatIntervalSecs: null,
		enableListenNotify: false,
	};

	const _badWorkerOpts1: WorkerOptions = {
		// @ts-expect-error invalid Duration unit
		pollInterval: "5 lightyears",
	};
	const _badWorkerOpts2: WorkerOptions = {
		// @ts-expect-error only heartbeatInterval accepts null
		orphanTimeout: null,
	};
	const _badWorkerOpts3: WorkerOptions = {
		// @ts-expect-error *Secs requires number
		pollIntervalSecs: "5 seconds",
	};

	// OK: DurableScheduler ergonomic overloads & type inference
	const _checkScheduler = (sched: DurableScheduler, tx: ClientTrx) => {
		const wf = Zart.durable(
			"typed-wf",
			(input: { count: number }) =>
				Effect.succeed({ doubled: input.count * 2 }),
			{
				inputSchema: Schema.Struct({ count: Schema.Number }),
				outputSchema: Schema.Struct({ doubled: Schema.Number }),
			},
		);

		const _s1 = sched.start("e-1", wf, { count: 3 });
		const _s2 = sched.start("e-2", "raw-task", { ok: true });
		const _s3 = sched.start(
			"e-3",
			"schema-task",
			Schema.Struct({ count: Schema.Number }),
			{ count: 1 },
		);

		// @ts-expect-error wrong input type for DurableDefinition
		sched.start("e-bad", wf, { count: "wrong" });

		const _tx1 = sched.startInTx(tx, {
			executionId: "e-tx",
			definition: wf,
			input: { count: 4 },
		});

		const _w1: Effect.Effect<
			{ doubled: number },
			ZartTaggedError | ZartSchemaError
		> = sched.waitCompletion("e-1", wf, "30 seconds", "100 millis");
		const _w2: Effect.Effect<unknown, ZartTaggedError | ZartSchemaError> =
			sched.waitCompletion("e-2", Duration.seconds(10));

		const _sw1: Effect.Effect<
			{ doubled: number },
			ZartTaggedError | ZartSchemaError
		> = sched.startAndWait("e-4", wf, { count: 5 }, "10 seconds", 0.05);
		const _sw2: Effect.Effect<unknown, ZartTaggedError | ZartSchemaError> =
			sched.startAndWait("e-5", "raw-task", { ok: true }, "5 seconds");

		const _ev1 = sched.offerEvent("e-1", "approved", { by: "alice" });
		const _ev2 = sched.offerEvent(
			"e-1",
			"approved",
			Schema.Struct({ by: Schema.String }),
			{ by: "alice" },
		);

		void [_s1, _s2, _s3, _tx1, _w1, _w2, _sw1, _sw2, _ev1, _ev2];
	};

	void [
		_connectDefault,
		_connectLegacy,
		_connectOpts,
		_scopedOpts,
		_withListener,
		_workerOpts,
		_badWorkerOpts1,
		_badWorkerOpts2,
		_badWorkerOpts3,
		_checkScheduler,
		stepContext,
		_fallbackVal,
		_fallbackEff,
		_durableTask,
	];
};

describe("compile-time safety", () => {
	it("is enforced by tsc (see @ts-expect-error above)", () => {
		expect(typeof neverRun).toBe("function");
	});
});
