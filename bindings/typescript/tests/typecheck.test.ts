import { Duration, Effect } from "effect";
import { SqlClient } from "effect/sql";
import { describe, expect, it } from "vitest";
import {
	PgBackend,
	type PgBackendConnectOptions,
	type WorkerOptions,
	Zart,
	type ZartTaggedError,
} from "../ts/src/index.js";

/**
 * Compile-time checks enforced by `tsc --noEmit` (never executed).
 * Workflow primitives need `ZartWorkflow`; `Zart.step` functions may only use
 * `ZartTrx` / `SqlClient` / `TrxSessionService` (never `ZartWorkflow`).
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
	];
};

describe("compile-time safety", () => {
	it("is enforced by tsc (see @ts-expect-error above)", () => {
		expect(typeof neverRun).toBe("function");
	});
});
