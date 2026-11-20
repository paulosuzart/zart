import { Data, Effect } from "effect";
import { SqlClient } from "effect/sql";
import { describe, expect, it } from "vitest";
import {
	DurableScheduler,
	type NativeClientTrx,
	type NativeDurableScheduler,
	type NativePgBackend,
	type NativeStepOutcome,
	type NativeTrx,
	type NativeWorkerBuilder,
	PgBackend,
	WorkerBuilder,
	Zart,
} from "../ts/src/index.js";
import type { NativeStepRun, NativeStepSpec } from "../ts/src/native.js";

class StepBoom extends Data.TaggedError("StepBoom")<{
	readonly message: string;
}> {}

/** In-memory transactional store simulating Postgres ACID commit/rollback across business + zart tables. */
class FakeTxStore {
	businessRows: Array<{ id: number; name: string }> = [];
	zartTasks: Array<{ executionId: string; taskName: string }> = [];
	zartSteps: Array<{ step: string; status: string }> = [];
}

function makeFakeNativeTrx(store: FakeTxStore) {
	let stagedBusiness = [...store.businessRows];
	let stagedTasks = [...store.zartTasks];
	let committed = false;
	let rolledBack = false;

	const trx: NativeClientTrx & {
		committed: () => boolean;
		rolledBack: () => boolean;
	} = {
		async execute(sql: string, params: unknown[] = []): Promise<number> {
			if (sql.startsWith("INSERT INTO items")) {
				stagedBusiness.push({ id: Number(params[0]), name: String(params[1]) });
				return 1;
			}
			return 0;
		},
		async fetchOne(sql: string, params: unknown[] = []) {
			const rows = await this.fetchAll(sql, params);
			const first = rows[0];
			if (!first) throw new Error("RowNotFound");
			return first;
		},
		async fetchAll(sql: string, params: unknown[] = []) {
			if (sql.startsWith("INSERT INTO items")) {
				stagedBusiness.push({ id: Number(params[0]), name: String(params[1]) });
				return [{ id: Number(params[0]), name: String(params[1]) }];
			}
			if (sql.startsWith("SELECT")) {
				return stagedBusiness.map((r) => ({ ...r }));
			}
			return [];
		},
		async fetchOptional(sql: string, params: unknown[] = []) {
			const rows = await this.fetchAll(sql, params);
			return rows[0] ?? null;
		},
		async commit() {
			committed = true;
			store.businessRows = stagedBusiness;
			store.zartTasks = stagedTasks;
		},
		async rollback() {
			rolledBack = true;
			stagedBusiness = [...store.businessRows];
			stagedTasks = [...store.zartTasks];
		},
		committed: () => committed,
		rolledBack: () => rolledBack,
		_stageTask(executionId: string, taskName: string) {
			stagedTasks.push({ executionId, taskName });
		},
	} as NativeClientTrx & {
		committed: () => boolean;
		rolledBack: () => boolean;
		_stageTask: (e: string, t: string) => void;
	};

	return trx;
}

class FakeTrxDispatcher {
	lastNeedsTrx: boolean | undefined;
	constructor(readonly store: FakeTxStore) {}

	async step(
		spec: NativeStepSpec,
		run: NativeStepRun,
	): Promise<NativeStepOutcome> {
		this.lastNeedsTrx = spec.needsTrx;
		const tx = makeFakeNativeTrx(this.store);
		const out = await run(
			spec.needsTrx === false ? undefined : (tx as NativeTrx),
		);
		if (out.kind === "ok") {
			await tx.commit();
			this.store.zartSteps.push({ step: spec.name, status: "ok" });
		} else {
			await tx.rollback();
		}
		return out;
	}
	async require() {
		throw new Error("unused");
	}
	async stepOr() {
		throw new Error("unused");
	}
	async schedule() {
		return 0;
	}
	async wait(): Promise<NativeStepOutcome> {
		return { kind: "ok", value: null };
	}
	async waitAll() {
		return [];
	}
	async sleep() {}
	async sleepUntil() {}
	async waitForEvent() {
		return null;
	}
	async capture(_: string, run: () => unknown) {
		return run();
	}
	async now() {
		return new Date().toISOString();
	}
	context() {
		return {
			executionId: "e1",
			taskName: "t",
			data: null,
			currentAttempt: 1,
			isRetry: false,
		};
	}
}

describe("Zart-Managed Zart.Trx", () => {
	it("commits business table + zart_steps atomically on success", async () => {
		const store = new FakeTxStore();
		const insertItem = Zart.step("insert-item", (id: number, name: string) =>
			Effect.gen(function* () {
				yield* Zart.Trx.execute("INSERT INTO items VALUES ($1, $2)", [
					id,
					name,
				]);
				return yield* Zart.Trx.fetchOne<{ id: number; name: string }>(
					"SELECT * FROM items",
				);
			}),
		);
		const wf = Zart.durable("wf-trx-ok", (id: number) =>
			insertItem(id, "widget"),
		);
		const out = await wf.run(new FakeTrxDispatcher(store), 1);
		expect(out).toEqual({ id: 1, name: "widget" });
		expect(store.businessRows).toEqual([{ id: 1, name: "widget" }]);
		expect(store.zartSteps).toEqual([{ step: "insert-item", status: "ok" }]);
	});

	it("rolls back business table + zart_steps when step fails", async () => {
		const store = new FakeTxStore();
		const failingStep = Zart.step("fail-step", () =>
			Effect.gen(function* () {
				yield* Zart.Trx.execute("INSERT INTO items VALUES ($1, $2)", [
					99,
					"ghost",
				]);
				return yield* Effect.fail(new StepBoom({ message: "abort" }));
			}),
		);
		const wf = Zart.durable("wf-trx-fail", () => failingStep());
		await expect(
			wf.run(new FakeTrxDispatcher(store), undefined),
		).rejects.toThrow("abort");
		expect(store.businessRows).toEqual([]);
		expect(store.zartSteps).toEqual([]);
	});
});

describe("backend.withTransaction + scheduler.startInTx", () => {
	const makeFakeBackendAndScheduler = (store: FakeTxStore) => {
		let lastTx: ReturnType<typeof makeFakeNativeTrx> | undefined;
		const nativeBackend: NativePgBackend = {
			withListenerUrl: () => nativeBackend,
			runMigrations: async () => {},
			ping: async () => {},
			close: async () => {},
			begin: async () => {
				lastTx = makeFakeNativeTrx(store);
				return lastTx;
			},
		};

		const nativeScheduler: NativeDurableScheduler = {
			start: async () => ({
				taskId: "t1",
				executionTime: "2024-01-01T00:00:00Z",
			}),
			startInTx: async (tx, executionId, taskName) => {
				(
					tx as unknown as { _stageTask: (e: string, t: string) => void }
				)._stageTask(executionId, taskName);
				return { taskId: executionId, executionTime: "2024-01-01T00:00:00Z" };
			},
			status: async () => ({
				executionId: "e1",
				taskName: "t",
				payload: null,
				status: "scheduled",
				scheduledAt: "2024-01-01T00:00:00Z",
				version: 1,
			}),
			wait: async () => nativeScheduler.status("e1"),
			waitCompletion: async () => null,
			cancel: async () => true,
			offerEvent: async () => {},
			stopRecurring: async () => {},
		};

		const backend = new (
			PgBackend as unknown as { new (n: NativePgBackend): PgBackend }
		)(nativeBackend);
		const scheduler = new (
			DurableScheduler as unknown as {
				new (n: NativeDurableScheduler): DurableScheduler;
			}
		)(nativeScheduler);
		return {
			backend,
			scheduler,
			getLastTx: () => {
				if (!lastTx) throw new Error("no tx");
				return lastTx;
			},
		};
	};

	it("commits both business row and zart_tasks on success", async () => {
		const store = new FakeTxStore();
		const { backend, scheduler, getLastTx } =
			makeFakeBackendAndScheduler(store);
		await Effect.runPromise(
			backend.withTransaction((tx) =>
				Effect.gen(function* () {
					yield* tx.execute("INSERT INTO items VALUES ($1, $2)", [1, "ok"]);
					yield* scheduler.startInTx(tx, {
						executionId: "ex-1",
						taskName: "job",
						payload: { x: 1 },
					});
				}),
			),
		);
		expect(getLastTx().committed()).toBe(true);
		expect(getLastTx().rolledBack()).toBe(false);
		expect(store.businessRows).toEqual([{ id: 1, name: "ok" }]);
		expect(store.zartTasks).toEqual([{ executionId: "ex-1", taskName: "job" }]);
	});

	it("rolls back both business row and zart_tasks on failure", async () => {
		const store = new FakeTxStore();
		const { backend, scheduler, getLastTx } =
			makeFakeBackendAndScheduler(store);
		await expect(
			Effect.runPromise(
				backend.withTransaction((tx) =>
					Effect.gen(function* () {
						yield* tx.execute("INSERT INTO items VALUES ($1, $2)", [2, "bad"]);
						yield* scheduler.startInTx(tx, {
							executionId: "ex-2",
							taskName: "job",
							payload: {},
						});
						return yield* Effect.fail(new StepBoom({ message: "tx-fail" }));
					}),
				),
			),
		).rejects.toThrow("tx-fail");
		expect(getLastTx().committed()).toBe(false);
		expect(getLastTx().rolledBack()).toBe(true);
		expect(store.businessRows).toEqual([]);
		expect(store.zartTasks).toEqual([]);
	});
});

describe("effect/sql ZartSqlClientLive", () => {
	it("runs SqlClient queries inside Zart.step and rolls back on failure", async () => {
		const store = new FakeTxStore();
		const sqlStep = Zart.step("sql-step", (failAfter: boolean) =>
			Effect.gen(function* () {
				const sql = yield* SqlClient.SqlClient;
				yield* sql`INSERT INTO items VALUES (${7}, ${"from-sql"})`;
				if (failAfter)
					return yield* Effect.fail(new StepBoom({ message: "sql-abort" }));
				return yield* sql<{ id: number; name: string }>`SELECT * FROM items`;
			}),
		);

		const okWf = Zart.durable("sql-ok", () => sqlStep(false));
		expect(await okWf.run(new FakeTrxDispatcher(store), undefined)).toEqual([
			{ id: 7, name: "from-sql" },
		]);
		expect(store.businessRows).toEqual([{ id: 7, name: "from-sql" }]);

		const failWf = Zart.durable("sql-fail", () => sqlStep(true));
		await expect(
			failWf.run(new FakeTrxDispatcher(store), undefined),
		).rejects.toThrow("sql-abort");
		expect(store.businessRows).toEqual([{ id: 7, name: "from-sql" }]);
	});
});

describe("User-Managed TrxProvider", () => {
	const makeProvider = (events: string[], failCommit = false) => ({
		begin: () =>
			Effect.sync(() => {
				events.push("begin");
				return {
					commit: () =>
						Effect.try({
							try: () => {
								events.push("commit");
								if (failCommit) throw new Error("commit-err");
							},
							catch: (e) => e,
						}),
					rollback: () => Effect.sync(() => void events.push("rollback")),
					release: () => Effect.sync(() => void events.push("release")),
				};
			}),
	});

	it("calls commit + release on success, rollback + release on failure, release even if commit fails", async () => {
		const okEvents: string[] = [];
		await Effect.runPromise(
			Zart.runWithTrxProvider(makeProvider(okEvents), () => Effect.succeed(42)),
		);
		expect(okEvents).toEqual(["begin", "commit", "release"]);

		const failEvents: string[] = [];
		await expect(
			Effect.runPromise(
				Zart.runWithTrxProvider(makeProvider(failEvents), () =>
					Effect.fail("boom"),
				),
			),
		).rejects.toThrow();
		expect(failEvents).toEqual(["begin", "rollback", "release"]);

		const commitFailEvents: string[] = [];
		await expect(
			Effect.runPromise(
				Zart.runWithTrxProvider(
					makeProvider(commitFailEvents, true),
					() => Effect.void,
				),
			),
		).rejects.toThrow();
		expect(commitFailEvents).toEqual([
			"begin",
			"commit",
			"rollback",
			"release",
		]);
	});

	it("integrates with WorkerBuilder.withTrxProvider", async () => {
		const events: string[] = [];
		const registered: Array<
			(d: FakeTrxDispatcher, data: unknown) => Promise<unknown>
		> = [];
		const nativeBuilder: NativeWorkerBuilder = {
			registerDurable: (_name, handler) => {
				registered.push(
					handler as (d: FakeTrxDispatcher, data: unknown) => Promise<unknown>,
				);
			},
			registerRecurringDurable: () => {},
			pollIntervalSecs: () => {},
			maxConcurrentTasks: () => {},
			maxTasksPerPoll: () => {},
			shutdownTimeoutSecs: () => {},
			orphanTimeoutSecs: () => {},
			heartbeatIntervalSecs: () => {},
			enableListenNotify: () => {},
			build: () => ({ run: async () => {}, stop: () => {} }),
		};
		const builder = new (
			WorkerBuilder as unknown as {
				new (n: NativeWorkerBuilder): WorkerBuilder;
			}
		)(nativeBuilder);
		const stepFn = Zart.step("ext", () =>
			Effect.gen(function* () {
				yield* Zart.TrxSessionService;
				return "ok";
			}),
		);
		const wf = Zart.durable("ext-wf", () => stepFn());
		builder.registerDurable(wf).withTrxProvider(makeProvider(events));
		const dispatcher = new FakeTrxDispatcher(new FakeTxStore());
		const handler = registered[0];
		if (!handler) throw new Error("no handler registered");
		const out = await handler(dispatcher, null);
		expect(out).toBe("ok");
		expect(dispatcher.lastNeedsTrx).toBe(false);
		expect(events).toEqual(["begin", "commit", "release"]);
	});
});
