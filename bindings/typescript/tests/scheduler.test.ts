import { Duration, Effect, Schema } from "effect";
import { describe, expect, it } from "vitest";
import {
	ClientTrx,
	DurableScheduler,
	type NativeClientTrx,
	type NativeDurableScheduler,
	Zart,
} from "../ts/src/index.js";

function makeMockScheduler(opts?: { completionResult?: unknown }): {
	scheduler: DurableScheduler;
	calls: Array<{ method: string; args: unknown[] }>;
} {
	const calls: Array<{ method: string; args: unknown[] }> = [];
	const native: NativeDurableScheduler = {
		start: async (executionId, taskName, payload) => {
			calls.push({ method: "start", args: [executionId, taskName, payload] });
			return {
				taskId: `task-${executionId}`,
				executionTime: "2026-01-01T00:00:00Z",
			};
		},
		startInTx: async (tx, executionId, taskName, payload) => {
			calls.push({
				method: "startInTx",
				args: [tx, executionId, taskName, payload],
			});
			return {
				taskId: `task-${executionId}`,
				executionTime: "2026-01-01T00:00:00Z",
			};
		},
		status: async (id) => ({
			executionId: id,
			taskName: "t",
			payload: null,
			status: "completed",
			scheduledAt: "2026-01-01T00:00:00Z",
			version: 1,
		}),
		wait: async (id, timeoutSecs, pollIntervalSecs) => {
			calls.push({ method: "wait", args: [id, timeoutSecs, pollIntervalSecs] });
			return {
				executionId: id,
				taskName: "t",
				payload: null,
				status: "completed",
				scheduledAt: "2026-01-01T00:00:00Z",
				version: 1,
			};
		},
		waitCompletion: async (id, timeoutSecs, pollIntervalSecs) => {
			calls.push({
				method: "waitCompletion",
				args: [id, timeoutSecs, pollIntervalSecs],
			});
			return opts?.completionResult ?? { ok: true };
		},
		cancel: async () => true,
		offerEvent: async (id, eventName, payload) => {
			calls.push({ method: "offerEvent", args: [id, eventName, payload] });
		},
		stopRecurring: async (taskId) => {
			calls.push({ method: "stopRecurring", args: [taskId] });
		},
	};
	const scheduler = new (
		DurableScheduler as unknown as {
			new (n: NativeDurableScheduler): DurableScheduler;
		}
	)(native);
	return { scheduler, calls };
}

function makeMockClientTrx(): ClientTrx {
	const native: NativeClientTrx = {
		execute: async () => 1,
		fetchOne: async () => ({}),
		fetchAll: async () => [],
		fetchOptional: async () => null,
		commit: async () => {},
		rollback: async () => {},
	};
	return new ClientTrx(native);
}

describe("DurableScheduler ergonomic overloads", () => {
	it("start, waitCompletion, and startAndWait accept DurableDefinition and reuse name + schemas", async () => {
		const date = new Date("2026-05-01T12:00:00.000Z");
		const wf = Zart.durable(
			"order-wf",
			(input: { createdAt: Date }) =>
				Effect.succeed({ processedAt: input.createdAt }),
			{
				inputSchema: Schema.Struct({ createdAt: Schema.DateFromString }),
				outputSchema: Schema.Struct({ processedAt: Schema.DateFromString }),
			},
		);

		const { scheduler, calls } = makeMockScheduler({
			completionResult: { processedAt: "2026-05-01T12:00:00.000Z" },
		});

		const res = await Effect.runPromise(
			scheduler.startAndWait(
				"exec-def-1",
				wf,
				{ createdAt: date },
				"30 seconds",
				Duration.millis(250),
			),
		);

		expect(res.processedAt).toBeInstanceOf(Date);
		expect(res.processedAt.toISOString()).toBe("2026-05-01T12:00:00.000Z");
		expect(calls).toEqual([
			{
				method: "start",
				args: [
					"exec-def-1",
					"order-wf",
					{ createdAt: "2026-05-01T12:00:00.000Z" },
				],
			},
			{
				method: "waitCompletion",
				args: ["exec-def-1", 30, 0.25],
			},
		]);
	});

	it("start, waitCompletion, startAndWait, and offerEvent accept raw JSON payloads without schemas", async () => {
		const { scheduler, calls } = makeMockScheduler({
			completionResult: { status: "done", count: 2 },
		});

		await Effect.runPromise(
			Effect.gen(function* () {
				yield* scheduler.start("exec-raw-1", "raw-task", { ok: true });
				const out = yield* scheduler.waitCompletion(
					"exec-raw-1",
					Duration.seconds(12),
					0.05,
				);
				expect(out).toEqual({ status: "done", count: 2 });

				const out2 = yield* scheduler.startAndWait(
					"exec-raw-2",
					"raw-task",
					{ step: "two" },
					"10 seconds",
					"50 millis",
				);
				expect(out2).toEqual({ status: "done", count: 2 });

				yield* scheduler.offerEvent("exec-raw-1", "approved", { by: "bob" });
			}),
		);

		expect(calls).toEqual([
			{ method: "start", args: ["exec-raw-1", "raw-task", { ok: true }] },
			{ method: "waitCompletion", args: ["exec-raw-1", 12, 0.05] },
			{ method: "start", args: ["exec-raw-2", "raw-task", { step: "two" }] },
			{ method: "waitCompletion", args: ["exec-raw-2", 10, 0.05] },
			{
				method: "offerEvent",
				args: ["exec-raw-1", "approved", { by: "bob" }],
			},
		]);
	});

	it("preserves legacy explicit-schema overloads and plain-number seconds in wait/waitCompletion", async () => {
		const { scheduler, calls } = makeMockScheduler({
			completionResult: { value: 42 },
		});
		const inSchema = Schema.Struct({ n: Schema.Number });
		const outSchema = Schema.Struct({ value: Schema.Number });

		await Effect.runPromise(
			Effect.gen(function* () {
				yield* scheduler.start("exec-legacy", "legacy-task", inSchema, {
					n: 7,
				});
				yield* scheduler.wait("exec-legacy", 15, 0.1);
				const out = yield* scheduler.waitCompletion(
					"exec-legacy",
					outSchema,
					20,
					0.2,
				);
				expect(out).toEqual({ value: 42 });

				const out2 = yield* scheduler.startAndWait(
					"exec-legacy-2",
					"legacy-task",
					inSchema,
					{ n: 9 },
					outSchema,
					"5 seconds",
				);
				expect(out2).toEqual({ value: 42 });

				yield* scheduler.offerEvent(
					"exec-legacy",
					"sig",
					Schema.Struct({ ok: Schema.Boolean }),
					{ ok: true },
				);
			}),
		);

		expect(calls).toEqual([
			{ method: "start", args: ["exec-legacy", "legacy-task", { n: 7 }] },
			{ method: "wait", args: ["exec-legacy", 15, 0.1] },
			{ method: "waitCompletion", args: ["exec-legacy", 20, 0.2] },
			{ method: "start", args: ["exec-legacy-2", "legacy-task", { n: 9 }] },
			{ method: "waitCompletion", args: ["exec-legacy-2", 5, undefined] },
			{ method: "offerEvent", args: ["exec-legacy", "sig", { ok: true }] },
		]);
	});

	it("startInTx accepts DurableDefinition directly", async () => {
		const { scheduler, calls } = makeMockScheduler();
		const tx = makeMockClientTrx();
		const wf = Zart.durable(
			"tx-wf",
			(input: { amount: bigint }) => Effect.succeed(input.amount),
			{
				inputSchema: Schema.Struct({ amount: Schema.BigIntFromString }),
			},
		);

		await Effect.runPromise(
			scheduler.startInTx(tx, {
				executionId: "exec-tx-def",
				definition: wf,
				input: { amount: 900n },
			}),
		);

		expect(calls).toEqual([
			{
				method: "startInTx",
				args: [tx.native, "exec-tx-def", "tx-wf", { amount: "900" }],
			},
		]);
	});
});
