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
		listExecutions: async (params) => {
			calls.push({ method: "listExecutions", args: [params] });
			return [
				{
					executionId: "exec-1",
					taskName: "t",
					payload: null,
					status: "completed",
					scheduledAt: "2026-01-01T00:00:00Z",
					version: 1,
				},
			];
		},
		stats: async () => {
			calls.push({ method: "stats", args: [] });
			return {
				scheduled: 1,
				running: 2,
				completed: 3,
				failed: 4,
				cancelled: 5,
			};
		},
		getCurrentRunId: async (executionId) => {
			calls.push({ method: "getCurrentRunId", args: [executionId] });
			return executionId === "missing" ? null : "run-1";
		},
		listRuns: async (executionId) => {
			calls.push({ method: "listRuns", args: [executionId] });
			return [
				{
					runId: "run-1",
					executionId,
					runIndex: 0,
					payload: { ok: true },
					status: "completed",
					result: { done: true },
					startedAt: "2026-01-01T00:00:00Z",
					completedAt: "2026-01-01T00:00:01Z",
					trigger: "initial",
				},
			];
		},
		executionDetail: async (executionId, runId) => {
			calls.push({ method: "executionDetail", args: [executionId, runId] });
			return {
				execution: {
					executionId,
					taskName: "t",
					payload: null,
					status: "completed",
					scheduledAt: "2026-01-01T00:00:00Z",
					version: 1,
				},
				runs: [
					{
						runId: runId ?? "run-1",
						executionId,
						runIndex: 0,
						payload: null,
						status: "completed",
						startedAt: "2026-01-01T00:00:00Z",
						trigger: "initial",
					},
				],
				steps: [
					{
						step: {
							stepId: "step-1",
							runId: runId ?? "run-1",
							stepName: "s1",
							stepKind: "step",
							status: "completed",
							retryAttempt: 0,
							scheduledAt: "2026-01-01T00:00:00Z",
						},
						attempts: [
							{
								attemptId: "att-1",
								stepId: "step-1",
								attemptNumber: 1,
								startedAt: "2026-01-01T00:00:00Z",
								completedAt: "2026-01-01T00:00:01Z",
								status: "completed",
								result: 42,
							},
						],
						retryable: false,
					},
				],
			};
		},
		retryStep: async (runId, stepName, triggeredBy) => {
			calls.push({
				method: "retryStep",
				args: [runId, stepName, triggeredBy],
			});
			return "task-retry-1";
		},
		retryStepCurrentRun: async (executionId, stepName, triggeredBy) => {
			calls.push({
				method: "retryStepCurrentRun",
				args: [executionId, stepName, triggeredBy],
			});
			return "task-retry-2";
		},
		restart: async (executionId, newPayload, triggeredBy) => {
			calls.push({
				method: "restart",
				args: [executionId, newPayload, triggeredBy],
			});
			return `${executionId}:run:1`;
		},
		rerunSteps: async (executionId, spec) => {
			calls.push({ method: "rerunSteps", args: [executionId, spec] });
			return {
				newRunNumber: 1,
				effectiveRerun: spec.forceRerun ?? [],
				potentiallyStale: [],
			};
		},
		pause: async (scope) => {
			calls.push({ method: "pause", args: [scope] });
			return {
				ruleId: "rule-1",
				scope,
				createdAt: "2026-01-01T00:00:00Z",
				reason: scope.reason,
			};
		},
		resume: async (scope, deletedBy) => {
			calls.push({ method: "resume", args: [scope, deletedBy] });
			return { rulesDeleted: 1 };
		},
		resumeRuleById: async (ruleId, deletedBy) => {
			calls.push({ method: "resumeRuleById", args: [ruleId, deletedBy] });
			return { rulesDeleted: 1 };
		},
		listPauseRules: async (filter) => {
			calls.push({ method: "listPauseRules", args: [filter] });
			return [];
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

	it("listExecutions, stats, getCurrentRunId, listRuns, and executionDetail forward params and return typed results", async () => {
		const { scheduler, calls } = makeMockScheduler();
		const from = new Date("2026-01-01T00:00:00.000Z");

		const out = await Effect.runPromise(
			Effect.gen(function* () {
				const list1 = yield* scheduler.listExecutions();
				const list2 = yield* scheduler.listExecutions({
					status: "completed",
					taskName: "order",
					from,
					to: "2026-01-02T00:00:00.000Z",
					search: "exec-",
					sortBy: "task_name",
					sortOrder: "asc",
					limit: 10,
					offset: 5,
				});
				const stats = yield* scheduler.stats();
				const runId = yield* scheduler.getCurrentRunId("exec-1");
				const missingRunId = yield* scheduler.getCurrentRunId("missing");
				const runs = yield* scheduler.listRuns("exec-1");
				const detail = yield* scheduler.executionDetail("exec-1", "run-1");
				return { list1, list2, stats, runId, missingRunId, runs, detail };
			}),
		);

		expect(out.list1).toHaveLength(1);
		expect(out.list2).toHaveLength(1);
		expect(out.stats).toEqual({
			scheduled: 1,
			running: 2,
			completed: 3,
			failed: 4,
			cancelled: 5,
		});
		expect(out.runId).toBe("run-1");
		expect(out.missingRunId).toBeNull();
		expect(out.runs[0]?.trigger).toBe("initial");
		expect(out.detail.steps[0]?.attempts[0]?.attemptNumber).toBe(1);

		expect(calls).toEqual([
			{ method: "listExecutions", args: [undefined] },
			{
				method: "listExecutions",
				args: [
					{
						status: "completed",
						taskName: "order",
						from: "2026-01-01T00:00:00.000Z",
						to: "2026-01-02T00:00:00.000Z",
						search: "exec-",
						sortBy: "task_name",
						sortOrder: "asc",
						limit: 10,
						offset: 5,
					},
				],
			},
			{ method: "stats", args: [] },
			{ method: "getCurrentRunId", args: ["exec-1"] },
			{ method: "getCurrentRunId", args: ["missing"] },
			{ method: "listRuns", args: ["exec-1"] },
			{ method: "executionDetail", args: ["exec-1", "run-1"] },
		]);
	});

	it("listExecutions rejects invalid Date with ZartSchemaError", async () => {
		const { scheduler } = makeMockScheduler();
		const err = await Effect.runPromise(
			Effect.flip(scheduler.listExecutions({ from: new Date("invalid") })),
		);
		expect(err._tag).toBe("ZartSchemaError");
	});

	it("admin and pause/resume methods forward args, encode restart schema, and validate expiresAt", async () => {
		const { scheduler, calls } = makeMockScheduler();
		const expiresAt = new Date("2026-12-01T00:00:00.000Z");

		const out = await Effect.runPromise(
			Effect.gen(function* () {
				const t1 = yield* scheduler.retryStep("exec-1:run:0", "step-a", "ops");
				const t2 = yield* scheduler.retryStepCurrentRun("exec-1", "step-a");
				const r1 = yield* scheduler.restart("exec-1", undefined, "ops");
				const r2 = yield* scheduler.restart(
					"exec-1",
					Schema.Struct({ count: Schema.BigIntFromString }),
					{ count: 42n },
					"ops",
				);
				const rr = yield* scheduler.rerunSteps("exec-1", {
					forceRerun: ["step-b"],
					preserve: ["step-a"],
					triggeredBy: "ops",
				});
				const rule = yield* scheduler.pause({
					executionId: "exec-1",
					stepPattern: "send-*",
					expiresAt,
					triggeredBy: "ops",
					reason: "maintenance",
				});
				const res1 = yield* scheduler.resume({ executionId: "exec-1" }, "ops");
				const res2 = yield* scheduler.resumeRuleById("rule-1", "ops");
				const rules = yield* scheduler.listPauseRules({
					executionId: "exec-1",
					includeDeleted: true,
				});
				return { t1, t2, r1, r2, rr, rule, res1, res2, rules };
			}),
		);

		expect(out.t1).toBe("task-retry-1");
		expect(out.t2).toBe("task-retry-2");
		expect(out.r1).toBe("exec-1:run:1");
		expect(out.r2).toBe("exec-1:run:1");
		expect(out.rr.newRunNumber).toBe(1);
		expect(out.rule.ruleId).toBe("rule-1");
		expect(out.res1.rulesDeleted).toBe(1);
		expect(out.res2.rulesDeleted).toBe(1);
		expect(out.rules).toEqual([]);

		expect(calls).toEqual([
			{ method: "retryStep", args: ["exec-1:run:0", "step-a", "ops"] },
			{
				method: "retryStepCurrentRun",
				args: ["exec-1", "step-a", undefined],
			},
			{ method: "restart", args: ["exec-1", undefined, "ops"] },
			{ method: "restart", args: ["exec-1", { count: "42" }, "ops"] },
			{
				method: "rerunSteps",
				args: [
					"exec-1",
					{
						forceRerun: ["step-b"],
						preserve: ["step-a"],
						triggeredBy: "ops",
					},
				],
			},
			{
				method: "pause",
				args: [
					{
						executionId: "exec-1",
						taskName: undefined,
						stepPattern: "send-*",
						expiresAt: "2026-12-01T00:00:00.000Z",
						triggeredBy: "ops",
						reason: "maintenance",
					},
				],
			},
			{
				method: "resume",
				args: [
					{
						executionId: "exec-1",
						taskName: undefined,
						stepPattern: undefined,
						expiresAt: undefined,
						triggeredBy: undefined,
						reason: undefined,
					},
					"ops",
				],
			},
			{ method: "resumeRuleById", args: ["rule-1", "ops"] },
			{
				method: "listPauseRules",
				args: [{ executionId: "exec-1", includeDeleted: true }],
			},
		]);

		const badDateErr = await Effect.runPromise(
			Effect.flip(
				scheduler.pause({
					executionId: "exec-1",
					expiresAt: new Date("invalid"),
				}),
			),
		);
		expect(badDateErr._tag).toBe("ZartSchemaError");
	});
});
