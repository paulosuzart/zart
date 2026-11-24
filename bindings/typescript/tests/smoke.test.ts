import { Effect, Fiber, Schedule, Schema } from "effect";
import { describe, expect, it } from "vitest";
import {
	DurableScheduler,
	ExecutionNotFound,
	isNativeAvailable,
	PgBackend,
	WorkerBuilder,
	Zart,
	ZartBackendError,
	ZartSchemaError,
} from "../ts/src/index.js";

const url = process.env.DATABASE_URL;
const run = isNativeAvailable() && !!url;

describe.skipIf(!run)("postgres smoke", () => {
	it("connects, migrates, pings", async () => {
		await Effect.runPromise(
			Effect.scoped(
				Effect.gen(function* () {
					const b = yield* PgBackend.scoped(url ?? "");
					yield* b.runMigrations();
					yield* b.ping();
				}),
			),
		);
		expect(true).toBe(true);
	});

	it("connects with PgBackendConnectOptions, preserves table names across withListenerUrl, and configures worker", async () => {
		await Effect.runPromise(
			Effect.scoped(
				Effect.gen(function* () {
					const b = yield* PgBackend.scoped(url ?? "", {
						listenerUrl: url ?? "",
						tablePrefix: "missing_",
						schema: "public",
					});
					yield* b.ping();
					const b2 = yield* b.withListenerUrl(url ?? "");
					yield* b2.ping();

					const sched = yield* DurableScheduler.fromBackend(b2);
					const missingErr = yield* Effect.flip(
						sched.start("smoke-missing-1", "noop", Schema.Unknown, {}),
					);
					expect(missingErr).toBeInstanceOf(ZartBackendError);
					expect(missingErr.message).toContain("missing_");

					const builder = yield* WorkerBuilder.fromBackend(b2);
					expect(() => builder.configure({ maxConcurrentTasks: 0 })).toThrow(
						ZartSchemaError,
					);
					expect(() => builder.configure({ maxTasksPerPoll: 0 })).toThrow(
						ZartSchemaError,
					);
					builder
						.configure({
							orphanTimeout: "2 minutes",
							heartbeatInterval: "15 seconds",
							enableListenNotify: false,
						})
						.configure({ heartbeatInterval: null })
						.build();
				}),
			),
		);
	});

	it("executes workflow parity features (context in step, stepOrElse, onFailure, sub-second timeout) with live worker", async () => {
		const uid = (prefix: string) =>
			`${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
		const stepAttempts: Array<{ attempt: number; isRetry: boolean }> = [];
		let stepCallCount = 0;

		const retryStepName = uid("smoke-retry-step");
		const businessFailStepName = uid("smoke-business-fail");
		const parityWorkflowName = uid("smoke-parity-wf");
		const failingWorkflowName = uid("smoke-failing-parity-wf");

		const retryStep = Zart.step(
			retryStepName,
			() =>
				Effect.gen(function* () {
					const ctx = yield* Zart.context;
					stepAttempts.push({
						attempt: ctx.currentAttempt,
						isRetry: ctx.isRetry,
					});
					if (++stepCallCount < 2) {
						return yield* Effect.fail(new Error("retry-me"));
					}
					return {
						attempt: ctx.currentAttempt,
						isRetry: ctx.isRetry,
					};
				}),
			{ retry: Zart.Retry.fixed(2, "10 millis") },
		);

		const businessFailStep = Zart.step(businessFailStepName, () =>
			Effect.fail(new Error("planned-business-error")),
		);

		const parityWorkflow = Zart.durable(
			parityWorkflowName,
			() =>
				Effect.gen(function* () {
					const stepRes = yield* retryStep();
					const fallbackRes = yield* Zart.stepOrElse(
						businessFailStep(),
						(err) => `recovered:${err.message}`,
					);
					return {
						stepRes,
						fallbackRes,
					};
				}),
			{
				timeout: "15 seconds",
			},
		);

		let failureCallbackInvoked = false;
		let receivedFailureKind = "";

		const failingWorkflow = Zart.durable(
			failingWorkflowName,
			() =>
				Effect.gen(function* () {
					yield* businessFailStep();
					return "unreachable";
				}),
			{
				timeout: "15 seconds",
				onFailure: (_input, failure) =>
					Effect.sync(() => {
						failureCallbackInvoked = true;
						receivedFailureKind = failure.kind;
						return "handled-by-on-failure";
					}),
			},
		);

		const ergoWorkflowName = uid("smoke-ergo-wf");
		const ergoWorkflow = Zart.durable(
			ergoWorkflowName,
			(input: { count: number }) =>
				Effect.succeed({ doubled: input.count * 2 }),
			{
				inputSchema: Schema.Struct({ count: Schema.Number }),
				outputSchema: Schema.Struct({ doubled: Schema.Number }),
				timeout: "15 seconds",
			},
		);

		await Effect.runPromise(
			Effect.scoped(
				Effect.gen(function* () {
					const b = yield* PgBackend.scoped(url ?? "");
					yield* b.runMigrations();

					const sched = yield* DurableScheduler.fromBackend(b);
					const workerBuilder = yield* WorkerBuilder.fromBackend(b);
					const worker = workerBuilder
						.configure({
							pollInterval: "15 millis",
							enableListenNotify: false,
						})
						.registerDurable(parityWorkflow)
						.registerDurable(failingWorkflow)
						.registerDurable(ergoWorkflow)
						.build();

					const workerFiber = yield* Effect.forkChild(worker.runScoped());
					yield* Effect.yieldNow;

					yield* Effect.gen(function* () {
						// 1. Run parity workflow
						const execId1 = uid("smoke-parity");
						yield* sched.start(execId1, parityWorkflowName, Schema.Unknown, {});
						const result1 = yield* sched.waitCompletion(
							execId1,
							Schema.Unknown,
							15,
							0.02,
						);

						expect(result1).toEqual({
							stepRes: { attempt: 1, isRetry: true },
							fallbackRes: "recovered:planned-business-error",
						});
						expect(stepAttempts).toEqual([
							{ attempt: 0, isRetry: false },
							{ attempt: 1, isRetry: true },
						]);

						// 2. Run failing workflow to test onFailure
						const execId2 = uid("smoke-failing");
						yield* sched.start(
							execId2,
							failingWorkflowName,
							Schema.Unknown,
							{},
						);
						const result2 = yield* sched.waitCompletion(
							execId2,
							Schema.Unknown,
							15,
							0.02,
						);

						expect(result2).toBe("handled-by-on-failure");
						expect(failureCallbackInvoked).toBe(true);
						expect(receivedFailureKind).toBe("step_failed");

						// 3. Run ergonomic startAndWait with DurableDefinition and Duration.Input
						const execId3 = uid("smoke-ergo");
						const result3 = yield* sched.startAndWait(
							execId3,
							ergoWorkflow,
							{ count: 21 },
							"15 seconds",
							"20 millis",
						);
						expect(result3).toEqual({ doubled: 42 });
					}).pipe(Effect.ensuring(Fiber.interrupt(workerFiber)));
				}),
			),
		);
	}, 30000);

	it("runs recurring durable execution ticks and stops idempotently via stopRecurring", async () => {
		const { OverlapPolicy, Recurrence } = await import("../ts/src/index.js");
		const recurringName = `smoke-recurring-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
		const idTemplate = `${recurringName}-tick-{occurrence}`;
		const observedCounts: number[] = [];

		const recurringWf = Zart.durable(
			recurringName,
			(input: { tickWeight: number }) =>
				Effect.sync(() => {
					observedCounts.push(input.tickWeight);
					return { count: observedCounts.length };
				}),
			{
				inputSchema: Schema.Struct({ tickWeight: Schema.Number }),
				outputSchema: Schema.Struct({ count: Schema.Number }),
				timeout: "10 seconds",
			},
		);

		await Effect.runPromise(
			Effect.scoped(
				Effect.gen(function* () {
					const b = yield* PgBackend.scoped(url ?? "");
					yield* b.runMigrations();

					const sched = yield* DurableScheduler.fromBackend(b);
					const worker = (yield* WorkerBuilder.fromBackend(b))
						.configure({
							pollInterval: "15 millis",
							enableListenNotify: false,
						})
						.registerRecurringDurable(recurringWf, {
							idTemplate,
							recurrence: Recurrence.fixedDelay("40 millis"),
							overlap: OverlapPolicy.SkipIfRunning,
							initialData: { tickWeight: 7 },
						})
						.build();

					const workerFiber = yield* Effect.forkChild(worker.runScoped());
					yield* Effect.yieldNow;

					yield* Effect.gen(function* () {
						const waitForTick = (tickId: string) =>
							sched
								.waitCompletion(tickId, recurringWf, "10 seconds", "20 millis")
								.pipe(
									Effect.retry({
										while: (e) => e._tag === "ExecutionNotFound",
										times: 50,
										schedule: Schedule.spaced("20 millis"),
									}),
								);

						const tick0 = yield* waitForTick(`${recurringName}-tick-0`);
						expect(tick0).toEqual({ count: 1 });

						const tick1 = yield* waitForTick(`${recurringName}-tick-1`);
						expect(tick1.count).toBeGreaterThanOrEqual(2);
						expect(observedCounts.every((w) => w === 7)).toBe(true);

						// Idempotent stopRecurring
						yield* sched.stopRecurring(recurringWf);
						yield* sched.stopRecurring(recurringName);
					}).pipe(Effect.ensuring(Fiber.interrupt(workerFiber)));
				}),
			),
		);
	}, 30000);

	it("supports introspection: listExecutions, stats, getCurrentRunId, listRuns, executionDetail", async () => {
		const uid = (prefix: string) =>
			`${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
		const wfName = uid("smoke-introspect-wf");
		const stepName = uid("smoke-introspect-step");
		const execId1 = uid("smoke-introspect-1");
		const execId2 = uid("smoke-introspect-2");

		const step = Zart.step(stepName, (n: number) => Effect.succeed(n * 2));
		const wf = Zart.durable(wfName, (input: { n: number }) =>
			Zart.require(step(input.n)),
		);

		await Effect.runPromise(
			Effect.scoped(
				Effect.gen(function* () {
					const b = yield* PgBackend.scoped(url ?? "");
					yield* b.runMigrations();

					const sched = yield* DurableScheduler.fromBackend(b);
					const worker = (yield* WorkerBuilder.fromBackend(b))
						.configure({
							pollInterval: "20 millis",
							enableListenNotify: false,
						})
						.registerDurable(wf)
						.build();

					const workerFiber = yield* Effect.forkChild(worker.runScoped());
					yield* Effect.yieldNow;

					yield* Effect.gen(function* () {
						const out1 = yield* sched.startAndWait(
							execId1,
							wf,
							{ n: 21 },
							"15 seconds",
							"20 millis",
						);
						expect(out1).toBe(42);

						const out2 = yield* sched.startAndWait(
							execId2,
							wf,
							{ n: 5 },
							"15 seconds",
							"20 millis",
						);
						expect(out2).toBe(10);

						// listExecutions with filters, sorting, pagination
						const listed = yield* sched.listExecutions({
							status: "completed",
							taskName: wfName,
							sortBy: "scheduled_at",
							sortOrder: "asc",
							limit: 10,
							offset: 0,
						});
						expect(listed.map((e) => e.executionId)).toEqual([
							execId1,
							execId2,
						]);

						const paged = yield* sched.listExecutions({
							taskName: wfName,
							sortBy: "scheduled_at",
							sortOrder: "asc",
							limit: 1,
							offset: 1,
						});
						expect(paged.map((e) => e.executionId)).toEqual([execId2]);

						const byPrefix = yield* sched.listExecutions({
							search: execId1,
						});
						expect(byPrefix.map((e) => e.executionId)).toEqual([execId1]);

						// stats
						const stats = yield* sched.stats();
						expect(stats.completed).toBeGreaterThanOrEqual(2);
						expect(typeof stats.scheduled).toBe("number");
						expect(typeof stats.running).toBe("number");
						expect(typeof stats.failed).toBe("number");
						expect(typeof stats.cancelled).toBe("number");

						// getCurrentRunId
						const runId = yield* sched.getCurrentRunId(execId1);
						expect(typeof runId).toBe("string");
						expect(runId).not.toBeNull();
						const missingRunId = yield* sched.getCurrentRunId(
							uid("missing-exec"),
						);
						expect(missingRunId).toBeNull();

						// listRuns
						const runs = yield* sched.listRuns(execId1);
						expect(runs).toHaveLength(1);
						expect(runs[0]?.runId).toBe(runId);
						expect(runs[0]?.executionId).toBe(execId1);
						expect(runs[0]?.runIndex).toBe(0);
						expect(runs[0]?.status).toBe("completed");
						expect(runs[0]?.trigger).toBe("initial");

						// executionDetail
						const detail = yield* sched.executionDetail(execId1);
						expect(detail.execution.executionId).toBe(execId1);
						expect(detail.runs).toHaveLength(1);
						expect(detail.steps).toHaveLength(1);
						expect(detail.steps[0]?.step.stepName).toBe(stepName);
						expect(detail.steps[0]?.step.stepKind).toBe("step");
						expect(detail.steps[0]?.step.status).toBe("completed");
						expect(detail.steps[0]?.retryable).toBe(false);
						expect(detail.steps[0]?.attempts).toHaveLength(1);
						expect(detail.steps[0]?.attempts[0]?.attemptNumber).toBe(1);
						expect(detail.steps[0]?.attempts[0]?.status).toBe("completed");
						expect(detail.steps[0]?.attempts[0]?.result).toBe(42);

						// executionDetail with explicit runId
						const detailWithRun = yield* sched.executionDetail(
							execId1,
							runId ?? undefined,
						);
						expect(detailWithRun.steps).toHaveLength(1);

						// executionDetail on missing execution -> ExecutionNotFound
						const notFound = yield* Effect.flip(
							sched.executionDetail(uid("missing-detail")),
						);
						expect(notFound).toBeInstanceOf(ExecutionNotFound);
					}).pipe(Effect.ensuring(Fiber.interrupt(workerFiber)));
				}),
			),
		);
	}, 30000);

	it("supports admin operations: restart, rerunSteps, retryStep, retryStepCurrentRun, and pause/resume", async () => {
		const uid = (prefix: string) =>
			`${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
		const wfName = uid("smoke-admin-wf");
		const stepAName = uid("smoke-admin-step-a");
		const stepBName = uid("smoke-admin-step-b");

		let stepACount = 0;
		let stepBCount = 0;
		let shouldFailB = true;

		const stepA = Zart.step(stepAName, (n: number) =>
			Effect.sync(() => {
				stepACount += 1;
				return n + 1;
			}),
		);
		const stepB = Zart.step(stepBName, (n: number) =>
			Effect.gen(function* () {
				stepBCount += 1;
				if (shouldFailB) {
					return yield* Effect.fail(new Error("boom-admin"));
				}
				return n * 10;
			}),
		);

		const wf = Zart.durable(wfName, (input: { n: number }) =>
			Effect.gen(function* () {
				const a = yield* Zart.require(stepA(input.n));
				const b = yield* Zart.require(stepB(a));
				return { a, b };
			}),
		);

		const execId = uid("smoke-admin-exec");

		await Effect.runPromise(
			Effect.scoped(
				Effect.gen(function* () {
					const b = yield* PgBackend.scoped(url ?? "");
					yield* b.runMigrations();

					const sched = yield* DurableScheduler.fromBackend(b);
					const worker = (yield* WorkerBuilder.fromBackend(b))
						.configure({
							pollInterval: "20 millis",
							enableListenNotify: false,
						})
						.registerDurable(wf)
						.build();

					const workerFiber = yield* Effect.forkChild(worker.runScoped());
					yield* Effect.yieldNow;

					yield* Effect.gen(function* () {
						// 1. Initial run fails on stepB -> execution fails
						yield* sched.start(execId, wf, { n: 2 });
						const failedRec = yield* sched.wait(
							execId,
							"15 seconds",
							"20 millis",
						);
						expect(failedRec.status).toBe("failed");
						expect(stepACount).toBe(1);
						expect(stepBCount).toBe(1);

						const run0 = yield* sched.getCurrentRunId(execId);
						expect(run0).not.toBeNull();

						// 2. Verify retryStep & retryStepCurrentRun against PostgreSQL
						const statusMismatch = yield* Effect.flip(
							sched.retryStep(run0 ?? "", stepBName, "admin-test"),
						);
						expect(statusMismatch).toBeInstanceOf(ZartBackendError);
						expect(statusMismatch.message).toContain("expected 'Dead'");

						const statusMismatchCurrent = yield* Effect.flip(
							sched.retryStepCurrentRun(execId, stepBName, "admin-test"),
						);
						expect(statusMismatchCurrent).toBeInstanceOf(ZartBackendError);
						expect(statusMismatchCurrent.message).toContain("expected 'Dead'");

						const retryMissingExec = yield* Effect.flip(
							sched.retryStepCurrentRun(
								uid("missing-retry"),
								stepBName,
								"admin-test",
							),
						);
						expect(retryMissingExec).toBeInstanceOf(ExecutionNotFound);

						// 3. Selective rerun: preserve stepA, forceRerun stepB (now succeeding)
						shouldFailB = false;

						// 4. Selective rerun: preserve stepA, forceRerun stepB
						const rerunRes = yield* sched.rerunSteps(execId, {
							forceRerun: [stepBName],
							preserve: [stepAName],
							triggeredBy: "admin-test",
						});
						expect(rerunRes.newRunNumber).toBe(1);
						expect(rerunRes.effectiveRerun).toContain(stepBName);
						const rerunRec = yield* sched.wait(
							execId,
							"15 seconds",
							"20 millis",
						);
						expect(rerunRec.status).toBe("completed");
						expect(rerunRec.result).toEqual({ a: 3, b: 30 });

						// 5. Full restart with new payload
						const newRunId = yield* sched.restart(
							execId,
							{ n: 5 },
							"admin-test",
						);
						expect(newRunId).toContain(":run:2");
						const restartedRec = yield* sched.wait(
							execId,
							"15 seconds",
							"20 millis",
						);
						expect(restartedRec.status).toBe("completed");
						expect(restartedRec.result).toEqual({ a: 6, b: 60 });

						const runs = yield* sched.listRuns(execId);
						expect(runs.map((r) => r.trigger)).toEqual([
							"initial",
							"selective_rerun",
							"restart",
						]);

						// 6. Pause / listPauseRules / resumeRuleById / resume
						const rule1 = yield* sched.pause({
							executionId: execId,
							stepPattern: `${stepBName}*`,
							triggeredBy: "admin-test",
							reason: "maintenance",
						});
						expect(rule1.ruleId.length).toBeGreaterThan(0);
						expect(rule1.scope.executionId).toBe(execId);
						expect(rule1.reason).toBe("maintenance");

						const activeRules = yield* sched.listPauseRules({
							executionId: execId,
						});
						expect(activeRules.some((r) => r.ruleId === rule1.ruleId)).toBe(
							true,
						);

						const resById = yield* sched.resumeRuleById(
							rule1.ruleId,
							"admin-test",
						);
						expect(resById.rulesDeleted).toBe(1);

						const afterDelete = yield* sched.listPauseRules({
							executionId: execId,
						});
						expect(afterDelete.some((r) => r.ruleId === rule1.ruleId)).toBe(
							false,
						);

						const withDeleted = yield* sched.listPauseRules({
							executionId: execId,
							includeDeleted: true,
						});
						expect(withDeleted.some((r) => r.ruleId === rule1.ruleId)).toBe(
							true,
						);

						// Pause again and resume by scope
						yield* sched.pause({
							taskName: wfName,
							triggeredBy: "admin-test",
						});
						const resByScope = yield* sched.resume(
							{ taskName: wfName },
							"admin-test",
						);
						expect(resByScope.rulesDeleted).toBeGreaterThanOrEqual(1);
					}).pipe(Effect.ensuring(Fiber.interrupt(workerFiber)));
				}),
			),
		);
	}, 30000);
});
