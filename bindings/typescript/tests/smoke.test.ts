import { Effect, Fiber, Schedule, Schema } from "effect";
import { describe, expect, it } from "vitest";
import {
	DurableScheduler,
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
});
