import { Duration, Effect, Schema } from "effect";
import { describe, expect, it } from "vitest";
import {
	DurableScheduler,
	type NativeDurableScheduler,
	type NativeWorkerBuilder,
	OverlapPolicy,
	Recurrence,
	WorkerBuilder,
	Zart,
} from "../ts/src/index.js";

describe("Recurrence & OverlapPolicy", () => {
	it("constructs fixedDelay and cron recurrences", () => {
		expect(Recurrence.fixedDelay("500 millis")).toEqual({
			type: "fixed_delay",
			durationMs: 500,
		});
		expect(Recurrence.fixedDelay(Duration.seconds(10))).toEqual({
			type: "fixed_delay",
			durationMs: 10_000,
		});
		expect(Recurrence.cron("0 */5 * * * *")).toEqual({
			type: "cron",
			expression: "0 */5 * * * *",
			timezone: "UTC",
		});
		expect(Recurrence.cron("0 0 9 * * *", "America/Sao_Paulo")).toEqual({
			type: "cron",
			expression: "0 0 9 * * *",
			timezone: "America/Sao_Paulo",
		});
	});

	it("exposes OverlapPolicy constants", () => {
		expect(OverlapPolicy.SkipIfRunning).toBe("SkipIfRunning");
		expect(OverlapPolicy.CancelAndRestart).toBe("CancelAndRestart");
		expect(OverlapPolicy.AlwaysStart).toBe("AlwaysStart");
	});
});

describe("WorkerBuilder.registerRecurringDurable & DurableScheduler.stopRecurring", () => {
	it("registers recurring durable task with encoded initialData and defaults idTemplate", () => {
		const recorded: unknown[] = [];
		const nativeBuilder: NativeWorkerBuilder = {
			registerDurable: () => {},
			registerRecurringDurable: (...args) => {
				recorded.push(args);
			},
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

		const wf = Zart.durable(
			"heartbeat-wf",
			(input: { since: Date }) => Effect.succeed(input.since.toISOString()),
			{
				inputSchema: Schema.Struct({ since: Schema.DateFromString }),
				timeout: "5 seconds",
				maxRetries: 2,
			},
		);

		builder.registerRecurringDurable(wf, {
			recurrence: Recurrence.fixedDelay("2 seconds"),
			overlap: OverlapPolicy.AlwaysStart,
			initialData: { since: new Date("2026-06-01T00:00:00.000Z") },
		});

		expect(recorded).toHaveLength(1);
		const call = recorded[0] as unknown[];
		expect(call[0]).toBe("heartbeat-wf");
		expect(typeof call[1]).toBe("function");
		expect(call[2]).toBe("heartbeat-wf-{occurrence}");
		expect(call[3]).toEqual({ kind: "fixed_delay", durationMs: 2000 });
		expect(call[4]).toBe("AlwaysStart");
		expect(call[5]).toEqual({ since: "2026-06-01T00:00:00.000Z" });
		expect(call[6]).toBe(5000);
		expect(call[7]).toBe(2);
	});

	it("stopRecurring accepts either taskId string or DurableDefinition", async () => {
		const stopped: string[] = [];
		const nativeScheduler: NativeDurableScheduler = {
			start: async () => ({
				taskId: "t",
				executionTime: "2026-01-01T00:00:00Z",
			}),
			startInTx: async () => ({
				taskId: "t",
				executionTime: "2026-01-01T00:00:00Z",
			}),
			status: async (id) => ({
				executionId: id,
				taskName: "t",
				payload: null,
				status: "completed",
				scheduledAt: "2026-01-01T00:00:00Z",
				version: 1,
			}),
			wait: async (id) => nativeScheduler.status(id),
			waitCompletion: async () => null,
			cancel: async () => true,
			offerEvent: async () => {},
			stopRecurring: async (taskId) => {
				stopped.push(taskId);
			},
		};
		const scheduler = new (
			DurableScheduler as unknown as {
				new (n: NativeDurableScheduler): DurableScheduler;
			}
		)(nativeScheduler);

		const wf = Zart.durable("cleanup-wf", () => Effect.void);

		await Effect.runPromise(
			Effect.gen(function* () {
				yield* scheduler.stopRecurring("manual-task");
				yield* scheduler.stopRecurring(wf);
			}),
		);

		expect(stopped).toEqual(["manual-task", "cleanup-wf"]);
	});
});
