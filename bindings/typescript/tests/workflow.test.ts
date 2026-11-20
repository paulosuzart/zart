import { Data, Effect, Schema } from "effect";
import { describe, expect, it } from "vitest";
import {
	ControlFlowDefect,
	RetryExhaustedError,
	StepFailedError,
	StepTimeoutError,
	Zart,
} from "../ts/src/index.js";
import { FakeDispatcher } from "./fake.js";

class Boom extends Data.TaggedError("Boom")<{ readonly message: string }> {}

describe("Zart.step", () => {
	it("is directly yieldable and supports .named for loops", async () => {
		const double = Zart.step("double", (n: number) => Effect.succeed(n * 2));
		const wf = Zart.durable("wf", (n: number) =>
			Effect.gen(function* () {
				const a = yield* double(n);
				const b = yield* double(a).named("double-iter-1");
				return a + b;
			}),
		);
		const d = new FakeDispatcher();
		expect(await wf.run(d, 3)).toBe(18);
		expect([...d.journal.keys()]).toEqual([
			"step:double",
			"step:double-iter-1",
		]);
	});

	it("retries fixed(3) failing twice, then succeeds; replay is cached", async () => {
		let calls = 0;
		const flaky = Zart.step(
			"flaky",
			() =>
				Effect.suspend(() =>
					++calls < 3
						? Effect.fail(new Boom({ message: "no" }))
						: Effect.succeed("ok"),
				),
			{ retry: Zart.Retry.fixed(3, "50 millis") },
		);
		const wf = Zart.durable("wf", () => flaky());
		const d = new FakeDispatcher();
		expect(await wf.run(d, undefined)).toBe("ok");
		expect(calls).toBe(3);
		expect(await wf.run(d, undefined)).toBe("ok");
		expect(calls).toBe(3);
	});

	it("surfaces RetryExhaustedError and plain business errors typed", async () => {
		const bad = Zart.step(
			"bad",
			() => Effect.fail(new Boom({ message: "x" })),
			{ retry: Zart.Retry.fixed(2, "1 millis") },
		);
		const once = Zart.step("once", () =>
			Effect.fail(new Boom({ message: "y" })),
		);
		const d = new FakeDispatcher();
		const r = await Effect.runPromise(
			Effect.flip(bad()).pipe(
				Effect.provideService(Zart.ZartWorkflow, { dispatcher: d }),
			),
		);
		expect(r).toBeInstanceOf(RetryExhaustedError);
		const r2 = await Effect.runPromise(
			Effect.flip(once()).pipe(
				Effect.provideService(Zart.ZartWorkflow, { dispatcher: d }),
			),
		);
		expect(r2).toBeInstanceOf(StepFailedError);
	});

	it("errorSchema restores typed business errors", async () => {
		const BoomS = Schema.Struct({
			_tag: Schema.Literal("Boom"),
			message: Schema.String,
		});
		const s = Zart.step(
			"typed",
			() => Effect.fail({ _tag: "Boom" as const, message: "m" }),
			{
				errorSchema: BoomS,
			},
		);
		const d = new FakeDispatcher();
		const e = await Effect.runPromise(
			Effect.flip(s()).pipe(
				Effect.provideService(Zart.ZartWorkflow, { dispatcher: d }),
			),
		);
		expect(e).toEqual({ _tag: "Boom", message: "m" });
	});

	it("outcome/stepOr do not throw", async () => {
		const s = Zart.step("o", () => Effect.fail(new Boom({ message: "z" })));
		const wf = Zart.durable("wf", () =>
			Effect.gen(function* () {
				const o = yield* Zart.outcome(s());
				const f = yield* Zart.stepOr(s().named("o2"), "fallback");
				return [o._tag, f];
			}),
		);
		expect(await wf.run(new FakeDispatcher(), undefined)).toEqual([
			"business_err",
			"fallback",
		]);
	});

	it("surfaces StepTimeoutError when step exceeds timeout", async () => {
		const slow = Zart.step(
			"slow-timeout",
			() =>
				Effect.promise(
					() => new Promise<string>((r) => setTimeout(() => r("late"), 80)),
				),
			{ timeout: "15 millis", timeoutScope: "attempt" },
		);
		const err = await Effect.runPromise(
			Effect.flip(slow()).pipe(
				Effect.provideService(Zart.ZartWorkflow, {
					dispatcher: new FakeDispatcher(),
				}),
			),
		);
		expect(err).toBeInstanceOf(StepTimeoutError);
	});
});

describe("control flow", () => {
	const wf = Zart.durable("evt", () =>
		Effect.gen(function* () {
			const v = yield* Zart.waitForEvent<string>("go").pipe(
				Effect.catch(() => Effect.succeed("swallowed")),
			);
			return v;
		}),
	);

	it("waitForEvent suspension is a defect not swallowed by catchAll, re-signalled at boundary", async () => {
		const d = new FakeDispatcher();
		const err = await wf.run(d, undefined).catch((e: Error) => e);
		expect(err).toBeInstanceOf(Error);
		expect(JSON.parse((err as Error).message)).toEqual({
			_tag: "ControlFlowSignal",
			kind: "scheduled",
		});
		d.events.set("go", "payload");
		expect(await wf.run(d, undefined)).toBe("payload");
	});

	it("ControlFlowDefect is a die", async () => {
		const d = new FakeDispatcher();
		const exit = await Effect.runPromiseExit(
			Zart.waitForEvent("go").pipe(
				Effect.provideService(Zart.ZartWorkflow, { dispatcher: d }),
			),
		);
		expect(exit._tag).toBe("Failure");
		expect(JSON.stringify(exit)).toContain("ControlFlowDefect");
		expect(new ControlFlowDefect({ kind: "x" })._tag).toBe("ControlFlowDefect");
	});

	it("sleep suspends then resumes", async () => {
		const w = Zart.durable("s", () =>
			Zart.sleep("nap", "1 second").pipe(Effect.as("done")),
		);
		const d = new FakeDispatcher();
		await expect(w.run(d, undefined)).rejects.toThrow("ControlFlowSignal");
		expect(await w.run(d, undefined)).toBe("done");
	});
});

describe("parallelism, capture, context", () => {
	it("schedule + wait run concurrently", async () => {
		const slow = Zart.step("slow", (ms: number) =>
			Effect.promise(
				() => new Promise<number>((r) => setTimeout(() => r(ms), ms)),
			),
		);
		const wf = Zart.durable("par", () =>
			Effect.gen(function* () {
				const h1 = yield* Zart.schedule(slow(60).named("a"));
				const h2 = yield* Zart.schedule(slow(60).named("b"));
				return [yield* Zart.wait(h1), yield* Zart.wait(h2)];
			}),
		);
		const t0 = Date.now();
		expect(await wf.run(new FakeDispatcher(), undefined)).toEqual([60, 60]);
		expect(Date.now() - t0).toBeLessThan(115);
	});

	it("waitAll collects scheduled steps concurrently", async () => {
		const slow = Zart.step("slow-all", (ms: number) =>
			Effect.promise(
				() => new Promise<number>((r) => setTimeout(() => r(ms), ms)),
			),
		);
		const wf = Zart.durable("par-all", () =>
			Effect.gen(function* () {
				const h1 = yield* Zart.schedule(slow(40).named("a"));
				const h2 = yield* Zart.schedule(slow(50).named("b"));
				return yield* Zart.waitAll([h1, h2]);
			}),
		);
		expect(await wf.run(new FakeDispatcher(), undefined)).toEqual([40, 50]);
	});

	it("capture, now, context and input/output schemas", async () => {
		const wf = Zart.durable(
			"c",
			(input: { at: Date }) =>
				Effect.gen(function* () {
					const r = yield* Zart.capture("rnd", () => 7);
					const n = yield* Zart.now("t0");
					const c = yield* Zart.context;
					return {
						r,
						year: n.getUTCFullYear(),
						id: c.executionId,
						at: input.at,
					};
				}),
			{
				inputSchema: Schema.Struct({ at: Schema.DateFromString }),
				outputSchema: Schema.Struct({
					r: Schema.Number,
					year: Schema.Number,
					id: Schema.String,
					at: Schema.DateFromString,
				}),
			},
		);
		const out = await wf.handler(new FakeDispatcher(), {
			at: "2024-01-02T00:00:00.000Z",
		});
		expect(out).toEqual({
			r: 7,
			year: 2024,
			id: "e1",
			at: "2024-01-02T00:00:00.000Z",
		});
	});

	it("Zart.context is accessible inside Zart.step and reflects isRetry/currentAttempt on retry", async () => {
		const attemptsSeen: Array<{ attempt: number; isRetry: boolean }> = [];
		let calls = 0;
		const retryStep = Zart.step(
			"retry-step",
			() =>
				Effect.gen(function* () {
					const ctx = yield* Zart.context;
					attemptsSeen.push({
						attempt: ctx.currentAttempt,
						isRetry: ctx.isRetry,
					});
					if (++calls < 3) {
						return yield* Effect.fail(new Boom({ message: "try again" }));
					}
					return ctx;
				}),
			{ retry: Zart.Retry.fixed(3, "10 millis") },
		);

		const wf = Zart.durable("wf-step-ctx", () => retryStep());
		const d = new FakeDispatcher();
		const finalCtx = await wf.run(d, undefined);
		expect(finalCtx).toBeDefined();
		expect(calls).toBe(3);
		expect(attemptsSeen).toEqual([
			{ attempt: 0, isRetry: false },
			{ attempt: 1, isRetry: true },
			{ attempt: 2, isRetry: true },
		]);
	});

	it("stepOrElse intercepts business error and falls back to value or effect", async () => {
		const failStep = Zart.step("fail-step", () =>
			Effect.fail(new Boom({ message: "business failure" })),
		);
		const wf = Zart.durable("wf-or-else", () =>
			Effect.gen(function* () {
				const valFallback = yield* Zart.stepOrElse(
					failStep(),
					(err) => `handled:${err.message}`,
				);
				const effFallback = yield* Zart.stepOrElse(
					failStep().named("fail-step-2"),
					(err) => Effect.succeed(`eff:${err.message}`),
				);
				return [valFallback, effFallback];
			}),
		);
		const d = new FakeDispatcher();
		const res = await wf.run(d, undefined);
		expect(res).toEqual(["handled:business failure", "eff:business failure"]);
	});

	it("stepOrElse does NOT intercept framework errors (RetryExhaustedError, StepTimeoutError)", async () => {
		const exhaustedStep = Zart.step(
			"exhausted",
			() => Effect.fail(new Boom({ message: "always fails" })),
			{ retry: Zart.Retry.fixed(2, "5 millis") },
		);
		const wfExhausted = Zart.durable("wf-exhausted", () =>
			Zart.stepOrElse(exhaustedStep(), () => "should-not-catch"),
		);
		const d = new FakeDispatcher();
		const err = await wfExhausted.run(d, undefined).catch((e: Error) => e);
		expect(err).toBeInstanceOf(Error);
		const parsed = JSON.parse((err as Error).message);
		expect(parsed._tag).toBe("RetryExhaustedError");

		const timeoutStep = Zart.step(
			"timed-out",
			() =>
				Effect.promise(
					() => new Promise<string>((r) => setTimeout(() => r("late"), 80)),
				),
			{ timeout: "10 millis", timeoutScope: "attempt" },
		);
		const wfTimeout = Zart.durable("wf-timeout", () =>
			Zart.stepOrElse(timeoutStep(), () => "should-not-catch"),
		);
		const err2 = await wfTimeout
			.run(new FakeDispatcher(), undefined)
			.catch((e: Error) => e);
		expect(err2).toBeInstanceOf(Error);
		const parsed2 = JSON.parse((err2 as Error).message);
		expect(parsed2._tag).toBe("StepTimeoutError");
	});

	it("Zart.durable preserves sub-second timeout precision and invokes onFailure callback", async () => {
		let failureReceived: unknown;
		let inputReceived: unknown;

		const wf = Zart.durable(
			"failing-task",
			(input: { count: number }) => Effect.succeed({ total: input.count * 10 }),
			{
				timeout: "50 millis",
				inputSchema: Schema.Struct({ count: Schema.Number }),
				outputSchema: Schema.Struct({ total: Schema.Number }),
				onFailure: (input, failure) => {
					inputReceived = input;
					failureReceived = failure;
					return Effect.succeed({ total: 999 });
				},
			},
		);

		// Sub-second precision check:
		expect(wf.timeoutMs).toBe(50);

		// onFailure invocation check with step_failed:
		expect(wf.onFailure).toBeDefined();
		const fallbackResult = await wf.onFailure?.(
			{ count: 5 },
			{
				kind: "step_failed",
				step: "charge_step",
				raw: { reason: "insufficient_funds" },
			},
		);

		expect(inputReceived).toEqual({ count: 5 });
		expect(failureReceived).toEqual({
			kind: "step_failed",
			step: "charge_step",
			raw: { reason: "insufficient_funds" },
		});
		expect(fallbackResult).toEqual({ total: 999 });

		// onFailure with retries_exhausted:
		await wf.onFailure?.(
			{ count: 7 },
			{ kind: "retries_exhausted", attempts: 3 },
		);
		expect(failureReceived).toEqual({
			kind: "retries_exhausted",
			attempts: 3,
		});

		// onFailure with execution_deadline_exceeded:
		await wf.onFailure?.({ count: 8 }, { kind: "execution_deadline_exceeded" });
		expect(failureReceived).toEqual({
			kind: "execution_deadline_exceeded",
		});
	});

	it("onFailure runs outputSchema transformation on recovered result", async () => {
		const wf = Zart.durable(
			"transforming-on-failure",
			() =>
				Effect.succeed({ recoveredAt: new Date("2024-01-01T00:00:00.000Z") }),
			{
				outputSchema: Schema.Struct({ recoveredAt: Schema.DateFromString }),
				onFailure: (_input, _failure) =>
					Effect.succeed({
						recoveredAt: new Date("2025-06-15T12:00:00.000Z"),
					}),
			},
		);

		const encoded = await wf.onFailure?.(null, {
			kind: "execution_deadline_exceeded",
		});
		expect(encoded).toEqual({
			recoveredAt: "2025-06-15T12:00:00.000Z",
		});
	});
});
