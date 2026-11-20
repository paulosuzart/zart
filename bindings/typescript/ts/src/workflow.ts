import { Cause, Duration, Effect, Exit, Option, type Schema } from "effect";
import { liftNative, withWorkflow, ZartContext, ZartWorkflow } from "./core.js";
import {
	ControlFlowDefect,
	type ExecutionFailure,
	type NativeStepOutcome,
	StepFailedError,
	type StepOutcome,
	ZartBackendError,
	ZartError,
	ZartSchemaError,
	type ZartTaggedError,
} from "./errors.js";
import type {
	NativeDispatcher,
	NativeDurableHandler,
	NativeExecutionFailure,
	NativeExecutionInfo,
} from "./native.js";
import { decodeOutput, encodeInput, encodeJsonValue } from "./schema.js";
import {
	type StepEffect,
	type StepError,
	StepMeta,
	zartContextStorage,
} from "./step.js";

export { ZartContext, ZartWorkflow } from "./core.js";
export type { ExecutionFailure } from "./errors.js";

/** Execute a step, failing the workflow Effect on any step error (same as `yield* step(...)`). */
export const require_ = <A, E>(call: StepEffect<A, E>): StepEffect<A, E> =>
	call;
export { require_ as require };

/** Handle to a scheduled (concurrent) step. */
export interface Scheduled<A, E> {
	readonly _tag: "Scheduled";
	readonly handle: number;
	readonly interpret: (o: NativeStepOutcome) => Effect.Effect<A, StepError<E>>;
}

/** Start a step concurrently; collect with `Zart.wait`. */
export const schedule = <A, E>(
	call: StepEffect<A, E>,
): Effect.Effect<Scheduled<A, E>, ZartTaggedError, ZartWorkflow> => {
	const meta = call[StepMeta];
	return withWorkflow((dispatcher) =>
		liftNative(() => dispatcher.schedule(meta.spec, meta.run)).pipe(
			Effect.map((handle) => ({
				_tag: "Scheduled" as const,
				handle,
				interpret: meta.interpret,
			})),
		),
	);
};

export const wait = <A, E>(
	s: Scheduled<A, E>,
): Effect.Effect<A, StepError<E>, ZartWorkflow> =>
	withWorkflow((dispatcher) =>
		liftNative(() => dispatcher.wait(s.handle)).pipe(
			Effect.flatMap(s.interpret),
		),
	);

export const waitAll = <A, E>(
	scheduled: ReadonlyArray<Scheduled<A, E>>,
): Effect.Effect<A[], StepError<E>, ZartWorkflow> =>
	withWorkflow((dispatcher) =>
		liftNative(() => dispatcher.waitAll(scheduled.map((s) => s.handle))).pipe(
			Effect.flatMap((outcomes) =>
				Effect.forEach(outcomes, (o, i) => {
					const item = scheduled[i];
					return item
						? item.interpret(o)
						: Effect.die("missing scheduled handle");
				}),
			),
		),
	);

/** Run a step and reify the result as a non-throwing `StepOutcome`. */
export const outcome = <A, E>(
	call: StepEffect<A, E>,
): Effect.Effect<StepOutcome<A>, never, ZartWorkflow> =>
	call.pipe(
		Effect.map((value): StepOutcome<A> => ({ _tag: "ok", value })),
		Effect.catch((e) => {
			if (e instanceof StepFailedError) {
				return Effect.succeed<StepOutcome<A>>({
					_tag: "business_err",
					errorType: e.errorType ?? "Error",
					message: e.message,
					data: e.data,
				});
			}
			if (
				e instanceof ZartError ||
				e instanceof ZartBackendError ||
				e instanceof ZartSchemaError ||
				isZartTagged(e)
			) {
				return Effect.succeed<StepOutcome<A>>({
					_tag: "zart_err",
					error: e as ZartTaggedError,
				});
			}
			return Effect.succeed<StepOutcome<A>>({
				_tag: "business_err",
				errorType:
					typeof (e as { _tag?: unknown })._tag === "string"
						? (e as { _tag: string })._tag
						: "Error",
				message:
					typeof (e as { message?: unknown }).message === "string"
						? (e as { message: string }).message
						: String(e),
				data: e,
			});
		}),
	);

const ZART_TAGS = new Set([
	"RetryExhaustedError",
	"StepTimeoutError",
	"DeadlineExceededError",
	"ZartBackendError",
	"ZartSchemaError",
	"ExecutionNotFound",
	"ExecutionAlreadyExists",
	"WaitTimedOut",
	"ZartError",
]);
const isZartTagged = (e: unknown): boolean =>
	typeof e === "object" &&
	e !== null &&
	ZART_TAGS.has((e as { _tag?: string })._tag ?? "");

/**
 * Test whether an error is a framework-level `ZartTaggedError` rather than a business failure.
 *
 * `StepFailedError` and user domain errors `E` return `false`.
 */
export const isFrameworkError = (e: unknown): boolean => {
	if (e instanceof StepFailedError) return false;
	if (
		e instanceof ZartError ||
		e instanceof ZartBackendError ||
		e instanceof ZartSchemaError ||
		isZartTagged(e)
	) {
		return true;
	}
	return false;
};

/** Run a step; on any typed failure yield `fallback`. Defects (replay signals) still propagate. */
export const stepOr = <A, E, B>(
	call: StepEffect<A, E>,
	fallback: B,
): Effect.Effect<A | B, never, ZartWorkflow> =>
	call.pipe(Effect.catch(() => Effect.succeed<A | B>(fallback)));

/**
 * Execute a step, computing a fallback on business error.
 *
 * Intercepts ONLY business errors (`E | StepFailedError`); NEVER swallows
 * framework `ZartTaggedError` (`RetryExhaustedError`, `StepTimeoutError`,
 * `DeadlineExceededError`, `ZartBackendError`, `ZartSchemaError`, etc.) or
 * `ControlFlowDefect`!
 */
export const stepOrElse = <A, E, B, E2 = never, R = never>(
	call: StepEffect<A, E>,
	fallback: (err: E | StepFailedError) => Effect.Effect<B, E2, R> | B,
): Effect.Effect<A | B, ZartTaggedError | E2, ZartWorkflow | R> =>
	Effect.catch(call, (err: StepError<E>) => {
		if (isFrameworkError(err)) {
			return Effect.fail<ZartTaggedError | E2>(err as ZartTaggedError);
		}
		const res = fallback(err as E | StepFailedError);
		return (Effect.isEffect(res) ? res : Effect.succeed(res)) as Effect.Effect<
			B,
			E2,
			R
		>;
	});

export const sleep = (
	name: string,
	duration: Duration.Input,
): Effect.Effect<void, ZartTaggedError, ZartWorkflow> =>
	withWorkflow((dispatcher) =>
		liftNative(() => dispatcher.sleep(name, Duration.toMillis(duration))),
	);

export const sleepUntil = (
	name: string,
	when: Date,
): Effect.Effect<void, ZartTaggedError, ZartWorkflow> =>
	withWorkflow((dispatcher) =>
		liftNative(() => dispatcher.sleepUntil(name, when.toISOString())),
	);

export const waitForEvent = <A = unknown, I = A>(
	name: string,
	opts?: {
		readonly schema?: Schema.Codec<A, I>;
		readonly timeout?: Duration.Input;
	},
): Effect.Effect<A, ZartTaggedError | ZartSchemaError, ZartWorkflow> =>
	withWorkflow((dispatcher) =>
		liftNative(() =>
			dispatcher.waitForEvent(
				name,
				opts?.timeout !== undefined
					? Duration.toMillis(opts.timeout)
					: undefined,
			),
		).pipe(
			Effect.flatMap((v) =>
				opts?.schema ? decodeOutput(opts.schema, v) : Effect.succeed(v as A),
			),
		),
	);

/** Memoize a non-deterministic value (recorded once, replayed afterwards). */
export const capture = <A, I = A>(
	name: string,
	fn: () => A | Promise<A>,
	schema?: Schema.Codec<A, I>,
): Effect.Effect<A, ZartTaggedError | ZartSchemaError, ZartWorkflow> =>
	withWorkflow((dispatcher) =>
		liftNative(() =>
			dispatcher.capture(name, async () => {
				const v = await fn();
				return schema
					? await Effect.runPromise(encodeInput(schema, v))
					: encodeJsonValue(v);
			}),
		).pipe(
			Effect.flatMap((v) =>
				schema ? decodeOutput(schema, v) : Effect.succeed(v as A),
			),
		),
	);

/** Deterministic current time (replay-safe). */
export const now = (
	name = "now",
): Effect.Effect<Date, ZartTaggedError, ZartWorkflow> =>
	withWorkflow((dispatcher) =>
		liftNative(() => dispatcher.now(name)).pipe(Effect.map((s) => new Date(s))),
	);

export type ExecutionInfo = NativeExecutionInfo;

/**
 * Read-only view of the current execution metadata (`ExecutionInfo`).
 *
 * Accessible inside both `Zart.durable` workflows and `Zart.step` bodies.
 * Reflects live retry metadata (`currentAttempt`, `isRetry`) during step retries.
 */
export const context: Effect.Effect<ExecutionInfo, never, ZartContext> =
	ZartContext;

export interface DurableOptions<A, B, I = A, O = B, E = never> {
	readonly inputSchema?: Schema.Codec<A, I>;
	readonly outputSchema?: Schema.Codec<B, O>;
	readonly timeout?: Duration.Input;
	readonly maxRetries?: number;
	/**
	 * Centralized failure handler invoked if step retries exhaust, execution timeout expires,
	 * or an unhandled step error escapes the workflow body.
	 *
	 * Receives the original workflow input and failure descriptor (`ExecutionFailure`),
	 * returning a fallback result encoded with `outputSchema`.
	 */
	readonly onFailure?: (
		input: A,
		failure: ExecutionFailure,
	) => Effect.Effect<B, E, never>;
}

export interface DurableDefinition<A, B, I = unknown, O = unknown> {
	readonly _tag: "DurableDefinition";
	/** Phantom: workflow output type. */
	readonly _output?: B;
	readonly name: string;
	readonly inputSchema?: Schema.Codec<A, I>;
	readonly outputSchema?: Schema.Codec<B, O>;
	readonly timeoutMs?: number;
	readonly maxRetries?: number;
	readonly handler: NativeDurableHandler;
	readonly onFailure?: (
		data: unknown,
		failure: NativeExecutionFailure,
	) => Promise<unknown>;
	/** Run directly against a dispatcher (used by tests and custom hosts). */
	readonly run: (dispatcher: NativeDispatcher, input: A) => Promise<unknown>;
}

const signalError = (kind: string, message?: string) =>
	new Error(
		JSON.stringify({
			_tag: "ControlFlowSignal",
			kind,
			...(message ? { message } : {}),
		}),
	);

/**
 * Define a durable workflow. Replay suspensions (`ControlFlowDefect`) are extracted at this
 * boundary and re-raised to the native layer as `ControlFlowSignal` errors.
 */
export const durable = <A, B, E, I = A, O = B, E2 = never>(
	name: string,
	fn: (input: A) => Effect.Effect<B, E, ZartWorkflow | ZartContext>,
	opts?: DurableOptions<A, B, I, O, E2>,
): DurableDefinition<A, B, I, O> => {
	const run = async (
		dispatcher: NativeDispatcher,
		input: A,
	): Promise<unknown> => {
		const program = fn(input).pipe(
			Effect.provideService(ZartWorkflow, { dispatcher }),
			Effect.provideService(ZartContext, dispatcher.context()),
		);
		const exit = await zartContextStorage.run(dispatcher.context(), () =>
			Effect.runPromiseExit(program),
		);
		if (Exit.isSuccess(exit)) {
			return opts?.outputSchema
				? Effect.runPromise(encodeInput(opts.outputSchema, exit.value))
				: encodeJsonValue(exit.value);
		}
		const cf = exit.cause.reasons
			.filter(Cause.isDieReason)
			.map((r) => r.defect)
			.find((d): d is ControlFlowDefect => d instanceof ControlFlowDefect);
		if (cf) throw signalError(cf.kind, cf.message);
		const failure = Cause.findErrorOption(exit.cause);
		if (Option.isSome(failure)) {
			const e = failure.value as { _tag?: string; message?: string };
			throw new Error(
				JSON.stringify({
					_tag:
						typeof e?._tag === "string" && ZART_TAGS.has(e._tag)
							? e._tag
							: "ZartError",
					message: typeof e?.message === "string" ? e.message : String(e),
				}),
			);
		}
		throw new Error(
			JSON.stringify({ _tag: "ZartError", message: Cause.pretty(exit.cause) }),
		);
	};
	const handler: NativeDurableHandler = async (dispatcher, data) => {
		const input = opts?.inputSchema
			? await Effect.runPromise(decodeOutput(opts.inputSchema, data))
			: (data as A);
		return run(dispatcher, input);
	};
	const onFailureFn = opts?.onFailure;
	const onFailure = onFailureFn
		? async (
				data: unknown,
				failure: NativeExecutionFailure,
			): Promise<unknown> => {
				const input = opts?.inputSchema
					? await Effect.runPromise(decodeOutput(opts.inputSchema, data))
					: (data as A);
				const execFailure: ExecutionFailure =
					failure.kind === "step_failed"
						? {
								kind: "step_failed",
								step: failure.step ?? "",
								raw: failure.raw,
							}
						: failure.kind === "retries_exhausted"
							? {
									kind: "retries_exhausted",
									attempts: failure.attempts ?? 0,
								}
							: { kind: "execution_deadline_exceeded" };
				const result = await Effect.runPromise(onFailureFn(input, execFailure));
				return opts?.outputSchema
					? await Effect.runPromise(encodeInput(opts.outputSchema, result))
					: encodeJsonValue(result);
			}
		: undefined;
	return {
		_tag: "DurableDefinition",
		name,
		...(opts?.inputSchema ? { inputSchema: opts.inputSchema } : {}),
		...(opts?.outputSchema ? { outputSchema: opts.outputSchema } : {}),
		...(opts?.timeout !== undefined
			? { timeoutMs: Duration.toMillis(opts.timeout) }
			: {}),
		...(opts?.maxRetries !== undefined ? { maxRetries: opts.maxRetries } : {}),
		handler,
		onFailure,
		run,
	};
};
