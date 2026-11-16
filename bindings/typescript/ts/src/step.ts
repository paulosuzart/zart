import { AsyncLocalStorage } from "node:async_hooks";
import { Cause, Duration, Effect, Exit, Layer, Option, Schema } from "effect";
import type { SqlClient } from "effect/sql";
import { liftNative, withWorkflow, type ZartWorkflow } from "./core.js";
import {
	type NativeStepOutcome,
	StepFailedError,
	toStepOutcome,
	type ZartTaggedError,
} from "./errors.js";
import type { NativeStepRun, NativeStepSpec, NativeTrx } from "./native.js";
import { decodeOutput, encodeJsonValue } from "./schema.js";
import { ZartSqlClientLive } from "./sql-client.js";
import {
	runWithTrxProvider,
	TrxHandle,
	type TrxProvider,
	TrxSessionService,
	ZartTrx,
} from "./trx.js";

/** Ambient `TrxProvider` set by `WorkerBuilder.withTrxProvider(...)`. */
export const trxProviderStorage = new AsyncLocalStorage<
	TrxProvider | undefined
>();

/** Services permitted inside a `Zart.step` body (`ZartWorkflow` is intentionally excluded). */
export type StepAllowedServices =
	| ZartTrx
	| SqlClient.SqlClient
	| TrxSessionService;

export interface RetryPolicy {
	readonly kind: "none" | "fixed" | "linear" | "exponential";
	readonly maxAttempts: number;
	readonly delayMs: number;
	readonly factor?: number;
	readonly maxDelayMs?: number;
}

export const Retry = {
	fixed: (maxAttempts: number, delay: Duration.Input): RetryPolicy => ({
		kind: "fixed",
		maxAttempts,
		delayMs: Duration.toMillis(delay),
	}),
	linear: (maxAttempts: number, delay: Duration.Input): RetryPolicy => ({
		kind: "linear",
		maxAttempts,
		delayMs: Duration.toMillis(delay),
	}),
	exponential: (
		maxAttempts: number,
		base: Duration.Input,
		opts?: { factor?: number; max?: Duration.Input },
	): RetryPolicy => ({
		kind: "exponential",
		maxAttempts,
		delayMs: Duration.toMillis(base),
		factor: opts?.factor ?? 2,
		...(opts?.max !== undefined
			? { maxDelayMs: Duration.toMillis(opts.max) }
			: {}),
	}),
	none: (): RetryPolicy => ({ kind: "none", maxAttempts: 1, delayMs: 0 }),
};

export interface StepOptions<A, E> {
	readonly retry?: RetryPolicy;
	readonly timeout?: Duration.Input;
	readonly timeoutScope?: "attempt" | "total" | "per_attempt" | "global";
	readonly outputSchema?: Schema.Codec<A, unknown>;
	/** If provided, business errors are decoded back to `E` (also across replays). */
	readonly errorSchema?: Schema.Codec<E, unknown>;
	/** Set `false` to opt out of Zart-Managed `ZartTrx` acquisition on this step. */
	readonly trx?: boolean;
}

export const StepMeta: unique symbol = Symbol.for("@zart/effect/StepMeta");

export interface StepMetaData<A, E> {
	readonly spec: NativeStepSpec;
	readonly run: NativeStepRun;
	readonly interpret: (
		o: NativeStepOutcome,
	) => Effect.Effect<A, E | StepFailedError | ZartTaggedError>;
}

export type StepError<E> = E | StepFailedError | ZartTaggedError;

/**
 * A yieldable step invocation. Requires `ZartWorkflow` only to be *executed*;
 * the user function itself may only use `StepAllowedServices` (never `ZartWorkflow`).
 */
export interface StepEffect<A, E>
	extends Effect.Effect<A, StepError<E>, ZartWorkflow> {
	/** Use an explicit durable step id (e.g. for loops). */
	readonly named: (id: string) => StepEffect<A, E>;
	readonly [StepMeta]: StepMetaData<A, E>;
}

export type StepFn<Args extends ReadonlyArray<unknown>, A, E> = (
	...args: Args
) => StepEffect<A, E>;

const taggedName = (e: unknown): string =>
	typeof e === "object" &&
	e !== null &&
	typeof (e as { _tag?: unknown })._tag === "string"
		? (e as { _tag: string })._tag
		: "Error";

const messageOfError = (e: unknown): string =>
	typeof e === "object" &&
	e !== null &&
	typeof (e as { message?: unknown }).message === "string"
		? (e as { message: string }).message
		: String(e);

const provideNativeTrx = <A, E>(
	eff: Effect.Effect<A, E, StepAllowedServices>,
	trx: NativeTrx | undefined,
): Effect.Effect<A, E, never> => {
	const provider = trxProviderStorage.getStore();
	if (provider) {
		return runWithTrxProvider(
			provider,
			(session) =>
				eff.pipe(
					Effect.provideService(TrxSessionService, session),
				) as Effect.Effect<A, E, never>,
		) as Effect.Effect<A, E, never>;
	}
	if (!trx) {
		return eff as Effect.Effect<A, E, never>;
	}
	const layer = Layer.merge(
		Layer.succeed(ZartTrx, new TrxHandle(trx)),
		ZartSqlClientLive(trx),
	);
	return eff.pipe(Effect.provide(layer)) as Effect.Effect<A, E, never>;
};

const build = <A, E>(
	spec: NativeStepSpec,
	run: NativeStepRun,
	opts: StepOptions<A, E> | undefined,
): StepEffect<A, E> => {
	const interpret = (n: NativeStepOutcome): Effect.Effect<A, StepError<E>> => {
		const o = toStepOutcome(n);
		switch (o._tag) {
			case "ok":
				return opts?.outputSchema
					? decodeOutput(opts.outputSchema, o.value)
					: Effect.succeed(o.value as A);
			case "business_err": {
				const fallback = new StepFailedError({
					stepName: spec.name,
					errorType: o.errorType,
					message: o.message,
					data: o.data,
				});
				const es = opts?.errorSchema;
				return es
					? Schema.decodeUnknownEffect(es)(o.data).pipe(
							Effect.matchEffect({
								onFailure: () => Effect.fail<StepError<E>>(fallback),
								onSuccess: (e) => Effect.fail<StepError<E>>(e),
							}),
						)
					: Effect.fail(fallback);
			}
			case "zart_err":
				return Effect.fail(o.error);
		}
	};

	const effect = withWorkflow((dispatcher) =>
		liftNative(() => dispatcher.step(spec, run)).pipe(
			Effect.flatMap(interpret),
		),
	);
	return Object.assign(effect, {
		named: (id: string) => build<A, E>({ ...spec, name: id }, run, opts),
		[StepMeta]: { spec, run, interpret } as StepMetaData<A, E>,
	}) as unknown as StepEffect<A, E>;
};

/**
 * Define a durable step. `fn` may use `ZartTrx`, `SqlClient`, or `TrxSessionService`,
 * but CANNOT use `ZartWorkflow` primitives (`Zart.sleep`, `Zart.step`, ...) — enforced by `tsc`.
 */
export const step = <
	Args extends ReadonlyArray<unknown>,
	A,
	E = never,
	R extends StepAllowedServices = never,
>(
	name: string,
	fn: (...args: Args) => Effect.Effect<A, E, R>,
	opts?: StepOptions<A, E>,
): StepFn<Args, A, E> => {
	const baseSpec: NativeStepSpec = {
		name,
		...(opts?.retry ? { retry: opts.retry } : {}),
		...(opts?.timeout !== undefined
			? { timeoutMs: Duration.toMillis(opts.timeout) }
			: {}),
		...(opts?.timeoutScope ? { timeoutScope: opts.timeoutScope } : {}),
	};
	return (...args: Args) => {
		const spec: NativeStepSpec = {
			...baseSpec,
			needsTrx: opts?.trx !== false && !trxProviderStorage.getStore(),
		};
		const run: NativeStepRun = async (
			trx?: NativeTrx,
		): Promise<NativeStepOutcome> => {
			const provided = provideNativeTrx(fn(...args), trx);
			const exit = await Effect.runPromiseExit(provided);
			if (Exit.isSuccess(exit)) {
				const v = opts?.outputSchema
					? await Effect.runPromise(
							Schema.encodeEffect(opts.outputSchema)(exit.value),
						)
					: exit.value;
				return { kind: "ok", value: encodeJsonValue(v) };
			}
			const failure = Cause.findErrorOption(exit.cause);
			if (Option.isSome(failure)) {
				const e = failure.value;
				const data = opts?.errorSchema
					? await Effect.runPromise(
							Schema.encodeEffect(opts.errorSchema)(e).pipe(
								Effect.orElseSucceed(() => e as unknown),
							),
						)
					: e;
				return {
					kind: "business_err",
					errorType: taggedName(e),
					message: messageOfError(e),
					data: encodeJsonValue(data),
				};
			}
			return {
				kind: "business_err",
				errorType: "Defect",
				message: Cause.pretty(exit.cause),
			};
		};
		return build<A, E>(spec, run, opts);
	};
};
