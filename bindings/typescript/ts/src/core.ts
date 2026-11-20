import { Context, Effect } from "effect";
import {
	asControlFlow,
	mapNativeError,
	type ZartTaggedError,
} from "./errors.js";
import type { NativeDispatcher, NativeExecutionInfo } from "./native.js";

/** Service available only inside `Zart.durable` bodies (never inside `Zart.step` functions). */
export class ZartWorkflow extends Context.Service<
	ZartWorkflow,
	{ readonly dispatcher: NativeDispatcher }
>()("@zart/effect/ZartWorkflow") {}

/** Service available inside both `Zart.durable` bodies and `Zart.step` functions. */
export class ZartContext extends Context.Service<
	ZartContext,
	NativeExecutionInfo
>()("@zart/effect/ZartContext") {}

/** Run an Effect using the `ZartWorkflow` dispatcher from context. */
export const withWorkflow = <A, E>(
	f: (dispatcher: NativeDispatcher) => Effect.Effect<A, E>,
): Effect.Effect<A, E, ZartWorkflow> =>
	Effect.gen(function* () {
		const { dispatcher } = yield* ZartWorkflow;
		return yield* f(dispatcher);
	});

/**
 * Lift a native call into Effect. Replay suspension signals become defects
 * (`ControlFlowDefect`) so `Effect.catch` cannot swallow them.
 */
export const liftNative = <A>(
	f: () => Promise<A>,
): Effect.Effect<A, ZartTaggedError> =>
	Effect.tryPromise({ try: f, catch: (e) => e }).pipe(
		Effect.catch((e) => {
			const cf = asControlFlow(e);
			return cf ? Effect.die(cf) : Effect.fail(mapNativeError(e));
		}),
	);
