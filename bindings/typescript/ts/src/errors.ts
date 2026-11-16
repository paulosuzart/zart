import { Data } from "effect";

export class RetryExhaustedError extends Data.TaggedError(
	"RetryExhaustedError",
)<{
	readonly stepName: string;
	readonly attempts: number;
	readonly lastError?: string;
}> {}

export class StepTimeoutError extends Data.TaggedError("StepTimeoutError")<{
	readonly stepName: string;
	readonly timeoutMs?: number;
}> {}

export class DeadlineExceededError extends Data.TaggedError(
	"DeadlineExceededError",
)<{
	readonly message?: string;
}> {}

export class StepFailedError extends Data.TaggedError("StepFailedError")<{
	readonly stepName: string;
	readonly errorType?: string;
	readonly message: string;
	readonly data?: unknown;
}> {}

export class ZartBackendError extends Data.TaggedError("ZartBackendError")<{
	readonly message: string;
}> {}

export class ZartSchemaError extends Data.TaggedError("ZartSchemaError")<{
	readonly message: string;
}> {}

export class ExecutionNotFound extends Data.TaggedError("ExecutionNotFound")<{
	readonly message: string;
}> {}

export class ExecutionAlreadyExists extends Data.TaggedError(
	"ExecutionAlreadyExists",
)<{
	readonly message: string;
}> {}

export class WaitTimedOut extends Data.TaggedError("WaitTimedOut")<{
	readonly message: string;
}> {}

/** Generic fallback for native `ZartError` payloads. */
export class ZartError extends Data.TaggedError("ZartError")<{
	readonly message: string;
}> {}

/** Control-flow signals that must never be handled as business errors. */
export class ControlFlowDefect extends Data.TaggedError("ControlFlowDefect")<{
	readonly kind: string;
	readonly message?: string;
}> {}

export type ZartTaggedError =
	| RetryExhaustedError
	| StepTimeoutError
	| DeadlineExceededError
	| StepFailedError
	| ZartBackendError
	| ZartSchemaError
	| ExecutionNotFound
	| ExecutionAlreadyExists
	| WaitTimedOut
	| ZartError;

/** Native step outcome (matches Rust `StepOutcome`). */
export type NativeStepOutcome = {
	readonly kind: "ok" | "business_err" | "zart_err";
	readonly value?: unknown;
	readonly errorType?: string;
	readonly message?: string;
	readonly data?: unknown;
};

export type StepOutcome<A = unknown> =
	| { readonly _tag: "ok"; readonly value: A }
	| {
			readonly _tag: "business_err";
			readonly errorType: string;
			readonly message: string;
			readonly data?: unknown;
	  }
	| { readonly _tag: "zart_err"; readonly error: ZartTaggedError };

const messageOf = (p: Record<string, unknown>, fallback: string): string =>
	typeof p.message === "string" ? p.message : fallback;

/** Map a decoded `{_tag, message, ...}` payload to a typed error. */
export function fromPayload(payload: unknown): ZartTaggedError {
	const p = (
		typeof payload === "object" && payload !== null ? payload : {}
	) as Record<string, unknown>;
	const message = messageOf(p, "unknown error");
	switch (p._tag) {
		case "RetryExhaustedError":
			return new RetryExhaustedError({
				stepName: String(p.stepName ?? ""),
				attempts: Number(p.attempts ?? 0),
				lastError: typeof p.lastError === "string" ? p.lastError : undefined,
			});
		case "StepTimeoutError":
			return new StepTimeoutError({
				stepName: String(p.stepName ?? ""),
				timeoutMs: typeof p.timeoutMs === "number" ? p.timeoutMs : undefined,
			});
		case "DeadlineExceededError":
			return new DeadlineExceededError({ message });
		case "StepFailedError":
			return new StepFailedError({
				stepName: String(p.stepName ?? ""),
				errorType: typeof p.errorType === "string" ? p.errorType : undefined,
				message,
				data: p.data,
			});
		case "ZartBackendError":
			return new ZartBackendError({ message });
		case "ZartSchemaError":
			return new ZartSchemaError({ message });
		case "ExecutionNotFound":
			return new ExecutionNotFound({ message });
		case "ExecutionAlreadyExists":
			return new ExecutionAlreadyExists({ message });
		case "WaitTimedOut":
			return new WaitTimedOut({ message });
		default:
			return new ZartError({ message });
	}
}

/** If the native error is a replay-suspension signal, return it as a defect. */
export function asControlFlow(err: unknown): ControlFlowDefect | undefined {
	const raw =
		err instanceof Error
			? err.message
			: typeof err === "string"
				? err
				: undefined;
	if (raw === undefined) return undefined;
	try {
		const p = JSON.parse(raw) as {
			_tag?: unknown;
			kind?: unknown;
			message?: unknown;
		};
		if (p && p._tag === "ControlFlowSignal") {
			return new ControlFlowDefect({
				kind: typeof p.kind === "string" ? p.kind : "unknown",
				message: typeof p.message === "string" ? p.message : undefined,
			});
		}
	} catch {
		/* not JSON */
	}
	return undefined;
}

/** Map a rejected native error (whose `.message` is JSON) or any thrown value. */
export function mapNativeError(err: unknown): ZartTaggedError {
	const raw =
		err instanceof Error
			? err.message
			: typeof err === "string"
				? err
				: undefined;
	if (raw !== undefined) {
		try {
			return fromPayload(JSON.parse(raw));
		} catch {
			return new ZartError({ message: raw });
		}
	}
	if (typeof err === "object" && err !== null) return fromPayload(err);
	return new ZartError({ message: String(err) });
}

/** Convert a native step outcome into the TS discriminated union. */
export function toStepOutcome<A = unknown>(
	n: NativeStepOutcome,
): StepOutcome<A> {
	switch (n.kind) {
		case "ok":
			return { _tag: "ok", value: n.value as A };
		case "business_err":
			return {
				_tag: "business_err",
				errorType: n.errorType ?? "Error",
				message: n.message ?? "",
				data: n.data,
			};
		case "zart_err":
			return {
				_tag: "zart_err",
				error: fromPayload(n.data ?? { _tag: n.errorType, message: n.message }),
			};
	}
}
