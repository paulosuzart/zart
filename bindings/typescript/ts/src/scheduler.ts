import { Duration, Effect, Schema } from "effect";
import { type ClientTrx, fromNative, type PgBackend } from "./backend.js";
import type { ZartSchemaError, ZartTaggedError } from "./errors.js";
import {
	loadNative,
	type NativeDurableScheduler,
	type NativeExecutionRecord,
} from "./native.js";
import { decodeOutput, encodeInput, encodeJsonValue } from "./schema.js";
import type { DurableDefinition } from "./workflow.js";

/** Options for {@link DurableScheduler.startInTx}. */
export type StartInTxOptions<A = unknown, I = A, B = unknown, O = B> =
	| {
			readonly executionId: string;
			readonly definition: DurableDefinition<A, B, I, O>;
			readonly input: A;
	  }
	| {
			readonly executionId: string;
			readonly taskName: string;
			readonly inputSchema: Schema.Codec<A, I>;
			readonly input: A;
	  }
	| {
			readonly executionId: string;
			readonly taskName: string;
			readonly payload?: unknown;
	  };

/** Result returned when scheduling a durable execution. */
export interface StartResult {
	readonly taskId: string;
	readonly executionTime: string;
}

const isDurableDefinition = (
	v: unknown,
): v is DurableDefinition<unknown, unknown, unknown, unknown> =>
	typeof v === "object" &&
	v !== null &&
	(v as { _tag?: unknown })._tag === "DurableDefinition";

const isSchemaCodec = (v: unknown): v is Schema.Codec<unknown, unknown> =>
	Schema.isSchema(v);

/**
 * Convert a timeout or poll-interval input to seconds for the native scheduler.
 *
 * A plain `number` is interpreted as seconds (preserving backward compatibility);
 * Effect `Duration` values and strings (e.g. `"30 seconds"`, `"500 millis"`) are
 * converted via `Duration.toSeconds`.
 */
const toSecs = (d: Duration.Input | undefined): number | undefined => {
	if (d === undefined) return undefined;
	if (typeof d === "number") return d;
	return Duration.toSeconds(d);
};

const encodeForDefinition = <A, B, I, O>(
	definition: DurableDefinition<A, B, I, O>,
	input: A,
): Effect.Effect<unknown, ZartSchemaError> =>
	definition.inputSchema
		? encodeInput(definition.inputSchema, input)
		: Effect.succeed(encodeJsonValue(input));

const decodeForDefinition = <A, B, I, O>(
	definition: DurableDefinition<A, B, I, O>,
	output: unknown,
): Effect.Effect<B, ZartSchemaError> =>
	definition.outputSchema
		? decodeOutput(definition.outputSchema, output)
		: Effect.succeed(output as B);

export class DurableScheduler {
	private constructor(readonly native: NativeDurableScheduler) {}

	static fromBackend(backend: PgBackend): Effect.Effect<DurableScheduler> {
		return Effect.sync(
			() =>
				new DurableScheduler(
					loadNative().DurableScheduler.fromBackend(backend.native),
				),
		);
	}

	/**
	 * Start a durable execution using a {@link DurableDefinition}, reusing its
	 * `name` and optional `inputSchema`.
	 */
	start<A, B, I, O>(
		executionId: string,
		definition: DurableDefinition<A, B, I, O>,
		input: A,
	): Effect.Effect<StartResult, ZartTaggedError | ZartSchemaError>;
	/** Start a durable execution with an explicit `inputSchema`. */
	start<A, I>(
		executionId: string,
		taskName: string,
		inputSchema: Schema.Codec<A, I>,
		input: A,
	): Effect.Effect<StartResult, ZartTaggedError | ZartSchemaError>;
	/** Start a durable execution with a raw JSON-serializable payload (no schema). */
	start(
		executionId: string,
		taskName: string,
		payload?: unknown,
	): Effect.Effect<StartResult, ZartTaggedError | ZartSchemaError>;
	start(
		executionId: string,
		taskOrDef: string | DurableDefinition<unknown, unknown, unknown, unknown>,
		schemaOrPayload?: unknown,
		maybeInput?: unknown,
	): Effect.Effect<StartResult, ZartTaggedError | ZartSchemaError> {
		let taskName: string;
		let encoded: Effect.Effect<unknown, ZartSchemaError>;

		if (isDurableDefinition(taskOrDef)) {
			taskName = taskOrDef.name;
			encoded = encodeForDefinition(taskOrDef, schemaOrPayload);
		} else if (isSchemaCodec(schemaOrPayload)) {
			taskName = taskOrDef;
			encoded = encodeInput(schemaOrPayload, maybeInput);
		} else {
			taskName = taskOrDef;
			encoded = Effect.succeed(encodeJsonValue(schemaOrPayload));
		}

		return encoded.pipe(
			Effect.flatMap((payload) =>
				fromNative(() => this.native.start(executionId, taskName, payload)),
			),
		);
	}

	/** Start a durable execution atomically inside an existing `ClientTrx` (`DurableScheduler::start_in_tx`). */
	startInTx<A = unknown, I = A, B = unknown, O = B>(
		tx: ClientTrx,
		opts: StartInTxOptions<A, I, B, O>,
	): Effect.Effect<StartResult, ZartTaggedError | ZartSchemaError> {
		let taskName: string;
		let encoded: Effect.Effect<unknown, ZartSchemaError>;

		if ("definition" in opts) {
			taskName = opts.definition.name;
			encoded = encodeForDefinition(opts.definition, opts.input);
		} else if ("inputSchema" in opts) {
			taskName = opts.taskName;
			encoded = encodeInput(opts.inputSchema, opts.input);
		} else {
			taskName = opts.taskName;
			encoded = Effect.succeed(encodeJsonValue(opts.payload));
		}

		return encoded.pipe(
			Effect.flatMap((payload) =>
				fromNative(() =>
					this.native.startInTx(tx.native, opts.executionId, taskName, payload),
				),
			),
		);
	}

	status(id: string): Effect.Effect<NativeExecutionRecord, ZartTaggedError> {
		return fromNative(() => this.native.status(id));
	}

	/**
	 * Wait for an execution record to reach a terminal state.
	 *
	 * Timeout and poll interval accept `Duration.Input` (e.g. `"30 seconds"`,
	 * `Duration.seconds(30)`) or plain numbers in seconds.
	 */
	wait(
		id: string,
		timeout?: Duration.Input,
		pollInterval?: Duration.Input,
	): Effect.Effect<NativeExecutionRecord, ZartTaggedError> {
		return fromNative(() =>
			this.native.wait(id, toSecs(timeout), toSecs(pollInterval)),
		);
	}

	/**
	 * Wait for an execution to complete and decode its output using a
	 * {@link DurableDefinition}'s `outputSchema`.
	 */
	waitCompletion<A, B, I, O>(
		id: string,
		definition: DurableDefinition<A, B, I, O>,
		timeout?: Duration.Input,
		pollInterval?: Duration.Input,
	): Effect.Effect<B, ZartTaggedError | ZartSchemaError>;
	/** Wait for an execution to complete and decode its output with `outputSchema`. */
	waitCompletion<A, I>(
		id: string,
		outputSchema: Schema.Codec<A, I>,
		timeout?: Duration.Input,
		pollInterval?: Duration.Input,
	): Effect.Effect<A, ZartTaggedError | ZartSchemaError>;
	/** Wait for an execution to complete and return its raw JSON output. */
	waitCompletion(
		id: string,
		timeout?: Duration.Input,
		pollInterval?: Duration.Input,
	): Effect.Effect<unknown, ZartTaggedError | ZartSchemaError>;
	waitCompletion(
		id: string,
		defOrSchemaOrTimeout?:
			| DurableDefinition<unknown, unknown, unknown, unknown>
			| Schema.Codec<unknown, unknown>
			| Duration.Input,
		timeoutOrPoll?: Duration.Input,
		maybePoll?: Duration.Input,
	): Effect.Effect<unknown, ZartTaggedError | ZartSchemaError> {
		const hasDefOrSchema =
			isDurableDefinition(defOrSchemaOrTimeout) ||
			isSchemaCodec(defOrSchemaOrTimeout);
		const timeout = hasDefOrSchema ? timeoutOrPoll : defOrSchemaOrTimeout;
		const poll = hasDefOrSchema ? maybePoll : timeoutOrPoll;

		return fromNative(() =>
			this.native.waitCompletion(
				id,
				toSecs(timeout as Duration.Input | undefined),
				toSecs(poll),
			),
		).pipe(
			Effect.flatMap((out) => {
				if (isDurableDefinition(defOrSchemaOrTimeout)) {
					return decodeForDefinition(defOrSchemaOrTimeout, out);
				}
				if (isSchemaCodec(defOrSchemaOrTimeout)) {
					return decodeOutput(defOrSchemaOrTimeout, out);
				}
				return Effect.succeed(out);
			}),
		);
	}

	/**
	 * Start a durable execution and wait for its completion result using a
	 * {@link DurableDefinition}.
	 */
	startAndWait<A, B, I, O>(
		executionId: string,
		definition: DurableDefinition<A, B, I, O>,
		input: A,
		timeout?: Duration.Input,
		pollInterval?: Duration.Input,
	): Effect.Effect<B, ZartTaggedError | ZartSchemaError>;
	/**
	 * Start a durable execution and wait for its completion result with explicit
	 * `inputSchema` and `outputSchema`.
	 */
	startAndWait<A, I, B, O>(
		executionId: string,
		taskName: string,
		inputSchema: Schema.Codec<A, I>,
		input: A,
		outputSchema: Schema.Codec<B, O>,
		timeout?: Duration.Input,
		pollInterval?: Duration.Input,
	): Effect.Effect<B, ZartTaggedError | ZartSchemaError>;
	/**
	 * Start a durable execution with a raw JSON payload and wait for its raw
	 * completion result.
	 */
	startAndWait(
		executionId: string,
		taskName: string,
		payload?: unknown,
		timeout?: Duration.Input,
		pollInterval?: Duration.Input,
	): Effect.Effect<unknown, ZartTaggedError | ZartSchemaError>;
	startAndWait(
		executionId: string,
		taskOrDef: string | DurableDefinition<unknown, unknown, unknown, unknown>,
		schemaOrPayload?: unknown,
		inputOrTimeout?: unknown,
		outputSchemaOrPoll?: unknown,
		maybeTimeout?: Duration.Input,
		maybePoll?: Duration.Input,
	): Effect.Effect<unknown, ZartTaggedError | ZartSchemaError> {
		if (isDurableDefinition(taskOrDef)) {
			return this.start(executionId, taskOrDef, schemaOrPayload).pipe(
				Effect.flatMap(() =>
					this.waitCompletion(
						executionId,
						taskOrDef,
						inputOrTimeout as Duration.Input | undefined,
						outputSchemaOrPoll as Duration.Input | undefined,
					),
				),
			);
		}
		if (isSchemaCodec(schemaOrPayload) && isSchemaCodec(outputSchemaOrPoll)) {
			return this.start(
				executionId,
				taskOrDef,
				schemaOrPayload,
				inputOrTimeout,
			).pipe(
				Effect.flatMap(() =>
					this.waitCompletion(
						executionId,
						outputSchemaOrPoll,
						maybeTimeout,
						maybePoll,
					),
				),
			);
		}
		return this.start(executionId, taskOrDef, schemaOrPayload).pipe(
			Effect.flatMap(() =>
				this.waitCompletion(
					executionId,
					inputOrTimeout as Duration.Input | undefined,
					outputSchemaOrPoll as Duration.Input | undefined,
				),
			),
		);
	}

	cancel(id: string): Effect.Effect<boolean, ZartTaggedError> {
		return fromNative(() => this.native.cancel(id));
	}

	/** Offer a named event with an explicit `Schema.Codec`. */
	offerEvent<A, I>(
		id: string,
		eventName: string,
		schema: Schema.Codec<A, I>,
		payload: A,
	): Effect.Effect<void, ZartTaggedError | ZartSchemaError>;
	/** Offer a named event with a raw JSON-serializable payload (no schema). */
	offerEvent(
		id: string,
		eventName: string,
		payload?: unknown,
	): Effect.Effect<void, ZartTaggedError | ZartSchemaError>;
	offerEvent(
		id: string,
		eventName: string,
		schemaOrPayload?: unknown,
		maybePayload?: unknown,
	): Effect.Effect<void, ZartTaggedError | ZartSchemaError> {
		const encoded = isSchemaCodec(schemaOrPayload)
			? encodeInput(schemaOrPayload, maybePayload)
			: Effect.succeed(encodeJsonValue(schemaOrPayload));

		return encoded.pipe(
			Effect.flatMap((p) =>
				fromNative(() => this.native.offerEvent(id, eventName, p)),
			),
		);
	}

	/**
	 * Stop a recurring durable execution from scheduling future ticks (idempotent).
	 *
	 * Accepts either a `taskId` string or a {@link DurableDefinition} (using `definition.name`).
	 */
	stopRecurring<A = unknown, B = unknown, I = unknown, O = unknown>(
		taskOrDef: string | DurableDefinition<A, B, I, O>,
	): Effect.Effect<void, ZartTaggedError> {
		const taskId = typeof taskOrDef === "string" ? taskOrDef : taskOrDef.name;
		return fromNative(() => this.native.stopRecurring(taskId));
	}
}
