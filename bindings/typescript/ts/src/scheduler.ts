import { Duration, Effect, Schema } from "effect";
import { type ClientTrx, fromNative, type PgBackend } from "./backend.js";
import { ZartSchemaError, type ZartTaggedError } from "./errors.js";
import {
	loadNative,
	type NativeDurableScheduler,
	type NativeExecutionDetail,
	type NativeExecutionRecord,
	type NativeExecutionRunRecord,
	type NativeExecutionSortField,
	type NativeExecutionStats,
	type NativeExecutionStatus,
	type NativeExecutionTrigger,
	type NativeListExecutionsParams,
	type NativePauseRule,
	type NativePauseRuleFilter,
	type NativePauseScope,
	type NativePotentiallyStaleDep,
	type NativeRerunResult,
	type NativeRerunSpec,
	type NativeResumeResult,
	type NativeSortOrder,
	type NativeStepAttemptRow,
	type NativeStepAttemptStatus,
	type NativeStepKind,
	type NativeStepRow,
	type NativeStepStatus,
	type NativeStepWithAttempts,
} from "./native.js";
import { decodeOutput, encodeInput, encodeJsonValue } from "./schema.js";
import type { DurableDefinition } from "./workflow.js";

/** Lifecycle status of a durable execution. */
export type ExecutionStatus = NativeExecutionStatus;
/** What triggered a run of a durable execution. */
export type ExecutionTrigger = NativeExecutionTrigger;
/** Field to sort execution listings by. */
export type ExecutionSortField = NativeExecutionSortField;
/** Sort direction for execution listings. */
export type SortOrder = NativeSortOrder;
/** Kind of step stored in `zart_steps`. */
export type StepKind = NativeStepKind;
/** Lifecycle status of a step row. */
export type StepStatus = NativeStepStatus;
/** Lifecycle status of a step attempt. */
export type StepAttemptStatus = NativeStepAttemptStatus;
/** A durable execution record. */
export type ExecutionRecord = NativeExecutionRecord;
/** Execution count statistics grouped by status. */
export type ExecutionStats = NativeExecutionStats;
/** A single run of a durable execution. */
export type ExecutionRunRecord = NativeExecutionRunRecord;
/** A step record from `zart_steps`. */
export type StepRow = NativeStepRow;
/** A single attempt record from `zart_step_attempts`. */
export type StepAttemptRow = NativeStepAttemptRow;
/** A step with its attempts and retryability flag. */
export type StepWithAttempts = NativeStepWithAttempts;
/** Full execution detail: record, runs, and steps with attempt history. */
export type ExecutionDetail = NativeExecutionDetail;
/** Specification for which steps to rerun vs preserve in `rerunSteps`. */
export type RerunSpec = NativeRerunSpec;
/** A preserved step that may have a stale dependency on a rerun step. */
export type PotentiallyStaleDep = NativePotentiallyStaleDep;
/** Result of a selective rerun operation. */
export type RerunResult = NativeRerunResult;
/** A pause rule stored in the database. */
export type PauseRule = NativePauseRule;
/** Result of a resume operation. */
export type ResumeResult = NativeResumeResult;
/** Filter for listing pause rules. */
export type PauseRuleFilter = NativePauseRuleFilter;

/** Scope of a pause or resume operation. */
export interface PauseScope {
	/** Target a specific execution. */
	readonly executionId?: string;
	/** Target all executions of a given task name. */
	readonly taskName?: string;
	/** Glob pattern for step names (e.g. `"send-*"`). */
	readonly stepPattern?: string;
	/** Optional auto-expiry for the pause rule (`Date` or RFC3339 string). */
	readonly expiresAt?: Date | string;
	/** Optional operator identifier for audit logging. */
	readonly triggeredBy?: string;
	/** Optional human-readable reason for this pause. */
	readonly reason?: string;
}

const toNativePauseScope = (
	scope: PauseScope,
): Effect.Effect<NativePauseScope, ZartSchemaError> =>
	toIsoOpt(scope.expiresAt, "expiresAt").pipe(
		Effect.map((expiresAt) => ({
			executionId: scope.executionId,
			taskName: scope.taskName,
			stepPattern: scope.stepPattern,
			expiresAt,
			triggeredBy: scope.triggeredBy,
			reason: scope.reason,
		})),
	);

/** Parameters for filtered, paginated execution listing (`DurableScheduler.listExecutions`). */
export interface ListExecutionsParams {
	/** Filter by execution status. */
	readonly status?: ExecutionStatus;
	/** Case-insensitive substring filter on `taskName`. */
	readonly taskName?: string;
	/** Lower bound on `startedAt` (`Date` or RFC3339 string). */
	readonly from?: Date | string;
	/** Upper bound on `startedAt` (`Date` or RFC3339 string). */
	readonly to?: Date | string;
	/** Prefix filter on `executionId`. */
	readonly search?: string;
	/** Sort field (default: `"scheduled_at"`). */
	readonly sortBy?: ExecutionSortField;
	/** Sort direction (default: `"desc"`). */
	readonly sortOrder?: SortOrder;
	/** Maximum number of records to return (default: `20`). */
	readonly limit?: number;
	/** Number of records to skip (default: `0`). */
	readonly offset?: number;
}

const toIsoOpt = (
	value: Date | string | undefined,
	field: string,
): Effect.Effect<string | undefined, ZartSchemaError> => {
	if (value === undefined) return Effect.succeed(undefined);
	if (typeof value === "string") return Effect.succeed(value);
	if (Number.isNaN(value.getTime())) {
		return Effect.fail(
			new ZartSchemaError({ message: `Invalid Date for '${field}'` }),
		);
	}
	return Effect.succeed(value.toISOString());
};

const toNativeListParams = (
	params: ListExecutionsParams | undefined,
): Effect.Effect<NativeListExecutionsParams | undefined, ZartSchemaError> => {
	if (params === undefined) return Effect.succeed(undefined);
	return Effect.all({
		from: toIsoOpt(params.from, "from"),
		to: toIsoOpt(params.to, "to"),
	}).pipe(
		Effect.map(({ from, to }) => ({
			status: params.status,
			taskName: params.taskName,
			from,
			to,
			search: params.search,
			sortBy: params.sortBy,
			sortOrder: params.sortOrder,
			limit: params.limit,
			offset: params.offset,
		})),
	);
};

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

	/** List durable execution records with optional filters, sorting, and pagination. */
	listExecutions(
		params?: ListExecutionsParams,
	): Effect.Effect<ReadonlyArray<ExecutionRecord>, ZartTaggedError> {
		return toNativeListParams(params).pipe(
			Effect.flatMap((nativeParams) =>
				fromNative(() => this.native.listExecutions(nativeParams)),
			),
		);
	}

	/** Return aggregate execution counts grouped by status. */
	stats(): Effect.Effect<ExecutionStats, ZartTaggedError> {
		return fromNative(() => this.native.stats());
	}

	/** Return the current `runId` for a durable execution, or `null` if never started. */
	getCurrentRunId(
		executionId: string,
	): Effect.Effect<string | null, ZartTaggedError> {
		return fromNative(() => this.native.getCurrentRunId(executionId));
	}

	/** List all runs for a durable execution. */
	listRuns(
		executionId: string,
	): Effect.Effect<ReadonlyArray<ExecutionRunRecord>, ZartTaggedError> {
		return fromNative(() => this.native.listRuns(executionId));
	}

	/**
	 * Return full execution detail: the execution record, all runs, and the steps
	 * (with attempt history) for the specified run (or the current run if omitted).
	 */
	executionDetail(
		executionId: string,
		runId?: string,
	): Effect.Effect<ExecutionDetail, ZartTaggedError> {
		return fromNative(() => this.native.executionDetail(executionId, runId));
	}

	/** Retry a single dead step within the specified run, returning the new `taskId`. */
	retryStep(
		runId: string,
		stepName: string,
		triggeredBy?: string,
	): Effect.Effect<string, ZartTaggedError> {
		return fromNative(() =>
			this.native.retryStep(runId, stepName, triggeredBy),
		);
	}

	/** Retry a single dead step within the current run of an execution, returning the new `taskId`. */
	retryStepCurrentRun(
		executionId: string,
		stepName: string,
		triggeredBy?: string,
	): Effect.Effect<string, ZartTaggedError> {
		return fromNative(() =>
			this.native.retryStepCurrentRun(executionId, stepName, triggeredBy),
		);
	}

	/** Restart an execution from scratch with a typed `Schema.Codec` payload, returning the new `runId`. */
	restart<A, I>(
		executionId: string,
		inputSchema: Schema.Codec<A, I>,
		input: A,
		triggeredBy?: string,
	): Effect.Effect<string, ZartTaggedError | ZartSchemaError>;
	/** Restart an execution from scratch (optionally replacing the raw payload), returning the new `runId`. */
	restart(
		executionId: string,
		newPayload?: unknown,
		triggeredBy?: string,
	): Effect.Effect<string, ZartTaggedError | ZartSchemaError>;
	restart(
		executionId: string,
		schemaOrPayload?: unknown,
		inputOrTriggeredBy?: unknown,
		maybeTriggeredBy?: string,
	): Effect.Effect<string, ZartTaggedError | ZartSchemaError> {
		let encoded: Effect.Effect<unknown, ZartSchemaError>;
		let triggeredBy: string | undefined;

		if (isSchemaCodec(schemaOrPayload)) {
			encoded = encodeInput(schemaOrPayload, inputOrTriggeredBy);
			triggeredBy = maybeTriggeredBy;
		} else {
			encoded =
				schemaOrPayload === undefined
					? Effect.succeed(undefined)
					: Effect.succeed(encodeJsonValue(schemaOrPayload));
			triggeredBy =
				typeof inputOrTriggeredBy === "string" ? inputOrTriggeredBy : undefined;
		}

		return encoded.pipe(
			Effect.flatMap((payload) =>
				fromNative(() =>
					this.native.restart(executionId, payload, triggeredBy),
				),
			),
		);
	}

	/** Selectively rerun a subset of steps while preserving others. */
	rerunSteps(
		executionId: string,
		spec: RerunSpec,
	): Effect.Effect<RerunResult, ZartTaggedError> {
		return fromNative(() => this.native.rerunSteps(executionId, spec));
	}

	/** Create a pause rule. */
	pause(scope: PauseScope): Effect.Effect<PauseRule, ZartTaggedError> {
		return toNativePauseScope(scope).pipe(
			Effect.flatMap((nativeScope) =>
				fromNative(() => this.native.pause(nativeScope)),
			),
		);
	}

	/** Resume execution by soft-deleting matching pause rules. */
	resume(
		scope: PauseScope,
		deletedBy?: string,
	): Effect.Effect<ResumeResult, ZartTaggedError> {
		return toNativePauseScope(scope).pipe(
			Effect.flatMap((nativeScope) =>
				fromNative(() => this.native.resume(nativeScope, deletedBy)),
			),
		);
	}

	/** Resume by soft-deleting a specific pause rule by ID. */
	resumeRuleById(
		ruleId: string,
		deletedBy?: string,
	): Effect.Effect<ResumeResult, ZartTaggedError> {
		return fromNative(() => this.native.resumeRuleById(ruleId, deletedBy));
	}

	/** List pause rules (active only by default, or including soft-deleted when `includeDeleted` is true). */
	listPauseRules(
		filter?: PauseRuleFilter,
	): Effect.Effect<ReadonlyArray<PauseRule>, ZartTaggedError> {
		return fromNative(() => this.native.listPauseRules(filter));
	}
}
