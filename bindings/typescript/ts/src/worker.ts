import { Duration, Effect } from "effect";
import { fromNative, type PgBackend } from "./backend.js";
import { mapNativeError, type ZartTaggedError } from "./errors.js";
import {
	loadNative,
	type NativeRecurrence,
	type NativeWorker,
	type NativeWorkerBuilder,
} from "./native.js";
import type { OverlapPolicy, Recurrence } from "./recurring.js";
import { encodeInput, encodeJsonValue } from "./schema.js";
import { trxProviderStorage } from "./step.js";
import type { TrxProvider } from "./trx.js";
import type { DurableDefinition } from "./workflow.js";

/** Configuration options for {@link WorkerBuilder.configure}. */
export interface WorkerOptions {
	/** Fallback polling interval in seconds. Overridden by `pollInterval` if both are set. */
	readonly pollIntervalSecs?: number;
	/** Fallback polling interval as an Effect `Duration.Input` (takes precedence over `pollIntervalSecs`). */
	readonly pollInterval?: Duration.Input;
	/** Maximum number of tasks executed concurrently (must be >= 1; otherwise throws `ZartSchemaError`). */
	readonly maxConcurrentTasks?: number;
	/** Maximum number of tasks fetched per poll cycle (must be >= 1; otherwise throws `ZartSchemaError`). */
	readonly maxTasksPerPoll?: number;
	/** Graceful shutdown timeout in seconds. Overridden by `shutdownTimeout` if both are set. */
	readonly shutdownTimeoutSecs?: number;
	/** Graceful shutdown timeout as an Effect `Duration.Input` (takes precedence over `shutdownTimeoutSecs`). */
	readonly shutdownTimeout?: Duration.Input;
	/** Time in seconds after which stale `picked_up` tasks are reclaimed as orphaned. Overridden by `orphanTimeout` if both are set. */
	readonly orphanTimeoutSecs?: number;
	/** Orphan recovery timeout as an Effect `Duration.Input` (takes precedence over `orphanTimeoutSecs`). */
	readonly orphanTimeout?: Duration.Input;
	/** Lease heartbeat interval in seconds (`null` restores default `orphanTimeout / 3`; `0` disables). Overridden by `heartbeatInterval` if both are set. */
	readonly heartbeatIntervalSecs?: number | null;
	/** Lease heartbeat interval as an Effect `Duration.Input` (`null` restores default `orphanTimeout / 3`; `0` / `Duration.zero` disables). Takes precedence over `heartbeatIntervalSecs`. */
	readonly heartbeatInterval?: Duration.Input | null;
	/** Enable PostgreSQL `LISTEN/NOTIFY` wakeup hints (`true` by default). */
	readonly enableListenNotify?: boolean;
}

/** Resolve a `Duration.Input` (preferred when defined) or raw seconds value into seconds, preserving explicit `null`. */
function resolveSeconds(
	duration: Duration.Input | undefined,
	secs: number | undefined,
): number | undefined;
function resolveSeconds(
	duration: Duration.Input | null | undefined,
	secs: number | null | undefined,
): number | null | undefined;
function resolveSeconds(
	duration: Duration.Input | null | undefined,
	secs: number | null | undefined,
): number | null | undefined {
	if (duration === undefined) return secs;
	return duration === null ? null : Duration.toSeconds(duration);
}

/** Re-throw already-tagged Effect errors untouched, or decode N-API JSON error payloads into `ZartTaggedError`. */
const rethrowNativeError = (err: unknown): never => {
	if (
		err instanceof Error &&
		typeof (err as { _tag?: unknown })._tag === "string"
	) {
		throw err;
	}
	throw mapNativeError(err);
};

/** Options for {@link WorkerBuilder.registerRecurringDurable}. */
export interface RecurringDurableOptions<A = unknown> {
	/** Execution ID template; must contain `"{occurrence}"` (defaults to `` `${def.name}-{occurrence}` ``). */
	readonly idTemplate?: string;
	/** Recurrence schedule (`Recurrence.fixedDelay(...)` or `Recurrence.cron(...)`). */
	readonly recurrence: Recurrence;
	/** Policy for overlapping executions (defaults to `"SkipIfRunning"`). */
	readonly overlap?: OverlapPolicy;
	/** Optional initial input passed to each tick (encoded with `def.inputSchema` if present). */
	readonly initialData?: A;
}

const toNativeRecurrence = (r: Recurrence): NativeRecurrence =>
	r.type === "fixed_delay"
		? { kind: "fixed_delay", durationMs: r.durationMs }
		: { kind: "cron", expression: r.expression, timezone: r.timezone };

export class WorkerBuilder {
	private trxProvider: TrxProvider | undefined;

	private constructor(private readonly native: NativeWorkerBuilder) {}

	static fromBackend(backend: PgBackend): Effect.Effect<WorkerBuilder> {
		return Effect.sync(
			() =>
				new WorkerBuilder(
					loadNative().WorkerBuilder.fromBackend(backend.native),
				),
		);
	}

	/** Configure a user-managed `TrxProvider` for steps executed by this worker. */
	withTrxProvider(provider: TrxProvider): WorkerBuilder {
		this.trxProvider = provider;
		return this;
	}

	private wrapHandler<A, B, I, O>(
		def: DurableDefinition<A, B, I, O>,
	): DurableDefinition<A, B, I, O>["handler"] {
		return (dispatcher, data) =>
			this.trxProvider
				? trxProviderStorage.run(this.trxProvider, () =>
						def.handler(dispatcher, data),
					)
				: def.handler(dispatcher, data);
	}

	/** Register a `Zart.durable` workflow with the native worker. */
	registerDurable<A, B, I, O>(
		def: DurableDefinition<A, B, I, O>,
	): WorkerBuilder {
		this.native.registerDurable(
			def.name,
			this.wrapHandler(def),
			def.timeoutMs,
			def.maxRetries,
			def.onFailure,
		);
		return this;
	}

	/**
	 * Register a `Zart.durable` workflow as a recurring task on a fixed-delay or
	 * cron schedule.
	 */
	registerRecurringDurable<A, B, I, O>(
		def: DurableDefinition<A, B, I, O>,
		opts: RecurringDurableOptions<A>,
	): WorkerBuilder {
		try {
			const idTemplate = opts.idTemplate ?? `${def.name}-{occurrence}`;
			const initialData =
				opts.initialData === undefined
					? null
					: def.inputSchema
						? Effect.runSync(encodeInput(def.inputSchema, opts.initialData))
						: encodeJsonValue(opts.initialData);
			this.native.registerRecurringDurable(
				def.name,
				this.wrapHandler(def),
				idTemplate,
				toNativeRecurrence(opts.recurrence),
				opts.overlap,
				initialData,
				def.timeoutMs,
				def.maxRetries,
				def.onFailure,
			);
			return this;
		} catch (e) {
			return rethrowNativeError(e);
		}
	}

	configure(o: WorkerOptions): WorkerBuilder {
		try {
			const poll = resolveSeconds(o.pollInterval, o.pollIntervalSecs);
			if (poll !== undefined) this.native.pollIntervalSecs(poll);
			if (o.maxConcurrentTasks !== undefined)
				this.native.maxConcurrentTasks(o.maxConcurrentTasks);
			if (o.maxTasksPerPoll !== undefined)
				this.native.maxTasksPerPoll(o.maxTasksPerPoll);
			const shutdown = resolveSeconds(o.shutdownTimeout, o.shutdownTimeoutSecs);
			if (shutdown !== undefined) this.native.shutdownTimeoutSecs(shutdown);
			const orphan = resolveSeconds(o.orphanTimeout, o.orphanTimeoutSecs);
			if (orphan !== undefined) this.native.orphanTimeoutSecs(orphan);
			const heartbeat = resolveSeconds(
				o.heartbeatInterval,
				o.heartbeatIntervalSecs,
			);
			if (heartbeat !== undefined) this.native.heartbeatIntervalSecs(heartbeat);
			if (o.enableListenNotify !== undefined)
				this.native.enableListenNotify(o.enableListenNotify);
			return this;
		} catch (e) {
			return rethrowNativeError(e);
		}
	}

	build(): Worker {
		return new Worker(this.native.build());
	}
}

export class Worker {
	constructor(private readonly native: NativeWorker) {}

	run(): Effect.Effect<void, ZartTaggedError> {
		return fromNative(() => this.native.run());
	}

	stop(): Effect.Effect<void> {
		return Effect.sync(() => {
			this.native.stop();
		});
	}

	/** Run until the surrounding fiber is interrupted, then stop the worker. */
	runScoped(): Effect.Effect<void, ZartTaggedError> {
		return this.run().pipe(Effect.onInterrupt(() => this.stop()));
	}
}
