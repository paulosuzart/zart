import { existsSync, readdirSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type { NativeStepOutcome } from "./errors.js";

export type NativeExecutionStatus =
	| "scheduled"
	| "running"
	| "completed"
	| "failed"
	| "cancelled";

export type NativeExecutionTrigger = "initial" | "restart" | "selective_rerun";

export type NativeExecutionSortField = "scheduled_at" | "status" | "task_name";

export type NativeSortOrder = "asc" | "desc";

export type NativeStepKind =
	| "step"
	| "sleep"
	| "wait_all"
	| "wait_for_event"
	| "wait_group"
	| "capture";

export type NativeStepStatus = "scheduled" | "running" | "completed" | "dead";

export type NativeStepAttemptStatus = "completed" | "failed";

export interface NativeExecutionRecord {
	readonly executionId: string;
	readonly taskName: string;
	readonly payload: unknown;
	readonly status: NativeExecutionStatus;
	readonly result?: unknown;
	readonly scheduledAt: string;
	readonly completedAt?: string;
	readonly version: number;
}

export interface NativeExecutionStats {
	readonly scheduled: number;
	readonly running: number;
	readonly completed: number;
	readonly failed: number;
	readonly cancelled: number;
}

export interface NativeListExecutionsParams {
	readonly status?: NativeExecutionStatus;
	readonly taskName?: string;
	readonly from?: string;
	readonly to?: string;
	readonly search?: string;
	readonly sortBy?: NativeExecutionSortField;
	readonly sortOrder?: NativeSortOrder;
	readonly limit?: number;
	readonly offset?: number;
}

export interface NativeExecutionRunRecord {
	readonly runId: string;
	readonly executionId: string;
	readonly runIndex: number;
	readonly payload: unknown;
	readonly status: NativeExecutionStatus;
	readonly result?: unknown;
	readonly startedAt: string;
	readonly completedAt?: string;
	readonly trigger: NativeExecutionTrigger;
}

export interface NativeStepRow {
	readonly stepId: string;
	readonly runId: string;
	readonly stepName: string;
	readonly stepKind: NativeStepKind;
	readonly taskId?: string;
	readonly status: NativeStepStatus;
	readonly retryAttempt: number;
	readonly retryConfig?: unknown;
	readonly result?: unknown;
	readonly lastError?: string;
	readonly wgTotal?: number;
	readonly wgRemaining?: number;
	readonly wgThreshold?: number;
	readonly wgFirstFailed?: boolean;
	readonly scheduledAt: string;
	readonly completedAt?: string;
}

export interface NativeStepAttemptRow {
	readonly attemptId: string;
	readonly stepId: string;
	readonly attemptNumber: number;
	readonly startedAt: string;
	readonly completedAt?: string;
	readonly status: NativeStepAttemptStatus;
	readonly result?: unknown;
	readonly error?: string;
}

export interface NativeStepWithAttempts {
	readonly step: NativeStepRow;
	readonly attempts: readonly NativeStepAttemptRow[];
	readonly retryable: boolean;
}

export interface NativeExecutionDetail {
	readonly execution: NativeExecutionRecord;
	readonly runs: readonly NativeExecutionRunRecord[];
	readonly steps: readonly NativeStepWithAttempts[];
}

export interface NativeRerunSpec {
	readonly forceRerun?: readonly string[];
	readonly preserve?: readonly string[];
	readonly triggeredBy?: string;
}

export interface NativePotentiallyStaleDep {
	readonly preservedStep: string;
	readonly possiblyDependsOn: readonly string[];
}

export interface NativeRerunResult {
	readonly newRunNumber: number;
	readonly effectiveRerun: readonly string[];
	readonly potentiallyStale: readonly NativePotentiallyStaleDep[];
}

export interface NativePauseScope {
	readonly executionId?: string;
	readonly taskName?: string;
	readonly stepPattern?: string;
	readonly expiresAt?: string;
	readonly triggeredBy?: string;
	readonly reason?: string;
}

export interface NativePauseRule {
	readonly ruleId: string;
	readonly scope: NativePauseScope;
	readonly createdAt: string;
	readonly deletedAt?: string;
	readonly reason?: string;
}

export interface NativeResumeResult {
	readonly rulesDeleted: number;
}

export interface NativePauseRuleFilter {
	readonly executionId?: string;
	readonly taskName?: string;
	readonly includeDeleted?: boolean;
}

export interface NativeSqlExecutor {
	execute(sql: string, params?: unknown[]): Promise<number>;
	fetchOne(sql: string, params?: unknown[]): Promise<Record<string, unknown>>;
	fetchAll(
		sql: string,
		params?: unknown[],
	): Promise<Array<Record<string, unknown>>>;
	fetchOptional(
		sql: string,
		params?: unknown[],
	): Promise<Record<string, unknown> | null>;
}

export interface NativeTrx extends NativeSqlExecutor {}

export interface NativeClientTrx extends NativeSqlExecutor {
	commit(): Promise<void>;
	rollback(): Promise<void>;
}

export interface NativePgBackend {
	withListenerUrl(url: string): NativePgBackend;
	runMigrations(): Promise<void>;
	ping(): Promise<void>;
	close(): Promise<void>;
	begin(): Promise<NativeClientTrx>;
}

export interface NativeDurableScheduler {
	start(
		executionId: string,
		taskName: string,
		payload: unknown,
	): Promise<{ taskId: string; executionTime: string }>;
	startInTx(
		tx: NativeClientTrx,
		executionId: string,
		taskName: string,
		payload: unknown,
	): Promise<{ taskId: string; executionTime: string }>;
	status(id: string): Promise<NativeExecutionRecord>;
	wait(
		id: string,
		timeoutSecs?: number,
		pollIntervalSecs?: number,
	): Promise<NativeExecutionRecord>;
	waitCompletion(
		id: string,
		timeoutSecs?: number,
		pollIntervalSecs?: number,
	): Promise<unknown>;
	cancel(id: string): Promise<boolean>;
	offerEvent(id: string, eventName: string, payload: unknown): Promise<void>;
	stopRecurring(taskId: string): Promise<void>;
	listExecutions(
		params?: NativeListExecutionsParams,
	): Promise<NativeExecutionRecord[]>;
	stats(): Promise<NativeExecutionStats>;
	getCurrentRunId(executionId: string): Promise<string | null>;
	listRuns(executionId: string): Promise<NativeExecutionRunRecord[]>;
	executionDetail(
		executionId: string,
		runId?: string,
	): Promise<NativeExecutionDetail>;
	retryStep(
		runId: string,
		stepName: string,
		triggeredBy?: string,
	): Promise<string>;
	retryStepCurrentRun(
		executionId: string,
		stepName: string,
		triggeredBy?: string,
	): Promise<string>;
	restart(
		executionId: string,
		newPayload?: unknown,
		triggeredBy?: string,
	): Promise<string>;
	rerunSteps(
		executionId: string,
		spec: NativeRerunSpec,
	): Promise<NativeRerunResult>;
	pause(scope: NativePauseScope): Promise<NativePauseRule>;
	resume(
		scope: NativePauseScope,
		deletedBy?: string,
	): Promise<NativeResumeResult>;
	resumeRuleById(
		ruleId: string,
		deletedBy?: string,
	): Promise<NativeResumeResult>;
	listPauseRules(filter?: NativePauseRuleFilter): Promise<NativePauseRule[]>;
}

export interface NativeWorker {
	run(): Promise<void>;
	stop(): void | Promise<void>;
}

export interface NativeStepSpec {
	name: string;
	retry?: {
		kind: "none" | "fixed" | "linear" | "exponential";
		maxAttempts: number;
		delayMs: number;
		factor?: number;
		maxDelayMs?: number;
	};
	timeoutMs?: number;
	timeoutScope?: "attempt" | "total" | "per_attempt" | "global";
	/** When true, Rust acquires `ZartTrx` and passes `NativeTrx` to `run`. */
	needsTrx?: boolean;
}

export type NativeStepRun = (
	args?: [trx: NativeTrx | undefined, info: NativeExecutionInfo] | NativeTrx,
	maybeInfo?: NativeExecutionInfo,
) => Promise<NativeStepOutcome>;

export interface NativeExecutionInfo {
	executionId: string;
	taskName: string;
	data: unknown;
	currentAttempt: number;
	maxRetries?: number;
	/** Whether this execution attempt is a retry (`currentAttempt > 0`). */
	isRetry: boolean;
}

/** Low-level failure descriptor passed by the native runtime to `on_failure`. */
export interface NativeExecutionFailure {
	/** Failure category: `"step_failed"` | `"execution_deadline_exceeded"` | `"retries_exhausted"`. */
	kind: string;
	/** Name of the failed step when `kind === "step_failed"`. */
	step?: string;
	/** Serialized error payload when `kind === "step_failed"`. */
	raw?: unknown;
	/** Number of completed attempts when `kind === "retries_exhausted"`. */
	attempts?: number;
}

/** Handle passed by the native worker into each durable handler invocation. */
export interface NativeDispatcher {
	step(spec: NativeStepSpec, run: NativeStepRun): Promise<NativeStepOutcome>;
	require(spec: NativeStepSpec, run: NativeStepRun): Promise<unknown>;
	stepOr(
		spec: NativeStepSpec,
		run: NativeStepRun,
		defaultVal: unknown,
	): Promise<unknown>;
	schedule(spec: NativeStepSpec, run: NativeStepRun): Promise<number>;
	wait(handle: number): Promise<NativeStepOutcome>;
	waitAll(handles: number[]): Promise<NativeStepOutcome[]>;
	sleep(name: string, ms: number): Promise<void>;
	sleepUntil(name: string, wakeTime: string): Promise<void>;
	waitForEvent(name: string, timeoutMs?: number): Promise<unknown>;
	capture(
		name: string,
		run: () => unknown | Promise<unknown>,
	): Promise<unknown>;
	now(name: string): Promise<string>;
	context(): NativeExecutionInfo;
}

export type NativeDurableHandler = (
	dispatcher: NativeDispatcher,
	data: unknown,
) => Promise<unknown>;

export interface NativeRecurrence {
	kind: string;
	durationMs?: number;
	expression?: string;
	timezone?: string;
}

export interface NativeWorkerBuilder {
	registerDurable(
		name: string,
		handler: NativeDurableHandler,
		timeoutMs?: number,
		maxRetries?: number,
		onFailure?: (
			data: unknown,
			failure: NativeExecutionFailure,
		) => Promise<unknown>,
	): void;
	registerRecurringDurable(
		name: string,
		handler: NativeDurableHandler,
		idTemplate: string,
		recurrence: NativeRecurrence,
		overlap?: string,
		initialData?: unknown,
		timeoutMs?: number,
		maxRetries?: number,
		onFailure?: (
			data: unknown,
			failure: NativeExecutionFailure,
		) => Promise<unknown>,
	): void;
	pollIntervalSecs(n: number): void;
	maxConcurrentTasks(n: number): void;
	maxTasksPerPoll(n: number): void;
	shutdownTimeoutSecs(n: number): void;
	orphanTimeoutSecs(n: number): void;
	heartbeatIntervalSecs(n: number | null | undefined): void;
	enableListenNotify(enabled: boolean): void;
	build(): NativeWorker;
}

export interface NativeBinding {
	PgBackend: {
		connect(
			url: string,
			listenerUrl?: string,
			tablePrefix?: string,
			schema?: string,
		): Promise<NativePgBackend>;
	};
	DurableScheduler: { fromBackend(b: NativePgBackend): NativeDurableScheduler };
	WorkerBuilder: { fromBackend(b: NativePgBackend): NativeWorkerBuilder };
}

let cached: NativeBinding | undefined;

/** Load the napi-rs generated `zart-ts.*.node` binary (override with ZART_NATIVE_PATH). */
export function loadNative(): NativeBinding {
	if (cached) return cached;
	const require = createRequire(import.meta.url);
	const override = process.env.ZART_NATIVE_PATH;
	if (override) {
		cached = require(override) as NativeBinding;
		return cached;
	}
	const here = dirname(fileURLToPath(import.meta.url));
	for (const dir of [
		join(here, ".."),
		join(here, "..", ".."),
		join(here, "..", "..", ".."),
	]) {
		if (!existsSync(dir)) continue;
		const file = readdirSync(dir).find((f) => /^zart-ts(\..+)?\.node$/.test(f));
		if (file) {
			cached = require(join(dir, file)) as NativeBinding;
			return cached;
		}
	}
	throw new Error(
		"zart-ts native binding not found; run `npm run build:native` or set ZART_NATIVE_PATH",
	);
}

export const isNativeAvailable = (): boolean => {
	try {
		loadNative();
		return true;
	} catch {
		return false;
	}
};
