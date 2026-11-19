import { existsSync, readdirSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type { NativeStepOutcome } from "./errors.js";

export interface NativeExecutionRecord {
	executionId: string;
	taskName: string;
	payload: unknown;
	status: "scheduled" | "running" | "completed" | "failed" | "cancelled";
	result?: unknown;
	scheduledAt: string;
	completedAt?: string;
	version: number;
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

export type NativeStepRun = (trx?: NativeTrx) => Promise<NativeStepOutcome>;

export interface NativeExecutionInfo {
	executionId: string;
	taskName: string;
	data: unknown;
	currentAttempt: number;
	maxRetries?: number;
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

export interface NativeWorkerBuilder {
	registerDurable(
		name: string,
		handler: NativeDurableHandler,
		timeoutSecs?: number,
		maxRetries?: number,
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
