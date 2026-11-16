import { Effect } from "effect";
import { fromNative, type PgBackend } from "./backend.js";
import type { ZartTaggedError } from "./errors.js";
import {
	loadNative,
	type NativeWorker,
	type NativeWorkerBuilder,
} from "./native.js";
import { trxProviderStorage } from "./step.js";
import type { TrxProvider } from "./trx.js";
import type { DurableDefinition } from "./workflow.js";

export interface WorkerOptions {
	pollIntervalSecs?: number;
	maxConcurrentTasks?: number;
	maxTasksPerPoll?: number;
	shutdownTimeoutSecs?: number;
}

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

	/** Register a `Zart.durable` workflow with the native worker. */
	registerDurable<A, B>(def: DurableDefinition<A, B>): WorkerBuilder {
		const handler = (
			dispatcher: Parameters<typeof def.handler>[0],
			data: unknown,
		) =>
			this.trxProvider
				? trxProviderStorage.run(this.trxProvider, () =>
						def.handler(dispatcher, data),
					)
				: def.handler(dispatcher, data);
		this.native.registerDurable(
			def.name,
			handler,
			def.timeoutSecs,
			def.maxRetries,
		);
		return this;
	}

	configure(o: WorkerOptions): WorkerBuilder {
		if (o.pollIntervalSecs !== undefined)
			this.native.pollIntervalSecs(o.pollIntervalSecs);
		if (o.maxConcurrentTasks !== undefined)
			this.native.maxConcurrentTasks(o.maxConcurrentTasks);
		if (o.maxTasksPerPoll !== undefined)
			this.native.maxTasksPerPoll(o.maxTasksPerPoll);
		if (o.shutdownTimeoutSecs !== undefined)
			this.native.shutdownTimeoutSecs(o.shutdownTimeoutSecs);
		return this;
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
		return Effect.promise(async () => {
			await this.native.stop();
		});
	}

	/** Run until the surrounding fiber is interrupted, then stop the worker. */
	runScoped(): Effect.Effect<void, ZartTaggedError> {
		return this.run().pipe(Effect.onInterrupt(() => this.stop()));
	}
}
