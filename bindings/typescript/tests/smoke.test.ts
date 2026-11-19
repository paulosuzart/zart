import { Effect, Schema } from "effect";
import { describe, expect, it } from "vitest";
import {
	DurableScheduler,
	isNativeAvailable,
	PgBackend,
	WorkerBuilder,
	ZartBackendError,
	ZartSchemaError,
} from "../ts/src/index.js";

const url = process.env.DATABASE_URL;
const run = isNativeAvailable() && !!url;

describe.skipIf(!run)("postgres smoke", () => {
	it("connects, migrates, pings", async () => {
		await Effect.runPromise(
			Effect.scoped(
				Effect.gen(function* () {
					const b = yield* PgBackend.scoped(url ?? "");
					yield* b.runMigrations();
					yield* b.ping();
				}),
			),
		);
		expect(true).toBe(true);
	});

	it("connects with PgBackendConnectOptions, preserves table names across withListenerUrl, and configures worker", async () => {
		await Effect.runPromise(
			Effect.scoped(
				Effect.gen(function* () {
					const b = yield* PgBackend.scoped(url ?? "", {
						listenerUrl: url ?? "",
						tablePrefix: "missing_",
						schema: "public",
					});
					yield* b.ping();
					const b2 = yield* b.withListenerUrl(url ?? "");
					yield* b2.ping();

					const sched = yield* DurableScheduler.fromBackend(b2);
					const missingErr = yield* Effect.flip(
						sched.start("smoke-missing-1", "noop", Schema.Unknown, {}),
					);
					expect(missingErr).toBeInstanceOf(ZartBackendError);
					expect(missingErr.message).toContain("missing_");

					const builder = yield* WorkerBuilder.fromBackend(b2);
					expect(() => builder.configure({ maxConcurrentTasks: 0 })).toThrow(
						ZartSchemaError,
					);
					expect(() => builder.configure({ maxTasksPerPoll: 0 })).toThrow(
						ZartSchemaError,
					);
					builder
						.configure({
							orphanTimeout: "2 minutes",
							heartbeatInterval: "15 seconds",
							enableListenNotify: false,
						})
						.configure({ heartbeatInterval: null })
						.build();
				}),
			),
		);
	});
});
