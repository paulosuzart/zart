import { Effect } from "effect";
import { describe, expect, it } from "vitest";
import { isNativeAvailable, PgBackend } from "../ts/src/index.js";

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
});
