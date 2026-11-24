import { Effect, Result } from "effect";
import { describe, expect, it, vi } from "vitest";
import {
	isNativeAvailable,
	loadNative,
	type NativePgBackend,
	PgBackend,
	type PgBackendConnectOptions,
	ZartBackendError,
	ZartSchemaError,
} from "../ts/src/index.js";

const wrapNativeBackend = (native: NativePgBackend): PgBackend =>
	new (PgBackend as unknown as { new (n: NativePgBackend): PgBackend })(native);

const makeFakeNativeBackend = (
	overrides?: Partial<NativePgBackend>,
): NativePgBackend => {
	const native: NativePgBackend = {
		withListenerUrl: () => native,
		runMigrations: async () => {},
		ping: async () => {},
		close: async () => {},
		begin: async () => {
			throw new Error("unused");
		},
		...overrides,
	};
	return native;
};

describe("PgBackend", () => {
	it("withListenerUrl wraps returned NativePgBackend and leaves original untouched", async () => {
		const originalNative = makeFakeNativeBackend();
		const updatedNative = makeFakeNativeBackend();
		let receivedUrl: string | undefined;
		originalNative.withListenerUrl = (url: string) => {
			receivedUrl = url;
			return updatedNative;
		};

		const original = wrapNativeBackend(originalNative);
		const updated = await Effect.runPromise(
			original.withListenerUrl("postgres://user:pass@direct:5432/zart"),
		);

		expect(receivedUrl).toBe("postgres://user:pass@direct:5432/zart");
		expect(updated).toBeInstanceOf(PgBackend);
		expect(updated).not.toBe(original);
		expect(updated.native).toBe(updatedNative);
		expect(original.native).toBe(originalNative);
	});

	it("withListenerUrl maps native errors to ZartTaggedError", async () => {
		const original = wrapNativeBackend(
			makeFakeNativeBackend({
				withListenerUrl: () => {
					throw new Error(
						JSON.stringify({
							_tag: "ZartBackendError",
							message: "Invalid listener URL: bad",
						}),
					);
				},
			}),
		);

		const res = await Effect.runPromise(
			Effect.result(original.withListenerUrl("not-a-url")),
		);
		expect(Result.isFailure(res)).toBe(true);
		if (Result.isFailure(res)) {
			expect(res.failure).toBeInstanceOf(ZartBackendError);
			expect(res.failure.message).toContain("Invalid listener URL");
		}
	});
});

describe.skipIf(!isNativeAvailable())("PgBackend (native)", () => {
	it("forwards legacy string and PgBackendConnectOptions to native connect and scoped", async () => {
		const nativeBinding = loadNative();
		const fakeNative = makeFakeNativeBackend();
		const originalPgBackend = nativeBinding.PgBackend;
		const connectSpy = vi.fn().mockResolvedValue(fakeNative);
		nativeBinding.PgBackend = { connect: connectSpy };

		try {
			await Effect.runPromise(
				PgBackend.connect("postgres://main/db", "postgres://listener/db"),
			);
			expect(connectSpy).toHaveBeenLastCalledWith(
				"postgres://main/db",
				"postgres://listener/db",
				undefined,
				undefined,
			);

			const opts: PgBackendConnectOptions = {
				listenerUrl: "postgres://listener/db",
				tablePrefix: "svc_",
				schema: "tenant_a",
			};
			await Effect.runPromise(
				Effect.scoped(PgBackend.scoped("postgres://main/db", opts)),
			);
			expect(connectSpy).toHaveBeenLastCalledWith(
				"postgres://main/db",
				"postgres://listener/db",
				"svc_",
				"tenant_a",
			);
		} finally {
			nativeBinding.PgBackend = originalPgBackend;
		}
	});

	it("rejects invalid tablePrefix and schema with ZartSchemaError before connecting", async () => {
		const cases: PgBackendConnectOptions[] = [
			{ tablePrefix: "bad-prefix" },
			{ tablePrefix: "" },
			{ schema: "1bad_schema" },
			{ schema: "bad-schema" },
		];

		for (const opts of cases) {
			const res = await Effect.runPromise(
				Effect.result(PgBackend.connect("postgres://127.0.0.1:1/zart", opts)),
			);
			expect(Result.isFailure(res)).toBe(true);
			if (Result.isFailure(res)) {
				expect(res.failure).toBeInstanceOf(ZartSchemaError);
			}
		}

		const scopedRes = await Effect.runPromise(
			Effect.result(
				Effect.scoped(
					PgBackend.scoped("postgres://127.0.0.1:1/zart", {
						tablePrefix: "bad-prefix",
					}),
				),
			),
		);
		expect(Result.isFailure(scopedRes)).toBe(true);
		if (Result.isFailure(scopedRes)) {
			expect(scopedRes.failure).toBeInstanceOf(ZartSchemaError);
		}
	});
});
