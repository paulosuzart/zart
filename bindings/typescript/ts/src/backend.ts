import { Effect, Exit } from "effect";
import { mapNativeError, type ZartTaggedError } from "./errors.js";
import {
	loadNative,
	type NativeClientTrx,
	type NativePgBackend,
} from "./native.js";
import { TrxHandle } from "./trx.js";

/** Run a native promise-returning call, mapping rejections to typed errors. */
export const fromNative = <A>(
	f: () => Promise<A>,
): Effect.Effect<A, ZartTaggedError> =>
	Effect.tryPromise({ try: f, catch: mapNativeError });

/** Effect-wrapped client transaction (`backend.begin()` / `backend.withTransaction()`). */
export class ClientTrx extends TrxHandle {
	constructor(override readonly native: NativeClientTrx) {
		super(native);
	}

	commit(): Effect.Effect<void, ZartTaggedError> {
		return fromNative(() => this.native.commit());
	}

	rollback(): Effect.Effect<void, ZartTaggedError> {
		return fromNative(() => this.native.rollback());
	}
}

export class PgBackend {
	private constructor(readonly native: NativePgBackend) {}

	static connect(
		url: string,
		listenerUrl?: string,
	): Effect.Effect<PgBackend, ZartTaggedError> {
		return fromNative(() =>
			loadNative().PgBackend.connect(url, listenerUrl),
		).pipe(Effect.map((n) => new PgBackend(n)));
	}

	runMigrations(): Effect.Effect<void, ZartTaggedError> {
		return fromNative(() => this.native.runMigrations());
	}

	ping(): Effect.Effect<void, ZartTaggedError> {
		return fromNative(() => this.native.ping());
	}

	close(): Effect.Effect<void, ZartTaggedError> {
		return fromNative(() => this.native.close());
	}

	begin(): Effect.Effect<ClientTrx, ZartTaggedError> {
		return fromNative(() => this.native.begin()).pipe(
			Effect.map((tx) => new ClientTrx(tx)),
		);
	}

	/**
	 * Run `fn` inside a database transaction (`begin` -> `fn` -> `commit` on success,
	 * `rollback` on failure or interruption).
	 */
	withTransaction<A, E, R>(
		fn: (tx: ClientTrx) => Effect.Effect<A, E, R>,
	): Effect.Effect<A, E | ZartTaggedError, R> {
		return Effect.acquireUseRelease(
			this.begin(),
			(tx) => fn(tx).pipe(Effect.tap(() => tx.commit())),
			(tx, exit) =>
				Exit.isSuccess(exit) ? Effect.void : Effect.ignore(tx.rollback()),
		);
	}

	/** Scoped acquisition: connects and closes on scope exit. */
	static scoped(url: string, listenerUrl?: string) {
		return Effect.acquireRelease(PgBackend.connect(url, listenerUrl), (b) =>
			Effect.ignore(b.close()),
		);
	}
}
