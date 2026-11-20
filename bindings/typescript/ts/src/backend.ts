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

/** Connection options for {@link PgBackend.connect} and {@link PgBackend.scoped}. */
export interface PgBackendConnectOptions {
	/** Dedicated PostgreSQL connection URL for `LISTEN/NOTIFY` (e.g. bypassing a transaction pooler). */
	readonly listenerUrl?: string;
	/** Validated custom prefix for Zart tables (defaults to `"zart_"`). Rejects invalid identifiers with `ZartSchemaError`. */
	readonly tablePrefix?: string;
	/** Validated PostgreSQL schema qualifier for Zart tables. Rejects invalid identifiers with `ZartSchemaError`. */
	readonly schema?: string;
}

export class PgBackend {
	private constructor(readonly native: NativePgBackend) {}

	/**
	 * Connect to PostgreSQL and construct a {@link PgBackend}.
	 *
	 * @param url PostgreSQL connection URL.
	 * @param opts Connection options, or a legacy `listenerUrl` string shorthand.
	 */
	static connect(
		url: string,
		opts?: string | PgBackendConnectOptions,
	): Effect.Effect<PgBackend, ZartTaggedError> {
		const options: PgBackendConnectOptions =
			typeof opts === "string" ? { listenerUrl: opts } : (opts ?? {});
		return fromNative(() =>
			loadNative().PgBackend.connect(
				url,
				options.listenerUrl,
				options.tablePrefix,
				options.schema,
			),
		).pipe(Effect.map((n) => new PgBackend(n)));
	}

	/** Return a new `PgBackend` sharing the same pool with a dedicated `LISTEN/NOTIFY` URL. */
	withListenerUrl(url: string): Effect.Effect<PgBackend, ZartTaggedError> {
		return Effect.try({
			try: () => new PgBackend(this.native.withListenerUrl(url)),
			catch: mapNativeError,
		});
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

	/**
	 * Scoped acquisition: connects and closes on scope exit.
	 *
	 * @param url PostgreSQL connection URL.
	 * @param opts Connection options, or a legacy `listenerUrl` string shorthand.
	 */
	static scoped(url: string, opts?: string | PgBackendConnectOptions) {
		return Effect.acquireRelease(PgBackend.connect(url, opts), (b) =>
			Effect.ignore(b.close()),
		);
	}
}
