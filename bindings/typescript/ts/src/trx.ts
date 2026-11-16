import { Context, Effect, Exit } from "effect";
import { mapNativeError, type ZartTaggedError } from "./errors.js";
import type { NativeSqlExecutor, NativeTrx } from "./native.js";
import { encodeJsonValue } from "./schema.js";

const runSql = <A>(f: () => Promise<A>): Effect.Effect<A, ZartTaggedError> =>
  Effect.tryPromise({ try: f, catch: mapNativeError });

const encodeParams = (params?: ReadonlyArray<unknown>): unknown[] =>
  params ? params.map(encodeJsonValue) : [];

/** Effect-wrapped SQL transaction handle (Zart-Managed `ZartTrx` or `ClientTrx`). */
export class TrxHandle {
  constructor(readonly native: NativeSqlExecutor) {}

  execute(sql: string, params?: ReadonlyArray<unknown>): Effect.Effect<number, ZartTaggedError> {
    return runSql(() => this.native.execute(sql, encodeParams(params)));
  }

  fetchOne<R extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: ReadonlyArray<unknown>,
  ): Effect.Effect<R, ZartTaggedError> {
    return runSql(() => this.native.fetchOne(sql, encodeParams(params)) as Promise<R>);
  }

  fetchAll<R extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: ReadonlyArray<unknown>,
  ): Effect.Effect<R[], ZartTaggedError> {
    return runSql(() => this.native.fetchAll(sql, encodeParams(params)) as Promise<R[]>);
  }

  fetchOptional<R extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: ReadonlyArray<unknown>,
  ): Effect.Effect<R | null, ZartTaggedError> {
    return runSql(() => this.native.fetchOptional(sql, encodeParams(params)) as Promise<R | null>);
  }
}

/** Zart-Managed transaction service provided inside `Zart.step` when `NativeTrx` is present. */
export class ZartTrx extends Context.Service<ZartTrx, TrxHandle>()("@zart/effect/ZartTrx") {}

const withTrx = <A>(f: (t: TrxHandle) => Effect.Effect<A, ZartTaggedError>): Effect.Effect<A, ZartTaggedError, ZartTrx> =>
  Effect.gen(function* () {
    const trx = yield* ZartTrx;
    return yield* f(trx);
  });

/** Ergonomic `Zart.Trx` helpers operating on the ambient `ZartTrx`. */
export const Trx = {
  execute: (sql: string, params?: ReadonlyArray<unknown>) => withTrx((t) => t.execute(sql, params)),
  fetchOne: <R extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: ReadonlyArray<unknown>,
  ) => withTrx((t) => t.fetchOne<R>(sql, params)),
  fetchAll: <R extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: ReadonlyArray<unknown>,
  ) => withTrx((t) => t.fetchAll<R>(sql, params)),
  fetchOptional: <R extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: ReadonlyArray<unknown>,
  ) => withTrx((t) => t.fetchOptional<R>(sql, params)),
  fromNative: (native: NativeTrx): TrxHandle => new TrxHandle(native),
};

/** User-managed transaction session for external pools (e.g. pg, drizzle, prisma). */
export interface TrxSession {
  readonly commit: () => Effect.Effect<void, unknown>;
  readonly rollback: () => Effect.Effect<void, unknown>;
  readonly release: () => Effect.Effect<void, unknown>;
}

/** User-managed transaction provider for `WorkerBuilder.withTrxProvider`. */
export interface TrxProvider<S extends TrxSession = TrxSession> {
  readonly begin: () => Effect.Effect<S, unknown>;
}

/** Ambient user-managed transaction session inside a `Zart.step` when `TrxProvider` is configured. */
export class TrxSessionService extends Context.Service<TrxSessionService, TrxSession>()(
  "@zart/effect/TrxSession",
) {}

/**
 * Execute `use` inside a `TrxProvider` session using `Effect.acquireUseRelease`:
 * - `commit()` only on success
 * - `rollback()` on failure or interruption
 * - `release()` always called
 */
export const runWithTrxProvider = <S extends TrxSession, A, E, R>(
  provider: TrxProvider<S>,
  use: (session: S) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E | unknown, R> =>
  Effect.acquireUseRelease(
    provider.begin(),
    (session) => use(session).pipe(Effect.tap(() => session.commit())),
    (session, exit) =>
      (Exit.isSuccess(exit) ? Effect.void : Effect.ignore(session.rollback())).pipe(
        Effect.ensuring(Effect.ignore(session.release())),
      ),
  );
