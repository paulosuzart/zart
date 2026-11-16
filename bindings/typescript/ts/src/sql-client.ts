import { Effect, Layer, Stream } from "effect";
import { Reactivity } from "effect/reactivity";
import { SqlClient, SqlConnection, SqlError, Statement } from "effect/sql";
import type { NativeSqlExecutor } from "./native.js";
import { encodeJsonValue } from "./schema.js";

const toSqlError = (e: unknown): SqlError.SqlError =>
  new SqlError.SqlError({
    reason: new SqlError.UnknownError({
      cause: e,
      message: e instanceof Error ? e.message : String(e),
    }),
  });

const pgCompiler = Statement.makeCompiler({
  dialect: "pg",
  placeholder: (index) => `$${index}`,
  onIdentifier: (value) => `"${value.replace(/"/g, '""')}"`,
  onRecordUpdate: (placeholders, alias, columns, values, returning) => {
    const colList = columns.split(",").map((c) => c.trim());
    const set = colList.map((c) => `${c} = v.${c}`).join(", ");
    const flat = values.flat();
    const ret = returning ? ` RETURNING ${returning[0]}` : "";
    const retParams = returning ? returning[1] : [];
    return [`SET ${set} FROM (VALUES ${placeholders}) AS v(${columns}) WHERE ${alias}.id = v.id${ret}`, [...flat, ...retParams]];
  },
  onCustom: () => ["", []],
});

const makeConnection = (trx: NativeSqlExecutor): SqlConnection.Connection => {
  const exec = (
    sql: string,
    params: ReadonlyArray<unknown>,
    transformRows: (<A extends object>(row: ReadonlyArray<A>) => ReadonlyArray<A>) | undefined,
  ) =>
    Effect.tryPromise({
      try: async () => {
        const rows = await trx.fetchAll(sql, params.map(encodeJsonValue));
        return transformRows ? transformRows(rows) : rows;
      },
      catch: toSqlError,
    });

  return {
    execute: exec,
    executeUnprepared: exec,
    executeRaw: (sql, params) =>
      Effect.tryPromise({
        try: () => trx.fetchAll(sql, params.map(encodeJsonValue)),
        catch: toSqlError,
      }),
    executeValues: (sql, params) =>
      Effect.tryPromise({
        try: async () => (await trx.fetchAll(sql, params.map(encodeJsonValue))).map((r) => Object.values(r)),
        catch: toSqlError,
      }),
    executeValuesUnprepared: (sql, params) =>
      Effect.tryPromise({
        try: async () => (await trx.fetchAll(sql, params.map(encodeJsonValue))).map((r) => Object.values(r)),
        catch: toSqlError,
      }),
    executeStream: (sql, params, transformRows) =>
      Stream.fromEffect(exec(sql, params, transformRows)).pipe(Stream.flatMap(Stream.fromIterable)),
  };
};

/** Build an `effect/sql` `SqlClient` backed by a `NativeTrx` / `NativeClientTrx`. */
export const makeZartSqlClient = (
  trx: NativeSqlExecutor,
): Effect.Effect<SqlClient.SqlClient, never, Reactivity.Reactivity> => {
  const conn = makeConnection(trx);
  return SqlClient.make({
    acquirer: Effect.succeed(conn),
    borrower: (f) => f(conn),
    compiler: pgCompiler,
    driver: "zart-pg",
    spanAttributes: [["db.system", "postgresql"]],
  });
};

/** `Layer` providing `SqlClient` from `effect/sql` backed by `NativeTrx` / `NativeClientTrx`. */
export const ZartSqlClientLive = (trx: NativeSqlExecutor): Layer.Layer<SqlClient.SqlClient> =>
  Layer.effect(SqlClient.SqlClient, makeZartSqlClient(trx)).pipe(Layer.provide(Reactivity.layer));
