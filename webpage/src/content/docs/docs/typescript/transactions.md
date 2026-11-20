---
title: TypeScript Transactions
description: Atomic step transactions, Effect SQL integration, and transactional scheduling in TypeScript.
---

Zart's TypeScript bindings (`@zart/effect`) support **transactional scheduling** (`backend.withTransaction` + `scheduler.startInTx`), **Zart-managed step transactions** (`Zart.Trx` and `ZartSqlClientLive` from `effect/sql`), and **user-managed transaction providers** (`TrxProvider`).

## Transactional Scheduling (`backend.withTransaction` + `scheduler.startInTx`)

Commit your application's database write and the durable execution record in a single PostgreSQL transaction. `backend.withTransaction` uses `Effect.acquireUseRelease` to commit on success and roll back on failure or fiber interruption:

```ts
import { Effect } from "effect";

yield* backend.withTransaction((tx) =>
  Effect.gen(function* () {
    yield* tx.execute(
      "INSERT INTO demo_users (id, email, balance) VALUES ($1::uuid, $2, 1000)",
      [userId, email],
    );
    yield* scheduler.startInTx(tx, {
      executionId: `onboard-${userId}`,
      taskName: "onboard-user",
      inputSchema: OnboardInput,
      input: { userId, email },
    });
  }),
);
```

`startInTx` also accepts a raw `{ executionId, taskName, payload }` object when you are not using an `inputSchema`.

## Zart-Managed Step Transactions (`Zart.Trx`)

By default (`opts?.trx !== false`), every `Zart.step` opens a `NativeTrx` and provides `ZartTrx` (plus `ZartSqlClientLive`) to the step body. Your SQL writes via `Zart.Trx` and Zart's step completion record commit atomically:

```ts
import { Effect } from "effect";
import { Zart } from "@zart/effect";

const deductBalance = Zart.step("deduct-balance", (userId: string, amount: number) =>
  Effect.gen(function* () {
    const row = yield* Zart.Trx.fetchOne<{ balance: number | string }>(
      `UPDATE demo_users
       SET balance = balance - $1
       WHERE id = $2::uuid
       RETURNING balance`,
      [amount, userId],
    );
    return Number(row.balance);
  }),
);

const onboardUser = Zart.durable(
  "onboard-user",
  (data: { userId: string; email: string }) => deductBalance(data.userId, 100),
);
```

`Zart.Trx` and `ClientTrx` expose `execute(sql, params?)`, `fetchOne(sql, params?)`, `fetchOptional(sql, params?)`, and `fetchAll(sql, params?)`. To opt a step out of Zart-managed transaction acquisition, pass `{ trx: false }` in `StepOptions`.

## Effect SQL Integration (`ZartSqlClientLive`)

Inside any `Zart.step`, Zart automatically provides a `SqlClient.SqlClient` layer (`ZartSqlClientLive`) backed by the same atomic step transaction. You can use standard `effect/sql` tagged template queries directly:

```ts
import { Effect } from "effect";
import { SqlClient } from "effect/sql";
import { Zart } from "@zart/effect";

const insertItem = Zart.step("insert-item", (id: number, name: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO items VALUES (${id}, ${name})`;
    return yield* sql<{ id: number; name: string }>`SELECT * FROM items`;
  }),
);
```

If the step fails with an Effect error or defect, `NativeTrx` rolls back both the SQL changes and Zart's step record.

## User-Managed `TrxProvider` (External Pools)

If you use an external database client or ORM (such as `pg`, Drizzle, or Prisma), implement `TrxProvider` and attach it to your worker with `WorkerBuilder.withTrxProvider(provider)`.

`TrxProvider` defines `begin(): Effect.Effect<TrxSession, unknown>`, where `TrxSession` provides `commit`, `rollback`, and `release` Effects. Zart runs each step via `runWithTrxProvider` (`Effect.acquireUseRelease`), calling `commit()` on success, `rollback()` on failure or interruption, and `release()` unconditionally:

```ts
import { Effect } from "effect";
import { WorkerBuilder, Zart, type TrxProvider } from "@zart/effect";

const makePoolProvider = (pool: {
  connect: () => Promise<{
    query: (sql: string, params?: unknown[]) => Promise<unknown>;
    release: () => void;
  }>;
}): TrxProvider => ({
  begin: () =>
    Effect.gen(function* () {
      const client = yield* Effect.promise(() => pool.connect());
      yield* Effect.promise(() => client.query("BEGIN"));
      return {
        client,
        commit: () => Effect.promise(() => client.query("COMMIT")).pipe(Effect.asVoid),
        rollback: () => Effect.promise(() => client.query("ROLLBACK")).pipe(Effect.asVoid),
        release: () => Effect.sync(() => client.release()),
      };
    }),
});

const stepWithPool = Zart.step("update-ledger", () =>
  Effect.gen(function* () {
    const session = yield* Zart.TrxSessionService;
    // Use `session` (or a Scope-backed client attached to your session)
    return session;
  }),
);

// On the worker:
const worker = (yield* WorkerBuilder.fromBackend(backend))
  .withTrxProvider(poolProvider)
  .registerDurable(myWorkflow)
  .build();
```

When a `TrxProvider` is configured on the worker, Zart sets `needsTrx: false` for native steps and binds `TrxSessionService` in the step's Effect context. If your external resource is managed with `Scope`, you can also close the `Scope` inside `session.release()`.
