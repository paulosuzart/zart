import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Effect, Fiber, Schema } from "effect";
import { DurableScheduler, PgBackend, WorkerBuilder, Zart } from "../dist/index.js";

const OnboardInput = Schema.Struct({
  userId: Zart.UuidString,
  email: Schema.String,
});

const OnboardOutput = Schema.Struct({
  userId: Zart.UuidString,
  email: Schema.String,
  balance: Schema.Number,
});

const deductBalance = Zart.step("deduct-balance", (userId: string, amount: number) =>
  Effect.gen(function* () {
    const row = yield* Zart.Trx.fetchOne<{ balance: number | string }>(
      `UPDATE demo_users SET balance = balance - $1 WHERE id = $2::uuid RETURNING balance`,
      [amount, userId],
    );
    return Number(row.balance);
  }),
);

const markOnboarded = Zart.step("mark-onboarded", (userId: string) =>
  Zart.Trx.execute("UPDATE demo_users SET onboarded = TRUE WHERE id = $1::uuid", [userId]),
);

const onboardUser = Zart.durable(
  "onboard-user",
  (data) =>
    Effect.gen(function* () {
      const balance = yield* deductBalance(data.userId, 100);
      yield* markOnboarded(data.userId);
      return { userId: data.userId, email: data.email, balance };
    }),
  { inputSchema: OnboardInput, outputSchema: OnboardOutput },
);

const ensureSchema = (backend: PgBackend) =>
  backend.withTransaction((tx) =>
    tx.execute(`
      CREATE TABLE IF NOT EXISTS demo_users (
        id        UUID PRIMARY KEY,
        email     TEXT NOT NULL,
        balance   BIGINT NOT NULL DEFAULT 0,
        onboarded BOOLEAN NOT NULL DEFAULT FALSE
      )
    `),
  );

const main = Effect.scoped(
  Effect.gen(function* () {
    const dbUrl = process.env.DATABASE_URL ?? "postgres://zart:zart@localhost:5432/zart";
    const backend = yield* PgBackend.scoped(dbUrl);
    yield* backend.runMigrations();
    yield* ensureSchema(backend);

    const scheduler = yield* DurableScheduler.fromBackend(backend);

    // 1. Demonstrate rollback: failing transaction persists neither user nor task.
    const rolledBackUserId = randomUUID();
    const rolledBackExecId = `rollback-${rolledBackUserId}`;
    yield* backend
      .withTransaction((tx) =>
        Effect.gen(function* () {
          yield* tx.execute("INSERT INTO demo_users (id, email, balance) VALUES ($1::uuid, $2, 500)", [
            rolledBackUserId,
            "rolled-back@example.com",
          ]);
          yield* scheduler.startInTx(tx, {
            executionId: rolledBackExecId,
            taskName: "onboard-user",
            inputSchema: OnboardInput,
            input: { userId: rolledBackUserId, email: "rolled-back@example.com" },
          });
          return yield* Effect.fail(new Error("simulated rollback"));
        }),
      )
      .pipe(Effect.ignore);

    const ghostRow = yield* backend.withTransaction((tx) =>
      tx.fetchOptional("SELECT id FROM demo_users WHERE id = $1::uuid", [rolledBackUserId]),
    );
    assert.equal(ghostRow, null, "Rolled-back user must not persist");

    // 2. Atomic commit: insert user + schedule durable execution in one transaction.
    const userId = randomUUID();
    const email = `user-${userId}@example.com`;
    const executionId = `onboard-${userId}`;

    yield* backend.withTransaction((tx) =>
      Effect.gen(function* () {
        yield* tx.execute("INSERT INTO demo_users (id, email, balance) VALUES ($1::uuid, $2, 1000)", [
          userId,
          email,
        ]);
        yield* scheduler.startInTx(tx, {
          executionId,
          taskName: "onboard-user",
          inputSchema: OnboardInput,
          input: { userId, email },
        });
      }),
    );

    const worker = (yield* WorkerBuilder.fromBackend(backend)).registerDurable(onboardUser).build();
    const workerFiber = yield* Effect.forkChild(worker.runScoped());
    const output = yield* scheduler
      .waitCompletion(executionId, OnboardOutput, 30)
      .pipe(Effect.ensuring(Fiber.interrupt(workerFiber)));

    assert.equal(output.balance, 900, `Expected balance 900, got ${output.balance}`);

    const row = yield* backend.withTransaction((tx) =>
      tx.fetchOne<{ balance: number | string; onboarded: boolean }>(
        "SELECT balance, onboarded FROM demo_users WHERE id = $1::uuid",
        [userId],
      ),
    );
    assert.equal(Number(row.balance), 900);
    assert.equal(row.onboarded, true);
    console.log(`Completed: user=${output.email}, final_balance=${output.balance}`);
  }),
);

await Effect.runPromise(main);
