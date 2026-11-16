import { describe, expect, it } from "vitest";
import { Effect } from "effect";
import { SqlClient } from "effect/sql";
import { Zart } from "../ts/src/index.js";

/**
 * Compile-time checks enforced by `tsc --noEmit` (never executed).
 * Workflow primitives need `ZartWorkflow`; `Zart.step` functions may only use
 * `ZartTrx` / `SqlClient` / `TrxSessionService` (never `ZartWorkflow`).
 */
export const neverRun = () => {
  // @ts-expect-error ZartWorkflow primitives are not allowed inside Zart.step
  Zart.step("bad-sleep", () => Zart.sleep("nap", "1 second"));

  // @ts-expect-error scheduling inside a step is not allowed either
  Zart.step("bad-event", () => Zart.waitForEvent("x"));

  // @ts-expect-error nested step calls require ZartWorkflow
  Zart.step("bad-nested", () => Zart.step("inner", () => Effect.void)());

  // OK: pure effects, Zart.Trx, and effect/sql SqlClient
  Zart.step("fine", (n: number) => Effect.succeed(n));
  Zart.step("fine-trx", () => Zart.Trx.execute("SELECT 1"));
  Zart.step("fine-sql", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      return yield* sql`SELECT 1`;
    }),
  );
};

describe("compile-time safety", () => {
  it("is enforced by tsc (see @ts-expect-error above)", () => {
    expect(typeof neverRun).toBe("function");
  });
});
