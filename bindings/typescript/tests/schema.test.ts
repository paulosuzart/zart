import { describe, expect, it } from "vitest";
import { Effect, Result, Schema } from "effect";
import { encodeInput, decodeOutput, encodeJsonValue, decodeJsonValue, BytesFromBase64, UuidString } from "../ts/src/schema.js";
import { ZartSchemaError } from "../ts/src/errors.js";

describe("encodeJsonValue", () => {
  it("encodes Date, BigInt, bytes, nested", () => {
    const d = new Date("2024-01-02T03:04:05.000Z");
    expect(
      encodeJsonValue({ d, n: 10n, b: Buffer.from("hi"), arr: [d], skip: undefined, u: new Uint8Array([1, 2]) }),
    ).toEqual({
      d: "2024-01-02T03:04:05.000Z",
      n: "10",
      b: "aGk=",
      arr: ["2024-01-02T03:04:05.000Z"],
      u: "AQI=",
    });
  });
  it("passes plain JSON through", () => {
    expect(encodeJsonValue({ a: 1, b: "x", c: [true, null] })).toEqual({ a: 1, b: "x", c: [true, null] });
    expect(decodeJsonValue(undefined)).toBeNull();
  });
});

describe("schema validation", () => {
  const S = Schema.Struct({
    id: UuidString,
    at: Schema.DateFromString,
    amount: Schema.BigIntFromString,
    data: BytesFromBase64,
  });
  const value = {
    id: "123e4567-e89b-42d3-a456-426614174000",
    at: new Date("2024-01-02T03:04:05.000Z"),
    amount: 42n,
    data: new Uint8Array([1, 2, 3]),
  };

  it("round-trips through encodeInput/decodeOutput", async () => {
    const wire = await Effect.runPromise(encodeInput(S, value));
    expect(wire).toEqual({
      id: value.id,
      at: "2024-01-02T03:04:05.000Z",
      amount: "42",
      data: "AQID",
    });
    const back = await Effect.runPromise(decodeOutput(S, wire));
    expect(back).toEqual(value);
  });

  it("fails with ZartSchemaError on invalid output", async () => {
    const r = await Effect.runPromise(Effect.result(decodeOutput(S, { id: "nope" })));
    expect(Result.isFailure(r)).toBe(true);
    if (Result.isFailure(r)) expect(r.failure).toBeInstanceOf(ZartSchemaError);
  });
});
