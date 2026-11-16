import { Effect } from "effect";
import { describe, expect, it } from "vitest";
import {
	ExecutionNotFound,
	mapNativeError,
	StepFailedError,
	toStepOutcome,
	WaitTimedOut,
	ZartBackendError,
	ZartError,
	ZartSchemaError,
} from "../ts/src/errors.js";

describe("errors", () => {
	it("tagged errors have _tag and are catchable by tag", async () => {
		const r = await Effect.fail(new ZartBackendError({ message: "x" })).pipe(
			Effect.catchTag("ZartBackendError", (e) => Effect.succeed(e.message)),
			Effect.runPromise,
		);
		expect(r).toBe("x");
	});

	it("maps native JSON error payloads", () => {
		expect(
			mapNativeError(
				new Error(JSON.stringify({ _tag: "ZartSchemaError", message: "bad" })),
			),
		).toBeInstanceOf(ZartSchemaError);
		expect(
			mapNativeError(
				new Error(JSON.stringify({ _tag: "ExecutionNotFound", message: "n" })),
			),
		).toBeInstanceOf(ExecutionNotFound);
		expect(
			mapNativeError(
				new Error(JSON.stringify({ _tag: "WaitTimedOut", message: "t" })),
			),
		).toBeInstanceOf(WaitTimedOut);
	});

	it("falls back to ZartError for non-JSON / unknown", () => {
		const e = mapNativeError(new Error("boom"));
		expect(e).toBeInstanceOf(ZartError);
		expect(e.message).toBe("boom");
		expect(mapNativeError({ _tag: "Nope", message: "m" })).toBeInstanceOf(
			ZartError,
		);
	});

	it("maps StepOutcome", () => {
		expect(toStepOutcome({ kind: "ok", value: 1 })).toEqual({
			_tag: "ok",
			value: 1,
		});
		expect(
			toStepOutcome({
				kind: "business_err",
				errorType: "E",
				message: "m",
				data: 2,
			}),
		).toEqual({
			_tag: "business_err",
			errorType: "E",
			message: "m",
			data: 2,
		});
		const z = toStepOutcome({
			kind: "zart_err",
			data: { _tag: "StepFailedError", stepName: "s", message: "m" },
		});
		expect(z._tag).toBe("zart_err");
		if (z._tag === "zart_err") expect(z.error).toBeInstanceOf(StepFailedError);
	});
});
