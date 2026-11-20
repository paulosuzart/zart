import { Effect, Schema } from "effect";
import { ZartSchemaError } from "./errors.js";

/**
 * JSON value encoding for the FFI boundary (native side only accepts plain JSON).
 * Date -> RFC3339 string, bigint -> decimal string, Uint8Array/Buffer -> base64 string.
 */
export function encodeJsonValue(value: unknown): unknown {
	if (value === undefined) return null;
	if (value === null) return null;
	if (value instanceof Date) return value.toISOString();
	if (typeof value === "bigint") return value.toString();
	if (value instanceof Uint8Array) return Buffer.from(value).toString("base64");
	if (Array.isArray(value)) return value.map(encodeJsonValue);
	if (typeof value === "object") {
		const out: Record<string, unknown> = {};
		for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
			if (v !== undefined) out[k] = encodeJsonValue(v);
		}
		return out;
	}
	return value;
}

/** Decode plain JSON from the native side (identity; typed revival happens via Schema). */
export function decodeJsonValue(value: unknown): unknown {
	return value === undefined ? null : value;
}

/** Common wire-aware schemas. */
export const DateFromRfc3339 = Schema.DateFromString;
export const BigIntFromDecimal = Schema.BigIntFromString;
export const BytesFromBase64 = Schema.Uint8ArrayFromBase64;
export const UuidString = Schema.String.check(Schema.isUUID());

const toSchemaError = (e: Schema.SchemaError) =>
	new ZartSchemaError({ message: e.message });

/** Validate + encode a typed input into plain JSON for the native layer. */
export const encodeInput = <A, I, RD, RE>(
	schema: Schema.Codec<A, I, RD, RE>,
	input: A,
): Effect.Effect<unknown, ZartSchemaError, RE> =>
	Schema.encodeEffect(schema)(input).pipe(
		Effect.map(encodeJsonValue),
		Effect.mapError(toSchemaError),
	);

/** Decode native JSON output into a typed value, validating with the schema. */
export const decodeOutput = <A, I, RD, RE>(
	schema: Schema.Codec<A, I, RD, RE>,
	output: unknown,
): Effect.Effect<A, ZartSchemaError, RD> =>
	Schema.decodeUnknownEffect(schema)(decodeJsonValue(output)).pipe(
		Effect.mapError(toSchemaError),
	);
