import { Effect, Schema } from "effect";
import { ClientTrx, PgBackend, fromNative } from "./backend.js";
import { loadNative, type NativeDurableScheduler, type NativeExecutionRecord } from "./native.js";
import { encodeInput, encodeJsonValue, decodeOutput } from "./schema.js";
import type { ZartTaggedError, ZartSchemaError } from "./errors.js";

export type StartInTxOptions<A = unknown, I = A> =
  | {
      readonly executionId: string;
      readonly taskName: string;
      readonly inputSchema: Schema.Codec<A, I>;
      readonly input: A;
    }
  | {
      readonly executionId: string;
      readonly taskName: string;
      readonly payload: unknown;
    };

export class DurableScheduler {
  private constructor(readonly native: NativeDurableScheduler) {}

  static fromBackend(backend: PgBackend): Effect.Effect<DurableScheduler> {
    return Effect.sync(() => new DurableScheduler(loadNative().DurableScheduler.fromBackend(backend.native)));
  }

  start<A, I>(
    executionId: string,
    taskName: string,
    inputSchema: Schema.Codec<A, I>,
    input: A,
  ): Effect.Effect<{ taskId: string; executionTime: string }, ZartTaggedError | ZartSchemaError> {
    return encodeInput(inputSchema, input).pipe(
      Effect.flatMap((payload) => fromNative(() => this.native.start(executionId, taskName, payload))),
    );
  }

  /** Start a durable execution atomically inside an existing `ClientTrx` (`DurableScheduler::start_in_tx`). */
  startInTx<A = unknown, I = A>(
    tx: ClientTrx,
    opts: StartInTxOptions<A, I>,
  ): Effect.Effect<{ taskId: string; executionTime: string }, ZartTaggedError | ZartSchemaError> {
    const encoded =
      "inputSchema" in opts
        ? encodeInput(opts.inputSchema, opts.input)
        : Effect.succeed(encodeJsonValue(opts.payload));
    return encoded.pipe(
      Effect.flatMap((payload) =>
        fromNative(() => this.native.startInTx(tx.native, opts.executionId, opts.taskName, payload)),
      ),
    );
  }

  status(id: string): Effect.Effect<NativeExecutionRecord, ZartTaggedError> {
    return fromNative(() => this.native.status(id));
  }

  wait(id: string, timeoutSecs?: number, pollIntervalSecs?: number) {
    return fromNative(() => this.native.wait(id, timeoutSecs, pollIntervalSecs));
  }

  waitCompletion<A, I>(
    id: string,
    outputSchema: Schema.Codec<A, I>,
    timeoutSecs?: number,
    pollIntervalSecs?: number,
  ): Effect.Effect<A, ZartTaggedError | ZartSchemaError> {
    return fromNative(() => this.native.waitCompletion(id, timeoutSecs, pollIntervalSecs)).pipe(
      Effect.flatMap((out) => decodeOutput(outputSchema, out)),
    );
  }

  cancel(id: string): Effect.Effect<boolean, ZartTaggedError> {
    return fromNative(() => this.native.cancel(id));
  }

  offerEvent<A, I>(
    id: string,
    eventName: string,
    schema: Schema.Codec<A, I>,
    payload: A,
  ): Effect.Effect<void, ZartTaggedError | ZartSchemaError> {
    return encodeInput(schema, payload).pipe(
      Effect.flatMap((p) => fromNative(() => this.native.offerEvent(id, eventName, p))),
    );
  }
}
