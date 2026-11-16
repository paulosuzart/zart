import type { NativeDispatcher, NativeStepSpec } from "../ts/src/native.js";
import type { NativeStepOutcome } from "../ts/src/errors.js";

const signal = (kind: string) => new Error(JSON.stringify({ _tag: "ControlFlowSignal", kind }));

/** In-memory dispatcher emulating Rust-side replay journal, retries and suspension. */
export class FakeDispatcher implements NativeDispatcher {
  readonly journal = new Map<string, NativeStepOutcome | unknown>();
  readonly events = new Map<string, unknown>();
  readonly runs: Record<string, number> = {};
  private handles: Promise<NativeStepOutcome>[] = [];

  async step(spec: NativeStepSpec, run: () => Promise<NativeStepOutcome>): Promise<NativeStepOutcome> {
    const key = `step:${spec.name}`;
    if (this.journal.has(key)) return this.journal.get(key) as NativeStepOutcome;
    const max = spec.retry?.maxAttempts ?? 1;
    let last: NativeStepOutcome = { kind: "business_err", message: "never ran" };
    for (let attempt = 1; attempt <= max; attempt++) {
      this.runs[spec.name] = (this.runs[spec.name] ?? 0) + 1;
      if (spec.timeoutMs !== undefined) {
        const timedOut = Symbol("timeout");
        const res = await Promise.race([
          run(),
          new Promise<typeof timedOut>((r) => setTimeout(() => r(timedOut), spec.timeoutMs)),
        ]);
        if (res === timedOut) {
          last = {
            kind: "zart_err",
            data: { _tag: "StepTimeoutError", stepName: spec.name, timeoutMs: spec.timeoutMs },
          };
          break;
        }
        last = res;
      } else {
        last = await run();
      }
      if (last.kind === "ok") break;
      if (attempt < max) await new Promise((r) => setTimeout(r, spec.retry?.delayMs ?? 0));
    }
    if (last.kind === "business_err" && max > 1) {
      last = {
        kind: "zart_err",
        data: { _tag: "RetryExhaustedError", stepName: spec.name, attempts: max, lastError: last.message },
      };
    }
    this.journal.set(key, last);
    return last;
  }
  async require(spec: NativeStepSpec, run: () => Promise<NativeStepOutcome>): Promise<unknown> {
    const out = await this.step(spec, run);
    if (out.kind === "ok") return out.value;
    throw new Error(JSON.stringify(out.data ?? { _tag: out.errorType ?? "StepFailedError", message: out.message }));
  }
  async stepOr(spec: NativeStepSpec, run: () => Promise<NativeStepOutcome>, defaultVal: unknown): Promise<unknown> {
    const out = await this.step(spec, run);
    return out.kind === "ok" ? out.value : defaultVal;
  }
  async schedule(spec: NativeStepSpec, run: () => Promise<NativeStepOutcome>): Promise<number> {
    this.handles.push(this.step(spec, run));
    return this.handles.length - 1;
  }
  wait(handle: number): Promise<NativeStepOutcome> {
    return this.handles[handle]!;
  }
  waitAll(handles: number[]): Promise<NativeStepOutcome[]> {
    return Promise.all(handles.map((h) => this.wait(h)));
  }
  async sleep(name: string, _ms: number): Promise<void> {
    const key = `sleep:${name}`;
    if (this.journal.has(key)) return;
    this.journal.set(key, true);
    throw signal("scheduled");
  }
  async sleepUntil(name: string, _t: string): Promise<void> {
    const key = `sleepUntil:${name}`;
    if (this.journal.has(key)) return;
    this.journal.set(key, true);
    throw signal("scheduled");
  }
  async waitForEvent(name: string): Promise<unknown> {
    if (this.events.has(name)) return this.events.get(name);
    throw signal("scheduled");
  }
  async capture(name: string, run: () => unknown | Promise<unknown>): Promise<unknown> {
    const key = `capture:${name}`;
    if (!this.journal.has(key)) this.journal.set(key, await run());
    return this.journal.get(key);
  }
  async now(_name: string): Promise<string> {
    return "2024-01-01T00:00:00.000Z";
  }
  context() {
    return { executionId: "e1", taskName: "t", data: null, currentAttempt: 1 };
  }
}
