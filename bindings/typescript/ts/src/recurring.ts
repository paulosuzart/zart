import { Duration } from "effect";

/**
 * Policy controlling how overlapping occurrences of a recurring durable execution
 * are handled when a new tick fires while the previous execution is still running.
 */
export type OverlapPolicy =
	| "SkipIfRunning"
	| "CancelAndRestart"
	| "AlwaysStart";

/** Companion constants for {@link OverlapPolicy}. */
export const OverlapPolicy = {
	SkipIfRunning: "SkipIfRunning",
	CancelAndRestart: "CancelAndRestart",
	AlwaysStart: "AlwaysStart",
} as const;

/** Recurrence schedule for recurring durable executions. */
export type Recurrence =
	| {
			readonly type: "fixed_delay";
			readonly durationMs: number;
	  }
	| {
			readonly type: "cron";
			readonly expression: string;
			readonly timezone: string;
	  };

/** Constructors for {@link Recurrence}. */
export const Recurrence = {
	/**
	 * Execute at a fixed delay after the previous run completes.
	 *
	 * Accepts Effect `Duration.Input` (e.g. `"500 millis"`, `"5 seconds"`,
	 * `Duration.seconds(10)`).
	 */
	fixedDelay: (delay: Duration.Input): Recurrence => ({
		type: "fixed_delay",
		durationMs: Duration.toMillis(delay),
	}),

	/**
	 * Execute on a 6-field cron schedule (`sec min hour dom month dow`) in the
	 * given IANA timezone (defaults to `"UTC"`).
	 */
	cron: (expression: string, timezone = "UTC"): Recurrence => ({
		type: "cron",
		expression,
		timezone,
	}),
} as const;
