import { Duration } from "effect";
import { describe, expect, it } from "vitest";
import {
	type NativeWorkerBuilder,
	WorkerBuilder,
	ZartSchemaError,
} from "../ts/src/index.js";

interface RecordedWorkerCalls {
	pollIntervalSecs: number[];
	maxConcurrentTasks: number[];
	maxTasksPerPoll: number[];
	shutdownTimeoutSecs: number[];
	orphanTimeoutSecs: number[];
	heartbeatIntervalSecs: Array<number | null | undefined>;
	enableListenNotify: boolean[];
}

const makeRecordingBuilder = () => {
	const calls: RecordedWorkerCalls = {
		pollIntervalSecs: [],
		maxConcurrentTasks: [],
		maxTasksPerPoll: [],
		shutdownTimeoutSecs: [],
		orphanTimeoutSecs: [],
		heartbeatIntervalSecs: [],
		enableListenNotify: [],
	};

	const native: NativeWorkerBuilder = {
		registerDurable: () => {},
		pollIntervalSecs: (n) => {
			if (!Number.isFinite(n) || n < 0) {
				throw new Error(
					JSON.stringify({
						_tag: "ZartSchemaError",
						message: `invalid pollIntervalSecs: ${n}`,
					}),
				);
			}
			calls.pollIntervalSecs.push(n);
		},
		maxConcurrentTasks: (n) => {
			if (n === 0) {
				throw new Error(
					JSON.stringify({
						_tag: "ZartSchemaError",
						message: "maxConcurrentTasks must be at least 1",
					}),
				);
			}
			calls.maxConcurrentTasks.push(n);
		},
		maxTasksPerPoll: (n) => {
			if (n === 0) {
				throw new Error(
					JSON.stringify({
						_tag: "ZartSchemaError",
						message: "maxTasksPerPoll must be at least 1",
					}),
				);
			}
			calls.maxTasksPerPoll.push(n);
		},
		shutdownTimeoutSecs: (n) => {
			calls.shutdownTimeoutSecs.push(n);
		},
		orphanTimeoutSecs: (n) => {
			calls.orphanTimeoutSecs.push(n);
		},
		heartbeatIntervalSecs: (n) => {
			calls.heartbeatIntervalSecs.push(n);
		},
		enableListenNotify: (enabled) => {
			calls.enableListenNotify.push(enabled);
		},
		build: () => ({ run: async () => {}, stop: () => {} }),
	};

	const builder = new (
		WorkerBuilder as unknown as {
			new (n: NativeWorkerBuilder): WorkerBuilder;
		}
	)(native);

	return { builder, calls };
};

describe("WorkerBuilder.configure", () => {
	it("supports numeric *Secs options and enableListenNotify", () => {
		const { builder, calls } = makeRecordingBuilder();
		builder.configure({
			pollIntervalSecs: 2.5,
			maxConcurrentTasks: 8,
			maxTasksPerPoll: 4,
			shutdownTimeoutSecs: 15,
			orphanTimeoutSecs: 120,
			heartbeatIntervalSecs: 30,
			enableListenNotify: false,
		});

		expect(calls).toEqual({
			pollIntervalSecs: [2.5],
			maxConcurrentTasks: [8],
			maxTasksPerPoll: [4],
			shutdownTimeoutSecs: [15],
			orphanTimeoutSecs: [120],
			heartbeatIntervalSecs: [30],
			enableListenNotify: [false],
		});
	});

	it("supports Effect Duration.Input options", () => {
		const { builder, calls } = makeRecordingBuilder();
		builder.configure({
			pollInterval: "5 seconds",
			shutdownTimeout: Duration.millis(1500),
			orphanTimeout: { minutes: 2 },
			heartbeatInterval: 500,
			enableListenNotify: true,
		});

		expect(calls).toEqual({
			pollIntervalSecs: [5],
			maxConcurrentTasks: [],
			maxTasksPerPoll: [],
			shutdownTimeoutSecs: [1.5],
			orphanTimeoutSecs: [120],
			heartbeatIntervalSecs: [0.5],
			enableListenNotify: [true],
		});
	});

	it("handles heartbeatInterval null and Duration.zero explicitly while leaving omitted options untouched", () => {
		const { builder, calls } = makeRecordingBuilder();
		builder.configure({ maxConcurrentTasks: 4 });
		expect(calls.heartbeatIntervalSecs).toEqual([]);

		builder.configure({ heartbeatInterval: Duration.zero });
		builder.configure({ heartbeatInterval: null });
		builder.configure({ heartbeatIntervalSecs: null });

		expect(calls.heartbeatIntervalSecs).toEqual([0, null, null]);
	});

	it("prefers Duration.Input over *Secs when both are supplied (including null)", () => {
		const { builder, calls } = makeRecordingBuilder();
		builder.configure({
			pollInterval: "3 seconds",
			pollIntervalSecs: 99,
			shutdownTimeout: Duration.seconds(10),
			shutdownTimeoutSecs: 99,
			orphanTimeout: { minutes: 1 },
			orphanTimeoutSecs: 999,
			heartbeatInterval: null,
			heartbeatIntervalSecs: 15,
		});

		expect(calls).toEqual({
			pollIntervalSecs: [3],
			maxConcurrentTasks: [],
			maxTasksPerPoll: [],
			shutdownTimeoutSecs: [10],
			orphanTimeoutSecs: [60],
			heartbeatIntervalSecs: [null],
			enableListenNotify: [],
		});
	});

	it("surfaces ZartSchemaError when rejecting invalid limits or durations", () => {
		const { builder } = makeRecordingBuilder();
		expect(() => builder.configure({ maxConcurrentTasks: 0 })).toThrow(
			ZartSchemaError,
		);
		expect(() => builder.configure({ maxTasksPerPoll: 0 })).toThrow(
			ZartSchemaError,
		);
		expect(() => builder.configure({ pollInterval: -100 })).toThrow(
			ZartSchemaError,
		);
	});
});
