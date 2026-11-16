import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Data, Effect, Fiber, Schema } from "effect";
import { DurableScheduler, PgBackend, WorkerBuilder, Zart } from "../dist/index.js";

class ValidationError extends Data.TaggedError("ValidationError")<{ readonly message: string }> {}

const ApprovalRequest = Schema.Struct({
  requesterName: Schema.String,
  resource: Schema.String,
  reason: Schema.String,
});

const ApprovalDecision = Schema.Struct({
  approved: Schema.Boolean,
  reviewer: Schema.String,
  comment: Schema.String,
});

const ApprovalOutput = Schema.Struct({
  decision: Schema.String,
  requester: Schema.String,
  resource: Schema.String,
  reviewer: Schema.String,
  comment: Schema.String,
});

const validateRequest = Zart.step(
  "validate-request",
  (request: typeof ApprovalRequest.Type) =>
    request.requesterName
      ? Effect.succeed(`Validated request from ${request.requesterName} for ${request.resource}`)
      : Effect.fail(new ValidationError({ message: "requesterName is required" })),
);

const processApproved = Zart.step("process-approved", (resource: string, requester: string) =>
  Effect.succeed(`Provisioned ${resource} for ${requester}`),
);

const approvalWorkflow = Zart.durable(
  "approval-task",
  (data) =>
    Effect.gen(function* () {
      yield* validateRequest(data);

      const decision = yield* Zart.waitForEvent("manager-approval", {
        schema: ApprovalDecision,
        timeout: "2 minutes",
      });

      if (decision.approved) {
        yield* processApproved(data.resource, data.requesterName);
      }

      return {
        decision: decision.approved ? "approved" : "rejected",
        requester: data.requesterName,
        resource: data.resource,
        reviewer: decision.reviewer,
        comment: decision.comment,
      };
    }),
  { inputSchema: ApprovalRequest, outputSchema: ApprovalOutput },
);

const main = Effect.scoped(
  Effect.gen(function* () {
    const dbUrl = process.env.DATABASE_URL ?? "postgres://zart:zart@localhost:5432/zart";
    const backend = yield* PgBackend.scoped(dbUrl);
    yield* backend.runMigrations();

    const scheduler = yield* DurableScheduler.fromBackend(backend);
    const executionId = `approval-demo-${randomUUID()}`;

    yield* scheduler.start(executionId, "approval-task", ApprovalRequest, {
      requesterName: "Bob",
      resource: "staging-environment",
      reason: "Need to test the new deployment pipeline",
    });

    const worker = (yield* WorkerBuilder.fromBackend(backend)).registerDurable(approvalWorkflow).build();
    const workerFiber = yield* Effect.forkChild(worker.runScoped());

    const output = yield* Effect.gen(function* () {
      yield* Effect.sleep("500 millis");
      yield* scheduler.offerEvent(executionId, "manager-approval", ApprovalDecision, {
        approved: true,
        reviewer: "Alice (Manager)",
        comment: "Approved for staging rollout",
      });
      return yield* scheduler.waitCompletion(executionId, ApprovalOutput, 30);
    }).pipe(Effect.ensuring(Fiber.interrupt(workerFiber)));

    assert.equal(output.decision, "approved", `Expected approved, got ${output.decision}`);
    console.log(
      `Completed: decision=${output.decision}, resource=${output.resource}, reviewer=${output.reviewer}`,
    );
  }),
);

await Effect.runPromise(main);
