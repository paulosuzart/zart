"""Human-in-the-loop approval workflow example for Zart Python bindings.

Demonstrates:
1. ``@zart.durable`` and ``@zart.step`` with typed ``dataclass`` payloads.
2. ``zart.wait_for_event("manager-approval", ...)`` suspending until an event arrives.
3. ``scheduler.offer_event(...)`` resuming the execution to completion.

Run:
    uv run python examples/approval_workflow.py
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from uuid import uuid4

import zart


@dataclass
class ApprovalRequest:
    requester_name: str
    resource: str
    reason: str


@dataclass
class ApprovalDecision:
    approved: bool
    reviewer: str
    comment: str


@dataclass
class ApprovalOutput:
    decision: str
    requester: str
    resource: str
    reviewer: str
    comment: str


@zart.step("validate-request")
async def validate_request(request: ApprovalRequest) -> str:
    if not request.requester_name:
        raise ValueError("requester_name is required")
    return f"Validated request from {request.requester_name} for {request.resource}"


@zart.step("process-approved")
async def process_approved(resource: str, requester: str) -> str:
    return f"Provisioned {resource} for {requester}"


@zart.durable("approval-task")
async def approval_workflow(data: ApprovalRequest) -> ApprovalOutput:
    await zart.require(validate_request(data))

    decision: ApprovalDecision = await zart.wait_for_event(
        "manager-approval",
        timeout="2m",
        target_type=ApprovalDecision,
    )

    if decision.approved:
        await zart.require(process_approved(data.resource, data.requester_name))

    return ApprovalOutput(
        decision="approved" if decision.approved else "rejected",
        requester=data.requester_name,
        resource=data.resource,
        reviewer=decision.reviewer,
        comment=decision.comment,
    )


async def main() -> None:
    db_url = os.environ.get("DATABASE_URL", "postgres://zart:zart@localhost:5432/zart")
    backend = await zart.PgBackend.connect(db_url)
    try:
        await backend.run_migrations()
        scheduler = zart.DurableScheduler(backend)

        execution_id = f"approval-demo-{uuid4()}"
        request = ApprovalRequest(
            requester_name="Bob",
            resource="staging-environment",
            reason="Need to test the new deployment pipeline",
        )
        await scheduler.start(execution_id, "approval-task", zart.to_json_value(request))
        print(f"Started approval execution {execution_id}")

        worker = zart.WorkerBuilder(backend).register_durable(approval_workflow).build()
        worker_task = asyncio.ensure_future(worker.run())
        try:
            await asyncio.sleep(0.5)
            decision = ApprovalDecision(
                approved=True,
                reviewer="Alice (Manager)",
                comment="Approved for staging rollout",
            )
            await scheduler.offer_event(
                execution_id,
                "manager-approval",
                zart.to_json_value(decision),
            )
            print("Delivered manager-approval event")

            raw_result = await scheduler.wait_completion(execution_id, timeout_secs=30.0)
            output = zart.from_json_value(raw_result, ApprovalOutput)
        finally:
            worker.stop()
            await worker_task

        assert output.decision == "approved", f"Expected approved, got {output.decision}"
        print(
            f"Completed: decision={output.decision}, "
            f"resource={output.resource}, reviewer={output.reviewer}"
        )
    finally:
        await backend.close()


if __name__ == "__main__":
    asyncio.run(main())
