"""Integration tests for durable execution, replay, retries, parallelism, and events."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
import zart
from conftest import run_with_worker
from zart import DurableScheduler, PgBackend, Retry, WorkerBuilder


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


@pytest.mark.asyncio
async def test_deterministic_step_replay(backend: PgBackend, scheduler: DurableScheduler) -> None:
    calls: dict[str, int] = {"step_one": 0, "step_two": 0}

    @zart.step("replay-step-one")
    async def step_one(x: int) -> int:
        calls["step_one"] += 1
        return x + 1

    @zart.step("replay-step-two")
    async def step_two(y: int) -> int:
        calls["step_two"] += 1
        return y * 10

    task_name = _uid("replay-wf")

    @zart.durable(task_name)
    async def workflow(x: int) -> int:
        a = await step_one(x)
        b = await step_two(a)
        return b

    worker = WorkerBuilder(backend).poll_interval(0.05).register_durable(workflow).build()
    exec_id = _uid("exec-replay")

    async def _run() -> int:
        await scheduler.start(exec_id, task_name, 4)
        return await scheduler.wait_completion(exec_id, timeout_secs=10.0, poll_interval_secs=0.05)

    result = await run_with_worker(worker, _run())
    assert result == 50
    assert calls["step_one"] == 1
    assert calls["step_two"] == 1


@pytest.mark.asyncio
async def test_loop_named_steps(backend: PgBackend, scheduler: DurableScheduler) -> None:
    @zart.step("process-item")
    async def process_item(i: int) -> int:
        return i * 2

    task_name = _uid("loop-wf")

    @zart.durable(task_name)
    async def workflow(n: int) -> list[int]:
        out: list[int] = []
        for i in range(n):
            val = await process_item(i).named(f"item-{i}")
            out.append(val)
        return out

    worker = WorkerBuilder(backend).poll_interval(0.05).register_durable(workflow).build()
    exec_id = _uid("exec-loop")

    async def _run() -> list[int]:
        await scheduler.start(exec_id, task_name, 3)
        return await scheduler.wait_completion(exec_id, timeout_secs=10.0, poll_interval_secs=0.05)

    result = await run_with_worker(worker, _run())
    assert result == [0, 2, 4]

    async with await backend.begin() as tx:
        rows = await tx.fetch_all(
            "SELECT step_name, status::text AS status FROM zart_steps "
            "WHERE run_id LIKE $1 ORDER BY step_name",
            f"{exec_id}:%",
        )
    assert [(r["step_name"], r["status"]) for r in rows] == [
        ("item-0", "completed"),
        ("item-1", "completed"),
        ("item-2", "completed"),
    ]


@pytest.mark.asyncio
async def test_step_retries_fixed(backend: PgBackend, scheduler: DurableScheduler) -> None:
    attempts = 0

    @zart.step("flaky-step", retry=Retry.fixed(2, "50ms"))
    async def flaky_step() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise RuntimeError("transient error")
        return "recovered"

    task_name = _uid("retry-wf")

    @zart.durable(task_name)
    async def workflow(_data: None) -> str:
        return await flaky_step()

    worker = WorkerBuilder(backend).poll_interval(0.05).register_durable(workflow).build()
    exec_id = _uid("exec-retry")

    async def _run() -> str:
        await scheduler.start(exec_id, task_name, None)
        return await scheduler.wait_completion(exec_id, timeout_secs=10.0, poll_interval_secs=0.05)

    result = await run_with_worker(worker, _run())
    assert result == "recovered"
    assert attempts == 2


@pytest.mark.asyncio
async def test_parallel_schedule_and_wait(backend: PgBackend, scheduler: DurableScheduler) -> None:
    @zart.step("par-compute")
    async def compute(i: int) -> int:
        return i * 10

    task_name = _uid("par-wf")

    @zart.durable(task_name)
    async def workflow(_data: Any) -> list[int]:
        handles = [zart.schedule(compute(i).named(f"par-{i}")) for i in range(3)]
        outcomes = await zart.wait(handles)
        assert all(o.is_ok for o in outcomes)
        return [o.unwrap() for o in outcomes]

    worker = WorkerBuilder(backend).poll_interval(0.05).register_durable(workflow).build()
    exec_id = _uid("exec-par")

    async def _run() -> list[int]:
        await scheduler.start(exec_id, task_name, None)
        return await scheduler.wait_completion(exec_id, timeout_secs=10.0, poll_interval_secs=0.05)

    result = await run_with_worker(worker, _run())
    assert result == [0, 10, 20]


@pytest.mark.asyncio
async def test_sleep_and_wait_for_event(backend: PgBackend, scheduler: DurableScheduler) -> None:
    task_name = _uid("event-wf")

    @zart.durable(task_name)
    async def workflow(_data: None) -> dict[str, object]:
        await zart.sleep("short-nap", "50ms")
        payload = await zart.wait_for_event("approval-evt", timeout="5s")
        return payload

    worker = WorkerBuilder(backend).poll_interval(0.05).register_durable(workflow).build()
    exec_id = _uid("exec-event")

    async def _run() -> dict[str, object]:
        await scheduler.start(exec_id, task_name, None)

        async def _deliver() -> None:
            for _ in range(50):
                await asyncio.sleep(0.1)
                try:
                    await scheduler.offer_event(exec_id, "approval-evt", {"approved": True})
                    return
                except LookupError:
                    continue
            raise AssertionError("Timed out waiting to deliver approval-evt")

        deliver_task = asyncio.create_task(_deliver())
        res = await scheduler.wait_completion(exec_id, timeout_secs=10.0, poll_interval_secs=0.05)
        await deliver_task
        return res

    result = await run_with_worker(worker, _run())
    assert result == {"approved": True}


@pytest.mark.asyncio
async def test_except_exception_does_not_swallow_control_flow(
    backend: PgBackend, scheduler: DurableScheduler
) -> None:
    @zart.step("cf-step")
    async def cf_step() -> str:
        return "real-result"

    task_name = _uid("cf-wf")

    @zart.durable(task_name)
    async def workflow(_data: None) -> str:
        try:
            return await zart.require(cf_step())
        except Exception:
            return "swallowed"

    worker = WorkerBuilder(backend).poll_interval(0.05).register_durable(workflow).build()
    exec_id = _uid("exec-cf")

    async def _run() -> str:
        await scheduler.start(exec_id, task_name, None)
        return await scheduler.wait_completion(exec_id, timeout_secs=10.0, poll_interval_secs=0.05)

    result = await run_with_worker(worker, _run())
    assert result == "real-result"
