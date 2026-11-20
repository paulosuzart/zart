"""Tests for Slice S2.3: Python DurableScheduler Ergonomia.

Covers:
1. start / start_in_tx accepting str | DurableTask and normalizing payloads via to_json_value.
2. offer_event normalizing payloads via to_json_value.
3. wait / wait_completion supporting DurationLike, timeout_secs/poll_interval_secs aliases,
   and target_type reconstruction via from_json_value.
4. start_and_wait combining start + wait_completion with typed input/output.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
import zart
from conftest import run_with_worker
from zart import DurableScheduler, PgBackend, WorkerBuilder


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


class Priority(Enum):
    HIGH = "high"
    LOW = "low"


@dataclass
class OrderInput:
    order_id: UUID
    priority: Priority
    created_at: datetime


@dataclass
class OrderOutput:
    order_id: UUID
    accepted: bool
    processed_at: datetime


@dataclass
class ApprovalEvent:
    approved_by: str
    approved_at: datetime


# ─────────────────────────────────────────────────────────────────────────────
# 1. Offline Unit Tests
# ─────────────────────────────────────────────────────────────────────────────


def _make_mocked_scheduler() -> tuple[DurableScheduler, MagicMock]:
    lazy_backend = PgBackend._connect_lazy("postgres://zart:zart@localhost:5432/zart")
    sched = DurableScheduler(lazy_backend)
    mock_native = MagicMock()
    mock_native.start = AsyncMock(return_value="schedule-res")
    mock_native.start_in_tx = AsyncMock(return_value="schedule-tx-res")
    mock_native.status = AsyncMock(return_value="status-rec")
    mock_native.wait = AsyncMock(return_value="wait-rec")
    mock_native.wait_completion = AsyncMock(return_value=None)
    mock_native.cancel = AsyncMock(return_value=True)
    mock_native.offer_event = AsyncMock(return_value=None)
    sched._native = mock_native  # type: ignore[assignment]
    return sched, mock_native


@pytest.mark.asyncio
async def test_scheduler_start_and_start_in_tx_normalize_payload_and_task() -> None:
    sched, mock_native = _make_mocked_scheduler()

    @zart.durable("unit-order-task")
    async def order_wf(data: OrderInput) -> OrderOutput:
        return OrderOutput(order_id=data.order_id, accepted=True, processed_at=data.created_at)

    oid = uuid4()
    dt = datetime(2026, 11, 20, 10, 30, 0, tzinfo=UTC)
    inp = OrderInput(order_id=oid, priority=Priority.HIGH, created_at=dt)
    expected_json = {
        "order_id": str(oid),
        "priority": "high",
        "created_at": dt.isoformat(),
    }

    res1 = await sched.start("exec-1", order_wf, inp)
    assert res1 == "schedule-res"
    mock_native.start.assert_awaited_once_with("exec-1", "unit-order-task", expected_json)

    fake_tx = MagicMock()
    res2 = await sched.start_in_tx(fake_tx, "exec-2", "raw-task-name", inp)
    assert res2 == "schedule-tx-res"
    mock_native.start_in_tx.assert_awaited_once_with(
        fake_tx, "exec-2", "raw-task-name", expected_json
    )

    with pytest.raises(TypeError, match="Expected str or DurableTask"):
        await sched.start("exec-bad", 123, None)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_scheduler_offer_event_normalizes_payload() -> None:
    sched, mock_native = _make_mocked_scheduler()
    dt = datetime(2026, 11, 20, 12, 0, 0, tzinfo=UTC)
    evt = ApprovalEvent(approved_by="alice", approved_at=dt)

    await sched.offer_event("exec-evt", "approval", evt)
    mock_native.offer_event.assert_awaited_once_with(
        "exec-evt",
        "approval",
        {"approved_by": "alice", "approved_at": dt.isoformat()},
    )


@pytest.mark.asyncio
async def test_scheduler_wait_duration_like_and_aliases() -> None:
    sched, mock_native = _make_mocked_scheduler()

    await sched.wait("exec-1")
    mock_native.wait.assert_awaited_with("exec-1", 30.0, None)

    await sched.wait("exec-2", "15s", "250ms")
    mock_native.wait.assert_awaited_with("exec-2", 15.0, 0.25)

    await sched.wait("exec-3", timeout=timedelta(minutes=1), poll_interval=0.5)
    mock_native.wait.assert_awaited_with("exec-3", 60.0, 0.5)

    await sched.wait("exec-4", timeout_secs=10.0, poll_interval_secs=0.05)
    mock_native.wait.assert_awaited_with("exec-4", 10.0, 0.05)

    with pytest.raises(TypeError, match="either 'timeout' or 'timeout_secs'"):
        await sched.wait("exec-5", 10.0, timeout_secs=5.0)

    with pytest.raises(TypeError, match="either 'poll_interval' or 'poll_interval_secs'"):
        await sched.wait("exec-6", poll_interval=0.1, poll_interval_secs=0.2)


@pytest.mark.asyncio
async def test_scheduler_wait_completion_and_start_and_wait_reconstruct_target_type() -> None:
    sched, mock_native = _make_mocked_scheduler()
    oid = uuid4()
    dt = datetime(2026, 11, 20, 14, 0, 0, tzinfo=UTC)
    mock_native.wait_completion = AsyncMock(
        return_value={
            "order_id": str(oid),
            "accepted": True,
            "processed_at": dt.isoformat(),
        }
    )

    @zart.durable("unit-start-wait")
    async def order_wf(data: OrderInput) -> OrderOutput:
        return OrderOutput(order_id=data.order_id, accepted=True, processed_at=data.created_at)

    inp = OrderInput(order_id=oid, priority=Priority.LOW, created_at=dt)
    out = await sched.start_and_wait(
        "exec-sw",
        order_wf,
        inp,
        timeout="10s",
        poll_interval="50ms",
        target_type=OrderOutput,
    )
    assert isinstance(out, OrderOutput)
    assert out.order_id == oid
    assert out.accepted is True
    assert out.processed_at == dt
    mock_native.start.assert_awaited_once()
    mock_native.wait_completion.assert_awaited_once_with("exec-sw", 10.0, 0.05)

    # Without target_type, returns raw dict
    raw = await sched.wait_completion("exec-raw", timeout_secs=5.0, poll_interval_secs=0.1)
    assert isinstance(raw, dict)
    assert raw["order_id"] == str(oid)


@pytest.mark.asyncio
async def test_scheduler_status_and_cancel_delegate() -> None:
    sched, mock_native = _make_mocked_scheduler()
    assert await sched.status("exec-s") == "status-rec"
    assert await sched.cancel("exec-c") is True
    mock_native.status.assert_awaited_once_with("exec-s")
    mock_native.cancel.assert_awaited_once_with("exec-c")


# ─────────────────────────────────────────────────────────────────────────────
# 2. PostgreSQL Integration Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_integration_scheduler_start_and_wait_typed_dataclasses(
    backend: PgBackend,
    scheduler: DurableScheduler,
) -> None:
    """Verify start_and_wait with DurableTask, InputDataclass -> OutputDataclass."""
    task_name = _uid("sched-typed-wf")

    @zart.step("process-order")
    async def process_order(order: OrderInput) -> OrderOutput:
        return OrderOutput(
            order_id=order.order_id,
            accepted=order.priority == Priority.HIGH,
            processed_at=order.created_at,
        )

    @zart.durable(task_name)
    async def order_workflow(data: OrderInput) -> OrderOutput:
        return await process_order(data)

    worker = WorkerBuilder(backend).poll_interval(0.05).register_durable(order_workflow).build()
    exec_id = _uid("exec-sw-typed")
    oid = uuid4()
    dt = datetime(2026, 11, 20, 15, 0, 0, tzinfo=UTC)
    inp = OrderInput(order_id=oid, priority=Priority.HIGH, created_at=dt)

    async def _run() -> OrderOutput:
        return await scheduler.start_and_wait(
            exec_id,
            order_workflow,
            inp,
            timeout="15s",
            poll_interval="50ms",
            target_type=OrderOutput,
        )

    out = await run_with_worker(worker, _run())
    assert isinstance(out, OrderOutput)
    assert out.order_id == oid
    assert out.accepted is True
    assert out.processed_at == dt


@pytest.mark.asyncio
async def test_integration_scheduler_start_in_tx_and_offer_event_dataclass(
    backend: PgBackend,
    scheduler: DurableScheduler,
) -> None:
    """Verify start_in_tx with DurableTask and offer_event with a dataclass payload."""
    task_name = _uid("sched-evt-wf")

    @zart.durable(task_name)
    async def approval_workflow(data: OrderInput) -> dict[str, Any]:
        evt: ApprovalEvent = await zart.wait_for_event(
            "manager-approval", timeout="10s", target_type=ApprovalEvent
        )
        return {
            "order_id": str(data.order_id),
            "approved_by": evt.approved_by,
            "approved_at": evt.approved_at.isoformat(),
        }

    worker = WorkerBuilder(backend).poll_interval(0.05).register_durable(approval_workflow).build()
    exec_id = _uid("exec-evt-dc")
    oid = uuid4()
    dt = datetime(2026, 11, 20, 16, 0, 0, tzinfo=UTC)
    inp = OrderInput(order_id=oid, priority=Priority.LOW, created_at=dt)
    evt = ApprovalEvent(approved_by="bob", approved_at=dt)

    async def _run() -> dict[str, Any]:
        async with await backend.begin() as tx:
            await scheduler.start_in_tx(tx, exec_id, approval_workflow, inp)
        # Wait briefly for workflow to reach wait_for_event, then offer dataclass event
        import asyncio

        await asyncio.sleep(0.25)
        await scheduler.offer_event(exec_id, "manager-approval", evt)
        return await scheduler.wait_completion(exec_id, timeout="15s", poll_interval="50ms")

    res = await run_with_worker(worker, _run())
    assert res["order_id"] == str(oid)
    assert res["approved_by"] == "bob"
    assert res["approved_at"] == dt.isoformat()
