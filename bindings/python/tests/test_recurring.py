"""Tests for Slice S2.4: Recurring Durable Executions.

Covers:
1. Recurrence.fixed_delay (DurationLike) and Recurrence.cron (expression + timezone).
2. OverlapPolicy variants, snake_case aliases, equality, and __repr__.
3. WorkerBuilder.register_recurring_durable validation (missing {occurrence} raises ValueError)
   and automatic durable handler registration.
4. DurableScheduler.stop_recurring with str | DurableTask.
5. Live PostgreSQL integration test running a FixedDelay recurring durable task for ~2 ticks
   and stopping it with scheduler.stop_recurring(...).
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
import zart
from conftest import run_with_worker
from zart import (
    DurableScheduler,
    OverlapPolicy,
    PgBackend,
    Recurrence,
    WorkerBuilder,
)


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


@dataclass
class RecurringPayload:
    tag: str


# ─────────────────────────────────────────────────────────────────────────────
# 1. Offline Unit Tests
# ─────────────────────────────────────────────────────────────────────────────


def test_recurrence_fixed_delay_and_validation() -> None:
    r1 = Recurrence.fixed_delay("500ms")
    assert r1.kind == "fixed_delay"
    assert r1.duration_ms == 500
    assert r1.expression is None
    assert r1.timezone is None
    assert "500" in repr(r1)

    r2 = Recurrence.fixed_delay(timedelta(seconds=2))
    assert r2.duration_ms == 2000

    r3 = Recurrence.fixed_delay(1.5)
    assert r3.duration_ms == 1500

    with pytest.raises(ValueError, match="non-negative"):
        Recurrence.fixed_delay(-1)

    with pytest.raises(ValueError, match="non-negative finite"):
        Recurrence.fixed_delay(float("nan"))

    with pytest.raises(ValueError, match="Invalid duration"):
        Recurrence.fixed_delay("10w")

    with pytest.raises(TypeError):
        Recurrence.fixed_delay(None)


def test_recurrence_cron_and_validation() -> None:
    r1 = Recurrence.cron("0 */5 * * * *")
    assert r1.kind == "cron"
    assert r1.expression == "0 */5 * * * *"
    assert r1.timezone == "UTC"
    assert r1.duration_ms is None
    assert "0 */5 * * * *" in repr(r1)

    r2 = Recurrence.cron("0 0 9 * * *", timezone="America/Sao_Paulo")
    assert r2.timezone == "America/Sao_Paulo"

    with pytest.raises(ValueError, match="empty"):
        Recurrence.cron("   ")

    with pytest.raises(ValueError, match="empty"):
        Recurrence.cron("0 * * * * *", timezone="")


def test_overlap_policy_variants_and_equality() -> None:
    assert OverlapPolicy.SkipIfRunning == OverlapPolicy.skip_if_running
    assert OverlapPolicy.CancelAndRestart == OverlapPolicy.cancel_and_restart
    assert OverlapPolicy.AlwaysStart == OverlapPolicy.always_start
    assert OverlapPolicy.SkipIfRunning != OverlapPolicy.AlwaysStart
    assert repr(OverlapPolicy.SkipIfRunning) == "OverlapPolicy.SkipIfRunning"
    assert repr(OverlapPolicy.CancelAndRestart) == "OverlapPolicy.CancelAndRestart"
    assert repr(OverlapPolicy.AlwaysStart) == "OverlapPolicy.AlwaysStart"


def test_register_recurring_durable_validates_id_template() -> None:
    lazy_backend = PgBackend._connect_lazy("postgres://zart:zart@localhost:5432/zart")

    @zart.durable("unit-recurring-task")
    async def my_task(_data: RecurringPayload) -> str:
        return "ok"

    builder = WorkerBuilder(lazy_backend)
    with pytest.raises(ValueError, match=r"\{occurrence\}"):
        builder.register_recurring_durable(
            my_task,
            "missing-placeholder",
            Recurrence.fixed_delay("1s"),
        )

    # Valid registration with dataclass initial_data
    chained = builder.register_recurring_durable(
        my_task,
        "unit-rec-{occurrence}",
        Recurrence.fixed_delay("1s"),
        overlap=OverlapPolicy.CancelAndRestart,
        initial_data=RecurringPayload(tag="v1"),
    )
    assert chained is builder


@pytest.mark.asyncio
async def test_scheduler_stop_recurring_accepts_str_or_durable_task() -> None:
    lazy_backend = PgBackend._connect_lazy("postgres://zart:zart@localhost:5432/zart")
    sched = DurableScheduler(lazy_backend)
    mock_native = MagicMock()
    mock_native.stop_recurring = AsyncMock(return_value=None)
    sched._native = mock_native  # type: ignore[assignment]

    @zart.durable("unit-stop-rec-task")
    async def my_task(_data: None) -> None:
        return None

    await sched.stop_recurring(my_task)
    mock_native.stop_recurring.assert_awaited_with("unit-stop-rec-task")

    await sched.stop_recurring("explicit-task-id")
    mock_native.stop_recurring.assert_awaited_with("explicit-task-id")


# ─────────────────────────────────────────────────────────────────────────────
# 2. PostgreSQL Integration Test
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_integration_recurring_durable_executions_and_stop(
    backend: PgBackend,
    scheduler: DurableScheduler,
) -> None:
    """Register a FixedDelay recurring task, let ~2 occurrences fire, then stop it."""
    task_name = _uid("rec-wf")
    exec_prefix = _uid("rec-occ")
    seen_tags: list[str] = []
    two_runs_done = asyncio.Event()

    @zart.durable(task_name)
    async def recurring_wf(data: RecurringPayload) -> str:
        seen_tags.append(data.tag)
        if len(seen_tags) >= 2:
            two_runs_done.set()
        return f"ran:{data.tag}"

    worker = (
        WorkerBuilder(backend)
        .poll_interval(0.05)
        .register_recurring_durable(
            recurring_wf,
            f"{exec_prefix}-{{occurrence}}",
            Recurrence.fixed_delay("100ms"),
            overlap=OverlapPolicy.SkipIfRunning,
            initial_data=RecurringPayload(tag="heartbeat"),
        )
        .build()
    )

    async def _run() -> None:
        try:
            await asyncio.wait_for(two_runs_done.wait(), timeout=15.0)
            await scheduler.stop_recurring(recurring_wf)

            rec0 = await scheduler.wait(f"{exec_prefix}-0", timeout="10s", poll_interval="50ms")
            rec1 = await scheduler.wait(f"{exec_prefix}-1", timeout="10s", poll_interval="50ms")
            assert rec0.status == "completed"
            assert rec0.result == "ran:heartbeat"
            assert rec1.status == "completed"
            assert rec1.result == "ran:heartbeat"
        finally:
            try:
                await scheduler.stop_recurring(recurring_wf)
            except Exception:
                pass

    await run_with_worker(worker, _run())
