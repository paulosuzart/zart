"""Tests for Slice S3.1: Python DurableScheduler Query & Introspection.

Covers:
1. ListExecutionsParams construction, defaults, datetime handling, and validation.
2. DurableScheduler.list_executions(params=..., **kwargs) offline wrapper behavior.
3. Integration tests against PostgreSQL for:
   - list_executions (filters, pagination, sorting)
   - stats
   - get_current_run_id
   - list_runs
   - execution_detail (default current run and explicit run_id, steps + attempts)
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
import zart
from conftest import run_with_worker
from zart import (
    DurableScheduler,
    ExecutionDetail,
    ExecutionRecord,
    ExecutionRunRecord,
    ExecutionStats,
    ListExecutionsParams,
    PgBackend,
    StepAttemptRow,
    StepRow,
    StepWithAttempts,
    WorkerBuilder,
)


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _make_mocked_scheduler() -> tuple[DurableScheduler, MagicMock]:
    lazy_backend = PgBackend._connect_lazy("postgres://zart:zart@localhost:5432/zart")
    sched = DurableScheduler(lazy_backend)
    mock_native = MagicMock()
    mock_native.list_executions = AsyncMock(return_value=[])
    mock_native.stats = AsyncMock(return_value="stats-res")
    mock_native.get_current_run_id = AsyncMock(return_value="run-1")
    mock_native.list_runs = AsyncMock(return_value=[])
    mock_native.execution_detail = AsyncMock(return_value="detail-res")
    sched._native = mock_native  # type: ignore[assignment]
    return sched, mock_native


# ─────────────────────────────────────────────────────────────────────────────
# 1. Offline Unit Tests
# ─────────────────────────────────────────────────────────────────────────────


def test_list_executions_params_defaults_and_validation() -> None:
    defaults = ListExecutionsParams()
    assert defaults.status is None
    assert defaults.task_name is None
    assert defaults.from_ is None
    assert defaults.to is None
    assert defaults.search is None
    assert defaults.sort_by == "scheduled_at"
    assert defaults.sort_order == "desc"
    assert defaults.limit == 20
    assert defaults.offset == 0

    dt_aware = datetime(2026, 11, 20, 10, 0, 0, tzinfo=UTC)
    dt_naive = datetime(2026, 11, 20, 12, 0, 0)
    custom = ListExecutionsParams(
        status="completed",
        task_name="orders",
        from_=dt_aware,
        to=dt_naive,
        search="ord-",
        sort_by="task_name",
        sort_order="asc",
        limit=50,
        offset=10,
    )
    assert custom.status == "completed"
    assert custom.task_name == "orders"
    assert custom.from_ == dt_aware.isoformat()
    assert custom.to == dt_naive.replace(tzinfo=UTC).isoformat()
    assert custom.search == "ord-"
    assert custom.sort_by == "task_name"
    assert custom.sort_order == "asc"
    assert custom.limit == 50
    assert custom.offset == 10

    with pytest.raises(ValueError, match="unknown execution status"):
        ListExecutionsParams(status="bogus")
    with pytest.raises(ValueError, match="unknown sort_by field"):
        ListExecutionsParams(sort_by="invalid_col")
    with pytest.raises(ValueError, match="unknown sort_order"):
        ListExecutionsParams(sort_order="sideways")
    with pytest.raises(ValueError, match="invalid from_ datetime"):
        ListExecutionsParams(from_="not-a-date")
    with pytest.raises(ValueError, match="invalid to datetime"):
        ListExecutionsParams(to="not-a-date")


@pytest.mark.asyncio
async def test_scheduler_introspection_wrappers_offline() -> None:
    sched, mock_native = _make_mocked_scheduler()

    assert await sched.list_executions() == []
    mock_native.list_executions.assert_awaited_once_with(None)

    mock_native.list_executions.reset_mock()
    params = ListExecutionsParams(status="running", limit=5)
    await sched.list_executions(params)
    mock_native.list_executions.assert_awaited_once_with(params)

    mock_native.list_executions.reset_mock()
    dt = datetime(2026, 11, 20, 9, 0, 0, tzinfo=UTC)
    await sched.list_executions(
        status="completed",
        task_name="invoice",
        limit=15,
        offset=3,
        sort_by="status",
        sort_order="asc",
        **{"from": dt},
    )
    passed_params = mock_native.list_executions.call_args.args[0]
    assert isinstance(passed_params, ListExecutionsParams)
    assert passed_params.status == "completed"
    assert passed_params.task_name == "invoice"
    assert passed_params.limit == 15
    assert passed_params.offset == 3
    assert passed_params.sort_by == "status"
    assert passed_params.sort_order == "asc"
    assert passed_params.from_ == dt.isoformat()

    with pytest.raises(TypeError, match="Cannot specify both"):
        await sched.list_executions(params, status="failed")

    with pytest.raises(TypeError, match="either 'from_' or 'from'"):
        await sched.list_executions(from_=dt, **{"from": dt})

    assert await sched.stats() == "stats-res"
    mock_native.stats.assert_awaited_once_with()

    assert await sched.get_current_run_id("exec-1") == "run-1"
    mock_native.get_current_run_id.assert_awaited_once_with("exec-1")

    assert await sched.list_runs("exec-1") == []
    mock_native.list_runs.assert_awaited_once_with("exec-1")

    assert await sched.execution_detail("exec-1", run_id="exec-1:run:0") == "detail-res"
    mock_native.execution_detail.assert_awaited_once_with("exec-1", "exec-1:run:0")


# ─────────────────────────────────────────────────────────────────────────────
# 2. PostgreSQL Integration Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_introspection_list_stats_runs_and_detail(
    backend: PgBackend,
    scheduler: DurableScheduler,
) -> None:
    task_name = _uid("introspect-wf")

    @zart.step("step-a")
    async def step_a(x: int) -> int:
        return x + 1

    @zart.step("step-b")
    async def step_b(x: int) -> int:
        return x * 2

    @zart.durable(task_name)
    async def workflow(data: dict[str, int]) -> dict[str, int]:
        a = await step_a(data["x"])
        b = await step_b(a)
        return {"result": b}

    worker = WorkerBuilder(backend).register_durable(workflow).build()

    exec_1 = _uid("exec-int-1")
    exec_2 = _uid("exec-int-2")
    exec_sched = _uid("exec-int-sched")

    await scheduler.start(exec_1, workflow, {"x": 5})
    await scheduler.start(exec_2, workflow, {"x": 10})

    async def _run() -> None:
        r1 = await scheduler.wait_completion(exec_1, timeout="10s", poll_interval="50ms")
        r2 = await scheduler.wait_completion(exec_2, timeout="10s", poll_interval="50ms")
        assert r1 == {"result": 12}
        assert r2 == {"result": 22}

    await run_with_worker(worker, _run())

    # Start a third execution that remains scheduled (no worker running)
    await scheduler.start(exec_sched, workflow, {"x": 1})

    # 1. list_executions with kwargs and ListExecutionsParams
    completed = await scheduler.list_executions(
        task_name=task_name,
        status="completed",
        sort_by="scheduled_at",
        sort_order="asc",
        limit=10,
        offset=0,
    )
    completed_ids = [r.execution_id for r in completed]
    assert completed_ids == [exec_1, exec_2]
    assert all(isinstance(r, ExecutionRecord) for r in completed)

    page2 = await scheduler.list_executions(
        ListExecutionsParams(
            task_name=task_name,
            status="completed",
            sort_by="scheduled_at",
            sort_order="asc",
            limit=1,
            offset=1,
        )
    )
    assert [r.execution_id for r in page2] == [exec_2]

    by_search = await scheduler.list_executions(search=exec_sched)
    assert [r.execution_id for r in by_search] == [exec_sched]

    now_utc = datetime.now(UTC)
    in_window = await scheduler.list_executions(
        task_name=task_name,
        from_=now_utc - timedelta(minutes=5),
        to=now_utc + timedelta(minutes=5),
    )
    assert {r.execution_id for r in in_window} == {exec_1, exec_2, exec_sched}

    # 2. stats
    stats = await scheduler.stats()
    assert isinstance(stats, ExecutionStats)
    assert stats.completed >= 2
    assert stats.scheduled >= 1
    assert stats.running >= 0
    assert stats.failed >= 0
    assert stats.cancelled >= 0

    # 3. get_current_run_id
    run_id = await scheduler.get_current_run_id(exec_1)
    assert isinstance(run_id, str) and len(run_id) > 0
    assert await scheduler.get_current_run_id(_uid("nonexistent")) is None

    # 4. list_runs
    runs = await scheduler.list_runs(exec_1)
    assert len(runs) == 1
    run = runs[0]
    assert isinstance(run, ExecutionRunRecord)
    assert run.run_id == run_id
    assert run.execution_id == exec_1
    assert run.run_index == 0
    assert run.payload == {"x": 5}
    assert run.status == "completed"
    assert run.result == {"result": 12}
    assert run.trigger == "initial"
    assert isinstance(run.started_at, str)
    assert isinstance(run.completed_at, str)

    # 5. execution_detail (default current run & explicit run_id)
    detail = await scheduler.execution_detail(exec_1)
    assert isinstance(detail, ExecutionDetail)
    assert detail.execution.execution_id == exec_1
    assert detail.execution.status == "completed"
    assert len(detail.runs) == 1
    assert len(detail.steps) == 2

    steps_by_name = {s.step.step_name: s for s in detail.steps}
    assert set(steps_by_name.keys()) == {"step-a", "step-b"}

    sa = steps_by_name["step-a"]
    assert isinstance(sa, StepWithAttempts)
    assert isinstance(sa.step, StepRow)
    assert sa.step.run_id == run_id
    assert sa.step.step_kind == "step"
    assert sa.step.status == "completed"
    assert sa.step.result == 6
    assert sa.retryable is False
    assert len(sa.attempts) == 1
    att = sa.attempts[0]
    assert isinstance(att, StepAttemptRow)
    assert att.step_id == sa.step.step_id
    assert att.attempt_number == 1
    assert att.status == "completed"
    assert att.result == 6
    assert att.error is None

    detail_explicit = await scheduler.execution_detail(exec_1, run_id=run_id)
    assert len(detail_explicit.steps) == 2

    with pytest.raises(LookupError):
        await scheduler.execution_detail(_uid("missing-exec"))
