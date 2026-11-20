"""Tests for Slice S3.2: Python DurableScheduler Admin & Pause/Resume Operations.

Covers:
1. RerunSpec, PauseScope, PauseRuleFilter construction and validation.
2. Offline wrapper delegation for retry_step, retry_step_current_run, restart,
   rerun_steps, pause, resume, resume_rule_by_id, list_pause_rules.
3. PostgreSQL integration tests for:
   - restart (with new_payload)
   - rerun_steps (selective rerun preserving completed steps)
   - retry_step / retry_step_current_run on dead steps
   - pause / list_pause_rules / resume / resume_rule_by_id
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import zart
from conftest import run_with_worker
from psycopg_pool import AsyncConnectionPool
from zart import (
    DurableScheduler,
    PauseRule,
    PauseRuleFilter,
    PauseScope,
    PgBackend,
    RerunResult,
    RerunSpec,
    ResumeResult,
    Retry,
    WorkerBuilder,
)


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _make_mocked_scheduler() -> tuple[DurableScheduler, MagicMock]:
    lazy_backend = PgBackend._connect_lazy("postgres://zart:zart@localhost:5432/zart")
    sched = DurableScheduler(lazy_backend)
    mock_native = MagicMock()
    mock_native.retry_step = AsyncMock(return_value="task-1")
    mock_native.retry_step_current_run = AsyncMock(return_value="task-2")
    mock_native.restart = AsyncMock(return_value="exec-1:run:1")
    mock_native.rerun_steps = AsyncMock(return_value="rerun-res")
    mock_native.pause = AsyncMock(return_value="pause-rule")
    mock_native.resume = AsyncMock(return_value="resume-res")
    mock_native.resume_rule_by_id = AsyncMock(return_value="resume-id-res")
    mock_native.list_pause_rules = AsyncMock(return_value=[])
    sched._native = mock_native  # type: ignore[assignment]
    return sched, mock_native


# ─────────────────────────────────────────────────────────────────────────────
# 1. Offline Unit Tests
# ─────────────────────────────────────────────────────────────────────────────


def test_admin_types_construction_and_validation() -> None:
    spec_default = RerunSpec()
    assert spec_default.force_rerun == []
    assert spec_default.preserve == []
    assert spec_default.triggered_by is None

    spec = RerunSpec(force_rerun=["step-b"], preserve=["step-a"], triggered_by="alice")
    assert spec.force_rerun == ["step-b"]
    assert spec.preserve == ["step-a"]
    assert spec.triggered_by == "alice"

    dt = datetime(2026, 11, 21, 10, 0, 0, tzinfo=UTC)
    scope = PauseScope(
        execution_id="exec-1",
        task_name="orders",
        step_pattern="charge-*",
        expires_at=dt,
        triggered_by="bob",
        reason="maintenance",
    )
    assert scope.execution_id == "exec-1"
    assert scope.task_name == "orders"
    assert scope.step_pattern == "charge-*"
    assert scope.expires_at == dt.isoformat()
    assert scope.triggered_by == "bob"
    assert scope.reason == "maintenance"

    with pytest.raises(ValueError, match="invalid expires_at datetime"):
        PauseScope(task_name="orders", expires_at="not-a-date")

    flt = PauseRuleFilter()
    assert flt.execution_id is None
    assert flt.task_name is None
    assert flt.include_deleted is False


@pytest.mark.asyncio
async def test_scheduler_admin_wrappers_offline() -> None:
    sched, mock_native = _make_mocked_scheduler()

    assert await sched.retry_step("exec-1:run:0", "step-a", triggered_by="alice") == "task-1"
    mock_native.retry_step.assert_awaited_once_with("exec-1:run:0", "step-a", "alice")

    assert await sched.retry_step_current_run("exec-1", "step-a") == "task-2"
    mock_native.retry_step_current_run.assert_awaited_once_with("exec-1", "step-a", None)

    @dataclass
    class Payload:
        amount: int

    assert await sched.restart("exec-1", Payload(amount=42), triggered_by="bob") == "exec-1:run:1"
    mock_native.restart.assert_awaited_once_with("exec-1", {"amount": 42}, "bob")

    mock_native.restart.reset_mock()
    await sched.restart("exec-1")
    mock_native.restart.assert_awaited_once_with("exec-1", None, None)

    assert await sched.rerun_steps("exec-1", force_rerun=["step-b"], triggered_by="carol") == (
        "rerun-res"
    )
    passed_spec = mock_native.rerun_steps.call_args.args[1]
    assert isinstance(passed_spec, RerunSpec)
    assert passed_spec.force_rerun == ["step-b"]
    assert passed_spec.triggered_by == "carol"

    with pytest.raises(TypeError, match="Cannot specify both"):
        await sched.rerun_steps("exec-1", RerunSpec(), force_rerun=["step-b"])

    scope = PauseScope(task_name="orders")
    assert await sched.pause(scope) == "pause-rule"
    mock_native.pause.assert_awaited_once_with(scope)

    assert await sched.resume(scope, deleted_by="dave") == "resume-res"
    mock_native.resume.assert_awaited_once_with(scope, "dave")

    assert await sched.resume_rule_by_id("rule-1", deleted_by="dave") == "resume-id-res"
    mock_native.resume_rule_by_id.assert_awaited_once_with("rule-1", "dave")

    flt = PauseRuleFilter(task_name="orders", include_deleted=True)
    assert await sched.list_pause_rules(flt) == []
    mock_native.list_pause_rules.assert_awaited_once_with(flt)


# ─────────────────────────────────────────────────────────────────────────────
# 2. PostgreSQL Integration Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_admin_restart_and_selective_rerun(
    backend: PgBackend,
    scheduler: DurableScheduler,
) -> None:
    task_name = _uid("admin-rerun-wf")
    counts = {"a": 0, "b": 0}

    @zart.step("step-a")
    async def step_a(x: int) -> int:
        counts["a"] += 1
        return x + 1

    @zart.step("step-b")
    async def step_b(x: int) -> int:
        counts["b"] += 1
        return x * 10

    @zart.durable(task_name)
    async def workflow(data: dict[str, int]) -> dict[str, int]:
        a = await step_a(data["x"])
        b = await step_b(a)
        return {"value": b}

    worker = WorkerBuilder(backend).register_durable(workflow).build()
    exec_id = _uid("exec-admin")
    await scheduler.start(exec_id, workflow, {"x": 2})

    async def _phase1() -> None:
        res = await scheduler.wait_completion(exec_id, timeout="10s", poll_interval="50ms")
        assert res == {"value": 30}

    await run_with_worker(worker, _phase1())
    assert counts == {"a": 1, "b": 1}

    # Selective rerun of step-b only: step-a is preserved and not re-executed.
    rerun_res = await scheduler.rerun_steps(
        exec_id,
        force_rerun=["step-b"],
        triggered_by="operator-1",
    )
    assert isinstance(rerun_res, RerunResult)
    assert rerun_res.new_run_number == 1
    assert "step-b" in rerun_res.effective_rerun

    worker2 = WorkerBuilder(backend).register_durable(workflow).build()

    async def _phase2() -> None:
        res = await scheduler.wait_completion(exec_id, timeout="10s", poll_interval="50ms")
        assert res == {"value": 30}

    await run_with_worker(worker2, _phase2())
    assert counts == {"a": 1, "b": 2}

    # Full restart with a new payload.
    new_run_id = await scheduler.restart(exec_id, new_payload={"x": 4}, triggered_by="operator-2")
    assert isinstance(new_run_id, str) and new_run_id != ""

    worker3 = WorkerBuilder(backend).register_durable(workflow).build()

    async def _phase3() -> None:
        res = await scheduler.wait_completion(exec_id, timeout="10s", poll_interval="50ms")
        assert res == {"value": 50}

    await run_with_worker(worker3, _phase3())
    assert counts == {"a": 2, "b": 3}

    runs = await scheduler.list_runs(exec_id)
    assert [r.trigger for r in runs] == ["initial", "selective_rerun", "restart"]


@pytest.mark.asyncio
async def test_admin_retry_step_and_current_run(
    backend: PgBackend,
    scheduler: DurableScheduler,
    psycopg_pool: AsyncConnectionPool[Any],
) -> None:
    task_name = _uid("admin-retry-wf")
    state = {"should_fail": True}

    @zart.step("flaky-step", retry=Retry.none())
    async def flaky_step(val: int) -> int:
        if state["should_fail"]:
            raise RuntimeError("transient outage")
        return val * 3

    @zart.durable(task_name)
    async def workflow(data: dict[str, int]) -> dict[str, int]:
        out = await flaky_step(data["v"])
        return {"out": out}

    exec_id = _uid("exec-retry")
    await scheduler.start(exec_id, workflow, {"v": 7})

    worker1 = WorkerBuilder(backend).register_durable(workflow).build()

    async def _fail_once() -> None:
        rec = await scheduler.wait(exec_id, timeout="10s", poll_interval="50ms")
        assert rec.status == "failed"

    await run_with_worker(worker1, _fail_once())

    # Completed step cannot be retried with retry_step_current_run.
    with pytest.raises(RuntimeError, match="expected 'Dead'"):
        await scheduler.retry_step_current_run(exec_id, "flaky-step", triggered_by="oncall")

    # Mark the step as dead (mirroring dead-lettered state) and retry via retry_step_current_run.
    run_id_1 = await scheduler.get_current_run_id(exec_id)
    assert run_id_1 is not None
    async with psycopg_pool.connection() as conn:
        await conn.execute(
            "UPDATE zart_steps SET status = 'dead' WHERE run_id = %s AND step_name = %s",
            (run_id_1, "flaky-step"),
        )
        await conn.commit()

    task_id = await scheduler.retry_step_current_run(
        exec_id,
        "flaky-step",
        triggered_by="oncall",
    )
    assert isinstance(task_id, str) and len(task_id) > 0

    detail_1 = await scheduler.execution_detail(exec_id)
    assert detail_1.runs[0].status == "running"
    assert detail_1.steps[0].step.status == "scheduled"
    assert detail_1.steps[0].step.task_id == task_id

    # Also verify retry_step directly with explicit run_id on a second execution.
    exec_id_2 = _uid("exec-retry-2")
    await scheduler.start(exec_id_2, workflow, {"v": 9})

    worker3 = WorkerBuilder(backend).register_durable(workflow).build()

    async def _fail_second() -> None:
        rec = await scheduler.wait(exec_id_2, timeout="10s", poll_interval="50ms")
        assert rec.status == "failed"

    await run_with_worker(worker3, _fail_second())

    run_id_2 = await scheduler.get_current_run_id(exec_id_2)
    assert run_id_2 is not None
    async with psycopg_pool.connection() as conn:
        await conn.execute(
            "UPDATE zart_steps SET status = 'dead' WHERE run_id = %s AND step_name = %s",
            (run_id_2, "flaky-step"),
        )
        await conn.commit()

    task_id_2 = await scheduler.retry_step(run_id_2, "flaky-step", triggered_by="oncall-2")
    assert isinstance(task_id_2, str) and len(task_id_2) > 0

    detail_2 = await scheduler.execution_detail(exec_id_2)
    assert detail_2.runs[0].status == "running"
    assert detail_2.steps[0].step.status == "scheduled"
    assert detail_2.steps[0].step.task_id == task_id_2


@pytest.mark.asyncio
async def test_admin_pause_list_and_resume_rules(
    scheduler: DurableScheduler,
) -> None:
    task_name = _uid("pause-task")
    expires = datetime.now(UTC) + timedelta(hours=1)

    rule1 = await scheduler.pause(
        PauseScope(
            task_name=task_name,
            step_pattern="charge-*",
            expires_at=expires,
            triggered_by="sre-1",
            reason="incident-123",
        )
    )
    assert isinstance(rule1, PauseRule)
    assert rule1.scope.task_name == task_name
    assert rule1.scope.step_pattern == "charge-*"
    assert rule1.deleted_at is None
    assert rule1.reason == "incident-123"

    active = await scheduler.list_pause_rules(PauseRuleFilter(task_name=task_name))
    assert [r.rule_id for r in active] == [rule1.rule_id]

    res_by_id = await scheduler.resume_rule_by_id(rule1.rule_id, deleted_by="sre-2")
    assert isinstance(res_by_id, ResumeResult)
    assert res_by_id.rules_deleted == 1

    assert await scheduler.list_pause_rules(PauseRuleFilter(task_name=task_name)) == []

    all_rules = await scheduler.list_pause_rules(
        PauseRuleFilter(task_name=task_name, include_deleted=True)
    )
    assert len(all_rules) == 1
    assert all_rules[0].deleted_at is not None

    rule2 = await scheduler.pause(PauseScope(task_name=task_name, triggered_by="sre-1"))
    assert rule2.rule_id != rule1.rule_id

    res_scope = await scheduler.resume(PauseScope(task_name=task_name), deleted_by="sre-3")
    assert isinstance(res_scope, ResumeResult)
    assert res_scope.rules_deleted == 1
    assert await scheduler.list_pause_rules(PauseRuleFilter(task_name=task_name)) == []
