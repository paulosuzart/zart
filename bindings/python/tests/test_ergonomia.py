"""Tests for Slice S2.2: Python Workflow Ergonomia.

Covers:
1. schedule + wait preserving return_type and reconstructing dataclasses/pydantic/UUID/datetime.
2. business_err vs zart_err mapping in wait outcomes.
3. Automatic zart.Trx parameter injection without '= zart.trx()'.
4. Optional {param} template interpolation in @zart.step("process-{index}").
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
import zart
from conftest import run_with_worker
from zart import (
    DurableScheduler,
    PgBackend,
    Retry,
    StepInvocation,
    Trx,
    TypedStepOutcome,
    WorkerBuilder,
    ZartStepFailedError,
)

try:
    from pydantic import BaseModel
except ImportError:
    BaseModel = None  # type: ignore[assignment,misc]


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


@dataclass
class ItemPayload:
    item_id: UUID
    name: str
    price: float
    created_at: datetime


class PydanticItem(BaseModel if BaseModel else object):  # type: ignore[misc]
    item_id: str
    count: int


# ─────────────────────────────────────────────────────────────────────────────
# 1. Template Interpolation Unit Tests
# ─────────────────────────────────────────────────────────────────────────────


def test_step_template_interpolation_basic() -> None:
    @zart.step("process-{index}")
    async def process(index: int) -> int:
        return index * 2

    inv = process(42)
    assert isinstance(inv, StepInvocation)
    assert inv.name == "process-42"

    # Override with .named()
    overridden = inv.named("custom-step-name")
    assert overridden.name == "custom-step-name"


def test_step_template_interpolation_multiple_params_and_defaults() -> None:
    @zart.step("job-{job_id}-task-{task_idx}")
    async def job_task(job_id: str, task_idx: int = 1) -> str:
        return f"{job_id}:{task_idx}"

    inv1 = job_task("j100")
    assert inv1.name == "job-j100-task-1"

    inv2 = job_task("j100", task_idx=5)
    assert inv2.name == "job-j100-task-5"

    inv3 = job_task(task_idx=9, job_id="j200")
    assert inv3.name == "job-j200-task-9"


def test_step_template_interpolation_format_specs() -> None:
    @zart.step("item-{code:04d}-{tag}")
    async def format_step(code: int, tag: str) -> None:
        pass

    inv = format_step(7, "alpha")
    assert inv.name == "item-0007-alpha"


def test_step_template_interpolation_attribute_access() -> None:
    class DummyUser:
        def __init__(self, uid: int) -> None:
            self.id = uid

    @zart.step("user-{user.id}-action")
    async def user_step(user: DummyUser) -> None:
        pass

    inv = user_step(DummyUser(999))
    assert inv.name == "user-999-action"


def test_step_template_validation_at_decoration_time() -> None:
    # Non-existent parameter
    with pytest.raises(ValueError, match="not in function signature"):

        @zart.step("bad-{missing}")
        async def bad_param(x: int) -> None:
            pass

    # Empty placeholder
    with pytest.raises(ValueError, match="not in function signature"):

        @zart.step("bad-{}")
        async def bad_empty(x: int) -> None:
            pass

    # Unclosed brace
    with pytest.raises(ValueError, match="Invalid step name format template"):

        @zart.step("bad-{unclosed")
        async def bad_unclosed(x: int) -> None:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# 2. Automatic zart.Trx Injection Unit Tests
# ─────────────────────────────────────────────────────────────────────────────


def test_step_auto_trx_omits_parameter_from_caller() -> None:
    @zart.step("insert-user")
    async def insert_user(user_id: int, tx: zart.Trx) -> str:
        return f"user:{user_id}"

    # Caller must omit tx
    inv = insert_user(101)
    assert isinstance(inv, StepInvocation)
    assert inv.name == "insert-user"

    # If caller attempts to pass tx, call_sig rejects it
    with pytest.raises(TypeError):
        insert_user(101, tx=None)  # type: ignore[call-arg]


def test_step_auto_trx_variations() -> None:
    # 1. Annotation as Trx
    @zart.step("step-trx")
    async def step_a(x: str, tx: Trx) -> str:
        return x

    inv_a = step_a("hello")
    assert inv_a.name == "step-trx"

    # 2. String annotation "Trx"
    @zart.step("step-str-trx")
    async def step_b(x: str, tx: "Trx") -> str:  # noqa: UP037
        return x

    inv_b = step_b("world")
    assert inv_b.name == "step-str-trx"

    # 3. String annotation "zart.Trx"
    @zart.step("step-zart-trx")
    async def step_c(x: str, tx: "zart.Trx") -> str:  # noqa: UP037
        return x

    inv_c = step_c("foo")
    assert inv_c.name == "step-zart-trx"

    # 4. Only parameter is tx
    @zart.step("step-only-tx")
    async def step_d(tx: zart.Trx) -> str:
        return "ok"

    inv_d = step_d()
    assert inv_d.name == "step-only-tx"


def test_step_auto_trx_multiple_params_raises() -> None:
    # Two auto zart.Trx parameters
    with pytest.raises(ValueError, match="at most one"):

        @zart.step("bad-two-auto")
        async def bad_two_auto(tx1: zart.Trx, tx2: zart.Trx) -> None:
            pass

    # One auto zart.Trx and one with = zart.trx()
    with pytest.raises(ValueError, match="at most one"):

        @zart.step("bad-mixed")
        async def bad_mixed(tx1: zart.Trx, tx2: zart.Trx = zart.trx()) -> None:
            pass

    # One auto zart.Trx and one untyped = zart.trx()
    with pytest.raises(ValueError, match="at most one"):

        @zart.step("bad-mixed-untyped")
        async def bad_mixed_untyped(tx1=zart.trx(), tx2: zart.Trx = None) -> None:  # type: ignore[assignment]
            pass


# ─────────────────────────────────────────────────────────────────────────────
# 3. TypedStepHandle and TypedStepOutcome Unit Tests
# ─────────────────────────────────────────────────────────────────────────────


def test_schedule_returns_typed_step_handle() -> None:
    @zart.step("compute-item")
    async def compute_item(name: str) -> ItemPayload:
        return ItemPayload(
            item_id=uuid4(),
            name=name,
            price=19.99,
            created_at=datetime.now(UTC),
        )

    inv = compute_item("widget")
    assert inv.return_type is ItemPayload

    # In unit test context without workflow dispatcher, schedule raises RuntimeError
    with pytest.raises(RuntimeError, match="zart.* functions must be called inside"):
        zart.schedule(inv)


def test_typed_step_outcome_reconstructs_types() -> None:
    item_id = uuid4()
    now_dt = datetime.now(UTC)
    raw_json = {
        "item_id": str(item_id),
        "name": "gadget",
        "price": 49.50,
        "created_at": now_dt.isoformat(),
    }

    class FakeStepOutcome:
        kind = "ok"
        is_ok = True
        is_business_err = False
        is_zart_err = False
        value = raw_json
        error = None

        def unwrap(self) -> Any:
            return self.value

    fake = FakeStepOutcome()
    typed = TypedStepOutcome(fake, ItemPayload)  # type: ignore[arg-type]

    assert typed.is_ok
    assert not typed.is_business_err
    assert not typed.is_zart_err
    assert typed.kind == "ok"

    reconstructed = typed.value
    assert isinstance(reconstructed, ItemPayload)
    assert reconstructed.item_id == item_id
    assert reconstructed.name == "gadget"
    assert reconstructed.price == 49.50
    assert reconstructed.created_at == now_dt

    unwrapped = typed.unwrap()
    assert isinstance(unwrapped, ItemPayload)
    assert unwrapped.item_id == item_id


def test_typed_step_outcome_business_err() -> None:
    class FakeErrOutcome:
        kind = "business_err"
        is_ok = False
        is_business_err = True
        is_zart_err = False
        value = None
        error = {"error_type": "ValueError", "message": "out of stock", "step": "item-step"}

        def unwrap(self) -> Any:
            raise ZartStepFailedError("Step 'item-step' failed: ValueError: out of stock")

    fake = FakeErrOutcome()
    typed = TypedStepOutcome(fake, ItemPayload)  # type: ignore[arg-type]

    assert not typed.is_ok
    assert typed.is_business_err
    assert not typed.is_zart_err
    assert typed.kind == "business_err"
    assert typed.value is None
    assert typed.error["error_type"] == "ValueError"

    with pytest.raises(ZartStepFailedError, match="out of stock"):
        typed.unwrap()


# ─────────────────────────────────────────────────────────────────────────────
# 4. PostgreSQL Integration Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_integration_schedule_wait_dataclass_and_interpolation(
    backend: PgBackend,
    scheduler: DurableScheduler,
) -> None:
    """Test parallel schedule + wait with template interpolation and dataclass reconstruction."""
    task_name = _uid("par-dataclass-wf")
    fixed_dt = datetime(2026, 11, 20, 12, 0, 0, tzinfo=UTC)
    fixed_uids = [uuid4() for _ in range(3)]

    @zart.step("fetch-item-{index}")
    async def fetch_item(index: int) -> ItemPayload:
        return ItemPayload(
            item_id=fixed_uids[index],
            name=f"item-{index}",
            price=float(index * 15),
            created_at=fixed_dt,
        )

    @zart.durable(task_name)
    async def workflow(_data: Any) -> list[dict[str, Any]]:
        handles = [zart.schedule(fetch_item(i)) for i in range(3)]
        # Verify handle step names match template interpolation
        for i, h in enumerate(handles):
            assert h.step_name == f"fetch-item-{i}"

        outcomes = await zart.wait(handles)
        results: list[dict[str, Any]] = []
        for o in outcomes:
            assert o.is_ok
            assert not o.is_business_err
            assert not o.is_zart_err
            # Verify reconstructed value is dataclass instance
            val = o.unwrap()
            assert isinstance(val, ItemPayload)
            assert isinstance(val.item_id, UUID)
            assert isinstance(val.created_at, datetime)
            results.append({"name": val.name, "price": val.price, "id": str(val.item_id)})
        return results

    worker = WorkerBuilder(backend).poll_interval(0.05).register_durable(workflow).build()
    exec_id = _uid("exec-dataclass")

    async def _run() -> list[dict[str, Any]]:
        await scheduler.start(exec_id, task_name, None)
        return await scheduler.wait_completion(exec_id, timeout_secs=15.0, poll_interval_secs=0.05)

    result = await run_with_worker(worker, _run())
    assert len(result) == 3
    assert result[0]["name"] == "item-0"
    assert result[1]["name"] == "item-1"
    assert result[2]["name"] == "item-2"


@pytest.mark.asyncio
async def test_integration_auto_trx_injection(
    backend: PgBackend,
    scheduler: DurableScheduler,
) -> None:
    """Test automatic zart.Trx injection without '= zart.trx()' against PostgreSQL."""
    task_name = _uid("auto-trx-wf")

    # Table setup
    client_tx = await backend.begin()
    await client_tx.execute(
        "CREATE TABLE IF NOT EXISTS test_auto_trx_users (id TEXT PRIMARY KEY, name TEXT)"
    )
    await client_tx.commit()

    @zart.step("insert-auto-trx-user")
    async def insert_user(user_id: str, name: str, tx: zart.Trx) -> str:
        # Injected zart.Trx executed without '= zart.trx()'
        await tx.execute(
            "INSERT INTO test_auto_trx_users (id, name) VALUES ($1, $2) "
            "ON CONFLICT (id) DO UPDATE SET name = $2",
            user_id,
            name,
        )
        row = await tx.fetch_one("SELECT name FROM test_auto_trx_users WHERE id = $1", user_id)
        return str(row["name"])

    @zart.durable(task_name)
    async def workflow(data: dict[str, str]) -> str:
        # Caller omits tx
        return await insert_user(data["id"], data["name"])

    worker = WorkerBuilder(backend).poll_interval(0.05).register_durable(workflow).build()
    exec_id = _uid("exec-auto-trx")

    async def _run() -> str:
        await scheduler.start(exec_id, task_name, {"id": "u42", "name": "Bob"})
        return await scheduler.wait_completion(exec_id, timeout_secs=15.0, poll_interval_secs=0.05)

    result = await run_with_worker(worker, _run())
    assert result == "Bob"

    # Verify DB persistence
    check_tx = await backend.begin()
    row = await check_tx.fetch_one("SELECT name FROM test_auto_trx_users WHERE id = $1", "u42")
    await check_tx.commit()
    assert row is not None
    assert row["name"] == "Bob"


@pytest.mark.asyncio
async def test_integration_schedule_wait_business_err_vs_zart_err(
    backend: PgBackend,
    scheduler: DurableScheduler,
) -> None:
    """Test that wait() correctly marks business_err for failed steps vs ok."""
    task_name = _uid("wait-err-wf")

    @zart.step("step-success")
    async def step_success(x: int) -> int:
        return x * 10

    @zart.step("step-fail-biz", retry=Retry.none())
    async def step_fail_biz(_x: int) -> int:
        raise ValueError("domain validation failed")

    @zart.durable(task_name)
    async def workflow(_data: Any) -> dict[str, Any]:
        h1 = zart.schedule(step_success(5))
        h2 = zart.schedule(step_fail_biz(0))

        outcomes = await zart.wait([h1, h2])
        o1, o2 = outcomes[0], outcomes[1]

        report = {
            "o1_ok": o1.is_ok,
            "o1_val": o1.unwrap(),
            "o2_ok": o2.is_ok,
            "o2_val": o2.value,
            "o2_business_err": o2.is_business_err,
            "o2_zart_err": o2.is_zart_err,
            "o2_kind": o2.kind,
            "o2_error": o2.error,
        }
        return report

    worker = WorkerBuilder(backend).poll_interval(0.05).register_durable(workflow).build()
    exec_id = _uid("exec-wait-err")

    async def _run() -> dict[str, Any]:
        await scheduler.start(exec_id, task_name, None)
        return await scheduler.wait_completion(exec_id, timeout_secs=15.0, poll_interval_secs=0.05)

    res = await run_with_worker(worker, _run())
    assert res["o1_ok"] is True
    assert res["o1_val"] == 50
    assert res["o2_ok"] is False
    assert res["o2_business_err"] is True
    assert res["o2_zart_err"] is False
    assert res["o2_kind"] == "business_err"
    assert res["o2_error"] is not None
    assert "validation failed" in str(res["o2_error"])
