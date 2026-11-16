"""Unit tests for zart.decorators, Retry, duration parsing, and Phase 2 bindings."""

from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
import zart
from zart import (
    DurableTask,
    Retry,
    RetryPolicy,
    StepInvocation,
    StepSpec,
    WorkerBuilder,
    ZartControlFlowSignal,
    ZartError,
    durable,
    parse_duration,
    step,
)


@dataclass
class OrderInput:
    order_id: UUID
    amount: float


@dataclass
class OrderOutput:
    receipt_id: UUID
    charged: float


def test_parse_duration() -> None:
    assert parse_duration(None) is None
    assert parse_duration(0) == 0.0
    assert parse_duration(5) == 5.0
    assert parse_duration(1.25) == 1.25
    assert parse_duration(timedelta(minutes=2, seconds=30)) == 150.0
    assert parse_duration("500ms") == pytest.approx(0.5)
    assert parse_duration("1.5s") == pytest.approx(1.5)
    assert parse_duration("5m") == pytest.approx(300.0)
    assert parse_duration("2h") == pytest.approx(7200.0)
    assert parse_duration("1d") == pytest.approx(86400.0)
    assert parse_duration("  10M ") == pytest.approx(600.0)

    with pytest.raises(ValueError, match="non-negative"):
        parse_duration(-1)
    with pytest.raises(ValueError, match="non-negative finite"):
        parse_duration(float("nan"))
    with pytest.raises(ValueError, match="non-negative finite"):
        parse_duration(float("inf"))
    with pytest.raises(ValueError, match="non-negative"):
        parse_duration(timedelta(seconds=-5))
    with pytest.raises(ValueError, match="Invalid duration"):
        parse_duration("10w")
    with pytest.raises(TypeError, match="bool"):
        parse_duration(True)


def test_retry_factories() -> None:
    none_pol = Retry.none()
    assert isinstance(none_pol, RetryPolicy)
    assert none_pol.max_attempts == 0

    fixed_pol = Retry.fixed(3, "500ms")
    assert fixed_pol.max_attempts == 3
    assert fixed_pol.initial_delay_secs == pytest.approx(0.5)
    assert fixed_pol.backoff_multiplier == pytest.approx(1.0)

    exp_pol = Retry.exponential(5, "1s", max_delay="1m", multiplier=3.0)
    assert exp_pol.max_attempts == 5
    assert exp_pol.initial_delay_secs == pytest.approx(1.0)
    assert exp_pol.max_delay_secs == pytest.approx(60.0)
    assert exp_pol.backoff_multiplier == pytest.approx(3.0)

    lin_pol = Retry.linear(4, timedelta(seconds=2), max_delay=10)
    assert lin_pol.max_attempts == 4
    assert lin_pol.initial_delay_secs == pytest.approx(2.0)
    assert lin_pol.max_delay_secs == pytest.approx(10.0)
    assert lin_pol.backoff_multiplier == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_step_decorator_and_named() -> None:
    @step("charge-card", retry=Retry.fixed(3, "1s"), timeout="30s", timeout_scope="per_attempt")
    async def charge_card(order: OrderInput) -> OrderOutput:
        return OrderOutput(receipt_id=order.order_id, charged=order.amount)

    uid = uuid4()
    inv = charge_card(OrderInput(order_id=uid, amount=49.99))
    assert isinstance(inv, StepInvocation)
    assert isinstance(inv, Awaitable)
    assert inv.name == "charge-card"
    assert isinstance(inv.spec, StepSpec)

    renamed = inv.named("charge-card-0")
    assert isinstance(renamed, Awaitable)
    assert renamed.name == "charge-card-0"
    assert inv.name == "charge-card"


def test_durable_decorator_returns_durable_task() -> None:
    async def handle_failure(data: OrderInput, failure: zart.ExecutionFailure) -> str:
        return f"failed:{data.order_id}:{failure.kind}"

    @durable("process-order", timeout="5m", max_retries=2, on_failure=handle_failure)
    async def process_order(order: OrderInput) -> OrderOutput:
        return OrderOutput(receipt_id=order.order_id, charged=order.amount)

    assert isinstance(process_order, DurableTask)
    assert process_order.name == "process-order"
    assert process_order.timeout_secs == pytest.approx(300.0)
    assert process_order.max_retries == 2
    assert hasattr(WorkerBuilder, "register_durable")


def test_control_flow_signal_not_caught_by_except_exception() -> None:
    assert issubclass(ZartControlFlowSignal, BaseException)
    assert not issubclass(ZartControlFlowSignal, Exception)
    assert issubclass(ZartError, Exception)

    caught_by_exception = False
    with pytest.raises(ZartControlFlowSignal):
        try:
            raise ZartControlFlowSignal("scheduled")
        except Exception:
            caught_by_exception = True

    assert caught_by_exception is False


def test_public_exports() -> None:
    for name in [
        "durable",
        "step",
        "require",
        "step_or",
        "step_or_else",
        "schedule",
        "wait",
        "sleep",
        "sleep_until",
        "wait_for_event",
        "capture",
        "now",
        "context",
        "Retry",
        "parse_duration",
        "ZartControlFlowSignal",
    ]:
        assert hasattr(zart, name), f"Missing export: {name}"
