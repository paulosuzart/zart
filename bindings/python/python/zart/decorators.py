"""Pythonic decorators, retry helpers, and durable workflow primitives for Zart."""

from __future__ import annotations

import functools
import inspect
import math
import re
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, ParamSpec, TypeVar, get_type_hints, overload

from zart._zart import (
    DurableTask,
    ExecutionFailure,
    ExecutionInfo,
    RetryPolicy,
    StepHandle,
    StepOutcome,
    StepSpec,
    capture as _native_capture,
    context as _native_context,
    now as _native_now,
    require as _native_require,
    schedule as _native_schedule,
    sleep as _native_sleep,
    sleep_until as _native_sleep_until,
    step as _native_step,
    step_or as _native_step_or,
    step_or_else as _native_step_or_else,
    wait as _native_wait,
    wait_for_event as _native_wait_for_event,
)
from zart.serde import from_json_value, to_json_value

P = ParamSpec("P")
T = TypeVar("T")
U = TypeVar("U")

DurationLike = int | float | timedelta | str

_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)\s*$", re.IGNORECASE)
_UNIT_SECONDS: dict[str, float] = {
    "ms": 0.001,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
    "d": 86400.0,
}


def parse_duration(value: DurationLike | None) -> float | None:
    """Parse a duration specification into non-negative seconds.

    Accepts ``None``, ``int``/``float`` seconds, ``datetime.timedelta``,
    or strings such as ``"500ms"``, ``"1.5s"``, ``"5m"``, ``"2h"``, ``"1d"``.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("Duration cannot be a bool")
    if isinstance(value, timedelta):
        secs = value.total_seconds()
        if secs < 0:
            raise ValueError("Duration must be non-negative")
        return secs
    if isinstance(value, (int, float)):
        secs = float(value)
        if not math.isfinite(secs) or secs < 0:
            raise ValueError("Duration must be a non-negative finite number")
        return secs
    if isinstance(value, str):
        match = _DURATION_RE.match(value)
        if not match:
            raise ValueError(
                f"Invalid duration string {value!r}; expected e.g. '500ms', '1s', '5m', '2h', '1d'"
            )
        amount = float(match.group(1))
        unit = match.group(2).lower()
        return amount * _UNIT_SECONDS[unit]
    raise TypeError(f"Unsupported duration type: {type(value).__name__}")


class Retry:
    """Factory helpers for constructing :class:`RetryPolicy` instances."""

    @staticmethod
    def none() -> RetryPolicy:
        """Return a policy that performs no retries."""
        return RetryPolicy.none()

    @staticmethod
    def fixed(attempts: int, delay: DurationLike) -> RetryPolicy:
        """Retry ``attempts`` times with a fixed delay between attempts."""
        delay_secs = parse_duration(delay)
        assert delay_secs is not None
        return RetryPolicy.fixed(attempts, delay_secs)

    @staticmethod
    def exponential(
        attempts: int,
        initial_delay: DurationLike,
        max_delay: DurationLike | None = None,
        multiplier: float = 2.0,
    ) -> RetryPolicy:
        """Retry ``attempts`` times with exponential backoff."""
        initial_secs = parse_duration(initial_delay)
        assert initial_secs is not None
        max_secs = parse_duration(max_delay)
        return RetryPolicy.exponential(attempts, initial_secs, max_secs, multiplier)

    @staticmethod
    def linear(
        attempts: int,
        initial_delay: DurationLike,
        max_delay: DurationLike | None = None,
    ) -> RetryPolicy:
        """Retry ``attempts`` times with linear (constant-interval) backoff."""
        initial_secs = parse_duration(initial_delay)
        assert initial_secs is not None
        max_secs = parse_duration(max_delay)
        return RetryPolicy.linear(attempts, initial_secs, max_secs)


def _safe_type_hints(fn: Callable[..., Any]) -> dict[str, Any]:
    try:
        return get_type_hints(fn)
    except Exception:
        return {}


class StepInvocation:
    """Deferred step invocation created by calling a ``@step``-decorated function."""

    def __init__(
        self,
        spec: StepSpec,
        return_type: Any = Any,
    ) -> None:
        self._spec = spec
        self.return_type = return_type

    @property
    def name(self) -> str:
        """Return the tracking identity of this step invocation."""
        return self._spec.name

    @property
    def spec(self) -> StepSpec:
        """Return the underlying native :class:`StepSpec`."""
        return self._spec

    def named(self, custom_id: str) -> StepInvocation:
        """Return a copy of this invocation with an overridden tracking identity."""
        return StepInvocation(self._spec.named(custom_id), return_type=self.return_type)

    def __repr__(self) -> str:
        return f"StepInvocation(name={self.name!r})"


class TypedStepOutcome:
    """Wrapper around :class:`StepOutcome` that reconstructs typed step results."""

    def __init__(self, inner: StepOutcome, return_type: Any = Any) -> None:
        self._inner = inner
        self._return_type = return_type

    @property
    def kind(self) -> str:
        return self._inner.kind

    @property
    def is_ok(self) -> bool:
        return self._inner.is_ok

    @property
    def is_business_err(self) -> bool:
        return self._inner.is_business_err

    @property
    def is_zart_err(self) -> bool:
        return self._inner.is_zart_err

    @property
    def value(self) -> Any:
        raw = self._inner.value
        if not self._inner.is_ok:
            return raw
        return from_json_value(raw, self._return_type)

    @property
    def error(self) -> Any:
        return self._inner.error

    def unwrap(self) -> Any:
        raw = self._inner.unwrap()
        return from_json_value(raw, self._return_type)

    def __repr__(self) -> str:
        return repr(self._inner)


def _to_step_spec(invocation: StepInvocation | StepSpec) -> tuple[StepSpec, Any]:
    if isinstance(invocation, StepInvocation):
        return invocation.spec, invocation.return_type
    return invocation, Any


def _make_step_decorator(
    name: str,
    *,
    retry: RetryPolicy | None = None,
    timeout: DurationLike | None = None,
    timeout_scope: str | None = None,
) -> Callable[[Callable[P, Any]], Callable[P, StepInvocation]]:
    timeout_secs = parse_duration(timeout)

    def decorator(fn: Callable[P, Any]) -> Callable[P, StepInvocation]:
        hints = _safe_type_hints(fn)
        return_type = hints.get("return", Any)

        @functools.wraps(fn)
        def invoker(*args: P.args, **kwargs: P.kwargs) -> StepInvocation:
            async def _run_step() -> Any:
                res = fn(*args, **kwargs)
                if inspect.isawaitable(res):
                    res = await res
                return to_json_value(res)

            spec = StepSpec(
                name=name,
                callable=_run_step,
                retry=retry,
                timeout_secs=timeout_secs,
                timeout_scope=timeout_scope,
            )
            return StepInvocation(spec, return_type=return_type)

        return invoker

    return decorator


@overload
def step(
    target: str,
    *,
    retry: RetryPolicy | None = None,
    timeout: DurationLike | None = None,
    timeout_scope: str | None = None,
) -> Callable[[Callable[P, Any]], Callable[P, StepInvocation]]: ...


@overload
def step(
    target: StepInvocation | StepSpec,
) -> Awaitable[TypedStepOutcome | StepOutcome]: ...


def step(
    target: str | StepInvocation | StepSpec,
    *,
    retry: RetryPolicy | None = None,
    timeout: DurationLike | None = None,
    timeout_scope: str | None = None,
) -> Any:
    """Dual-mode step decorator and step execution helper.

    - As a decorator: ``@zart.step("charge-card", retry=Retry.fixed(3, "1s"))``
    - As an awaitable: ``outcome = await zart.step(charge_card(order_id))``
    """
    if isinstance(target, str):
        return _make_step_decorator(
            target,
            retry=retry,
            timeout=timeout,
            timeout_scope=timeout_scope,
        )

    spec, return_type = _to_step_spec(target)

    async def _execute() -> TypedStepOutcome | StepOutcome:
        outcome = await _native_step(spec)
        if return_type is Any:
            return outcome
        return TypedStepOutcome(outcome, return_type)

    return _execute()


def durable(
    name: str,
    *,
    timeout: DurationLike | None = None,
    max_retries: int = 0,
    on_failure: Callable[[Any, ExecutionFailure], Any] | None = None,
) -> Callable[[Callable[..., Awaitable[Any]]], DurableTask]:
    """Wrap an ``async def`` workflow function into a :class:`DurableTask`.

    Automatically deserializes input payloads into the first parameter's type
    annotation and normalizes return values via :mod:`zart.serde`.
    """
    timeout_secs = parse_duration(timeout)

    def decorator(fn: Callable[..., Awaitable[Any]]) -> DurableTask:
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        hints = _safe_type_hints(fn)
        input_type = hints.get(params[0].name, Any) if params else Any

        async def _wrapped_handler(raw_data: Any) -> Any:
            if not params:
                res = await fn()
            else:
                typed_arg = from_json_value(raw_data, input_type)
                res = await fn(typed_arg)
            return to_json_value(res)

        wrapped_on_failure: Callable[[Any, ExecutionFailure], Awaitable[Any]] | None = None
        if on_failure is not None:
            fail_sig = inspect.signature(on_failure)
            fail_params = list(fail_sig.parameters.values())
            fail_hints = _safe_type_hints(on_failure)
            fail_input_type = (
                fail_hints.get(fail_params[0].name, input_type)
                if fail_params
                else input_type
            )

            async def _wrapped_on_failure(raw_data: Any, failure: ExecutionFailure) -> Any:
                typed_arg = from_json_value(raw_data, fail_input_type)
                res = on_failure(typed_arg, failure)
                if inspect.isawaitable(res):
                    res = await res
                return to_json_value(res)

            wrapped_on_failure = _wrapped_on_failure

        return DurableTask(
            name=name,
            handler=_wrapped_handler,
            timeout_secs=timeout_secs,
            max_retries=max_retries,
            on_failure=wrapped_on_failure,
        )

    return decorator


async def require(invocation: StepInvocation | StepSpec) -> Any:
    """Execute a step durably and return its deserialized result, raising on failure."""
    spec, return_type = _to_step_spec(invocation)
    raw = await _native_require(spec)
    return from_json_value(raw, return_type)


async def step_or(invocation: StepInvocation | StepSpec, default: T) -> T | Any:
    """Execute a step durably, returning ``default`` if it fails."""
    spec, return_type = _to_step_spec(invocation)
    raw = await _native_step_or(spec, to_json_value(default))
    return from_json_value(raw, return_type)


async def step_or_else(
    invocation: StepInvocation | StepSpec,
    fallback: Callable[[Any], Any],
) -> Any:
    """Execute a step durably, invoking ``fallback(err)`` if it fails."""
    spec, return_type = _to_step_spec(invocation)

    def _wrapped_fallback(err: Any) -> Any:
        return to_json_value(fallback(err))

    raw = await _native_step_or_else(spec, _wrapped_fallback)
    return from_json_value(raw, return_type)


def schedule(invocation: StepInvocation | StepSpec) -> StepHandle:
    """Schedule a step for parallel execution and return its :class:`StepHandle`."""
    spec, _ = _to_step_spec(invocation)
    return _native_schedule(spec)


async def wait(handles: Sequence[StepHandle]) -> list[StepOutcome]:
    """Wait for all scheduled :class:`StepHandle` instances, preserving handle order."""
    return await _native_wait(list(handles))


async def sleep(name: str, duration: DurationLike) -> None:
    """Durably sleep for ``duration`` (seconds, ``timedelta``, or duration string)."""
    secs = parse_duration(duration)
    assert secs is not None
    await _native_sleep(name, secs)


async def sleep_until(name: str, wake_time: datetime | str) -> None:
    """Durably sleep until ``wake_time`` (``datetime`` or ISO-8601 string)."""
    if isinstance(wake_time, datetime):
        if wake_time.tzinfo is None:
            wake_time = wake_time.replace(tzinfo=timezone.utc)
        wake_iso = wake_time.isoformat()
    else:
        wake_iso = wake_time
    await _native_sleep_until(name, wake_iso)


async def wait_for_event(
    name: str,
    timeout: DurationLike | None = None,
    *,
    target_type: type[T] | Any = None,
) -> T | Any:
    """Suspend execution until external event ``name`` arrives or ``timeout`` expires."""
    timeout_secs = parse_duration(timeout)
    raw = await _native_wait_for_event(name, timeout_secs)
    return from_json_value(raw, target_type)


async def capture(
    name: str,
    func: Callable[[], T],
    *,
    target_type: type[T] | Any = None,
) -> T | Any:
    """Capture a synchronous pure computation durably."""
    def _wrapped() -> Any:
        return to_json_value(func())

    raw = await _native_capture(name, _wrapped)
    return from_json_value(raw, target_type)


async def now(name: str) -> datetime:
    """Return a deterministic, durably recorded UTC timestamp for ``name``."""
    iso_str = await _native_now(name)
    return datetime.fromisoformat(iso_str)


def context() -> ExecutionInfo:
    """Return the :class:`ExecutionInfo` for the currently running durable execution."""
    return _native_context()
