"""Pythonic wrapper around the native :class:`zart._zart.DurableScheduler`."""

from __future__ import annotations

from typing import Any, overload

from zart._zart import (
    ClientTrx,
    DurableTask,
    ExecutionRecord,
    PgBackend,
    ScheduleResult,
)
from zart._zart import (
    DurableScheduler as _NativeDurableScheduler,
)
from zart.decorators import DurationLike, parse_duration
from zart.serde import from_json_value, to_json_value


def _resolve_task_name(task: str | DurableTask) -> str:
    if isinstance(task, DurableTask):
        return task.name
    if isinstance(task, str):
        return task
    raise TypeError(f"Expected str or DurableTask, got {type(task).__name__}")


def _resolve_timeout_and_poll(
    timeout: DurationLike | None,
    timeout_secs: DurationLike | None,
    poll_interval: DurationLike | None,
    poll_interval_secs: DurationLike | None,
) -> tuple[float, float | None]:
    if timeout is not None and timeout_secs is not None:
        raise TypeError("Specify either 'timeout' or 'timeout_secs', not both")
    effective_timeout: DurationLike = (
        timeout_secs if timeout_secs is not None else (30.0 if timeout is None else timeout)
    )
    parsed_timeout = parse_duration(effective_timeout)
    assert parsed_timeout is not None

    if poll_interval is not None and poll_interval_secs is not None:
        raise TypeError("Specify either 'poll_interval' or 'poll_interval_secs', not both")
    effective_poll = poll_interval_secs if poll_interval_secs is not None else poll_interval
    parsed_poll = parse_duration(effective_poll)
    return parsed_timeout, parsed_poll


class DurableScheduler:
    """Ergonomic control-plane client for starting, querying, and awaiting durable executions.

    Wraps :class:`zart._zart.DurableScheduler` with:
    - ``str | DurableTask`` task arguments on ``start``, ``start_in_tx``, and ``start_and_wait``
    - Automatic JSON normalization via :func:`zart.serde.to_json_value` for payloads and events
    - :data:`zart.DurationLike` support (e.g. ``"30s"``, ``timedelta``, ``float``) for timeouts
      and poll intervals, while preserving ``timeout_secs`` / ``poll_interval_secs`` aliases
    - Typed result reconstruction via ``target_type`` on ``wait_completion`` and ``start_and_wait``
    """

    def __init__(self, backend: PgBackend) -> None:
        """Create a :class:`DurableScheduler` backed by ``backend``."""
        self._native = _NativeDurableScheduler(backend)

    @property
    def native(self) -> _NativeDurableScheduler:
        """Return the underlying native :class:`zart._zart.DurableScheduler`."""
        return self._native

    async def start(
        self,
        execution_id: str,
        task: str | DurableTask,
        payload: Any = None,
    ) -> ScheduleResult:
        """Start a new durable execution.

        Args:
            execution_id: Unique identifier for this execution.
            task: Registered task name or :class:`DurableTask` instance.
            payload: Input payload; automatically normalized via :func:`to_json_value`.
        """
        task_name = _resolve_task_name(task)
        normalized = to_json_value(payload)
        return await self._native.start(execution_id, task_name, normalized)

    async def start_in_tx(
        self,
        tx: ClientTrx,
        execution_id: str,
        task: str | DurableTask,
        payload: Any = None,
    ) -> ScheduleResult:
        """Start a new durable execution inside a caller-owned :class:`ClientTrx` transaction.

        Args:
            tx: Active caller-managed PostgreSQL transaction.
            execution_id: Unique identifier for this execution.
            task: Registered task name or :class:`DurableTask` instance.
            payload: Input payload; automatically normalized via :func:`to_json_value`.
        """
        task_name = _resolve_task_name(task)
        normalized = to_json_value(payload)
        return await self._native.start_in_tx(tx, execution_id, task_name, normalized)

    async def status(self, execution_id: str) -> ExecutionRecord:
        """Fetch the current status and metadata of a durable execution."""
        return await self._native.status(execution_id)

    async def wait(
        self,
        execution_id: str,
        timeout: DurationLike | None = None,
        poll_interval: DurationLike | None = None,
        *,
        timeout_secs: DurationLike | None = None,
        poll_interval_secs: DurationLike | None = None,
    ) -> ExecutionRecord:
        """Wait until ``execution_id`` reaches a terminal state and return its record.

        Args:
            execution_id: Execution identifier to await.
            timeout: Maximum wait duration (default ``30.0`` seconds).
            poll_interval: Optional polling interval as a :data:`DurationLike`.
            timeout_secs: Backward-compatible alias for ``timeout``.
            poll_interval_secs: Backward-compatible alias for ``poll_interval``.
        """
        t_secs, p_secs = _resolve_timeout_and_poll(
            timeout, timeout_secs, poll_interval, poll_interval_secs
        )
        return await self._native.wait(execution_id, t_secs, p_secs)

    @overload
    async def wait_completion[T](
        self,
        execution_id: str,
        timeout: DurationLike | None = None,
        poll_interval: DurationLike | None = None,
        target_type: type[T] = ...,
        *,
        timeout_secs: DurationLike | None = None,
        poll_interval_secs: DurationLike | None = None,
    ) -> T: ...

    @overload
    async def wait_completion(
        self,
        execution_id: str,
        timeout: DurationLike | None = None,
        poll_interval: DurationLike | None = None,
        target_type: Any = None,
        *,
        timeout_secs: DurationLike | None = None,
        poll_interval_secs: DurationLike | None = None,
    ) -> Any: ...

    async def wait_completion[T](
        self,
        execution_id: str,
        timeout: DurationLike | None = None,
        poll_interval: DurationLike | None = None,
        target_type: type[T] | Any = None,
        *,
        timeout_secs: DurationLike | None = None,
        poll_interval_secs: DurationLike | None = None,
    ) -> T | Any:
        """Wait until ``execution_id`` completes and return its result.

        Args:
            execution_id: Execution identifier to await.
            timeout: Maximum wait duration (default ``30.0`` seconds).
            poll_interval: Optional polling interval as a :data:`DurationLike`.
            target_type: Optional target type for reconstructing the JSON result via
                :func:`from_json_value` (e.g. a ``dataclass``, ``BaseModel``, ``UUID``).
            timeout_secs: Backward-compatible alias for ``timeout``.
            poll_interval_secs: Backward-compatible alias for ``poll_interval``.
        """
        t_secs, p_secs = _resolve_timeout_and_poll(
            timeout, timeout_secs, poll_interval, poll_interval_secs
        )
        raw = await self._native.wait_completion(execution_id, t_secs, p_secs)
        return from_json_value(raw, target_type)

    @overload
    async def start_and_wait[T](
        self,
        execution_id: str,
        task: str | DurableTask,
        payload: Any = None,
        timeout: DurationLike | None = None,
        poll_interval: DurationLike | None = None,
        target_type: type[T] = ...,
        *,
        timeout_secs: DurationLike | None = None,
        poll_interval_secs: DurationLike | None = None,
    ) -> T: ...

    @overload
    async def start_and_wait(
        self,
        execution_id: str,
        task: str | DurableTask,
        payload: Any = None,
        timeout: DurationLike | None = None,
        poll_interval: DurationLike | None = None,
        target_type: Any = None,
        *,
        timeout_secs: DurationLike | None = None,
        poll_interval_secs: DurationLike | None = None,
    ) -> Any: ...

    async def start_and_wait[T](
        self,
        execution_id: str,
        task: str | DurableTask,
        payload: Any = None,
        timeout: DurationLike | None = None,
        poll_interval: DurationLike | None = None,
        target_type: type[T] | Any = None,
        *,
        timeout_secs: DurationLike | None = None,
        poll_interval_secs: DurationLike | None = None,
    ) -> T | Any:
        """Start a durable execution and wait for its completion result.

        Equivalent to calling :meth:`start` followed by :meth:`wait_completion`.
        """
        await self.start(execution_id, task, payload)
        return await self.wait_completion(
            execution_id,
            timeout=timeout,
            poll_interval=poll_interval,
            target_type=target_type,
            timeout_secs=timeout_secs,
            poll_interval_secs=poll_interval_secs,
        )

    async def cancel(self, execution_id: str) -> bool:
        """Cancel a running or scheduled durable execution."""
        return await self._native.cancel(execution_id)

    async def offer_event(
        self,
        execution_id: str,
        event_name: str,
        payload: Any = None,
    ) -> None:
        """Deliver an external event payload to a waiting durable execution.

        Args:
            execution_id: Target execution identifier.
            event_name: Name of the awaited event.
            payload: Event payload; automatically normalized via :func:`to_json_value`.
        """
        normalized = to_json_value(payload)
        await self._native.offer_event(execution_id, event_name, normalized)

    async def stop_recurring(self, task_id: str | DurableTask) -> None:
        """Stop and remove a recurring durable task schedule.

        Args:
            task_id: Recurring task identifier or :class:`DurableTask` instance.
        """
        resolved_id = _resolve_task_name(task_id)
        await self._native.stop_recurring(resolved_id)
