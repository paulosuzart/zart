"""Pythonic wrapper around the native :class:`zart._zart.DurableScheduler`."""

from __future__ import annotations

from typing import Any, overload

from zart._zart import (
    ClientTrx,
    DurableTask,
    ExecutionDetail,
    ExecutionRecord,
    ExecutionRunRecord,
    ExecutionStats,
    ListExecutionsParams,
    PauseRule,
    PauseRuleFilter,
    PauseScope,
    PgBackend,
    RerunResult,
    RerunSpec,
    ResumeResult,
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

    async def list_executions(
        self,
        params: ListExecutionsParams | None = None,
        **kwargs: Any,
    ) -> list[ExecutionRecord]:
        """List durable execution records with optional filters and pagination.

        Args:
            params: Optional :class:`ListExecutionsParams` filter/pagination object.
            **kwargs: Filter/pagination keyword arguments forwarded to
                :class:`ListExecutionsParams` (``status``, ``task_name``, ``from_``,
                ``to``, ``search``, ``sort_by``, ``sort_order``, ``limit``, ``offset``).
                Cannot be combined with ``params``.
        """
        if params is not None and kwargs:
            raise TypeError("Cannot specify both 'params' and keyword filter arguments")
        if params is None and kwargs:
            if "from" in kwargs:
                if "from_" in kwargs:
                    raise TypeError("Specify either 'from_' or 'from', not both")
                kwargs["from_"] = kwargs.pop("from")
            params = ListExecutionsParams(**kwargs)
        return await self._native.list_executions(params)

    async def stats(self) -> ExecutionStats:
        """Return aggregate execution counts grouped by status."""
        return await self._native.stats()

    async def get_current_run_id(self, execution_id: str) -> str | None:
        """Return the current ``run_id`` for a durable execution, or ``None`` if never started."""
        return await self._native.get_current_run_id(execution_id)

    async def list_runs(self, execution_id: str) -> list[ExecutionRunRecord]:
        """List all runs for a durable execution."""
        return await self._native.list_runs(execution_id)

    async def execution_detail(
        self,
        execution_id: str,
        run_id: str | None = None,
    ) -> ExecutionDetail:
        """Return full execution detail: the record, all runs, and steps with attempts.

        Args:
            execution_id: Execution identifier to inspect.
            run_id: Optional run identifier; defaults to the current (latest) run.
        """
        return await self._native.execution_detail(execution_id, run_id)

    async def retry_step(
        self,
        run_id: str,
        step_name: str,
        triggered_by: str | None = None,
    ) -> str:
        """Retry a single dead step within ``run_id`` and return the new task ID."""
        return await self._native.retry_step(run_id, step_name, triggered_by)

    async def retry_step_current_run(
        self,
        execution_id: str,
        step_name: str,
        triggered_by: str | None = None,
    ) -> str:
        """Retry a single dead step within the current run of ``execution_id``."""
        return await self._native.retry_step_current_run(execution_id, step_name, triggered_by)

    async def restart(
        self,
        execution_id: str,
        new_payload: Any = None,
        triggered_by: str | None = None,
    ) -> str:
        """Restart an entire execution from scratch and return the new ``run_id``.

        Args:
            execution_id: Stable execution identifier.
            new_payload: Optional replacement payload (normalized via :func:`to_json_value`).
                When ``None``, the existing payload is preserved.
            triggered_by: Optional operator identifier for audit logging.
        """
        normalized = to_json_value(new_payload) if new_payload is not None else None
        return await self._native.restart(execution_id, normalized, triggered_by)

    async def rerun_steps(
        self,
        execution_id: str,
        spec: RerunSpec | None = None,
        **kwargs: Any,
    ) -> RerunResult:
        """Selectively rerun a subset of steps while preserving others.

        Args:
            execution_id: Stable execution identifier.
            spec: Optional :class:`RerunSpec` specification.
            **kwargs: Ergonomic keyword arguments forwarded to :class:`RerunSpec`
                (``force_rerun``, ``preserve``, ``triggered_by``). Cannot be combined with ``spec``.
        """
        if spec is not None and kwargs:
            raise TypeError("Cannot specify both 'spec' and keyword arguments")
        if spec is None and kwargs:
            spec = RerunSpec(**kwargs)
        return await self._native.rerun_steps(execution_id, spec)

    async def pause(self, scope: PauseScope) -> PauseRule:
        """Create a pause rule for the given :class:`PauseScope`."""
        return await self._native.pause(scope)

    async def resume(
        self,
        scope: PauseScope,
        deleted_by: str | None = None,
    ) -> ResumeResult:
        """Resume execution by soft-deleting pause rules matching ``scope``."""
        return await self._native.resume(scope, deleted_by)

    async def resume_rule_by_id(
        self,
        rule_id: str,
        deleted_by: str | None = None,
    ) -> ResumeResult:
        """Resume by soft-deleting a specific pause rule by ``rule_id``."""
        return await self._native.resume_rule_by_id(rule_id, deleted_by)

    async def list_pause_rules(
        self,
        filter: PauseRuleFilter | None = None,
    ) -> list[PauseRule]:
        """List pause rules matching the optional :class:`PauseRuleFilter`."""
        return await self._native.list_pause_rules(filter)
